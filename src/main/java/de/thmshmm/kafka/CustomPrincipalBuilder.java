package de.thmshmm.kafka;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import javax.security.auth.x500.X500Principal;
import java.security.Principal;
import java.util.Map;

/**
 * Created by Thomas Hamm on 21.07.17.
 */
public class CustomPrincipalBuilder implements PrincipalBuilder {

    public void configure(Map<String, ?> map) { }

    public Principal buildPrincipal(TransportLayer transportLayer, Authenticator authenticator) throws KafkaException {
        try {
            if ((transportLayer.peerPrincipal() instanceof X500Principal) && !transportLayer.peerPrincipal().getName().equals("ANONYMOUS")) {
                String[] split = null;
                CustomPrincipal newPrincipal = null;

                try {
                    split = transportLayer.peerPrincipal().getName().split(",");

                    newPrincipal = new CustomPrincipal("User", split[0].split("=")[1]);
                } catch (Exception e) {
                    throw new KafkaException("failed to build principal");
                }

                return newPrincipal;
            } else {
                return transportLayer.peerPrincipal();
            }
        } catch (Exception e) {
            throw new KafkaException("Failed to build principal due to: ", e);
        }
    }

    public void close() throws KafkaException { }
}
