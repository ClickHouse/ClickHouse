#pragma once

#include <Storages/NATS/INATSProducer.h>

namespace DB
{

/// Publishes to a JetStream stream and waits for the acknowledgement of the stream for every message.
class NATSJetStreamProducer : public INATSProducer
{
public:
    using INATSProducer::INATSProducer;

private:
    PublishStatus publishMessage(const String & message, String & error) override;
};

}
