#pragma once

#include <Storages/NATS/INATSProducer.h>

namespace DB
{

class NATSCoreProducer : public INATSProducer
{
public:
    using INATSProducer::INATSProducer;

private:
    natsStatus publishMessage(const String & message) override;
};

}
