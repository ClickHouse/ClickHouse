#pragma once

#include <Storages/NATS/INATSProducer.h>

namespace DB
{

class NATSCoreProducer : public INATSProducer
{
public:
    using INATSProducer::INATSProducer;

private:
    PublishStatus publishMessage(const String & message, String & error) override;
};

}
