#pragma once

#include <Storages/NATS/INATSProducer.h>

namespace DB
{

class NATSCoreProducer : public INATSProducer
{
public:
    NATSCoreProducer(NATSConnectionPtr connection_, const String & subject_, std::atomic<bool> & shutdown_called_, LoggerPtr log_);

private:
    natsStatus publishMessage(const String & message) override;
};

}
