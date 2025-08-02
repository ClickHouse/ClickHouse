#include <Storages/NATS/NATSCoreProducer.h>

namespace DB
{

NATSCoreProducer::NATSCoreProducer(NATSConnectionPtr connection_, String subject_, std::atomic<bool> & shutdown_called_, LoggerPtr log_)
    : INATSProducer(std::move(connection_), std::move(subject_), shutdown_called_, log_)
{
}

natsStatus NATSCoreProducer::publishMessage(const String & message)
{
    return natsConnection_Publish(getNativeConnection(), getSubject().c_str(), message.c_str(), static_cast<int>(message.size()));
}

}
