#include <Storages/NATS/NATSCoreProducer.h>

namespace DB
{

natsStatus NATSCoreProducer::publishMessage(const String & message)
{
    return natsConnection_Publish(getNativeConnection(), getSubject().c_str(), message.c_str(), static_cast<int>(message.size()));
}

}
