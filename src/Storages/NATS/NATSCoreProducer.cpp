#include <Storages/NATS/NATSCoreProducer.h>

namespace DB
{

INATSProducer::PublishStatus NATSCoreProducer::publishMessage(const String & message, String & error)
{
    /// The server would close the connection for a message over its limit.
    if (message.size() > getConnection().getMaxPayload())
    {
        error = fmt::format("Maximum Payload Exceeded: the message has {} bytes, the server accepts at most {}",
            message.size(), getConnection().getMaxPayload());
        return PublishStatus::Fail;
    }

    getConnection().publish(getSubject(), "", message);
    return PublishStatus::Ok;
}

}
