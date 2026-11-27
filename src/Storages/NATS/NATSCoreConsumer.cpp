#include <Storages/NATS/NATSCoreConsumer.h>

#include <Common/logger_useful.h>

namespace DB
{

void NATSCoreConsumer::subscribeImpl()
{
    sids.clear();
    for (const auto & subject : getSubjects())
    {
        sids.push_back(getConnection().subscribe(subject, getQueueName()));
        LOG_DEBUG(getLogger(), "Subscribed to subject {}", subject);
    }

    /// Prevent early message loss by waiting until the server has processed the subscriptions.
    getConnection().flush();

    LOG_DEBUG(getLogger(), "Consumer subscribed to {} subjects", sids.size());
}

void NATSCoreConsumer::unsubscribeImpl()
{
    for (const auto sid : sids)
        getConnection().unsubscribe(sid);
    sids.clear();
    getConnection().flush();
}

std::optional<NATSClient::Message> NATSCoreConsumer::receiveImpl(UInt64 timeout_ms)
{
    return getConnection().receive(Poco::Timespan(static_cast<Poco::Timespan::TimeDiff>(timeout_ms) * 1000));
}

}
