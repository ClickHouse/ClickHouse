#include <Storages/NATS/NATSCoreConsumer.h>

#include <Common/logger_useful.h>

#include <algorithm>

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
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (true)
    {
        const auto wait = std::chrono::duration_cast<std::chrono::microseconds>(
            std::max(deadline - std::chrono::steady_clock::now(), std::chrono::steady_clock::duration::zero()));
        auto message = getConnection().receive(Poco::Timespan(wait.count()));
        if (!message)
            return {};
        if (auto accepted = acceptMessage(std::move(*message)))
            return accepted;
    }
}

std::optional<NATSClient::Message> NATSCoreConsumer::acceptMessage(NATSClient::Message message)
{
    if (std::find(sids.begin(), sids.end(), message.sid) == sids.end())
        return {};
    return message;
}

}
