#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <Storages/Pulsar/Client/Configuration.h>
#include <Storages/Pulsar/Client/Message.h>
#include <Storages/Pulsar/Client/Result.h>
#include <Common/Logger.h>

namespace DB::Pulsar
{

class ConsumerImpl;
class ConnectionRegistry;

/// A handle to a shared subscription over one or more topics. Default-constructed empty and filled by
/// `Client::subscribe`. Copies share the same subscription.
class Consumer
{
public:
    /// One topic of the subscription, already resolved to its owning broker by lookup.
    struct TopicBroker
    {
        std::string topic;
        std::string host;
        uint16_t port;
    };

    Consumer();
    ~Consumer();
    Consumer(const Consumer &);
    Consumer & operator=(const Consumer &);

    /// Opens a connection and subscribes for every resolved topic. On failure returns an empty handle,
    /// sets `result`, and leaves nothing subscribed (any partial connections are closed).
    static Consumer open(
        const std::vector<TopicBroker> & topics,
        const std::string & subscription,
        const ClientConfiguration & client_config,
        const ConsumerConfiguration & consumer_config,
        ConnectionRegistry & registry,
        const LoggerPtr & log,
        Result & result);

    /// Receives up to `batch_receive_policy.max_messages`, waiting at most its timeout. An empty result
    /// with `ResultOk` means the subscription was idle. Returns an error only on a terminal condition
    /// (the connection dropped or the broker closed the consumer), after which the handle is unusable.
    Result batchReceive(Messages & messages);

    /// Acknowledges the given messages. For messages inside a batch the broker is told which indices of
    /// the entry are still outstanding, so the entry is released only once all of them are acknowledged.
    Result acknowledge(const MessageIdList & message_ids);

    /// Requests immediate redelivery of one message (used to roll back an aborted read).
    Result negativeAcknowledge(const MessageId & message_id);

    /// Closes the subscription's connections. The broker redelivers any unacknowledged messages to the
    /// other consumers of the shared subscription.
    Result close();

private:
    explicit Consumer(std::shared_ptr<ConsumerImpl> impl_);
    std::shared_ptr<ConsumerImpl> impl;
};

}
