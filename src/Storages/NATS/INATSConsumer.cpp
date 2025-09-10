#include <atomic>
#include <chrono>
#include <memory>
#include <utility>
#include <Storages/NATS/INATSConsumer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Poco/Timer.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_STATE;
}

INATSConsumer::INATSConsumer(
    NATSConnectionPtr connection_,
    const std::vector<String> & subjects_,
    const String & subscribe_queue_name,
    LoggerPtr log_,
    uint32_t queue_size_,
    const std::atomic<bool> & stopped_)
    : connection(std::move(connection_))
    , subjects(subjects_)
    , log(log_)
    , stopped(stopped_)
    , queue_name(subscribe_queue_name)
    , received(queue_size_)
{
}

bool INATSConsumer::isSubscribed() const
{
    return !subscriptions.empty();
}
void INATSConsumer::unsubscribe()
{
    subscriptions.clear();

    LOG_DEBUG(log, "Consumer {} unsubscribed", static_cast<void*>(this));
}

ReadBufferPtr INATSConsumer::consume()
{
    if (stopped || !received.tryPop(current))
        return nullptr;

    return std::make_shared<ReadBufferFromMemory>(current.message.data(), current.message.size());
}

void INATSConsumer::onMsg(natsConnection *, natsSubscription *, natsMsg * msg, void * consumer)
{
    auto * nats_consumer = static_cast<INATSConsumer *>(consumer);
    const int msg_length = natsMsg_GetDataLength(msg);

    if (msg_length)
    {
        String message_received = std::string(natsMsg_GetData(msg), msg_length);
        String subject = natsMsg_GetSubject(msg);

        MessageData data = {
            .message = message_received,
            .subject = subject,
        };
        if (!nats_consumer->received.push(std::move(data)))
            throw Exception(ErrorCodes::INVALID_STATE, "Could not push to received queue");
    }

    natsMsg_Destroy(msg);
}

}
