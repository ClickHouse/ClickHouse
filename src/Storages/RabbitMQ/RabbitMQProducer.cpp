#include <Storages/RabbitMQ/RabbitMQProducer.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

static const auto FINISH_PRODUCER_NUM_TRIES = 50;

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
}

RabbitMQProducer::RabbitMQProducer(
    RabbitMQPublisherPoolPtr publisher_pool_,
    std::atomic<bool> & shutdown_called_,
    LoggerPtr log_)
    : AsynchronousMessageProducer(log_)
    , publisher_pool(std::move(publisher_pool_))
    , shutdown_called(shutdown_called_)
    , payloads(RabbitMQPublisher::BATCH)
{
}

void RabbitMQProducer::initialize()
{
    LOG_TRACE(log, "Initializing producer");
    publisher = publisher_pool->acquire();
}

void RabbitMQProducer::stopProducingTask()
{
    payloads.finish();
}

void RabbitMQProducer::finishImpl()
{
    if (can_reuse_publisher)
        publisher_pool->release(std::move(publisher));
    else
        publisher->disconnect();
}

void RabbitMQProducer::produce(const String & message, size_t, const Columns &, size_t)
{
    Payload payload;
    payload.message = message;
    payload.id = ++payload_counter;
    LOG_TEST(log, "Pushing message with id {}", payload.id);
    if (!payloads.push(std::move(payload)))
    {
        /// The producing task has failed and closed the queue: rethrows its exception.
        finish();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
    }
}

void RabbitMQProducer::cancel() noexcept
{
    try
    {
      finish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void RabbitMQProducer::startProducingTaskLoop()
{
    LOG_TRACE(log, "Starting producer loop");

    while (!payloads.isFinishedAndEmpty() || publisher->hasPendingDeliveries())
    {
        if (shutdown_called.load())
        {
            /// Nothing pops `payloads` anymore, so `produce` must not wait for free space in it.
            /// `can_reuse_publisher` stays false, so `finishImpl` disconnects the publisher instead of returning
            /// a channel with unconfirmed messages to the pool.
            payloads.finish();
            throw Exception(ErrorCodes::ABORTED, "Table is detached");
        }

        publisher->publishStep(payloads);
    }

    size_t try_num = 0;
    while (++try_num <= FINISH_PRODUCER_NUM_TRIES)
    {
        int res = publisher->iterateEventLoop();
        if (!res)
            break;
        LOG_TEST(log, "Waiting for pending callbacks to finish (count: {}, try: {})", res, try_num);
    }

    /// All messages are confirmed, so the next insert can continue on the same channel.
    if (!shutdown_called.load() && publisher->isReusable())
        can_reuse_publisher = true;
    else
        publisher->closeChannel();
}

}
