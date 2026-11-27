#pragma once

#include <atomic>
#include <optional>
#include <Core/Names.h>
#include <Storages/NATS/NATSConnection.h>
#include <Storages/IMessageProducer.h>
#include <Common/ConcurrentBoundedQueue.h>

namespace DB
{

/// Publishes the messages of an INSERT from its producing task, which owns the connection.
class INATSProducer : public AsynchronousMessageProducer
{
public:
    INATSProducer(NATSConnectionPtr connection_, String subject_, std::atomic<bool> & shutdown_called_, LoggerPtr log_);

    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override;
    void cancel() noexcept override;

protected:
    enum class PublishStatus : uint8_t
    {
        Ok,
        /// A transient failure, e.g. a timeout during a leader change of the stream.
        Retry,
        /// A retry cannot succeed, e.g. the stream rejects the message.
        Fail,
    };

    NATSClient::Connection & getConnection() { return connection->get(); }
    const String & getSubject() const { return subject; }

    /// Throws if the connection fails: the message is published again after connecting again.
    virtual PublishStatus publishMessage(const String & message, String & error) = 0;

private:
    String getProducingTaskName() const override { return "NatsProducingTask"; }

    void stopProducingTask() override;
    void finishImpl() override;

    void startProducingTaskLoop() override;

    void publish();

    NATSConnectionPtr connection;
    const String subject;

    std::atomic<bool> & shutdown_called;

    /* payloads.queue:
     *      - payloads are pushed to queue in countRow and popped by another thread in writingFunc, each payload gets into queue only once
     */
    ConcurrentBoundedQueue<String> payloads;

    /// A message whose publish failed with a retryable status: it is published again before the next one is popped.
    std::optional<String> pending_payload;
    size_t pending_payload_retries = 0;

    /// Set by `cancel`: the INSERT has already failed, so a failed publish is not retried.
    std::atomic<bool> cancelled = false;

    /// Written by the producing task, read in `finishImpl` after `deactivate` waited for the task.
    String publish_error;
};

}
