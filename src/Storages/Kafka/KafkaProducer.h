#pragma once

#include <Columns/IColumn_fwd.h>
#include <Storages/IMessageProducer.h>
#include <cppkafka/cppkafka.h>

#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric KafkaProducers;
}

namespace DB
{
class Block;
using ProducerPtr = std::shared_ptr<cppkafka::Producer>;

class KafkaProducer : public IMessageProducer
{
public:
    KafkaProducer(
        ProducerPtr producer_,
        const std::string & topic_,
        std::chrono::milliseconds poll_timeout,
        std::atomic<bool> & shutdown_called_,
        const Block & header);

    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override;

    void start(const ContextPtr &) override {}
    void finish() override;
    void cancel() noexcept override;

private:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaProducers};

    ProducerPtr producer;
    const std::string topic;
    const std::chrono::milliseconds timeout;

    std::atomic<bool> & shutdown_called;

    std::optional<size_t> key_column_index;
    std::optional<size_t> timestamp_column_index;
};

}
