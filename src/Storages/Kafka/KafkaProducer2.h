#pragma once

#include <Columns/IColumn_fwd.h>
#include <Storages/IMessageProducer.h>
#include <Storages/Kafka/Clients/Client.h>
#include <Storages/Kafka/Clients/TopicProducer.h>

#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric KafkaProducers;
}

namespace DB
{
class Block;

/// Producer of the Kafka2 engine, built on the in-tree Kafka client: one
/// client + one pipelined TopicProducer per INSERT. Keyed messages land on the
/// same partitions as with the Java client (murmur2), unkeyed messages stick
/// to one partition per batch. Delivery is confirmed by `finish` draining all
/// outstanding acks.
class KafkaProducer2 : public IMessageProducer
{
public:
    KafkaProducer2(
        Kafka::ClientConfig client_config,
        Kafka::TopicProducer::Options producer_options,
        const std::string & topic_,
        std::chrono::milliseconds send_timeout_,
        std::atomic<bool> & shutdown_called_,
        const Block & header,
        bool map_virtual_columns_on_write = false);

    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override;

    void start(const ContextPtr &) override {}
    void finish() override;
    void cancel() noexcept override;

private:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaProducers};

    /// Order matters: the producer references the client.
    Kafka::Client client;
    Kafka::TopicProducer producer;
    const std::chrono::milliseconds send_timeout;

    std::atomic<bool> & shutdown_called;

    std::optional<size_t> key_column_index;
    std::optional<size_t> timestamp_column_index;
    std::optional<size_t> headers_name_column_index;
    std::optional<size_t> headers_value_column_index;
};

}
