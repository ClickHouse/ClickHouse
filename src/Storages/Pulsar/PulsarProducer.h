#pragma once

#include <atomic>
#include <chrono>
#include <string>
#include <Storages/IMessageProducer.h>
#include <Storages/Pulsar/Client/Producer.h>

namespace DB
{

using ProducerPtr = std::unique_ptr<Pulsar::Producer>;

class PulsarProducer : public IMessageProducer
{
public:
    PulsarProducer(ProducerPtr producer_, const std::string & topic_, std::atomic<bool> & shutdown_called_, const Block & header);
    ~PulsarProducer() override;

    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override;

    void start(const ContextPtr &) override { }
    void finish() override;
    void cancel() noexcept override;

private:
    ProducerPtr producer;
    const std::string topic;

    std::atomic<bool> & shutdown_called;

    std::optional<size_t> key_column_index;
};
}
