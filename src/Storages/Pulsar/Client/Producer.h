#pragma once

#include "config.h"

#if USE_PULSAR

#include <memory>
#include <optional>
#include <string_view>


namespace DB::Pulsar
{

class ProducerImpl;

/// Created by `Client::createProducer`.
class Producer
{
public:
    explicit Producer(std::unique_ptr<ProducerImpl> impl_);
    ~Producer();

    /// Sends one message and waits for its receipt from the broker, within the send timeout,
    /// reconnecting and sending again if the connection is lost.
    void send(std::string_view payload, std::optional<std::string_view> ordering_key = {});

    /// Thread-safe. The `send` in progress and all later ones throw `ABORTED`.
    void interrupt() noexcept;

    void close() noexcept;

private:
    std::unique_ptr<ProducerImpl> impl;
};

}

#endif
