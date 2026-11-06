#pragma once

/// Transport abstraction: blocking-with-deadline byte I/O. The TCP
/// implementation lives in TCPTransport.{h,cpp}; tests use an in-memory
/// scripted implementation. TLS later = another implementation, additive.

#include <Storages/Kafka/Clients/Error.h>

#include <base/types.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string_view>

namespace Kafka::Net
{

/// Absolute point in time an operation must finish by (steady clock).
class Deadline
{
public:
    static Deadline after(std::chrono::milliseconds timeout) { return Deadline{std::chrono::steady_clock::now() + timeout}; }
    static Deadline never() { return Deadline{}; }

    bool expired() const { return at && std::chrono::steady_clock::now() >= *at; }

    /// Remaining time clamped to [0, cap]; `cap` bounds each poll slice so
    /// wakeups are observed promptly even if the eventfd race is lost.
    std::chrono::milliseconds remainingClamped(std::chrono::milliseconds cap) const
    {
        if (!at)
            return cap;
        const auto now = std::chrono::steady_clock::now();
        if (now >= *at)
            return std::chrono::milliseconds::zero();
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(*at - now);
        return left < cap ? left : cap;
    }

private:
    Deadline() = default;
    explicit Deadline(std::chrono::steady_clock::time_point at_) : at(at_) { }
    std::optional<std::chrono::steady_clock::time_point> at;
};

/// Cross-thread interruption for blocked calls (the analogue of
/// RabbitMQConsumer::wakeUp in ClickHouse). wakeup() bumps a generation
/// counter and pokes a self-pipe; every blocking loop polls the pipe and
/// re-checks the generation each slice.
class WakeupSource
{
public:
    WakeupSource();
    ~WakeupSource();
    WakeupSource(const WakeupSource &) = delete;
    WakeupSource & operator=(const WakeupSource &) = delete;

    /// Interrupt every call currently blocked on this source. One-shot:
    /// calls started after wakeup() proceed normally.
    void wakeup();

    UInt64 getGeneration() const { return generation.load(); }
    /// Read end of the self-pipe, for poll().
    int getFd() const { return read_fd; }
    /// Non-blocking drain of the pipe.
    void drain() const;

private:
    std::atomic<UInt64> generation{0};
    int read_fd = -1;
    int write_fd = -1;
};

/// Byte transport with deadlines. Implementations throw Exception with kind
/// Connection (socket failure / peer closed), Timeout (deadline reached) or
/// Wakeup (interrupted via WakeupSource).
class Transport
{
public:
    virtual ~Transport() = default;

    virtual void connect(const String & host, UInt16 port, const Deadline & deadline) = 0;
    /// Read 1..size bytes (blocks until data, deadline or wakeup).
    virtual size_t readSome(char * data, size_t size, const Deadline & deadline) = 0;
    /// Write all bytes.
    virtual void writeAll(std::string_view data, const Deadline & deadline) = 0;
    /// True when readSome would not block (data buffered, EOF or error
    /// pending). Zero-timeout probe; never throws.
    virtual bool readable() = 0;
    virtual void close() = 0;
    virtual bool isOpen() const = 0;
};

}
