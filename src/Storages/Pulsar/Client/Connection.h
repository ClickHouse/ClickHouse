#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include <Storages/Pulsar/Client/Configuration.h>
#include <Storages/Pulsar/Client/Result.h>
#include <Common/Logger.h>

#include "PulsarApi.pb.h"

namespace DB::Pulsar
{

/// A single blocking TCP connection to one Pulsar broker, speaking the binary protocol.
///
/// The design is deliberately synchronous: there is no event loop and no background thread. Each
/// producer and each consumer owns one `Connection` and drives it from the thread that calls the
/// client. `close` shuts the socket down from any thread, which unblocks a read in progress, so a
/// consumer blocked in `receive` can be interrupted by the engine's shutdown.
///
/// A connection is used for exactly one role (a lookup helper, one producer, or one consumer), so the
/// request/response exchanges never interleave with each other.
class Connection
{
public:
    /// Opens the connection and performs the CONNECT/CONNECTED handshake. On failure returns nullptr
    /// and sets `result`. `config.connection_validator`, if set, is consulted for `host:port` first.
    static std::unique_ptr<Connection>
    open(const std::string & host, uint16_t port, const ClientConfiguration & config, const LoggerPtr & log, Result & result);

    ~Connection();

    Connection(const Connection &) = delete;
    Connection & operator=(const Connection &) = delete;

    /// The parsed command plus, for a MESSAGE frame, the metadata and payload byte regions.
    struct Incoming
    {
        pulsar::proto::BaseCommand command;
        bool is_message = false;
        std::string metadata;
        std::string payload;
    };

    /// Receives the next frame. PING frames are answered with PONG transparently and the read
    /// continues, so the caller only sees real commands. Returns `ResultTimeout` if nothing arrived
    /// within `timeout_ms`, `ResultAlreadyClosed` if the connection was closed.
    Result receive(Incoming & incoming, int timeout_ms);

    /// Sends a command and waits for the response carrying the same `request_id`.
    Result request(const pulsar::proto::BaseCommand & command, uint64_t request_id, pulsar::proto::BaseCommand & response);

    /// Sends a command without expecting a response (FLOW, ACK, REDELIVER).
    Result send(const pulsar::proto::BaseCommand & command);

    /// Sends a SEND command followed by the message metadata and payload, protected by a CRC32C.
    Result sendMessage(const pulsar::proto::BaseCommand & command, const std::string & metadata, const std::string & payload);

    void close();
    bool isClosed() const { return closed.load(std::memory_order_acquire); }

    uint64_t newRequestId() { return next_request_id.fetch_add(1, std::memory_order_relaxed); }
    int32_t getMaxMessageSize() const { return max_message_size; }

private:
    Connection(int fd_, const ClientConfiguration & config_, LoggerPtr log_);

    /// Writes all `size` bytes or fails.
    Result writeAll(const char * data, size_t size);
    /// Reads exactly `size` bytes into `buffer`, waiting until `deadline` (a steady-clock millisecond
    /// count). Returns `ResultTimeout` if the first byte does not arrive in time.
    Result readAll(char * buffer, size_t size, int64_t deadline_ms);
    /// Reads one whole frame body (everything after the 4-byte length prefix) into `frame`.
    Result readFrame(std::string & frame, int64_t deadline_ms);

    int fd;
    ClientConfiguration config;
    LoggerPtr log;
    std::atomic<bool> closed{false};
    std::atomic<uint64_t> next_request_id{0};
    int32_t max_message_size = 5 * 1024 * 1024;
};

using ConnectionPtr = std::shared_ptr<Connection>;

}

