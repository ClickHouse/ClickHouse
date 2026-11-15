#pragma once

#include "config.h"

#if USE_CASSANDRA

#include <base/types.h>
#include <Common/Logger.h>
#include <Common/StringWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// A minimal client of the Cassandra native protocol, version 4:
/// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
///
/// It implements only what the `cassandra` dictionary source needs: connecting, optionally with TLS,
/// optional password authentication (`PasswordAuthenticator`, SASL `PLAIN`), compression of frames,
/// and running a query with paging.
/// There are no prepared statements or cluster topology discovery: every request
/// goes to one of the configured hosts, which acts as the coordinator.
/// The connection is synchronous: one request at a time.
namespace Cassandra
{

/// The values are from the protocol specification.
enum class Consistency : UInt16
{
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Serial = 0x0008,
    LocalSerial = 0x0009,
    LocalOne = 0x000A,
};

enum class Compression : uint8_t
{
    None,
    LZ4,
    Snappy,
};

/// Parses `none`, `lz4`, or `snappy`.
Compression parseCompression(const String & name);

/// Column types (the `[option]` ids in the result metadata).
enum class ColumnType : UInt16
{
    Custom = 0x0000,
    Ascii = 0x0001,
    Bigint = 0x0002,
    Blob = 0x0003,
    Boolean = 0x0004,
    Counter = 0x0005,
    Decimal = 0x0006,
    Double = 0x0007,
    Float = 0x0008,
    Int = 0x0009,
    Timestamp = 0x000B,
    Uuid = 0x000C,
    Varchar = 0x000D,
    Varint = 0x000E,
    Timeuuid = 0x000F,
    Inet = 0x0010,
    Date = 0x0011,
    Time = 0x0012,
    Smallint = 0x0013,
    Tinyint = 0x0014,
    Duration = 0x0015,
    List = 0x0020,
    Map = 0x0021,
    Set = 0x0022,
    UDT = 0x0030,
    Tuple = 0x0031,
};

/// The name of the type in CQL, for error messages.
String columnTypeToString(ColumnType type);

struct ColumnSpec
{
    String name;
    ColumnType type = ColumnType::Custom;
};

/// One page of rows returned by a query.
class ResultPage
{
public:
    size_t columns() const { return column_specs.size(); }
    size_t rows() const { return rows_count; }
    const VectorWithMemoryTracking<ColumnSpec> & columnSpecs() const { return column_specs; }

    /// The serialized value of a column in a row, or `std::nullopt` for NULL.
    std::optional<std::string_view> value(size_t row, size_t column) const
    {
        const Value & v = values[row * column_specs.size() + column];
        if (v.size < 0)
            return std::nullopt;
        return std::string_view(body.data() + v.offset, v.size);
    }

    /// Opaque state to request the next page. Empty if this is the last page.
    const String & pagingState() const { return paging_state; }
    bool hasMorePages() const { return has_more_pages; }

private:
    friend class Connection;

    struct Value
    {
        UInt32 offset;
        /// Negative for NULL.
        Int32 size;
    };

    /// The body of the response frame. The values reference it by offsets.
    StringWithMemoryTracking body;
    VectorWithMemoryTracking<ColumnSpec> column_specs;
    size_t rows_count = 0;
    VectorWithMemoryTracking<Value> values;
    bool has_more_pages = false;
    String paging_state;
};

struct ConnectionSettings
{
    String user;
    String password;
    /// Use TLS with the client settings from the `openSSL.client` section of the server configuration.
    bool secure = false;
    Compression compression = Compression::None;
    Poco::Timespan connect_timeout{5, 0};
    /// The timeout for a single read or write on the socket.
    Poco::Timespan request_timeout{12, 0};
};

/// One connection to a Cassandra node. Not thread-safe.
class Connection
{
public:
    /// Connects and performs the handshake (`STARTUP` and authentication).
    /// `host` is the name the address was resolved from, used for TLS (SNI and certificate verification).
    Connection(const String & host, const Poco::Net::SocketAddress & address, const ConnectionSettings & settings);
    ~Connection();

    /// Runs a query and returns one page of its result.
    /// `paging_state` is empty for the first page, otherwise it is `ResultPage::pagingState` of the previous page.
    ResultPage query(const String & query_text, Consistency consistency, Int32 page_size, const String & paging_state);

    /// Whether the connection is alive and has no unread data, so it can be reused for another request.
    bool isReusable() const;

    const Poco::Net::SocketAddress & getAddress() const { return address; }

private:
    struct Frame
    {
        UInt8 opcode = 0;
        StringWithMemoryTracking body;
        /// The offset in the body where the message starts, after the tracing id, warnings, and custom payload.
        size_t message_offset = 0;
    };

    void handshake(const ConnectionSettings & settings);
    void sendFrame(UInt8 opcode, std::string_view body);
    StringWithMemoryTracking compress(std::string_view body) const;
    void decompress(StringWithMemoryTracking & body) const;
    Frame receiveFrame();
    [[noreturn]] void throwServerError(const Frame & frame) const;

    Poco::Net::SocketAddress address;
    std::unique_ptr<Poco::Net::StreamSocket> socket;
    const Compression compression;
    /// Compression is used after the `STARTUP` message, which negotiates it.
    bool compression_started = false;
    std::unique_ptr<ReadBuffer> in;
    std::unique_ptr<WriteBuffer> out;
    LoggerPtr log;
};

using ConnectionPtr = std::unique_ptr<Connection>;

/// Keeps idle connections to a set of hosts. Thread-safe.
/// A connection is taken from the pool for the duration of a single request and is returned
/// to it only after the request completed successfully.
class ConnectionPool
{
public:
    ConnectionPool(VectorWithMemoryTracking<String> hosts_, UInt16 port_, ConnectionSettings settings_, Consistency consistency_, size_t max_idle_connections_);

    ResultPage query(const String & query_text, Int32 page_size, const String & paging_state);

private:
    ConnectionPtr acquire();
    ConnectionPtr connect();
    void release(ConnectionPtr connection);

    const VectorWithMemoryTracking<String> hosts;
    const UInt16 port;
    const ConnectionSettings settings;
    const Consistency consistency;
    const size_t max_idle_connections;

    /// The host to try first for the next connection, to distribute connections across the hosts.
    std::atomic<size_t> next_host = 0;

    std::mutex mutex;
    VectorWithMemoryTracking<ConnectionPtr> idle_connections;

    LoggerPtr log;
};

using ConnectionPoolPtr = std::shared_ptr<ConnectionPool>;

}

}

#endif
