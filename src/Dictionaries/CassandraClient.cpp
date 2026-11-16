#include <Dictionaries/CassandraClient.h>

#if USE_CASSANDRA

#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/SocketPeerClosed.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteHelpers.h>
#include <base/unaligned.h>

#include <lz4.h>

#if USE_SNAPPY
#include <snappy.h>
#endif

#if USE_SSL
#include <Poco/Net/SecureStreamSocket.h>
#endif

#include <fmt/format.h>
#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int AUTHENTICATION_FAILED;
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int CASSANDRA_INTERNAL_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SUPPORT_IS_DISABLED;
    extern const int INCORRECT_DATA;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

namespace Cassandra
{

namespace
{

constexpr UInt8 PROTOCOL_VERSION = 0x04;
constexpr UInt8 RESPONSE_DIRECTION_BIT = 0x80;
constexpr size_t FRAME_HEADER_SIZE = 9;

/// The protocol limits the frame length to 256 MB.
constexpr size_t MAX_FRAME_BODY_SIZE = 256 * 1024 * 1024;

/// The nesting of collection, tuple, and UDT types in the column metadata.
constexpr size_t MAX_TYPE_NESTING_DEPTH = 64;

/// The number of columns in a result. A column spec takes only a few bytes in a message,
/// but much more in memory, so the size of the message alone does not bound the allocation.
constexpr size_t MAX_RESULT_COLUMNS = 65536;

namespace Opcode
{
    constexpr UInt8 ERROR = 0x00;
    constexpr UInt8 STARTUP = 0x01;
    constexpr UInt8 READY = 0x02;
    constexpr UInt8 AUTHENTICATE = 0x03;
    constexpr UInt8 QUERY = 0x07;
    constexpr UInt8 RESULT = 0x08;
    constexpr UInt8 AUTH_CHALLENGE = 0x0E;
    constexpr UInt8 AUTH_RESPONSE = 0x0F;
    constexpr UInt8 AUTH_SUCCESS = 0x10;
}

namespace FrameFlag
{
    constexpr UInt8 COMPRESSION = 0x01;
    constexpr UInt8 TRACING = 0x02;
    constexpr UInt8 CUSTOM_PAYLOAD = 0x04;
    constexpr UInt8 WARNING = 0x08;
}

namespace QueryFlag
{
    /// Not `PAGE_SIZE`, which is a macro in musl.
    constexpr UInt8 WITH_PAGE_SIZE = 0x04;
    constexpr UInt8 WITH_PAGING_STATE = 0x08;
}

namespace ResultKind
{
    constexpr Int32 ROWS = 0x0002;
}

namespace RowsFlag
{
    constexpr Int32 GLOBAL_TABLES_SPEC = 0x0001;
    constexpr Int32 HAS_MORE_PAGES = 0x0002;
    constexpr Int32 NO_METADATA = 0x0004;
}

const char * opcodeToString(UInt8 opcode)
{
    switch (opcode)
    {
        case 0x00: return "ERROR";
        case 0x01: return "STARTUP";
        case 0x02: return "READY";
        case 0x03: return "AUTHENTICATE";
        case 0x05: return "OPTIONS";
        case 0x06: return "SUPPORTED";
        case 0x07: return "QUERY";
        case 0x08: return "RESULT";
        case 0x09: return "PREPARE";
        case 0x0A: return "EXECUTE";
        case 0x0B: return "REGISTER";
        case 0x0C: return "EVENT";
        case 0x0D: return "BATCH";
        case 0x0E: return "AUTH_CHALLENGE";
        case 0x0F: return "AUTH_RESPONSE";
        case 0x10: return "AUTH_SUCCESS";
        default: return "unknown";
    }
}

const char * errorCodeToString(Int32 code)
{
    switch (code)
    {
        case 0x0000: return "Server error";
        case 0x000A: return "Protocol error";
        case 0x0100: return "Authentication error";
        case 0x1000: return "Unavailable exception";
        case 0x1001: return "Overloaded";
        case 0x1002: return "Is bootstrapping";
        case 0x1003: return "Truncate error";
        case 0x1100: return "Write timeout";
        case 0x1200: return "Read timeout";
        case 0x1300: return "Read failure";
        case 0x1400: return "Function failure";
        case 0x1500: return "Write failure";
        case 0x2000: return "Syntax error";
        case 0x2100: return "Unauthorized";
        case 0x2200: return "Invalid";
        case 0x2300: return "Config error";
        case 0x2400: return "Already exists";
        case 0x2500: return "Unprepared";
        default: return "Unknown error";
    }
}

constexpr Int32 ERROR_CODE_BAD_CREDENTIALS = 0x0100;


/// Builds the body of a request. All integers are big-endian.
class BodyWriter
{
public:
    void writeByte(UInt8 x) { body.push_back(static_cast<char>(x)); }

    void writeShort(UInt16 x)
    {
        writeByte(static_cast<UInt8>(x >> 8));
        writeByte(static_cast<UInt8>(x));
    }

    void writeInt(Int32 x)
    {
        const auto u = static_cast<UInt32>(x);
        for (int shift = 24; shift >= 0; shift -= 8)
            writeByte(static_cast<UInt8>(u >> shift));
    }

    /// [string]: a [short] length followed by the bytes.
    void writeString(std::string_view s)
    {
        if (s.size() > std::numeric_limits<UInt16>::max())
            throw Exception(ErrorCodes::INCORRECT_DATA, "String of size {} is too long for the Cassandra protocol", s.size());
        writeShort(static_cast<UInt16>(s.size()));
        body.append(s);
    }

    /// [long string] and [bytes]: an [int] length followed by the bytes.
    void writeLongString(std::string_view s)
    {
        if (s.size() > static_cast<size_t>(std::numeric_limits<Int32>::max()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "String of size {} is too long for the Cassandra protocol", s.size());
        writeInt(static_cast<Int32>(s.size()));
        body.append(s);
    }

    void writeBytes(std::string_view s) { writeLongString(s); }

    std::string_view str() const { return body; }

private:
    StringWithMemoryTracking body;
};


/// Reads the body of a response with bounds checks. All integers are big-endian.
class BodyReader
{
public:
    BodyReader(std::string_view body_, size_t offset) : body(body_), pos(offset) {}

    size_t position() const { return pos; }
    size_t remaining() const { return body.size() - pos; }

    UInt16 readShort()
    {
        need(2);
        UInt16 x = unalignedLoadBigEndian<UInt16>(body.data() + pos);
        pos += 2;
        return x;
    }

    Int32 readInt()
    {
        need(4);
        UInt32 x = unalignedLoadBigEndian<UInt32>(body.data() + pos);
        pos += 4;
        return static_cast<Int32>(x);
    }

    std::string_view readRaw(size_t size)
    {
        need(size);
        std::string_view res = body.substr(pos, size);
        pos += size;
        return res;
    }

    /// [string]
    std::string_view readString() { return readRaw(readShort()); }

    /// [bytes]: returns std::nullopt for a negative length (NULL).
    std::optional<std::string_view> readBytes()
    {
        Int32 size = readInt();
        if (size < 0)
            return std::nullopt;
        return readRaw(size);
    }

    /// [string list]
    void skipStringList()
    {
        UInt16 n = readShort();
        for (size_t i = 0; i < n; ++i)
            readString();
    }

    /// [bytes map]
    void skipBytesMap()
    {
        UInt16 n = readShort();
        for (size_t i = 0; i < n; ++i)
        {
            readString();
            readBytes();
        }
    }

    /// [option] describing a column type. Returns the top-level type, skipping the parameters of nested types.
    ColumnType readType(size_t depth = 0)
    {
        if (depth > MAX_TYPE_NESTING_DEPTH)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Too deep nesting of types in the result metadata from Cassandra server");

        auto type = static_cast<ColumnType>(readShort());
        switch (type)
        {
            case ColumnType::Custom:
                readString();
                break;
            case ColumnType::List:
            case ColumnType::Set:
                readType(depth + 1);
                break;
            case ColumnType::Map:
                readType(depth + 1);
                readType(depth + 1);
                break;
            case ColumnType::UDT:
            {
                readString();
                readString();
                UInt16 n = readShort();
                for (size_t i = 0; i < n; ++i)
                {
                    readString();
                    readType(depth + 1);
                }
                break;
            }
            case ColumnType::Tuple:
            {
                UInt16 n = readShort();
                for (size_t i = 0; i < n; ++i)
                    readType(depth + 1);
                break;
            }
            default:
                break;
        }
        return type;
    }

private:
    void need(size_t size) const
    {
        if (size > remaining())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Malformed message from Cassandra server: need {} bytes at offset {}, but the message size is {}",
                size, pos, body.size());
    }

    std::string_view body;
    size_t pos;
};

}


Compression parseCompression(const String & name)
{
    if (name == "none")
        return Compression::None;
    if (name == "lz4")
        return Compression::LZ4;
    if (name == "snappy")
    {
#if USE_SNAPPY
        return Compression::Snappy;
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Snappy compression is disabled, because ClickHouse was built without Snappy support");
#endif
    }
    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unknown compression '{}' for Cassandra, supported: 'none', 'lz4', 'snappy'", name);
}

String columnTypeToString(ColumnType type)
{
    switch (type)
    {
        case ColumnType::Custom: return "custom";
        case ColumnType::Ascii: return "ascii";
        case ColumnType::Bigint: return "bigint";
        case ColumnType::Blob: return "blob";
        case ColumnType::Boolean: return "boolean";
        case ColumnType::Counter: return "counter";
        case ColumnType::Decimal: return "decimal";
        case ColumnType::Double: return "double";
        case ColumnType::Float: return "float";
        case ColumnType::Int: return "int";
        case ColumnType::Timestamp: return "timestamp";
        case ColumnType::Uuid: return "uuid";
        case ColumnType::Varchar: return "text";
        case ColumnType::Varint: return "varint";
        case ColumnType::Timeuuid: return "timeuuid";
        case ColumnType::Inet: return "inet";
        case ColumnType::Date: return "date";
        case ColumnType::Time: return "time";
        case ColumnType::Smallint: return "smallint";
        case ColumnType::Tinyint: return "tinyint";
        case ColumnType::Duration: return "duration";
        case ColumnType::List: return "list";
        case ColumnType::Map: return "map";
        case ColumnType::Set: return "set";
        case ColumnType::UDT: return "user-defined type";
        case ColumnType::Tuple: return "tuple";
    }
    return fmt::format("unknown type 0x{:04x}", static_cast<UInt16>(type));
}


Connection::Connection(const String & host, const Poco::Net::SocketAddress & address_, const ConnectionSettings & settings)
    : address(address_)
    , compression(settings.compression)
    , log(getLogger("CassandraConnection"))
{
    if (settings.secure)
    {
#if USE_SSL
        /// The default client context, configured in the `openSSL.client` section.
        auto secure_socket = std::make_unique<Poco::Net::SecureStreamSocket>();
        secure_socket->setPeerHostName(host);
        /// The handshake is postponed until the first read or write, which are bounded by the request timeout.
        secure_socket->setLazyHandshake(true);
        socket = std::move(secure_socket);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "TLS connections to Cassandra are disabled, because ClickHouse was built without SSL support");
#endif
    }
    else
    {
        socket = std::make_unique<Poco::Net::StreamSocket>();
    }

    socket->connect(address, settings.connect_timeout);
    socket->setReceiveTimeout(settings.request_timeout);
    socket->setSendTimeout(settings.request_timeout);
    socket->setNoDelay(true);

    in = std::make_unique<ReadBufferFromPocoSocket>(*socket);
    out = std::make_unique<WriteBufferFromPocoSocket>(*socket);

    handshake(settings);
    LOG_TRACE(log, "Connected to {}", address.toString());
}

Connection::~Connection()
{
    /// Nothing is buffered between requests, so there is nothing to flush.
    out->cancel();
}

void Connection::sendFrame(UInt8 opcode, std::string_view body)
{
    if (body.size() > MAX_FRAME_BODY_SIZE)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Request of size {} is too large for the Cassandra protocol", body.size());

    const StringWithMemoryTracking compressed_body = compression_started ? compress(body) : StringWithMemoryTracking{};
    const std::string_view payload = compression_started ? std::string_view(compressed_body) : body;

    char header[FRAME_HEADER_SIZE];
    header[0] = static_cast<char>(PROTOCOL_VERSION);
    header[1] = static_cast<char>(compression_started ? FrameFlag::COMPRESSION : 0);
    header[2] = 0;  /// stream id, two bytes: every request uses stream 0, because there is one request at a time.
    header[3] = 0;
    header[4] = static_cast<char>(opcode);
    unalignedStoreBigEndian<UInt32>(header + 5, static_cast<UInt32>(payload.size()));

    out->write(header, FRAME_HEADER_SIZE);
    out->write(payload.data(), payload.size());
    out->next();
}

Connection::Frame Connection::receiveFrame()
{
    char header[FRAME_HEADER_SIZE];
    in->readStrict(header, FRAME_HEADER_SIZE);

    const auto version = static_cast<UInt8>(header[0]);
    const auto flags = static_cast<UInt8>(header[1]);
    const auto stream = unalignedLoadBigEndian<UInt16>(header + 2);
    const auto opcode = static_cast<UInt8>(header[4]);
    const auto length = unalignedLoadBigEndian<UInt32>(header + 5);

    /// A server that does not support the protocol version replies with an error in the version it supports,
    /// but the layout of the header is the same, so the error message can still be read.
    const bool version_mismatch = version != (PROTOCOL_VERSION | RESPONSE_DIRECTION_BIT);
    if (version_mismatch && (opcode != Opcode::ERROR || !(version & RESPONSE_DIRECTION_BIT)))
        throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER,
            "Unexpected protocol version byte 0x{:02x} in a frame from Cassandra server {}, expected 0x{:02x}",
            static_cast<UInt32>(version), address.toString(), static_cast<UInt32>(PROTOCOL_VERSION | RESPONSE_DIRECTION_BIT));

    if (stream != 0)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
            "Unexpected stream id {} in a frame from Cassandra server {}", stream, address.toString());

    if (length > MAX_FRAME_BODY_SIZE)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Too large frame of size {} from Cassandra server {}, the maximum is {}", length, address.toString(), MAX_FRAME_BODY_SIZE);

    Frame frame;
    frame.opcode = opcode;
    frame.body.resize(length);
    in->readStrict(frame.body.data(), length);

    /// The server may send uncompressed frames even if compression is used, for example, small ones.
    if (flags & FrameFlag::COMPRESSION)
    {
        if (!compression_started)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                "Cassandra server {} sent a compressed frame, but compression was not negotiated", address.toString());
        decompress(frame.body);
    }

    /// The optional parts preceding the message, in the order defined by the protocol.
    BodyReader reader(frame.body, 0);
    if (flags & FrameFlag::TRACING)
        reader.readRaw(16);
    if (flags & FrameFlag::WARNING)
        reader.skipStringList();
    if (flags & FrameFlag::CUSTOM_PAYLOAD)
        reader.skipBytesMap();
    frame.message_offset = reader.position();

    return frame;
}

StringWithMemoryTracking Connection::compress(std::string_view body) const
{
    StringWithMemoryTracking res;
    switch (compression)
    {
        case Compression::None:
            return StringWithMemoryTracking(body);
        case Compression::LZ4:
        {
            /// The uncompressed size, then an LZ4 block.
            const int bound = LZ4_compressBound(static_cast<int>(body.size()));
            res.resize(4 + bound);
            unalignedStoreBigEndian<UInt32>(res.data(), static_cast<UInt32>(body.size()));
            const int compressed_size = LZ4_compress_default(body.data(), res.data() + 4, static_cast<int>(body.size()), bound);
            if (compressed_size <= 0)
                throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress a request to Cassandra with LZ4");
            res.resize(4 + compressed_size);
            return res;
        }
        case Compression::Snappy:
#if USE_SNAPPY
        {
            res.resize(snappy::MaxCompressedLength(body.size()));
            size_t compressed_size = 0;
            snappy::RawCompress(body.data(), body.size(), res.data(), &compressed_size);
            res.resize(compressed_size);
            return res;
        }
#else
            UNREACHABLE();
#endif
    }
    UNREACHABLE();
}

void Connection::decompress(StringWithMemoryTracking & body) const
{
    StringWithMemoryTracking res;
    switch (compression)
    {
        case Compression::None:
            UNREACHABLE();
        case Compression::LZ4:
        {
            BodyReader reader(body, 0);
            const auto size = static_cast<UInt32>(reader.readInt());
            if (size > MAX_FRAME_BODY_SIZE)
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                    "Too large uncompressed size {} of a frame from Cassandra server {}", size, address.toString());
            res.resize(size);
            if (size != 0)
            {
                const int decompressed_size = LZ4_decompress_safe(
                    body.data() + 4, res.data(), static_cast<int>(body.size() - 4), static_cast<int>(size));
                if (decompressed_size != static_cast<int>(size))
                    throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress a frame from Cassandra server {} with LZ4", address.toString());
            }
            break;
        }
        case Compression::Snappy:
        {
#if USE_SNAPPY
            size_t size = 0;
            if (!snappy::GetUncompressedLength(body.data(), body.size(), &size))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress a frame from Cassandra server {} with Snappy", address.toString());
            if (size > MAX_FRAME_BODY_SIZE)
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                    "Too large uncompressed size {} of a frame from Cassandra server {}", size, address.toString());
            res.resize(size);
            if (!snappy::RawUncompress(body.data(), body.size(), res.data()))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress a frame from Cassandra server {} with Snappy", address.toString());
            break;
#else
            UNREACHABLE();
#endif
        }
    }
    body = std::move(res);
}

void Connection::throwServerError(const Frame & frame) const
{
    if (frame.opcode != Opcode::ERROR)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
            "Unexpected message {} (opcode 0x{:02x}) from Cassandra server {}",
            opcodeToString(frame.opcode), static_cast<UInt32>(frame.opcode), address.toString());

    BodyReader reader(frame.body, frame.message_offset);
    Int32 code = reader.readInt();
    std::string_view message = reader.readString();

    throw Exception(code == ERROR_CODE_BAD_CREDENTIALS ? ErrorCodes::AUTHENTICATION_FAILED : ErrorCodes::CASSANDRA_INTERNAL_ERROR,
        "Cassandra server {} returned error 0x{:04x} ({}): {}",
        address.toString(), static_cast<UInt32>(code), errorCodeToString(code), message);
}

void Connection::handshake(const ConnectionSettings & settings)
{
    BodyWriter startup;
    startup.writeShort(compression == Compression::None ? 1 : 2);  /// The number of entries in the [string map].
    startup.writeString("CQL_VERSION");
    startup.writeString("3.0.0");
    if (compression != Compression::None)
    {
        startup.writeString("COMPRESSION");
        startup.writeString(compression == Compression::LZ4 ? "lz4" : "snappy");
    }
    sendFrame(Opcode::STARTUP, startup.str());
    /// Even the response to `STARTUP` may be compressed.
    compression_started = compression != Compression::None;

    Frame frame = receiveFrame();
    if (frame.opcode == Opcode::READY)
        return;

    if (frame.opcode != Opcode::AUTHENTICATE)
        throwServerError(frame);

    BodyReader authenticate_reader(frame.body, frame.message_offset);
    /// Copy the name: `frame` is reused for the next response, and the name is still needed for the error message.
    String authenticator(authenticate_reader.readString());

    if (settings.user.empty())
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "Cassandra server {} requires authentication with {}, but the user is not specified",
            address.toString(), authenticator);

    /// SASL `PLAIN`: an empty authorization id, the user, and the password, separated by zero bytes.
    String token;
    token.push_back('\0');
    token += settings.user;
    token.push_back('\0');
    token += settings.password;

    BodyWriter auth_response;
    auth_response.writeBytes(token);
    sendFrame(Opcode::AUTH_RESPONSE, auth_response.str());

    frame = receiveFrame();
    if (frame.opcode == Opcode::AUTH_SUCCESS)
        return;

    if (frame.opcode == Opcode::AUTH_CHALLENGE)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "Cassandra server {} sent an authentication challenge for {}, but only plain password authentication is supported",
            address.toString(), authenticator);

    throwServerError(frame);
}

ResultPage Connection::query(const String & query_text, Consistency consistency, Int32 page_size, const String & paging_state)
{
    BodyWriter request;
    request.writeLongString(query_text);
    request.writeShort(static_cast<UInt16>(consistency));
    request.writeByte(QueryFlag::WITH_PAGE_SIZE | (paging_state.empty() ? 0 : QueryFlag::WITH_PAGING_STATE));
    request.writeInt(page_size);
    if (!paging_state.empty())
        request.writeBytes(paging_state);
    sendFrame(Opcode::QUERY, request.str());

    Frame frame = receiveFrame();
    if (frame.opcode != Opcode::RESULT)
        throwServerError(frame);

    ResultPage page;
    page.body = std::move(frame.body);
    BodyReader reader(page.body, frame.message_offset);

    Int32 kind = reader.readInt();
    if (kind != ResultKind::ROWS)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
            "Cassandra server {} returned a result of kind {} instead of rows. Only SELECT queries are supported", address.toString(), kind);

    Int32 flags = reader.readInt();
    Int32 columns_count = reader.readInt();
    if (columns_count < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Negative number of columns {} in a result from Cassandra server {}", columns_count, address.toString());

    if (flags & RowsFlag::HAS_MORE_PAGES)
    {
        page.has_more_pages = true;
        auto state = reader.readBytes();
        if (!state || state->empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cassandra server {} indicated more pages, but did not send the paging state", address.toString());
        page.paging_state = *state;
    }

    if (flags & RowsFlag::NO_METADATA)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "Cassandra server {} returned a result without metadata", address.toString());

    if (flags & RowsFlag::GLOBAL_TABLES_SPEC)
    {
        reader.readString();
        reader.readString();
    }

    if (static_cast<size_t>(columns_count) > MAX_RESULT_COLUMNS)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Too many columns {} in a result from Cassandra server {}, the maximum is {}",
            columns_count, address.toString(), MAX_RESULT_COLUMNS);

    page.column_specs.reserve(columns_count);
    for (Int32 i = 0; i < columns_count; ++i)
    {
        if (!(flags & RowsFlag::GLOBAL_TABLES_SPEC))
        {
            reader.readString();
            reader.readString();
        }
        ColumnSpec & spec = page.column_specs.emplace_back();
        spec.name = reader.readString();
        spec.type = reader.readType();
    }

    Int32 rows_count = reader.readInt();
    if (rows_count < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Negative number of rows {} in a result from Cassandra server {}", rows_count, address.toString());

    /// Every value takes at least four bytes (its length) in the message and eight bytes in memory,
    /// so the allocation is at most twice the size of the message.
    static_assert(sizeof(ResultPage::Value) == 8);
    const size_t values_count = static_cast<size_t>(rows_count) * static_cast<size_t>(columns_count);
    if (values_count > reader.remaining() / 4)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Too many values ({} rows, {} columns) for a result of size {} from Cassandra server {}",
            rows_count, columns_count, page.body.size(), address.toString());

    page.rows_count = rows_count;
    page.values.reserve(values_count);
    for (size_t i = 0; i < values_count; ++i)
    {
        Int32 size = reader.readInt();
        size_t offset = reader.position();
        if (size >= 0)
            reader.readRaw(size);
        /// The body is not larger than `MAX_FRAME_BODY_SIZE`, so the offset fits.
        page.values.push_back({static_cast<UInt32>(offset), size});
    }

    if (reader.remaining() != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Unexpected {} bytes at the end of a result from Cassandra server {}", reader.remaining(), address.toString());

    return page;
}

bool Connection::isReusable() const
{
    return getSocketState(*socket) == SocketState::Idle;
}


ConnectionPool::ConnectionPool(
    VectorWithMemoryTracking<String> hosts_, UInt16 port_, ConnectionSettings settings_, Consistency consistency_, size_t max_idle_connections_)
    : hosts(std::move(hosts_))
    , port(port_)
    , settings(std::move(settings_))
    , consistency(consistency_)
    , max_idle_connections(max_idle_connections_)
    , log(getLogger("CassandraConnectionPool"))
{
    if (hosts.empty())
        throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "No hosts are specified for Cassandra");
}

ResultPage ConnectionPool::query(const String & query_text, Int32 page_size, const String & paging_state)
{
    /// If the request fails, the connection is destroyed instead of being returned to the pool,
    /// because its state is unknown.
    ConnectionPtr connection = acquire();
    ResultPage page = connection->query(query_text, consistency, page_size, paging_state);
    release(std::move(connection));
    return page;
}

ConnectionPtr ConnectionPool::acquire()
{
    {
        std::lock_guard lock(mutex);
        while (!idle_connections.empty())
        {
            ConnectionPtr connection = std::move(idle_connections.back());
            idle_connections.pop_back();

            /// The server may have closed an idle connection, for example, on restart.
            if (connection->isReusable())
                return connection;

            LOG_TRACE(log, "Dropping a stale idle connection to {}", connection->getAddress().toString());
        }
    }

    return connect();
}

void ConnectionPool::release(ConnectionPtr connection)
{
    std::lock_guard lock(mutex);
    if (idle_connections.size() < max_idle_connections)
        idle_connections.push_back(std::move(connection));
}

ConnectionPtr ConnectionPool::connect()
{
    /// The configured hosts are equivalent contact points: try them all, starting from a different one each time.
    VectorWithMemoryTracking<String> errors;
    const size_t start = next_host.fetch_add(1, std::memory_order_relaxed);
    for (size_t i = 0; i < hosts.size(); ++i)
    {
        const String & host = hosts[(start + i) % hosts.size()];

        std::vector<Poco::Net::SocketAddress> addresses; /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type returned by `DNSResolver`
        try
        {
            addresses = DNSResolver::instance().resolveAddressList(host, port);
        }
        catch (...)
        {
            errors.push_back(fmt::format("{}: {}", host, getCurrentExceptionMessage(false)));
            LOG_DEBUG(log, "Cannot resolve Cassandra host {}: {}", host, errors.back());
            continue;
        }

        for (const auto & address : addresses)
        {
            try
            {
                return std::make_unique<Connection>(host, address, settings);
            }
            catch (const Exception & e)
            {
                /// The server answered, and another one would answer the same, for example, to wrong credentials.
                if (e.code() == ErrorCodes::AUTHENTICATION_FAILED)
                    throw;
                errors.push_back(fmt::format("{} ({}): {}", host, address.toString(), e.message()));
            }
            catch (const Poco::Exception & e)
            {
                errors.push_back(fmt::format("{} ({}): {}", host, address.toString(), e.displayText()));
            }
            LOG_DEBUG(log, "Cannot connect to Cassandra host {}: {}", host, errors.back());
        }
    }

    throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
        "Cannot connect to any of the Cassandra hosts on port {}: {}", port, fmt::join(errors, "; "));
}

}

}

#endif
