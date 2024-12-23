#include "MySQLBinlog.h"
#include <Core/MySQL/Authentication.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsProtocolText.h>
#include <Databases/MySQL/tryParseTableIDFromDDL.h>
#include <Databases/MySQL/tryQuoteUnrecognizedTokens.h>
#include <Databases/MySQL/tryConvertStringLiterals.h>
#include <Common/DNSResolver.h>
#include <Common/randomNumber.h>
#include <Poco/String.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/LimitReadBuffer.h>
#include <IO/MySQLBinlogEventReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/Operators.h>

#include <random>

namespace DB
{
using namespace Replication;
using namespace Authentication;
using namespace ConnectionPhase;

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
}

namespace MySQLReplication
{

class WriteCommand : public IMySQLWritePacket
{
public:
    const char command;
    const String query;

    WriteCommand(char command_, const String & query_) : command(command_), query(query_) { }

    size_t getPayloadSize() const override { return 1 + query.size(); }

    void writePayloadImpl(WriteBuffer & buffer) const override
    {
        buffer.write(command);
        if (!query.empty())
            buffer.write(query.data(), query.size());
    }
};

IBinlog::Checksum IBinlog::checksumFromString(const String & checksum)
{
    auto str = Poco::toUpper(checksum);
    if (str == "CRC32")
        return IBinlog::CRC32;
    if (str != "NONE")
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown checksum: {}", checksum);
    return IBinlog::NONE;
}

void BinlogParser::setChecksum(Checksum checksum)
{
    switch (checksum)
    {
        case Checksum::CRC32:
            checksum_signature_length = 4;
            break;
        case Checksum::NONE:
            checksum_signature_length = 0;
            break;
    }
}

void BinlogParser::parseEvent(EventHeader & event_header, ReadBuffer & event_payload)
{
    switch (event_header.type)
    {
        case FORMAT_DESCRIPTION_EVENT:
        {
            event = std::make_shared<FormatDescriptionEvent>(EventHeader(event_header));
            event->parseEvent(event_payload);
            break;
        }
        case ROTATE_EVENT:
        {
            event = std::make_shared<RotateEvent>(EventHeader(event_header));
            event->parseEvent(event_payload);
            break;
        }
        case QUERY_EVENT:
        {
            event = std::make_shared<QueryEvent>(EventHeader(event_header));
            event->parseEvent(event_payload);

            auto query = std::static_pointer_cast<QueryEvent>(event);
            switch (query->typ)
            {
                case QUERY_EVENT_MULTI_TXN_FLAG:
                case QUERY_EVENT_XA:
                case QUERY_SAVEPOINT:
                {
                    event = std::make_shared<DryRunEvent>(EventHeader(query->header));
                    break;
                }
                default:
                {
                    String quoted_query = query->query;
                    tryQuoteUnrecognizedTokens(quoted_query);
                    tryConvertStringLiterals(quoted_query);
                    auto table_id = tryParseTableIDFromDDL(query->query, query->schema);
                    query->query_database_name = table_id.database_name;
                    query->query_table_name = table_id.table_name;
                    break;
                }
            }
            break;
        }
        case XID_EVENT:
        {
            event = std::make_shared<XIDEvent>(EventHeader(event_header));
            event->parseEvent(event_payload);
            break;
        }
        case TABLE_MAP_EVENT:
        {
            TableMapEventHeader map_event_header;
            map_event_header.parse(event_payload);
            event = std::make_shared<TableMapEvent>(EventHeader(event_header), map_event_header, flavor_charset);
            try
            {
                event->parseEvent(event_payload);
                auto table_map = std::static_pointer_cast<TableMapEvent>(event);
                table_maps[table_map->table_id] = table_map;
            }
            catch (const Poco::Exception & exc)
            {
                /// Ignore parsing issues
                if (exc.code() != ErrorCodes::UNKNOWN_EXCEPTION)
                    throw;
                event = std::make_shared<DryRunEvent>(std::move(event_header));
                event->parseEvent(event_payload);
            }
            break;
        }
        case WRITE_ROWS_EVENT_V1:
        case WRITE_ROWS_EVENT_V2:
        case DELETE_ROWS_EVENT_V1:
        case DELETE_ROWS_EVENT_V2:
        case UPDATE_ROWS_EVENT_V1:
        case UPDATE_ROWS_EVENT_V2:
        {
            RowsEventHeader rows_header(event_header.type);
            rows_header.parse(event_payload);
            if (table_maps.contains(rows_header.table_id))
                event = std::make_shared<UnparsedRowsEvent>(table_maps.at(rows_header.table_id), EventHeader(event_header), rows_header);
            else
                event = std::make_shared<DryRunEvent>(std::move(event_header));
            event->parseEvent(event_payload);
            if (rows_header.flags & ROWS_END_OF_STATEMENT)
                table_maps.clear();
            break;
        }
        case GTID_EVENT:
        {
            event = std::make_shared<GTIDEvent>(EventHeader(event_header));
            event->parseEvent(event_payload);
            break;
        }
        default:
        {
            event = std::make_shared<DryRunEvent>(EventHeader(event_header));
            event->parseEvent(event_payload);
            break;
        }
    }
    updatePosition(event, position);
}

void BinlogParser::updatePosition(const BinlogEventPtr & event, Position & position)
{
    const UInt64 binlog_pos_prev = position.binlog_pos;
    position.binlog_pos = event->header.log_pos;
    if (event->header.timestamp > 0)
        position.timestamp = event->header.timestamp;

    switch (event->header.type)
    {
        case QUERY_EVENT:
            if (event->type() == MYSQL_UNHANDLED_EVENT)
                break;
            [[fallthrough]];
        case GTID_EVENT:
        case XID_EVENT:
        case ROTATE_EVENT:
            position.update(event);
            break;
        default:
            break;
    }

    if (event->header.type != ROTATE_EVENT)
    {
        /// UInt32 overflow when Pos > End_log_pos
        /// https://dev.mysql.com/doc/refman/8.0/en/show-binlog-events.html
        ///     binlog_pos - The position at which the next event begins, which is equal to Pos plus the size of the event
        const UInt64 binlog_pos_correct = binlog_pos_prev + event->header.event_size;
        if (position.binlog_pos < binlog_pos_prev && binlog_pos_correct > std::numeric_limits<UInt32>::max())
            position.binlog_pos = binlog_pos_correct;
    }
}

bool BinlogParser::isNew(const Position & older, const Position & newer)
{
    if (older.gtid_sets.contains(newer.gtid_sets))
        return false;
    /// Check if all sets in newer position have the same UUID from older sets
    std::set<UUID> older_set;
    for (const auto & set : older.gtid_sets.sets)
        older_set.insert(set.uuid);
    for (const auto & set : newer.gtid_sets.sets)
    {
        if (!older_set.contains(set.uuid))
            return false;
    }
    return true;
}

void BinlogFromSocket::connect(const String & host, UInt16 port, const String & user, const String & password)
{
    if (connected)
        disconnect();

    const Poco::Timespan connection_timeout(10'000'000'000);
    const Poco::Timespan receive_timeout(5'000'000'000);
    const Poco::Timespan send_timeout(5'000'000'000);

    socket = std::make_unique<Poco::Net::StreamSocket>();
    address = DNSResolver::instance().resolveAddress(host, port);
    socket->connect(*address, connection_timeout);
    socket->setReceiveTimeout(receive_timeout);
    socket->setSendTimeout(send_timeout);
    socket->setNoDelay(true);
    connected = true;

    in = std::make_unique<ReadBufferFromPocoSocket>(*socket);
    out = std::make_unique<WriteBufferFromPocoSocket>(*socket);
    packet_endpoint = std::make_shared<MySQLProtocol::PacketEndpoint>(*in, *out, sequence_id);

    handshake(user, password);
}

void BinlogFromSocket::disconnect()
{
    in = nullptr;
    out = nullptr;
    if (socket)
        socket->close();
    socket = nullptr;
    connected = false;
    sequence_id = 0;

    GTIDSets sets;
    position.gtid_sets = sets;
    position.resetPendingGTID();
}

/// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
void BinlogFromSocket::handshake(const String & user, const String & password)
{
    const String mysql_native_password = "mysql_native_password";
    Handshake handshake;
    packet_endpoint->receivePacket(handshake);
    if (handshake.auth_plugin_name != mysql_native_password)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_PACKET_FROM_SERVER,
            "Only support {} auth plugin name, but got {}",
            mysql_native_password,
            handshake.auth_plugin_name);
    }

    Native41 native41(password, handshake.auth_plugin_data);
    String auth_plugin_data = native41.getAuthPluginData();

    const UInt8 charset_utf8 = 33;
    HandshakeResponse handshake_response(
        client_capabilities, MAX_PACKET_LENGTH, charset_utf8, user, "", auth_plugin_data, mysql_native_password);
    packet_endpoint->sendPacket<HandshakeResponse>(handshake_response, true);

    ResponsePacket packet_response(client_capabilities, true);
    packet_endpoint->receivePacket(packet_response);
    packet_endpoint->resetSequenceId();

    if (packet_response.getType() == PACKET_ERR)
        throw Exception::createDeprecated(packet_response.err.error_message, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    if (packet_response.getType() == PACKET_AUTH_SWITCH)
        throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Access denied for user {}", user);
}

void BinlogFromSocket::writeCommand(char command, const String & query)
{
    WriteCommand write_command(command, query);
    packet_endpoint->sendPacket<WriteCommand>(write_command, true);

    ResponsePacket packet_response(client_capabilities);
    packet_endpoint->receivePacket(packet_response);
    switch (packet_response.getType())
    {
        case PACKET_ERR:
            throw Exception::createDeprecated(packet_response.err.error_message, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
        case PACKET_OK:
            break;
        default:
            break;
    }
    packet_endpoint->resetSequenceId();
}

void BinlogFromSocket::registerSlaveOnMaster(UInt32 slave_id)
{
    RegisterSlave register_slave(slave_id);
    packet_endpoint->sendPacket<RegisterSlave>(register_slave, true);

    ResponsePacket packet_response(client_capabilities);
    packet_endpoint->receivePacket(packet_response);
    packet_endpoint->resetSequenceId();
    if (packet_response.getType() == PACKET_ERR)
        throw Exception::createDeprecated(packet_response.err.error_message, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
}

void BinlogFromSocket::start(UInt32 slave_id, const String & executed_gtid_set)
{
    if (!connected)
        return;

    /// Maybe CRC32 or NONE. mysqlbinlog.cc use NONE, see its below comments:
    /// Make a notice to the server that this client is checksum-aware.
    /// It does not need the first fake Rotate necessary checksummed.
    writeCommand(Command::COM_QUERY, "SET @master_binlog_checksum = 'CRC32'");

    /// Set heartbeat 1s
    const UInt64 period_ns = 1'000'000'000;
    writeCommand(Command::COM_QUERY, "SET @master_heartbeat_period = " + std::to_string(period_ns));

    /// Register slave.
    registerSlaveOnMaster(slave_id);

    position.gtid_sets = {};
    position.gtid_sets.parse(executed_gtid_set);

    BinlogDumpGTID binlog_dump(slave_id, position.gtid_sets.toPayload());
    packet_endpoint->sendPacket<BinlogDumpGTID>(binlog_dump, true);
}

class ReadPacketFromSocket : public IMySQLReadPacket
{
public:
    using ReadPayloadFunc = std::function<void(ReadBuffer & payload)>;
    explicit ReadPacketFromSocket(ReadPayloadFunc fn) : read_payload_func(std::move(fn)) { }
    void readPayloadImpl(ReadBuffer & payload) override;
    ReadPayloadFunc read_payload_func;
};

void ReadPacketFromSocket::readPayloadImpl(ReadBuffer & payload)
{
    if (payload.eof())
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Attempt to read after EOF.");

    UInt8 header = static_cast<unsigned char>(*payload.position());
    switch (header) // NOLINT(bugprone-switch-missing-default-case)
    {
        case PACKET_EOF:
            throw ReplicationError(ErrorCodes::CANNOT_READ_ALL_DATA, "Master maybe lost");
        case PACKET_ERR:
        {
            ERRPacket err;
            err.readPayloadWithUnpacked(payload);
            throw ReplicationError::createDeprecated(err.error_message, ErrorCodes::UNKNOWN_EXCEPTION);
        }
        default:
            break;
    }
    /// Skip the generic response packets header flag
    payload.ignore(1);
    read_payload_func(payload);
}

bool BinlogFromSocket::tryReadEvent(BinlogEventPtr & to, UInt64 ms)
{
    ReadPacketFromSocket packet([this](ReadBuffer & payload)
    {
        MySQLBinlogEventReadBuffer event_payload(payload, checksum_signature_length);

        EventHeader event_header;
        event_header.parse(event_payload);

        parseEvent(event_header, event_payload);
    });

    if (packet_endpoint && packet_endpoint->tryReceivePacket(packet, ms))
    {
        to = event;
        return static_cast<bool>(to);
    }

    return false;
}

void BinlogFromFile::open(const String & filename)
{
    in = std::make_unique<ReadBufferFromFile>(filename);
    assertString("\xfe\x62\x69\x6e", *in); /// magic number
}

bool BinlogFromFile::tryReadEvent(BinlogEventPtr & to, UInt64 /*ms*/)
{
    if (in && !in->eof())
    {
        EventHeader event_header;
        event_header.parse(*in);

        LimitReadBuffer limit_read_buffer(*in, event_header.event_size - EVENT_HEADER_LENGTH, /* throw_exception */ false, /* exact_limit */ {});
        MySQLBinlogEventReadBuffer event_payload(limit_read_buffer, checksum_signature_length);
        parseEvent(event_header, event_payload);
        to = event;
        return static_cast<bool>(to);
    }

    return false;
}

BinlogFromFileFactory::BinlogFromFileFactory(const String & filename_)
    : filename(filename_)
{
}

BinlogPtr BinlogFromFileFactory::createBinlog(const String & executed_gtid_set)
{
    auto ret = std::make_shared<BinlogFromFile>();
    ret->open(filename);
    if (!executed_gtid_set.empty())
    {
        /// NOTE: Used for testing only!
        GTIDSets sets;
        sets.parse(executed_gtid_set);
        if (sets.sets.size() != 1 || sets.sets[0].intervals.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many intervals: {}", executed_gtid_set);
        BinlogEventPtr event;
        while (ret->tryReadEvent(event, 0))
        {
            const auto & s = ret->getPosition().gtid_sets.sets;
            if (!s.empty() && !s[0].intervals.empty() && s[0].intervals[0].end == sets.sets[0].intervals[0].end)
                break;
        }

        auto pos = ret->getPosition();
        pos.gtid_sets.sets.front().intervals.front().start = sets.sets.front().intervals.front().start;
        ret->setPosition(pos);
    }
    return ret;
}

BinlogFromSocketFactory::BinlogFromSocketFactory(const String & host_, UInt16 port_, const String & user_, const String & password_)
    : host(host_)
    , port(port_)
    , user(user_)
    , password(password_)
{
}

BinlogPtr BinlogFromSocketFactory::createBinlog(const String & executed_gtid_set)
{
    auto ret = std::make_shared<BinlogFromSocket>();
    ret->connect(host, port, user, password);
    ret->start(randomNumber(), executed_gtid_set);
    auto pos = ret->getPosition();
    if (pos.gtid_sets.sets.empty() || pos.gtid_sets.sets.front().intervals.front().start != 1)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Could not create: Wrong executed_gtid_set: {} -> {}", executed_gtid_set, pos.gtid_sets.toString());
    return ret;
}

/// Should be in MySQLReplication namespace
bool operator==(const Position & left, const Position & right)
{
    return left.binlog_name == right.binlog_name &&
           left.binlog_pos == right.binlog_pos &&
           left.gtid_sets == right.gtid_sets;
}

}
}
