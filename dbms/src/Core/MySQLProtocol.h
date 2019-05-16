#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBufferFromString.h>
#include <Core/Types.h>
#include <Poco/RandomStream.h>
#include <Poco/Net/StreamSocket.h>
#include <random>
#include <sstream>
#include <common/logger_useful.h>
#include <Poco/Logger.h>

/// Implementation of MySQL wire protocol

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
}

namespace MySQLProtocol
{

const size_t MAX_PACKET_LENGTH = (1 << 24) - 1; // 16 mb
const size_t SCRAMBLE_LENGTH = 20;
const size_t AUTH_PLUGIN_DATA_PART_1_LENGTH = 8;
const size_t MYSQL_ERRMSG_SIZE = 512;

namespace Authentication
{
    const String CachingSHA2 = "caching_sha2_password";
}

enum CharacterSet
{
    utf8_general_ci = 33,
    binary = 63
};

enum StatusFlags
{
    SERVER_SESSION_STATE_CHANGED = 0x4000
};

enum Capability
{
    CLIENT_CONNECT_WITH_DB = 0x00000008,
    CLIENT_PROTOCOL_41 = 0x00000200,
    CLIENT_SSL = 0x00000800,
    CLIENT_TRANSACTIONS = 0x00002000, // TODO
    CLIENT_SESSION_TRACK = 0x00800000, // TODO
    CLIENT_SECURE_CONNECTION = 0x00008000,
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000,
    CLIENT_PLUGIN_AUTH = 0x00080000,
    CLIENT_DEPRECATE_EOF = 0x01000000,
};

enum Command
{
    COM_SLEEP = 0x0,
    COM_QUIT = 0x1,
    COM_INIT_DB = 0x2,
    COM_QUERY = 0x3,
    COM_FIELD_LIST = 0x4,
    COM_CREATE_DB = 0x5,
    COM_DROP_DB = 0x6,
    COM_REFRESH = 0x7,
    COM_SHUTDOWN = 0x8,
    COM_STATISTICS = 0x9,
    COM_PROCESS_INFO = 0xa,
    COM_CONNECT = 0xb,
    COM_PROCESS_KILL = 0xc,
    COM_DEBUG = 0xd,
    COM_PING = 0xe,
    COM_TIME = 0xf,
    COM_DELAYED_INSERT = 0x10,
    COM_CHANGE_USER = 0x11,
    COM_RESET_CONNECTION = 0x1f,
    COM_DAEMON = 0x1d
};

enum ColumnType
{
    MYSQL_TYPE_DECIMAL = 0x00,
    MYSQL_TYPE_TINY = 0x01,
    MYSQL_TYPE_SHORT = 0x02,
    MYSQL_TYPE_LONG = 0x03,
    MYSQL_TYPE_FLOAT = 0x04,
    MYSQL_TYPE_DOUBLE = 0x05,
    MYSQL_TYPE_NULL = 0x06,
    MYSQL_TYPE_TIMESTAMP = 0x07,
    MYSQL_TYPE_LONGLONG = 0x08,
    MYSQL_TYPE_INT24 = 0x09,
    MYSQL_TYPE_DATE = 0x0a,
    MYSQL_TYPE_TIME = 0x0b,
    MYSQL_TYPE_DATETIME = 0x0c,
    MYSQL_TYPE_YEAR = 0x0d,
    MYSQL_TYPE_VARCHAR = 0x0f,
    MYSQL_TYPE_BIT = 0x10,
    MYSQL_TYPE_NEWDECIMAL = 0xf6,
    MYSQL_TYPE_ENUM = 0xf7,
    MYSQL_TYPE_SET = 0xf8,
    MYSQL_TYPE_TINY_BLOB = 0xf9,
    MYSQL_TYPE_MEDIUM_BLOB = 0xfa,
    MYSQL_TYPE_LONG_BLOB = 0xfb,
    MYSQL_TYPE_BLOB = 0xfc,
    MYSQL_TYPE_VAR_STRING = 0xfd,
    MYSQL_TYPE_STRING = 0xfe,
    MYSQL_TYPE_GEOMETRY = 0xff
};


class ProtocolError : public DB::Exception
{
public:
    using Exception::Exception;
};


class WritePacket
{
public:
    virtual String getPayload() const = 0;

    virtual ~WritePacket() = default;
};


class ReadPacket
{
public:
    ReadPacket() = default;
    ReadPacket(const ReadPacket &) = default;
    virtual void readPayload(String payload) = 0;

    virtual ~ReadPacket() = default;
};


/* Writes and reads packets, keeping sequence-id.
 * Throws ProtocolError, if packet with incorrect sequence-id was received.
 */
class PacketSender
{
public:
    size_t & sequence_id;
    ReadBuffer * in;
    WriteBuffer * out;

    /// For reading and writing.
    PacketSender(ReadBuffer & in, WriteBuffer & out, size_t & sequence_id, const String logger_name)
        : sequence_id(sequence_id)
        , in(&in)
        , out(&out)
        , log(&Poco::Logger::get(logger_name))
    {
    }

    /// For writing.
    PacketSender(WriteBuffer & out, size_t & sequence_id, const String logger_name)
        : sequence_id(sequence_id)
        , in(nullptr)
        , out(&out)
        , log(&Poco::Logger::get(logger_name))
    {
    }

    String receivePacketPayload()
    {
        WriteBufferFromOwnString buf;

        size_t payload_length = 0;
        size_t packet_sequence_id = 0;

        // packets which are larger than or equal to 16MB are splitted
        do
        {
            LOG_TRACE(log, "Reading from buffer");

            in->readStrict(reinterpret_cast<char *>(&payload_length), 3);

            if (payload_length > MAX_PACKET_LENGTH)
            {
                std::ostringstream tmp;
                tmp << "Received packet with payload larger than MAX_PACKET_LENGTH: " << payload_length;
                throw ProtocolError(tmp.str(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
            }

            in->readStrict(reinterpret_cast<char *>(&packet_sequence_id), 1);

            if (packet_sequence_id != sequence_id)
            {
                std::ostringstream tmp;
                tmp << "Received packet with wrong sequence-id: " << packet_sequence_id << ". Expected: " << sequence_id << '.';
                throw ProtocolError(tmp.str(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
            }
            sequence_id++;

            LOG_TRACE(log, "Received packet. Sequence-id: " << packet_sequence_id << ", payload length: " << payload_length);

            copyData(*in, static_cast<WriteBuffer &>(buf), payload_length);
        } while (payload_length == MAX_PACKET_LENGTH);

        return std::move(buf.str());
    }

    void receivePacket(ReadPacket & packet)
    {
        packet.readPayload(receivePacketPayload());
    }

    template<class T>
    void sendPacket(const T & packet, bool flush = false)
    {
        static_assert(std::is_base_of<WritePacket, T>());
        String payload = packet.getPayload();
        size_t pos = 0;
        do
        {
            size_t payload_length = std::min(payload.length() - pos, MAX_PACKET_LENGTH);

            LOG_TRACE(log, "Writing packet of size " << payload_length << " with sequence-id " << static_cast<int>(sequence_id));
            LOG_TRACE(log, packetToText(payload));

            out->write(reinterpret_cast<const char *>(&payload_length), 3);
            out->write(reinterpret_cast<const char *>(&sequence_id), 1);
            out->write(payload.data() + pos, payload_length);

            pos += payload_length;
            sequence_id++;
        } while (pos < payload.length());

        LOG_TRACE(log, "Packet was sent.");

        if (flush)
        {
            out->next();
        }
    }

    /// Sets sequence-id to 0. Must be called before each command phase.
    void resetSequenceId();

private:
    /// Converts packet to text. Is used for debug output.
    static String packetToText(String payload);

    Poco::Logger * log;
};


uint64_t readLengthEncodedNumber(std::istringstream & ss);

String writeLengthEncodedNumber(uint64_t x);

void writeLengthEncodedString(String & payload, const String & s);

void writeNulTerminatedString(String & payload, const String & s);


class Handshake : public WritePacket
{
    int protocol_version = 0xa;
    String server_version;
    uint32_t connection_id;
    uint32_t capability_flags;
    uint8_t character_set;
    uint32_t status_flags;
    String auth_plugin_data;
public:
    explicit Handshake(uint32_t connection_id, String server_version, String auth_plugin_data)
        : protocol_version(0xa)
        , server_version(std::move(server_version))
        , connection_id(connection_id)
        , capability_flags(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF | CLIENT_SSL)
        , character_set(CharacterSet::utf8_general_ci)
        , status_flags(0)
        , auth_plugin_data(auth_plugin_data)
    {
    }

    String getPayload() const override
    {
        String result;
        result.append(1, protocol_version);
        writeNulTerminatedString(result, server_version);
        result.append(reinterpret_cast<const char *>(&connection_id), 4);
        writeNulTerminatedString(result, auth_plugin_data.substr(0, AUTH_PLUGIN_DATA_PART_1_LENGTH));
        result.append(reinterpret_cast<const char *>(&capability_flags), 2);
        result.append(reinterpret_cast<const char *>(&character_set), 1);
        result.append(reinterpret_cast<const char *>(&status_flags), 2);
        result.append((reinterpret_cast<const char *>(&capability_flags)) + 2, 2);
        result.append(1, auth_plugin_data.size());
        result.append(10, 0x0);
        result.append(auth_plugin_data.substr(AUTH_PLUGIN_DATA_PART_1_LENGTH, auth_plugin_data.size() - AUTH_PLUGIN_DATA_PART_1_LENGTH));
        result.append(Authentication::CachingSHA2);
        result.append(1, 0x0);
        return result;
    }
};

class SSLRequest : public ReadPacket
{
public:
    uint32_t capability_flags;
    uint32_t max_packet_size;
    uint8_t character_set;

    void readPayload(String s) override
    {
        std::istringstream ss(s);
        ss.readsome(reinterpret_cast<char *>(&capability_flags), 4);
        ss.readsome(reinterpret_cast<char *>(&max_packet_size), 4);
        ss.readsome(reinterpret_cast<char *>(&character_set), 1);
    }
};

class HandshakeResponse : public ReadPacket
{
public:
    uint32_t capability_flags;
    uint32_t max_packet_size;
    uint8_t character_set;
    String username;
    String auth_response;
    String database;
    String auth_plugin_name;

    HandshakeResponse() = default;

    HandshakeResponse(const HandshakeResponse &) = default;

    void readPayload(String s) override
    {
        std::istringstream ss(s);

        ss.readsome(reinterpret_cast<char *>(&capability_flags), 4);
        ss.readsome(reinterpret_cast<char *>(&max_packet_size), 4);
        ss.readsome(reinterpret_cast<char *>(&character_set), 1);
        ss.ignore(23);

        std::getline(ss, username, static_cast<char>(0x0));

        if (capability_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
        {
            auto len = readLengthEncodedNumber(ss);
            auth_response.resize(len);
            ss.read(auth_response.data(), static_cast<std::streamsize>(len));
        }
        else if (capability_flags & CLIENT_SECURE_CONNECTION)
        {
            uint8_t len;
            ss.read(reinterpret_cast<char *>(&len), 1);
            auth_response.resize(len);
            ss.read(auth_response.data(), len);
        }
        else
        {
            std::getline(ss, auth_response, static_cast<char>(0x0));
        }

        if (capability_flags & CLIENT_CONNECT_WITH_DB)
        {
            std::getline(ss, database, static_cast<char>(0x0));
        }

        if (capability_flags & CLIENT_PLUGIN_AUTH)
        {
            std::getline(ss, auth_plugin_name, static_cast<char>(0x0));
        }
    }
};

class AuthSwitchRequest : public WritePacket
{
    String plugin_name;
    String auth_plugin_data;
public:
    AuthSwitchRequest(String plugin_name, String auth_plugin_data)
        : plugin_name(std::move(plugin_name)), auth_plugin_data(std::move(auth_plugin_data))
    {
    }

    String getPayload() const override
    {
        String result;
        result.append(1, 0xfe);
        writeNulTerminatedString(result, plugin_name);
        result.append(auth_plugin_data);
        return result;
    }
};

class AuthSwitchResponse : public ReadPacket
{
public:
    String value;

    void readPayload(String s) override
    {
        value = std::move(s);
    }
};

class AuthMoreData : public WritePacket
{
    String data;
public:
    AuthMoreData(String data): data(std::move(data)) {}

    String getPayload() const override
    {
        String result;
        result.append(1, 0x01);
        result.append(data);
        return result;
    }
};

/// Packet with a single null-terminated string. Is used for clear text authentication.
class NullTerminatedString : public ReadPacket
{
public:
    String value;

    void readPayload(String s) override
    {
        if (s.length() == 0 || s.back() != 0)
        {
            throw ProtocolError("String is not null terminated.", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        }
        value = s;
        value.pop_back();
    }
};

class OK_Packet : public WritePacket
{
    uint8_t header;
    uint32_t capabilities;
    uint64_t affected_rows;
    uint64_t last_insert_id;
    int16_t warnings = 0;
    uint32_t status_flags;
    String info;
    String session_state_changes;
public:
    OK_Packet(uint8_t header, uint32_t capabilities, uint64_t affected_rows, uint64_t last_insert_id, uint32_t status_flags,
              int16_t warnings, String session_state_changes)
        : header(header), capabilities(capabilities), affected_rows(affected_rows), last_insert_id(last_insert_id), warnings(warnings),
          status_flags(status_flags), session_state_changes(std::move(session_state_changes))
    {
    }

    String getPayload() const override
    {
        String result;
        result.append(1, header);
        result.append(writeLengthEncodedNumber(affected_rows));
        result.append(writeLengthEncodedNumber(last_insert_id));

        if (capabilities & CLIENT_PROTOCOL_41)
        {
            result.append(reinterpret_cast<const char *>(&status_flags), 2);
            result.append(reinterpret_cast<const char *>(&warnings), 2);
        }
        else if (capabilities & CLIENT_TRANSACTIONS)
        {
            result.append(reinterpret_cast<const char *>(&status_flags), 2);
        }

        if (capabilities & CLIENT_SESSION_TRACK)
        {
            result.append(writeLengthEncodedNumber(info.length()));
            result.append(info);
            if (status_flags & SERVER_SESSION_STATE_CHANGED)
            {
                result.append(writeLengthEncodedNumber(session_state_changes.length()));
                result.append(session_state_changes);
            }
        }
        else
        {
            result.append(info);
        }
        return result;
    }
};

class EOF_Packet : public WritePacket
{
    int warnings;
    int status_flags;
public:
    EOF_Packet(int warnings, int status_flags) : warnings(warnings), status_flags(status_flags)
    {}

    String getPayload() const override
    {
        String result;
        result.append(1, 0xfe); // EOF header
        result.append(reinterpret_cast<const char *>(&warnings), 2);
        result.append(reinterpret_cast<const char *>(&status_flags), 2);
        return result;
    }
};

class ERR_Packet : public WritePacket
{
    int error_code;
    String sql_state;
    String error_message;
public:
    ERR_Packet(int error_code, String sql_state, String error_message)
        : error_code(error_code), sql_state(std::move(sql_state)), error_message(std::move(error_message))
    {
    }

    String getPayload() const override
    {
        String result;
        result.append(1, 0xff);
        result.append(reinterpret_cast<const char *>(&error_code), 2);
        result.append("#", 1);
        result.append(sql_state.data(), sql_state.length());
        result.append(error_message.data(), std::min(error_message.length(), MYSQL_ERRMSG_SIZE));
        return result;
    }
};

class ColumnDefinition : public WritePacket
{
    String schema;
    String table;
    String org_table;
    String name;
    String org_name;
    size_t next_length = 0x0c;
    uint16_t character_set;
    uint32_t column_length;
    ColumnType column_type;
    uint16_t flags;
    uint8_t decimals = 0x00;
public:
    ColumnDefinition(
        String schema,
        String table,
        String org_table,
        String name,
        String org_name,
        uint16_t character_set,
        uint32_t column_length,
        ColumnType column_type,
        uint16_t flags,
        uint8_t decimals)

        : schema(std::move(schema)), table(std::move(table)), org_table(std::move(org_table)), name(std::move(name)),
          org_name(std::move(org_name)), character_set(character_set), column_length(column_length), column_type(column_type), flags(flags),
          decimals(decimals)
    {
    }

    /// Should be used when column metadata (original name, table, original table, database) is unknown.
    ColumnDefinition(
        String name,
        uint16_t character_set,
        uint32_t column_length,
        ColumnType column_type,
        uint16_t flags,
        uint8_t decimals)
        : ColumnDefinition("", "", "", std::move(name), "", character_set, column_length, column_type, flags, decimals)
    {
    }

    String getPayload() const override
    {
        String result;
        writeLengthEncodedString(result, "def"); /// always "def"
        writeLengthEncodedString(result, ""); /// schema
        writeLengthEncodedString(result, ""); /// table
        writeLengthEncodedString(result, ""); /// org_table
        writeLengthEncodedString(result, name);
        writeLengthEncodedString(result, ""); /// org_name
        result.append(writeLengthEncodedNumber(next_length));
        result.append(reinterpret_cast<const char *>(&character_set), 2);
        result.append(reinterpret_cast<const char *>(&column_length), 4);
        result.append(reinterpret_cast<const char *>(&column_type), 1);
        result.append(reinterpret_cast<const char *>(&flags), 2);
        result.append(reinterpret_cast<const char *>(&decimals), 2);
        result.append(2, 0x0);
        return result;
    }
};

class ComFieldList : public ReadPacket
{
public:
    String table, field_wildcard;

    void readPayload(String payload)
    {
        std::istringstream ss(payload);
        ss.ignore(1); // command byte
        std::getline(ss, table, static_cast<char>(0x0));
        field_wildcard = payload.substr(table.length() + 2); // rest of the packet
    }
};

class LengthEncodedNumber : public WritePacket
{
    uint64_t value;
public:
    LengthEncodedNumber(uint64_t value): value(value)
    {
    }

    String getPayload() const override
    {
        return writeLengthEncodedNumber(value);
    }
};

class ResultsetRow : public WritePacket
{
    std::vector<String> columns;
public:
    ResultsetRow()
    {
    }

    void appendColumn(String value)
    {
        columns.emplace_back(std::move(value));
    }

    String getPayload() const override
    {
        String result;
        for (const String & column : columns)
        {
            writeLengthEncodedString(result, column);
        }
        return result;
    }
};

}
}
