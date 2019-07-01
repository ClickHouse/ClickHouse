#pragma once

#include <Common/MemoryTracker.h>
#include <Common/PODArray.h>
#include <Core/Types.h>
#include <IO/copyData.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/RandomStream.h>
#include <random>
#include <sstream>

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
const size_t PACKET_HEADER_SIZE = 4;
const size_t SSL_REQUEST_PAYLOAD_SIZE = 32;

namespace Authentication
{
    const String SHA256 = "sha256_password"; /// Caching SHA2 plugin is not used because it would be possible to authenticate knowing hash from users.xml.
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


// For tracking memory of packets with String fields.
class PacketMemoryTracker
{
    Int64 allocated = 0;
public:
    void alloc(Int64 size)
    {
        CurrentMemoryTracker::alloc(allocated);
        allocated = size;
    }

    ~PacketMemoryTracker()
    {
        CurrentMemoryTracker::free(allocated);
    }
};


using Vector = PODArray<char>;


class ProtocolError : public DB::Exception
{
public:
    using Exception::Exception;
};


class WritePacket
{
public:
    virtual void writePayload(WriteBuffer & buffer) const = 0;

    virtual ~WritePacket() = default;
};


class ReadPacket
{
public:
    ReadPacket() = default;
    ReadPacket(ReadPacket &&) = default;
    virtual void readPayload(Vector && payload) = 0;

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
    size_t max_packet_size = MAX_PACKET_LENGTH;

    /// For reading and writing.
    PacketSender(ReadBuffer & in, WriteBuffer & out, size_t & sequence_id)
        : sequence_id(sequence_id)
        , in(&in)
        , out(&out)
    {
    }

    /// For writing.
    PacketSender(WriteBuffer & out, size_t & sequence_id)
        : sequence_id(sequence_id)
        , in(nullptr)
        , out(&out)
    {
    }

  Vector receivePacketPayload()
    {
        Vector result;
        WriteBufferFromVector<Vector> buf(result);

        size_t payload_length = 0;
        size_t packet_sequence_id = 0;

        // packets which are larger than or equal to 16MB are splitted
        do
        {
            in->readStrict(reinterpret_cast<char *>(&payload_length), 3);

            if (payload_length > max_packet_size)
            {
                std::ostringstream tmp;
                tmp << "Received packet with payload larger than max_packet_size: " << payload_length;
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

            copyData(*in, static_cast<WriteBuffer &>(buf), payload_length);
        } while (payload_length == max_packet_size);

        return result;
    }

    void receivePacket(ReadPacket & packet)
    {
        auto payload = receivePacketPayload();
        packet.readPayload(std::move(payload));
    }

    template<class T>
    void sendPacket(const T & packet, bool flush = false)
    {
        static_assert(std::is_base_of<WritePacket, T>());
        Vector payload;
        WriteBufferFromVector buffer(payload);
        packet.writePayload(buffer);
        size_t pos = 0;
        do
        {
            size_t payload_length = std::min(payload.size() - pos, max_packet_size);

            out->write(reinterpret_cast<const char *>(&payload_length), 3);
            out->write(reinterpret_cast<const char *>(&sequence_id), 1);
            out->write(payload.data() + pos, payload_length);

            pos += payload_length;
            sequence_id++;
        } while (pos < payload.size());

        if (flush)
            out->next();
    }

    /// Sets sequence-id to 0. Must be called before each command phase.
    void resetSequenceId();

    /// Converts packet to text. Is used for debug output.
    static String packetToText(const Vector & payload);
};


uint64_t readLengthEncodedNumber(ReadBuffer & ss);

void writeLengthEncodedNumber(uint64_t x, WriteBuffer & buffer);

template <class T>
void writeLengthEncodedString(const T & s, WriteBuffer & buffer)
{
    writeLengthEncodedNumber(s.size(), buffer);
    buffer.write(s.data(), s.size());
}

template <class T>
void writeNulTerminatedString(const T & s, WriteBuffer & buffer)
{
    buffer.write(s.data(), s.size());
    buffer.write(0);
}


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
    explicit Handshake(uint32_t capability_flags, uint32_t connection_id, String server_version, String auth_plugin_data)
        : protocol_version(0xa)
        , server_version(std::move(server_version))
        , connection_id(connection_id)
        , capability_flags(capability_flags)
        , character_set(CharacterSet::utf8_general_ci)
        , status_flags(0)
        , auth_plugin_data(std::move(auth_plugin_data))
    {
    }

    void writePayload(WriteBuffer & buffer) const override
    {
        buffer.write(static_cast<char>(protocol_version));
        writeNulTerminatedString(server_version, buffer);
        buffer.write(reinterpret_cast<const char *>(&connection_id), 4);
        writeNulTerminatedString(auth_plugin_data.substr(0, AUTH_PLUGIN_DATA_PART_1_LENGTH), buffer);
        buffer.write(reinterpret_cast<const char *>(&capability_flags), 2);
        buffer.write(reinterpret_cast<const char *>(&character_set), 1);
        buffer.write(reinterpret_cast<const char *>(&status_flags), 2);
        buffer.write((reinterpret_cast<const char *>(&capability_flags)) + 2, 2);
        buffer.write(static_cast<char>(auth_plugin_data.size()));
        writeChar(0x0, 10, buffer);
        writeString(auth_plugin_data.substr(AUTH_PLUGIN_DATA_PART_1_LENGTH, auth_plugin_data.size() - AUTH_PLUGIN_DATA_PART_1_LENGTH), buffer);
        writeString(Authentication::SHA256, buffer);
        writeChar(0x0, 1, buffer);
    }
};

class SSLRequest : public ReadPacket
{
public:
    uint32_t capability_flags;
    uint32_t max_packet_size;
    uint8_t character_set;

    void readPayload(Vector && s) override
    {
        ReadBufferFromMemory buffer(s.data(), s.size());
        buffer.readStrict(reinterpret_cast<char *>(&capability_flags), 4);
        buffer.readStrict(reinterpret_cast<char *>(&max_packet_size), 4);
        buffer.readStrict(reinterpret_cast<char *>(&character_set), 1);
    }
};

class HandshakeResponse : public ReadPacket
{
public:
    uint32_t capability_flags = 0;
    uint32_t max_packet_size = 0;
    uint8_t character_set = 0;
    String username;
    String auth_response;
    String database;
    String auth_plugin_name;
    PacketMemoryTracker tracker;

    HandshakeResponse() = default;

    void readPayload(Vector && payload) override
    {
        std::cerr << PacketSender::packetToText(payload) << std::endl;
        tracker.alloc(payload.size());
        ReadBufferFromMemory buffer(payload.data(), payload.size());

        buffer.readStrict(reinterpret_cast<char *>(&capability_flags), 4);
        buffer.readStrict(reinterpret_cast<char *>(&max_packet_size), 4);
        buffer.readStrict(reinterpret_cast<char *>(&character_set), 1);
        buffer.ignore(23);

        readNullTerminated(username, buffer);

        if (capability_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
        {
            auto len = readLengthEncodedNumber(buffer);
            auth_response.resize(len);
            buffer.readStrict(auth_response.data(), len);
        }
        else if (capability_flags & CLIENT_SECURE_CONNECTION)
        {
            char len;
            buffer.readStrict(len);
            auth_response.resize(static_cast<unsigned int>(len));
            buffer.readStrict(auth_response.data(), len);
        }
        else
        {
            readNullTerminated(auth_response, buffer);
        }

        if (capability_flags & CLIENT_CONNECT_WITH_DB)
        {
            readNullTerminated(database, buffer);
        }

        if (capability_flags & CLIENT_PLUGIN_AUTH)
        {
            readNullTerminated(auth_plugin_name, buffer);
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

    void writePayload(WriteBuffer & buffer) const override
    {
        buffer.write(0xfe);
        writeNulTerminatedString(plugin_name, buffer);
        writeString(auth_plugin_data, buffer);
        std::cerr << auth_plugin_data.size() << std::endl;
    }
};

class AuthSwitchResponse : public ReadPacket
{
public:
    String value;
    PacketMemoryTracker tracker;

    void readPayload(Vector && payload) override
    {
        tracker.alloc(payload.size());
        value.assign(payload.data(), payload.size());
    }
};

class AuthMoreData : public WritePacket
{
    String data;
public:
    explicit AuthMoreData(String data): data(std::move(data)) {}

    void writePayload(WriteBuffer & buffer) const override
    {
        buffer.write(0x01);
        writeString(data, buffer);
    }
};

/// Packet with a single null-terminated string. Is used for clear text authentication.
class NullTerminatedString : public ReadPacket
{
public:
    Vector value;

    void readPayload(Vector && payload) override
    {
        if (payload.empty() || payload.back() != 0)
        {
            throw ProtocolError("String is not null terminated.", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        }
        value = std::move(payload);
        value.pop_back();
    }
};

class OK_Packet : public WritePacket
{
    uint8_t header;
    uint32_t capabilities;
    uint64_t affected_rows;
    int16_t warnings = 0;
    uint32_t status_flags;
    String session_state_changes;
    String info;
public:
    OK_Packet(uint8_t header,
        uint32_t capabilities,
        uint64_t affected_rows,
        uint32_t status_flags,
        int16_t warnings,
        String session_state_changes = "",
        String info = "")
        : header(header)
        , capabilities(capabilities)
        , affected_rows(affected_rows)
        , warnings(warnings)
        , status_flags(status_flags)
        , session_state_changes(std::move(session_state_changes))
        , info(info)
    {
    }

    void writePayload(WriteBuffer & buffer) const override
    {
        buffer.write(header);
        writeLengthEncodedNumber(affected_rows, buffer);
        writeLengthEncodedNumber(0, buffer); /// last insert-id

        if (capabilities & CLIENT_PROTOCOL_41)
        {
            buffer.write(reinterpret_cast<const char *>(&status_flags), 2);
            buffer.write(reinterpret_cast<const char *>(&warnings), 2);
        }
        else if (capabilities & CLIENT_TRANSACTIONS)
        {
            buffer.write(reinterpret_cast<const char *>(&status_flags), 2);
        }

        if (capabilities & CLIENT_SESSION_TRACK)
        {
            writeLengthEncodedNumber(info.length(), buffer);
            writeString(info, buffer);
            if (status_flags & SERVER_SESSION_STATE_CHANGED)
            {
                writeLengthEncodedNumber(session_state_changes.length(), buffer);
                writeString(session_state_changes, buffer);
            }
        }
        else
        {
            writeString(info, buffer);
        }
    }
};

class EOF_Packet : public WritePacket
{
    int warnings;
    int status_flags;
public:
    EOF_Packet(int warnings, int status_flags) : warnings(warnings), status_flags(status_flags)
    {}

    void writePayload(WriteBuffer & buffer) const override
    {
        buffer.write(0xfe); // EOF header
        buffer.write(reinterpret_cast<const char *>(&warnings), 2);
        buffer.write(reinterpret_cast<const char *>(&status_flags), 2);
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

    void writePayload(WriteBuffer & buffer) const override
    {
        buffer.write(0xff);
        buffer.write(reinterpret_cast<const char *>(&error_code), 2);
        buffer.write('#');
        buffer.write(sql_state.data(), sql_state.length());
        buffer.write(error_message.data(), std::min(error_message.length(), MYSQL_ERRMSG_SIZE));
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

    void writePayload(WriteBuffer & buffer) const override
    {
        writeLengthEncodedString(std::string("def"), buffer); /// always "def"
        writeLengthEncodedString(schema, buffer);
        writeLengthEncodedString(table, buffer);
        writeLengthEncodedString(org_table, buffer);
        writeLengthEncodedString(name, buffer);
        writeLengthEncodedString(org_name, buffer);
        writeLengthEncodedNumber(next_length, buffer);
        buffer.write(reinterpret_cast<const char *>(&character_set), 2);
        buffer.write(reinterpret_cast<const char *>(&column_length), 4);
        buffer.write(reinterpret_cast<const char *>(&column_type), 1);
        buffer.write(reinterpret_cast<const char *>(&flags), 2);
        buffer.write(reinterpret_cast<const char *>(&decimals), 2);
        writeChar(0x0, 2, buffer);
    }
};

class ComFieldList : public ReadPacket
{
public:
    String table, field_wildcard;

    void readPayload(Vector && payload) override
    {
        ReadBufferFromMemory buffer(payload.data(), payload.size());
        buffer.ignore(1); // command byte
        readNullTerminated(table, buffer);
        field_wildcard.assign(payload.data() + table.size() + 2);
        readStringUntilEOFInto(field_wildcard, buffer);
    }
};

class LengthEncodedNumber : public WritePacket
{
    uint64_t value;
public:
    LengthEncodedNumber(uint64_t value): value(value)
    {
    }

    void writePayload(WriteBuffer & buffer) const override
    {
        writeLengthEncodedNumber(value, buffer);
    }
};

class ResultsetRow : public WritePacket
{
    std::vector<Vector> columns;
public:
    ResultsetRow()
    {
    }

    void appendColumn(Vector value)
    {
        columns.emplace_back(std::move(value));
    }

    void writePayload(WriteBuffer & buffer) const override
    {
        for (const Vector & column : columns)
            writeLengthEncodedString(column, buffer);
    }
};

}
}
