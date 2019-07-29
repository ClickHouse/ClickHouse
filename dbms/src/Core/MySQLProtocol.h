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
#include <IO/LimitReadBuffer.h>

/// Implementation of MySQL wire protocol.
/// Works only on little-endian architecture.

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
    const String Native = "mysql_native_password";
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


class ProtocolError : public DB::Exception
{
public:
    using Exception::Exception;
};


/** Reading packets.
 *  Internally, it calls (if no more data) next() method of the underlying ReadBufferFromPocoSocket, and sets the working buffer to the rest part of the current packet payload.
 */
class PacketPayloadReadBuffer : public ReadBuffer
{
public:
    PacketPayloadReadBuffer(ReadBuffer & in, uint8_t & sequence_id)
        : ReadBuffer(in.position(), 0)  // not in.buffer().begin(), because working buffer may include previous packet
        , in(in)
        , sequence_id(sequence_id)
    {
    }

private:
    ReadBuffer & in;
    uint8_t & sequence_id;
    const size_t max_packet_size = MAX_PACKET_LENGTH;

    // Size of packet which is being read now.
    size_t payload_length = 0;

    // Offset in packet payload.
    size_t offset = 0;

protected:
    bool nextImpl() override
    {
        if (payload_length == 0 || (payload_length == max_packet_size && offset == payload_length))
        {
            working_buffer.resize(0);
            offset = 0;
            payload_length = 0;
            in.readStrict(reinterpret_cast<char *>(&payload_length), 3);

            if (payload_length > max_packet_size)
            {
                std::ostringstream tmp;
                tmp << "Received packet with payload larger than max_packet_size: " << payload_length;
                throw ProtocolError(tmp.str(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
            }
            else if (payload_length == 0)
            {
                return false;
            }

            size_t packet_sequence_id = 0;
            in.read(reinterpret_cast<char &>(packet_sequence_id));
            if (packet_sequence_id != sequence_id)
            {
                std::ostringstream tmp;
                tmp << "Received packet with wrong sequence-id: " << packet_sequence_id << ". Expected: " << static_cast<unsigned int>(sequence_id) << '.';
                throw ProtocolError(tmp.str(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
            }
            sequence_id++;
        }
        else if (offset == payload_length)
        {
            return false;
        }

        in.nextIfAtEnd();
        working_buffer = ReadBuffer::Buffer(in.position(), in.buffer().end());
        size_t count = std::min(in.available(), payload_length - offset);
        working_buffer.resize(count);
        in.ignore(count);

        offset += count;

        return true;
    }
};


class ClientPacket
{
public:
    ClientPacket() = default;
    ClientPacket(ClientPacket &&) = default;

    virtual void read(ReadBuffer & in, uint8_t & sequence_id)
    {
        PacketPayloadReadBuffer payload(in, sequence_id);
        readPayload(payload);
        if (!payload.eof())
        {
            std::stringstream tmp;
            tmp << "Packet payload is not fully read. Stopped after " << payload.count() << " bytes, while " << payload.available() << " bytes are in buffer.";
            throw ProtocolError(tmp.str(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        }
    }

    virtual void readPayload(ReadBuffer & buf) = 0;

    virtual ~ClientPacket() = default;
};


class LimitedClientPacket : public ClientPacket
{
public:
    void read(ReadBuffer & in, uint8_t & sequence_id) override
    {
        LimitReadBuffer limited(in, 10000, true, "too long MySQL packet.");
        ClientPacket::read(limited, sequence_id);
    }
};


/** Writing packets.
 *  https://dev.mysql.com/doc/internals/en/mysql-packet.html
 */
class PacketPayloadWriteBuffer : public WriteBuffer
{
public:
    PacketPayloadWriteBuffer(WriteBuffer & out, size_t payload_length, uint8_t & sequence_id)
        : WriteBuffer(out.position(), 0)
        , out(out)
        , sequence_id(sequence_id)
        , total_left(payload_length)
    {
        startPacket();
    }

    void checkPayloadSize()
    {
        if (bytes_written + offset() < payload_length)
        {
            std::stringstream ss;
            ss << "Incomplete payload. Written " << bytes << " bytes, expected " << payload_length << " bytes.";
            throw Exception(ss.str(), 0);

        }
    }

    ~PacketPayloadWriteBuffer() override { next(); }
private:
    WriteBuffer & out;
    uint8_t & sequence_id;

    size_t total_left = 0;
    size_t payload_length = 0;
    size_t bytes_written = 0;

    void startPacket()
    {
        payload_length = std::min(total_left, MAX_PACKET_LENGTH);
        bytes_written = 0;
        total_left -= payload_length;

        out.write(reinterpret_cast<char *>(&payload_length), 3);
        out.write(sequence_id++);

        working_buffer = WriteBuffer::Buffer(out.position(), out.position() + std::min(payload_length - bytes_written, out.available()));
        pos = working_buffer.begin();
    }
protected:
    void nextImpl() override
    {
        int written = pos - working_buffer.begin();
        out.position() += written;
        bytes_written += written;

        if (bytes_written < payload_length)
        {
            out.nextIfAtEnd();
            working_buffer = WriteBuffer::Buffer(out.position(), out.position() + std::min(payload_length - bytes_written, out.available()));
        }
        else if (total_left > 0 || payload_length == MAX_PACKET_LENGTH)
        {
            // Starting new packet, since packets of size greater than MAX_PACKET_LENGTH should be split.
            startPacket();
        }
        else
        {
            // Finished writing packet. Buffer is set to empty to prevent rewriting (pos will be set to the beginning of a working buffer in next()).
            // Further attempts to write will stall in the infinite loop.
            working_buffer = WriteBuffer::Buffer(out.position(), out.position());
        }
    }
};


class WritePacket
{
public:
    virtual void writePayload(WriteBuffer & buffer, uint8_t & sequence_id) const
    {
        PacketPayloadWriteBuffer buf(buffer, getPayloadSize(), sequence_id);
        writePayloadImpl(buf);
        buf.checkPayloadSize();
    }

    virtual ~WritePacket() = default;

protected:
    virtual size_t getPayloadSize() const = 0;

    virtual void writePayloadImpl(WriteBuffer & buffer) const = 0;
};


/* Writes and reads packets, keeping sequence-id.
 * Throws ProtocolError, if packet with incorrect sequence-id was received.
 */
class PacketSender
{
public:
    uint8_t & sequence_id;
    ReadBuffer * in;
    WriteBuffer * out;
    size_t max_packet_size = MAX_PACKET_LENGTH;

    /// For reading and writing.
    PacketSender(ReadBuffer & in, WriteBuffer & out, uint8_t & sequence_id)
        : sequence_id(sequence_id)
        , in(&in)
        , out(&out)
    {
    }

    /// For writing.
    PacketSender(WriteBuffer & out, uint8_t & sequence_id)
        : sequence_id(sequence_id)
        , in(nullptr)
        , out(&out)
    {
    }

    void receivePacket(ClientPacket & packet)
    {
        packet.read(*in, sequence_id);
    }

    template<class T>
    void sendPacket(const T & packet, bool flush = false)
    {
        static_assert(std::is_base_of<WritePacket, T>());
        packet.writePayload(*out, sequence_id);
        if (flush)
            out->next();
    }

    PacketPayloadReadBuffer getPayload()
    {
        return PacketPayloadReadBuffer(*in, sequence_id);
    }

    /// Sets sequence-id to 0. Must be called before each command phase.
    void resetSequenceId();

    /// Converts packet to text. Is used for debug output.
    static String packetToText(const String & payload);
};


uint64_t readLengthEncodedNumber(ReadBuffer & ss);

void writeLengthEncodedNumber(uint64_t x, WriteBuffer & buffer);

inline void writeLengthEncodedString(const String & s, WriteBuffer & buffer)
{
    writeLengthEncodedNumber(s.size(), buffer);
    buffer.write(s.data(), s.size());
}

inline void writeNulTerminatedString(const String & s, WriteBuffer & buffer)
{
    buffer.write(s.data(), s.size());
    buffer.write(0);
}

size_t getLengthEncodedNumberSize(uint64_t x);

size_t getLengthEncodedStringSize(const String & s);


class Handshake : public WritePacket
{
    int protocol_version = 0xa;
    String server_version;
    uint32_t connection_id;
    uint32_t capability_flags;
    uint8_t character_set;
    uint32_t status_flags;
    String auth_plugin_name;
    String auth_plugin_data;
public:
    explicit Handshake(uint32_t capability_flags, uint32_t connection_id, String server_version, String auth_plugin_name, String auth_plugin_data)
        : protocol_version(0xa)
        , server_version(std::move(server_version))
        , connection_id(connection_id)
        , capability_flags(capability_flags)
        , character_set(CharacterSet::utf8_general_ci)
        , status_flags(0)
        , auth_plugin_name(std::move(auth_plugin_name))
        , auth_plugin_data(std::move(auth_plugin_data))
    {
    }

protected:
    size_t getPayloadSize() const override
    {
        return 26 + server_version.size() + auth_plugin_data.size() + auth_plugin_name.size();
    }

    void writePayloadImpl(WriteBuffer & buffer) const override
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
        // A workaround for PHP mysqlnd extension bug which occurs when sha256_password is used as a default authentication plugin.
        // Instead of using client response for mysql_native_password plugin, the server will always generate authentication method mismatch
        // and switch to sha256_password to simulate that mysql_native_password is used as a default plugin.
        writeString(auth_plugin_name, buffer);
        writeChar(0x0, 1, buffer);
    }
};

class SSLRequest : public ClientPacket
{
public:
    uint32_t capability_flags;
    uint32_t max_packet_size;
    uint8_t character_set;

    void readPayload(ReadBuffer & buf) override
    {
        buf.readStrict(reinterpret_cast<char *>(&capability_flags), 4);
        buf.readStrict(reinterpret_cast<char *>(&max_packet_size), 4);
        buf.readStrict(reinterpret_cast<char *>(&character_set), 1);
    }
};

class HandshakeResponse : public LimitedClientPacket
{
public:
    uint32_t capability_flags = 0;
    uint32_t max_packet_size = 0;
    uint8_t character_set = 0;
    String username;
    String auth_response;
    String database;
    String auth_plugin_name;

    HandshakeResponse() = default;

    void readPayload(ReadBuffer & payload) override
    {
        payload.readStrict(reinterpret_cast<char *>(&capability_flags), 4);
        payload.readStrict(reinterpret_cast<char *>(&max_packet_size), 4);
        payload.readStrict(reinterpret_cast<char *>(&character_set), 1);
        payload.ignore(23);

        readNullTerminated(username, payload);

        if (capability_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
        {
            auto len = readLengthEncodedNumber(payload);
            auth_response.resize(len);
            payload.readStrict(auth_response.data(), len);
        }
        else if (capability_flags & CLIENT_SECURE_CONNECTION)
        {
            char len;
            payload.readStrict(len);
            auth_response.resize(static_cast<unsigned int>(len));
            payload.readStrict(auth_response.data(), len);
        }
        else
        {
            readNullTerminated(auth_response, payload);
        }

        if (capability_flags & CLIENT_CONNECT_WITH_DB)
        {
            readNullTerminated(database, payload);
        }

        if (capability_flags & CLIENT_PLUGIN_AUTH)
        {
            readNullTerminated(auth_plugin_name, payload);
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

protected:
    size_t getPayloadSize() const override
    {
        return 2 + plugin_name.size() + auth_plugin_data.size();
    }

    void writePayloadImpl(WriteBuffer & buffer) const override
    {
        buffer.write(0xfe);
        writeNulTerminatedString(plugin_name, buffer);
        writeString(auth_plugin_data, buffer);
    }
};

class AuthSwitchResponse : public LimitedClientPacket
{
public:
    String value;

    void readPayload(ReadBuffer & payload) override
    {
        readStringUntilEOF(value, payload);
    }
};

class AuthMoreData : public WritePacket
{
    String data;
public:
    explicit AuthMoreData(String data): data(std::move(data)) {}

protected:
    size_t getPayloadSize() const override
    {
        return 1 + data.size();
    }

    void writePayloadImpl(WriteBuffer & buffer) const override
    {
        buffer.write(0x01);
        writeString(data, buffer);
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
        , info(std::move(info))
    {
    }

protected:
    size_t getPayloadSize() const override
    {
        size_t result = 2 + getLengthEncodedNumberSize(affected_rows);

        if (capabilities & CLIENT_PROTOCOL_41)
        {
            result += 4;
        }
        else if (capabilities & CLIENT_TRANSACTIONS)
        {
            result += 2;
        }

        if (capabilities & CLIENT_SESSION_TRACK)
        {
            result += getLengthEncodedStringSize(info);
            if (status_flags & SERVER_SESSION_STATE_CHANGED)
                result += getLengthEncodedStringSize(session_state_changes);
        }
        else
        {
            result += info.size();
        }

        return result;
    }

    void writePayloadImpl(WriteBuffer & buffer) const override
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
            writeLengthEncodedString(info, buffer);
            if (status_flags & SERVER_SESSION_STATE_CHANGED)
                writeLengthEncodedString(session_state_changes, buffer);
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

protected:
    size_t getPayloadSize() const override
    {
        return 5;
    }

    void writePayloadImpl(WriteBuffer & buffer) const override
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

protected:
    size_t getPayloadSize() const override
    {
        return 4 + sql_state.length() + std::min(error_message.length(), MYSQL_ERRMSG_SIZE);
    }

    void writePayloadImpl(WriteBuffer & buffer) const override
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

protected:
    size_t getPayloadSize() const override
    {
        return 13 + getLengthEncodedStringSize("def") + getLengthEncodedStringSize(schema) + getLengthEncodedStringSize(table) + getLengthEncodedStringSize(org_table) + \
            getLengthEncodedStringSize(name) + getLengthEncodedStringSize(org_name) + getLengthEncodedNumberSize(next_length);
    }

    void writePayloadImpl(WriteBuffer & buffer) const override
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

class ComFieldList : public LimitedClientPacket
{
public:
    String table, field_wildcard;

    void readPayload(ReadBuffer & payload) override
    {
        // Command byte has been already read from payload.
        readNullTerminated(table, payload);
        readStringUntilEOF(field_wildcard, payload);
    }
};

class LengthEncodedNumber : public WritePacket
{
    uint64_t value;
public:
    explicit LengthEncodedNumber(uint64_t value): value(value)
    {
    }

protected:
    size_t getPayloadSize() const override
    {
        return getLengthEncodedNumberSize(value);
    }

    void writePayloadImpl(WriteBuffer & buffer) const override
    {
        writeLengthEncodedNumber(value, buffer);
    }
};

class ResultsetRow : public WritePacket
{
    std::vector<String> columns;
    size_t payload_size = 0;
public:
    ResultsetRow() = default;

    void appendColumn(String && value)
    {
        payload_size += getLengthEncodedStringSize(value);
        columns.emplace_back(std::move(value));
    }

protected:
    size_t getPayloadSize() const override
    {
        return payload_size;
    }

    void writePayloadImpl(WriteBuffer & buffer) const override
    {
        for (const String & column : columns)
            writeLengthEncodedString(column, buffer);
    }
};

}
}
