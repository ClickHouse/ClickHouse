#pragma once
#include <ext/scope_guard.h>
#include <random>
#include <sstream>
#include <Common/MemoryTracker.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/PODArray.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <IO/copyData.h>
#include <IO/LimitReadBuffer.h>
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
#include <Poco/SHA1Engine.h>
#include <Core/MySQL/MySQLPackets.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_SSL
#    include <openssl/pem.h>
#    include <openssl/rsa.h>
#endif

/// Implementation of MySQL wire protocol.
/// Works only on little-endian architecture.

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES;
    extern const int OPENSSL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
}

namespace MySQLProtocol
{

const size_t MAX_PACKET_LENGTH = (1 << 24) - 1; // 16 mb
const size_t SCRAMBLE_LENGTH = 20;
const size_t AUTH_PLUGIN_DATA_PART_1_LENGTH = 8;
const size_t MYSQL_ERRMSG_SIZE = 512;
const size_t PACKET_HEADER_SIZE = 4;
const size_t SSL_REQUEST_PAYLOAD_SIZE = 32;


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
    COM_BINLOG_DUMP = 0x12,
    COM_REGISTER_SLAVE = 0x15,
    COM_RESET_CONNECTION = 0x1f,
    COM_DAEMON = 0x1d
};

// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
enum ColumnDefinitionFlags
{
    UNSIGNED_FLAG = 32,
    BINARY_FLAG = 128
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
private:
    ReadBuffer & in;
    uint8_t & sequence_id;
    const size_t max_packet_size = MAX_PACKET_LENGTH;

    bool has_read_header = false;

    // Size of packet which is being read now.
    size_t payload_length = 0;

    // Offset in packet payload.
    size_t offset = 0;

protected:
    bool nextImpl() override;

public:
    PacketPayloadReadBuffer(ReadBuffer & in_, uint8_t & sequence_id_);
};


/** Writing packets.
 *  https://dev.mysql.com/doc/internals/en/mysql-packet.html
 */
class PacketPayloadWriteBuffer : public WriteBuffer
{
public:
    PacketPayloadWriteBuffer(WriteBuffer & out_, size_t payload_length_, uint8_t & sequence_id_)
        : WriteBuffer(out_.position(), 0), out(out_), sequence_id(sequence_id_), total_left(payload_length_)
    {
        startNewPacket();
        setWorkingBuffer();
        pos = out.position();
    }

    bool remainingPayloadSize()
    {
        return total_left;
    }

private:
    WriteBuffer & out;
    uint8_t & sequence_id;

    size_t total_left = 0;
    size_t payload_length = 0;
    size_t bytes_written = 0;
    bool eof = false;

    void startNewPacket()
    {
        payload_length = std::min(total_left, MAX_PACKET_LENGTH);
        bytes_written = 0;
        total_left -= payload_length;

        out.write(reinterpret_cast<char *>(&payload_length), 3);
        out.write(sequence_id++);
        bytes += 4;
    }

    /// Sets working buffer to the rest of current packet payload.
    void setWorkingBuffer()
    {
        out.nextIfAtEnd();
        working_buffer = WriteBuffer::Buffer(out.position(), out.position() + std::min(payload_length - bytes_written, out.available()));

        if (payload_length - bytes_written == 0)
        {
            /// Finished writing packet. Due to an implementation of WriteBuffer, working_buffer cannot be empty. Further write attempts will throw Exception.
            eof = true;
            working_buffer.resize(1);
        }
    }

protected:
    void nextImpl() override
    {
        const int written = pos - working_buffer.begin();
        if (eof)
            throw Exception("Cannot write after end of buffer.", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);

        out.position() += written;
        bytes_written += written;

        /// Packets of size greater than MAX_PACKET_LENGTH are split into few packets of size MAX_PACKET_LENGTH and las packet of size < MAX_PACKET_LENGTH.
        if (bytes_written == payload_length && (total_left > 0 || payload_length == MAX_PACKET_LENGTH))
            startNewPacket();

        setWorkingBuffer();
    }
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
    PacketSender(ReadBuffer & in_, WriteBuffer & out_, uint8_t & sequence_id_)
        : sequence_id(sequence_id_)
        , in(&in_)
        , out(&out_)
    {
    }

    /// For writing.
    PacketSender(WriteBuffer & out_, uint8_t & sequence_id_)
        : sequence_id(sequence_id_)
        , in(nullptr)
        , out(&out_)
    {
    }

    void receivePacket(IMySQLReadPacket & packet)
    {
        packet.readPayload(*in, sequence_id);
    }

    bool tryReceivePacket(IMySQLReadPacket & packet, UInt64 millisecond = 0)
    {
        if (millisecond != 0)
        {
            ReadBufferFromPocoSocket * socket_in = typeid_cast<ReadBufferFromPocoSocket *>(in);

            if (!socket_in)
                throw Exception("LOGICAL ERROR: Attempt to pull the duration in a non socket stream", ErrorCodes::LOGICAL_ERROR);

            if (!socket_in->poll(millisecond * 1000))
                return false;
        }

        packet.readPayload(*in, sequence_id);
        return true;
    }

    template<class T>
    void sendPacket(const T & packet, bool flush = false)
    {
        static_assert(std::is_base_of<IMySQLWritePacket, T>());
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

inline void readLengthEncodedString(String & s, ReadBuffer & buffer)
{
    uint64_t len = readLengthEncodedNumber(buffer);
    s.resize(len);
    buffer.readStrict(reinterpret_cast<char *>(s.data()), len);
}

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

ColumnDefinitionPacket getColumnDefinition(const String & column_name, const TypeIndex index);


namespace ProtocolText
{

class ResultsetRow : public IMySQLWritePacket
{
    const Columns & columns;
    int row_num;
    size_t payload_size = 0;
    std::vector<String> serialized;
public:
    ResultsetRow(const DataTypes & data_types, const Columns & columns_, int row_num_)
        : columns(columns_)
        , row_num(row_num_)
    {
        for (size_t i = 0; i < columns.size(); i++)
        {
            if (columns[i]->isNullAt(row_num))
            {
                payload_size += 1;
                serialized.emplace_back("\xfb");
            }
            else
            {
                WriteBufferFromOwnString ostr;
                data_types[i]->serializeAsText(*columns[i], row_num, ostr, FormatSettings());
                payload_size += getLengthEncodedStringSize(ostr.str());
                serialized.push_back(std::move(ostr.str()));
            }
        }
    }
protected:
    size_t getPayloadSize() const override
    {
        return payload_size;
    }

    void writePayloadImpl(WriteBuffer & buffer) const override
    {
        for (size_t i = 0; i < columns.size(); i++)
        {
            if (columns[i]->isNullAt(row_num))
                buffer.write(serialized[i].data(), 1);
            else
                writeLengthEncodedString(serialized[i], buffer);
        }
    }
};

}

namespace Authentication
{

class IPlugin
{
public:
    virtual String getName() = 0;

    virtual String getAuthPluginData() = 0;

    virtual void authenticate(const String & user_name, std::optional<String> auth_response, Context & context, std::shared_ptr<PacketSender> packet_sender, bool is_secure_connection,
                              const Poco::Net::SocketAddress & address) = 0;

    virtual ~IPlugin() = default;
};

/// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
class Native41 : public IPlugin
{
public:
    Native41()
    {
        scramble.resize(SCRAMBLE_LENGTH + 1, 0);
        Poco::RandomInputStream generator;

       /** Generate a random string using ASCII characters but avoid separator character,
         * produce pseudo random numbers between with about 7 bit worth of entropty between 1-127.
         * https://github.com/mysql/mysql-server/blob/8.0/mysys/crypt_genhash_impl.cc#L427
         */
        for (size_t i = 0; i < SCRAMBLE_LENGTH; ++i)
        {
            generator >> scramble[i];
            scramble[i] &= 0x7f;
            if (scramble[i] == '\0' || scramble[i] == '$')
                scramble[i] = scramble[i] + 1;
        }
    }

    Native41(const String & password, const String & auth_plugin_data)
    {
        /// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
        /// SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
        Poco::SHA1Engine engine1;
        engine1.update(password.data());
        const Poco::SHA1Engine::Digest & password_sha1 = engine1.digest();

        Poco::SHA1Engine engine2;
        engine2.update(password_sha1.data(), password_sha1.size());
        const Poco::SHA1Engine::Digest & password_double_sha1 = engine2.digest();

        Poco::SHA1Engine engine3;
        engine3.update(auth_plugin_data.data(), auth_plugin_data.size());
        engine3.update(password_double_sha1.data(), password_double_sha1.size());
        const Poco::SHA1Engine::Digest & digest = engine3.digest();

        scramble.resize(SCRAMBLE_LENGTH);
        for (size_t i = 0; i < SCRAMBLE_LENGTH; i++)
        {
            scramble[i] = static_cast<unsigned char>(password_sha1[i] ^ digest[i]);
        }
    }

    String getName() override
    {
        return "mysql_native_password";
    }

    String getAuthPluginData() override
    {
        return scramble;
    }

    void authenticate(
        const String & user_name,
        std::optional<String> auth_response,
        Context & context,
        std::shared_ptr<PacketSender> packet_sender,
        bool /* is_secure_connection */,
        const Poco::Net::SocketAddress & address) override
    {
        if (!auth_response)
        {
            packet_sender->sendPacket(AuthSwitchRequest(getName(), scramble), true);
            AuthSwitchResponse response;
            packet_sender->receivePacket(response);
            auth_response = response.value;
        }

        if (auth_response->empty())
        {
            context.setUser(user_name, "", address);
            return;
        }

        if (auth_response->size() != Poco::SHA1Engine::DIGEST_SIZE)
            throw Exception("Wrong size of auth response. Expected: " + std::to_string(Poco::SHA1Engine::DIGEST_SIZE) + " bytes, received: " + std::to_string(auth_response->size()) + " bytes.",
                            ErrorCodes::UNKNOWN_EXCEPTION);

        auto user = context.getAccessControlManager().read<User>(user_name);

        Poco::SHA1Engine::Digest double_sha1_value = user->authentication.getPasswordDoubleSHA1();
        assert(double_sha1_value.size() == Poco::SHA1Engine::DIGEST_SIZE);

        Poco::SHA1Engine engine;
        engine.update(scramble.data(), SCRAMBLE_LENGTH);
        engine.update(double_sha1_value.data(), double_sha1_value.size());

        String password_sha1(Poco::SHA1Engine::DIGEST_SIZE, 0x0);
        const Poco::SHA1Engine::Digest & digest = engine.digest();
        for (size_t i = 0; i < password_sha1.size(); i++)
        {
            password_sha1[i] = digest[i] ^ static_cast<unsigned char>((*auth_response)[i]);
        }
        context.setUser(user_name, password_sha1, address);
    }
private:
    String scramble;
};

#if USE_SSL
/// Caching SHA2 plugin is not used because it would be possible to authenticate knowing hash from users.xml.
/// https://dev.mysql.com/doc/internals/en/sha256.html
class Sha256Password : public IPlugin
{
public:
    Sha256Password(RSA & public_key_, RSA & private_key_, Poco::Logger * log_)
        : public_key(public_key_)
        , private_key(private_key_)
        , log(log_)
    {
        /** Native authentication sent 20 bytes + '\0' character = 21 bytes.
         *  This plugin must do the same to stay consistent with historical behavior if it is set to operate as a default plugin. [1]
         *  https://github.com/mysql/mysql-server/blob/8.0/sql/auth/sql_authentication.cc#L3994
         */
        scramble.resize(SCRAMBLE_LENGTH + 1, 0);
        Poco::RandomInputStream generator;

        for (size_t i = 0; i < SCRAMBLE_LENGTH; ++i)
        {
            generator >> scramble[i];
            scramble[i] &= 0x7f;
            if (scramble[i] == '\0' || scramble[i] == '$')
                scramble[i] = scramble[i] + 1;
        }
    }

    String getName() override
    {
        return "sha256_password";
    }

    String getAuthPluginData() override
    {
        return scramble;
    }

    void authenticate(
        const String & user_name,
        std::optional<String> auth_response,
        Context & context,
        std::shared_ptr<PacketSender> packet_sender,
        bool is_secure_connection,
        const Poco::Net::SocketAddress & address) override
    {
        if (!auth_response)
        {
            packet_sender->sendPacket(AuthSwitchRequest(getName(), scramble), true);

            if (packet_sender->in->eof())
                throw Exception("Client doesn't support authentication method " + getName() + " used by ClickHouse. Specifying user password using 'password_double_sha1_hex' may fix the problem.",
                    ErrorCodes::MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES);

            AuthSwitchResponse response;
            packet_sender->receivePacket(response);
            auth_response = response.value;
            LOG_TRACE(log, "Authentication method mismatch.");
        }
        else
        {
            LOG_TRACE(log, "Authentication method match.");
        }

        bool sent_public_key = false;
        if (auth_response == "\1")
        {
            LOG_TRACE(log, "Client requests public key.");
            BIO * mem = BIO_new(BIO_s_mem());
            SCOPE_EXIT(BIO_free(mem));
            if (PEM_write_bio_RSA_PUBKEY(mem, &public_key) != 1)
            {
                throw Exception("Failed to write public key to memory. Error: " + getOpenSSLErrors(), ErrorCodes::OPENSSL_ERROR);
            }
            char * pem_buf = nullptr;
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wold-style-cast"
            long pem_size = BIO_get_mem_data(mem, &pem_buf);
#    pragma GCC diagnostic pop
            String pem(pem_buf, pem_size);

            LOG_TRACE(log, "Key: {}", pem);

            AuthMoreData data(pem);
            packet_sender->sendPacket(data, true);
            sent_public_key = true;

            AuthSwitchResponse response;
            packet_sender->receivePacket(response);
            auth_response = response.value;
        }
        else
        {
            LOG_TRACE(log, "Client didn't request public key.");
        }

        String password;

        /** Decrypt password, if it's not empty.
         *  The original intention was that the password is a string[NUL] but this never got enforced properly so now we have to accept that
         *  an empty packet is a blank password, thus the check for auth_response.empty() has to be made too.
         *  https://github.com/mysql/mysql-server/blob/8.0/sql/auth/sql_authentication.cc#L4017
         */
        if (!is_secure_connection && !auth_response->empty() && auth_response != String("\0", 1))
        {
            LOG_TRACE(log, "Received nonempty password.");
            auto ciphertext = reinterpret_cast<unsigned char *>(auth_response->data());

            unsigned char plaintext[RSA_size(&private_key)];
            int plaintext_size = RSA_private_decrypt(auth_response->size(), ciphertext, plaintext, &private_key, RSA_PKCS1_OAEP_PADDING);
            if (plaintext_size == -1)
            {
                if (!sent_public_key)
                    LOG_WARNING(log, "Client could have encrypted password with different public key since it didn't request it from server.");
                throw Exception("Failed to decrypt auth data. Error: " + getOpenSSLErrors(), ErrorCodes::OPENSSL_ERROR);
            }

            password.resize(plaintext_size);
            for (int i = 0; i < plaintext_size; i++)
            {
                password[i] = plaintext[i] ^ static_cast<unsigned char>(scramble[i % scramble.size()]);
            }
        }
        else if (is_secure_connection)
        {
            password = *auth_response;
        }
        else
        {
            LOG_TRACE(log, "Received empty password");
        }

        if (!password.empty() && password.back() == 0)
        {
            password.pop_back();
        }

        context.setUser(user_name, password, address);
    }

private:
    RSA & public_key;
    RSA & private_key;
    Poco::Logger * log;
    String scramble;
};
#endif

}

namespace Replication
{
    /// https://dev.mysql.com/doc/internals/en/com-register-slave.html
    class RegisterSlave : public IMySQLWritePacket
    {
    public:
        UInt8 header = COM_REGISTER_SLAVE;
        UInt32 server_id;
        String slaves_hostname;
        String slaves_users;
        String slaves_password;
        size_t slaves_mysql_port;
        UInt32 replication_rank;
        UInt32 master_id;

        RegisterSlave(UInt32 server_id_) : server_id(server_id_), slaves_mysql_port(0x00), replication_rank(0x00), master_id(0x00) { }

        void writePayloadImpl(WriteBuffer & buffer) const override
        {
            buffer.write(header);
            buffer.write(reinterpret_cast<const char *>(&server_id), 4);
            writeLengthEncodedString(slaves_hostname, buffer);
            writeLengthEncodedString(slaves_users, buffer);
            writeLengthEncodedString(slaves_password, buffer);
            buffer.write(reinterpret_cast<const char *>(&slaves_mysql_port), 2);
            buffer.write(reinterpret_cast<const char *>(&replication_rank), 4);
            buffer.write(reinterpret_cast<const char *>(&master_id), 4);
        }

    protected:
        size_t getPayloadSize() const override
        {
            return 1 + 4 + getLengthEncodedStringSize(slaves_hostname) + getLengthEncodedStringSize(slaves_users)
                + getLengthEncodedStringSize(slaves_password) + 2 + 4 + 4;
        }
    };

    /// https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
    class BinlogDump : public IMySQLWritePacket
    {
    public:
        UInt8 header = COM_BINLOG_DUMP;
        UInt32 binlog_pos;
        UInt16 flags;
        UInt32 server_id;
        String binlog_file_name;

        BinlogDump(UInt32 binlog_pos_, String binlog_file_name_, UInt32 server_id_)
            : binlog_pos(binlog_pos_), flags(0x00), server_id(server_id_), binlog_file_name(std::move(binlog_file_name_))
        {
        }

        void writePayloadImpl(WriteBuffer & buffer) const override
        {
            buffer.write(header);
            buffer.write(reinterpret_cast<const char *>(&binlog_pos), 4);
            buffer.write(reinterpret_cast<const char *>(&flags), 2);
            buffer.write(reinterpret_cast<const char *>(&server_id), 4);
            buffer.write(binlog_file_name.data(), binlog_file_name.length());
            buffer.write(0x00);
        }

    protected:
        size_t getPayloadSize() const override { return 1 + 4 + 2 + 4 + binlog_file_name.size() + 1; }
    };
}
}

}
