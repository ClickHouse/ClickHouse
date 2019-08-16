#include "MySQLHandler.h"

#include <limits>
#include <ext/scope_guard.h>
#include <openssl/rsa.h>
#include <Columns/ColumnVector.h>
#include <Common/config_version.h>
#include <Common/NetException.h>
#include <Common/OpenSSLHelpers.h>
#include <Core/MySQLProtocol.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/copyData.h>
#include <Interpreters/executeQuery.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Poco/Crypto/CipherFactory.h>
#include <Poco/Crypto/RSAKey.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SSLManager.h>
#include <Storages/IStorage.h>


namespace DB
{

using namespace MySQLProtocol;


using Poco::Net::SecureStreamSocket;
using Poco::Net::SSLManager;


namespace ErrorCodes
{
    extern const int MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES;
    extern const int OPENSSL_ERROR;
}

MySQLHandler::MySQLHandler(IServer & server_, const Poco::Net::StreamSocket & socket_, RSA & public_key_, RSA & private_key_, bool ssl_enabled, size_t connection_id_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , log(&Poco::Logger::get("MySQLHandler"))
    , connection_context(server.context())
    , connection_id(connection_id_)
    , public_key(public_key_)
    , private_key(private_key_)
{
    server_capability_flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF;
    if (ssl_enabled)
        server_capability_flags |= CLIENT_SSL;
}

void MySQLHandler::run()
{
    connection_context.makeSessionContext();
    connection_context.setDefaultFormat("MySQLWire");

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
    packet_sender = std::make_shared<PacketSender>(*in, *out, connection_context.mysql.sequence_id);

    try
    {
        String scramble = generateScramble();

        Handshake handshake(server_capability_flags, connection_id, VERSION_STRING + String("-") + VERSION_NAME, Authentication::Native, scramble + '\0');
        packet_sender->sendPacket<Handshake>(handshake, true);

        LOG_TRACE(log, "Sent handshake");

        HandshakeResponse handshake_response;
        finishHandshake(handshake_response);
        connection_context.mysql.client_capabilities = handshake_response.capability_flags;
        if (handshake_response.max_packet_size)
            connection_context.mysql.max_packet_size = handshake_response.max_packet_size;
        if (!connection_context.mysql.max_packet_size)
            connection_context.mysql.max_packet_size = MAX_PACKET_LENGTH;

/*        LOG_TRACE(log, "Capabilities: " << handshake_response.capability_flags
                                        << "\nmax_packet_size: "
                                        << handshake_response.max_packet_size
                                        << "\ncharacter_set: "
                                        << handshake_response.character_set
                                        << "\nuser: "
                                        << handshake_response.username
                                        << "\nauth_response length: "
                                        << handshake_response.auth_response.length()
                                        << "\nauth_response: "
                                        << handshake_response.auth_response
                                        << "\ndatabase: "
                                        << handshake_response.database
                                        << "\nauth_plugin_name: "
                                        << handshake_response.auth_plugin_name);*/

        client_capability_flags = handshake_response.capability_flags;
        if (!(client_capability_flags & CLIENT_PROTOCOL_41))
            throw Exception("Required capability: CLIENT_PROTOCOL_41.", ErrorCodes::MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES);
        if (!(client_capability_flags & CLIENT_PLUGIN_AUTH))
            throw Exception("Required capability: CLIENT_PLUGIN_AUTH.", ErrorCodes::MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES);

        authenticate(handshake_response, scramble);
        OK_Packet ok_packet(0, handshake_response.capability_flags, 0, 0, 0);
        packet_sender->sendPacket(ok_packet, true);

        while (true)
        {
            packet_sender->resetSequenceId();
            PacketPayloadReadBuffer payload = packet_sender->getPayload();

            char command = 0;
            payload.readStrict(command);

            // For commands which are executed without MemoryTracker.
            LimitReadBuffer limited_payload(payload, 10000, true, "too long MySQL packet.");

            LOG_DEBUG(log, "Received command: " << static_cast<int>(static_cast<unsigned char>(command)) << ". Connection id: " << connection_id << ".");
            try
            {
                switch (command)
                {
                    case COM_QUIT:
                        return;
                    case COM_INIT_DB:
                        comInitDB(limited_payload);
                        break;
                    case COM_QUERY:
                        comQuery(payload);
                        break;
                    case COM_FIELD_LIST:
                        comFieldList(limited_payload);
                        break;
                    case COM_PING:
                        comPing();
                        break;
                    default:
                        throw Exception(Poco::format("Command %d is not implemented.", command), ErrorCodes::NOT_IMPLEMENTED);
                }
            }
            catch (const NetException & exc)
            {
                log->log(exc);
                throw;
            }
            catch (const Exception & exc)
            {
                log->log(exc);
                packet_sender->sendPacket(ERR_Packet(exc.code(), "00000", exc.message()), true);
            }
        }
    }
    catch (const Poco::Exception & exc)
    {
        log->log(exc);
    }
}

/** Reads 3 bytes, finds out whether it is SSLRequest or HandshakeResponse packet, starts secure connection, if it is SSLRequest.
 *  Reading is performed from socket instead of ReadBuffer to prevent reading part of SSL handshake.
 *  If we read it from socket, it will be impossible to start SSL connection using Poco. Size of SSLRequest packet payload is 32 bytes, thus we can read at most 36 bytes.
 */
void MySQLHandler::finishHandshake(MySQLProtocol::HandshakeResponse & packet)
{
    size_t packet_size = PACKET_HEADER_SIZE + SSL_REQUEST_PAYLOAD_SIZE;

    /// Buffer for SSLRequest or part of HandshakeResponse.
    char buf[packet_size];
    size_t pos = 0;

    /// Reads at least count and at most packet_size bytes.
    auto read_bytes = [this, &buf, &pos, &packet_size](size_t count) -> void {
        while (pos < count)
        {
            int ret = socket().receiveBytes(buf + pos, packet_size - pos);
            if (ret == 0)
            {
                throw Exception("Cannot read all data. Bytes read: " + std::to_string(pos) + ". Bytes expected: 3.", ErrorCodes::CANNOT_READ_ALL_DATA);
            }
            pos += ret;
        }
    };
    read_bytes(3); /// We can find out whether it is SSLRequest of HandshakeResponse by first 3 bytes.

    size_t payload_size = unalignedLoad<uint32_t>(buf) & 0xFFFFFFu;
    LOG_TRACE(log, "payload size: " << payload_size);

    if (payload_size == SSL_REQUEST_PAYLOAD_SIZE)
    {
        read_bytes(packet_size); /// Reading rest SSLRequest.
        SSLRequest ssl_request;
        ReadBufferFromMemory payload(buf, pos);
        payload.ignore(PACKET_HEADER_SIZE);
        ssl_request.readPayload(payload);
        connection_context.mysql.client_capabilities = ssl_request.capability_flags;
        connection_context.mysql.max_packet_size = ssl_request.max_packet_size ? ssl_request.max_packet_size : MAX_PACKET_LENGTH;
        secure_connection = true;
        ss = std::make_shared<SecureStreamSocket>(SecureStreamSocket::attach(socket(), SSLManager::instance().defaultServerContext()));
        in = std::make_shared<ReadBufferFromPocoSocket>(*ss);
        out = std::make_shared<WriteBufferFromPocoSocket>(*ss);
        connection_context.mysql.sequence_id = 2;
        packet_sender = std::make_shared<PacketSender>(*in, *out, connection_context.mysql.sequence_id);
        packet_sender->max_packet_size = connection_context.mysql.max_packet_size;
        packet_sender->receivePacket(packet); /// Reading HandshakeResponse from secure socket.
    }
    else
    {
        /// Reading rest of HandshakeResponse.
        packet_size = PACKET_HEADER_SIZE + payload_size;
        WriteBufferFromOwnString buf_for_handshake_response;
        buf_for_handshake_response.write(buf, pos);
        copyData(*packet_sender->in, buf_for_handshake_response, packet_size - pos);
        ReadBufferFromString payload(buf_for_handshake_response.str());
        payload.ignore(PACKET_HEADER_SIZE);
        packet.readPayload(payload);
        packet_sender->sequence_id++;
    }
}

String MySQLHandler::generateScramble()
{
    String scramble(MySQLProtocol::SCRAMBLE_LENGTH, 0);
    Poco::RandomInputStream generator;
    for (size_t i = 0; i < scramble.size(); i++)
    {
        generator >> scramble[i];
    }
    return scramble;
}

void MySQLHandler::authenticate(const HandshakeResponse & handshake_response, const String & scramble)
{

    String auth_response;
    AuthSwitchResponse response;
    if (handshake_response.auth_plugin_name != Authentication::SHA256)
    {
        /** Native authentication sent 20 bytes + '\0' character = 21 bytes.
         *  This plugin must do the same to stay consistent with historical behavior if it is set to operate as a default plugin.
         *  https://github.com/mysql/mysql-server/blob/8.0/sql/auth/sql_authentication.cc#L3994
         */
        packet_sender->sendPacket(AuthSwitchRequest(Authentication::SHA256, scramble + '\0'), true);
        if (in->eof())
            throw Exception(
                "Client doesn't support authentication method " + String(Authentication::SHA256) + " used by ClickHouse",
                ErrorCodes::MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES);
        packet_sender->receivePacket(response);
        auth_response = response.value;
        LOG_TRACE(log, "Authentication method mismatch.");
    }
    else
    {
        auth_response = handshake_response.auth_response;
        LOG_TRACE(log, "Authentication method match.");
    }

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
        long pem_size = BIO_get_mem_data(mem, &pem_buf);
        String pem(pem_buf, pem_size);

        LOG_TRACE(log, "Key: " << pem);

        AuthMoreData data(pem);
        packet_sender->sendPacket(data, true);
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
    if (!secure_connection && !auth_response.empty() && auth_response != String("\0", 1))
    {
        LOG_TRACE(log, "Received nonempty password");
        auto ciphertext = reinterpret_cast<unsigned char *>(auth_response.data());

        unsigned char plaintext[RSA_size(&private_key)];
        int plaintext_size = RSA_private_decrypt(auth_response.size(), ciphertext, plaintext, &private_key, RSA_PKCS1_OAEP_PADDING);
        if (plaintext_size == -1)
        {
            throw Exception("Failed to decrypt auth data. Error: " + getOpenSSLErrors(), ErrorCodes::OPENSSL_ERROR);
        }

        password.resize(plaintext_size);
        for (int i = 0; i < plaintext_size; ++i)
        {
            password[i] = plaintext[i] ^ static_cast<unsigned char>(scramble[i % scramble.size()]);
        }
    }
    else if (secure_connection)
    {
        password = auth_response;
    }
    else
    {
        LOG_TRACE(log, "Received empty password");
    }

    if (!password.empty() && password.back() == 0)
    {
        password.pop_back();
    }

    try
    {
        connection_context.setUser(handshake_response.username, password, socket().address(), "");
        if (!handshake_response.database.empty()) connection_context.setCurrentDatabase(handshake_response.database);
        connection_context.setCurrentQueryId("");
        LOG_INFO(log, "Authentication for user " << handshake_response.username << " succeeded.");
    }
    catch (const Exception & exc)
    {
        LOG_ERROR(log, "Authentication for user " << handshake_response.username << " failed.");
        packet_sender->sendPacket(ERR_Packet(exc.code(), "00000", exc.message()), true);
        throw;
    }
}

void MySQLHandler::comInitDB(ReadBuffer & payload)
{
    String database;
    readStringUntilEOF(database, payload);
    LOG_DEBUG(log, "Setting current database to " << database);
    connection_context.setCurrentDatabase(database);
    packet_sender->sendPacket(OK_Packet(0, client_capability_flags, 0, 0, 1), true);
}

void MySQLHandler::comFieldList(ReadBuffer & payload)
{
    ComFieldList packet;
    packet.readPayload(payload);
    String database = connection_context.getCurrentDatabase();
    StoragePtr tablePtr = connection_context.getTable(database, packet.table);
    for (const NameAndTypePair & column: tablePtr->getColumns().getAll())
    {
        ColumnDefinition column_definition(
            database, packet.table, packet.table, column.name, column.name, CharacterSet::binary, 100, ColumnType::MYSQL_TYPE_STRING, 0, 0
        );
        packet_sender->sendPacket(column_definition);
    }
    packet_sender->sendPacket(OK_Packet(0xfe, client_capability_flags, 0, 0, 0), true);
}

void MySQLHandler::comPing()
{
    packet_sender->sendPacket(OK_Packet(0x0, client_capability_flags, 0, 0, 0), true);
}

void MySQLHandler::comQuery(ReadBuffer & payload)
{
    bool with_output = false;
    std::function<void(const String &)> set_content_type = [&with_output](const String &) -> void {
        with_output = true;
    };

    const String query("select ''");
    ReadBufferFromString empty_select(query);

    bool should_replace = false;
    // Translate query from MySQL to ClickHouse.
    // This is a temporary workaround until ClickHouse supports the syntax "@@var_name".
    if (std::string(payload.position(), payload.buffer().end()) == "select @@version_comment limit 1")  // MariaDB client starts session with that query
    {
        should_replace = true;
    }

    executeQuery(should_replace ? empty_select : payload, *out, true, connection_context, set_content_type, nullptr);

    if (!with_output)
        packet_sender->sendPacket(OK_Packet(0x00, client_capability_flags, 0, 0, 0), true);
}

}
