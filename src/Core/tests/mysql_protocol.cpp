#include <string>

#include <Core/MySQLProtocol.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>


int main(int, char **)
{
    using namespace DB;
    using namespace MySQLProtocol;
    using namespace MySQLProtocol::Authentication;

    /*
    UInt16 port = 9001;
    String host = "127.0.0.1", user = "default", password = "123";
    MySQLClient client(host, port, user, password, "");
    if (!client.connect())
    {
        std::cerr << "Connect Error: " << client.error() << std::endl;
    }
     */
    String user = "default";
    String password = "123";
    String database = "";

    UInt8 charset_utf8 = 33;
    UInt32 max_packet_size = MySQLProtocol::MAX_PACKET_LENGTH;
    String mysql_native_password = "mysql_native_password";

    UInt32 server_capability_flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF;

    UInt32 client_capability_flags = CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | CLIENT_SECURE_CONNECTION;

    /// Handshake packet
    {
        /// 1. Greeting:
        /// 1.1 Server writes greeting to client
        std::string s0;
        WriteBufferFromString out0(s0);

        Handshake server_handshake(server_capability_flags, -1, "ClickHouse", "mysql_native_password", "aaaaaaaaaaaaaaaaaaaaa");
        server_handshake.writePayloadImpl(out0);

        /// 1.2 Client reads the greeting
        ReadBufferFromString in0(s0);
        Handshake client_handshake;
        client_handshake.readPayloadImpl(in0);

        /// Check packet
        ASSERT(server_handshake.capability_flags == client_handshake.capability_flags);
        ASSERT(server_handshake.status_flags == client_handshake.status_flags);
        ASSERT(server_handshake.server_version == client_handshake.server_version);
        ASSERT(server_handshake.protocol_version == client_handshake.protocol_version);
        ASSERT(server_handshake.auth_plugin_data.substr(0, 20) == client_handshake.auth_plugin_data);
        ASSERT(server_handshake.auth_plugin_name == client_handshake.auth_plugin_name);

        /// 2. Greeting Response:
        std::string s1;
        WriteBufferFromString out1(s1);

        /// 2.1 Client writes to server
        Native41 native41(password, client_handshake.auth_plugin_data);
        String auth_plugin_data = native41.getAuthPluginData();
        HandshakeResponse client_handshake_response(
            client_capability_flags, max_packet_size, charset_utf8, user, database, auth_plugin_data, mysql_native_password);
        client_handshake_response.writePayloadImpl(out1);

        /// 2.2 Server reads the response
        ReadBufferFromString in1(s1);
        HandshakeResponse server_handshake_response;
        server_handshake_response.readPayloadImpl(in1);

        /// Check
        ASSERT(server_handshake_response.capability_flags == client_handshake_response.capability_flags);
        ASSERT(server_handshake_response.character_set == client_handshake_response.character_set);
        ASSERT(server_handshake_response.username == client_handshake_response.username);
        ASSERT(server_handshake_response.database == client_handshake_response.database);
        ASSERT(server_handshake_response.auth_response == client_handshake_response.auth_response);
        ASSERT(server_handshake_response.auth_plugin_name == client_handshake_response.auth_plugin_name);
    }

    /// OK Packet
    {
        // 1. Server writes packet
        std::string s0;
        WriteBufferFromString out0(s0);
        OK_Packet server(0x00, server_capability_flags, 0, 0, 0, "", "");
        server.writePayloadImpl(out0);

        // 2. Client reads packet
        ReadBufferFromString in1(s0);
        PacketResponse client(server_capability_flags);
        client.readPayloadImpl(in1);

        // Check
        ASSERT(client.getType() == PACKET_OK);
        ASSERT(client.ok.header == server.header);
        ASSERT(client.ok.status_flags == server.status_flags);
        ASSERT(client.ok.capabilities == server.capabilities);
    }

    /// ERR Packet
    {
        // 1. Server writes packet
        std::string s0;
        WriteBufferFromString out0(s0);
        ERR_Packet server(123, "12345", "This is the error message");
        server.writePayloadImpl(out0);

        // 2. Client reads packet
        ReadBufferFromString in1(s0);
        PacketResponse client(server_capability_flags);
        client.readPayloadImpl(in1);

        // Check
        ASSERT(client.getType() == PACKET_ERR);
        ASSERT(client.err.header == server.header);
        ASSERT(client.err.error_code == server.error_code);
        ASSERT(client.err.sql_state == server.sql_state);
        ASSERT(client.err.error_message == server.error_message);
    }

    /// EOF Packet
    {
        // 1. Server writes packet
        std::string s0;
        WriteBufferFromString out0(s0);
        EOF_Packet server(1, 1);
        server.writePayloadImpl(out0);

        // 2. Client reads packet
        ReadBufferFromString in1(s0);
        PacketResponse client(server_capability_flags);
        client.readPayloadImpl(in1);

        // Check
        ASSERT(client.getType() == PACKET_EOF);
        ASSERT(client.eof.header == server.header);
        ASSERT(client.eof.warnings == server.warnings);
        ASSERT(client.eof.status_flags == server.status_flags);
    }

    return 0;
}
