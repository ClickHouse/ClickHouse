#include <string>
#include <gtest/gtest.h>

#include <Core/MySQLProtocol.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;
using namespace MySQLProtocol;

TEST(MySQLProtocol, Handshake)
{
    UInt32 server_capability_flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF;

    std::string s;
    WriteBufferFromString out(s);
    Handshake server_handshake(server_capability_flags, 0, "ClickHouse", "mysql_native_password", "aaaaaaaaaaaaaaaaaaaaa");
    server_handshake.writePayloadImpl(out);

    ReadBufferFromString in(s);
    Handshake client_handshake;
    client_handshake.readPayloadImpl(in);

    EXPECT_EQ(server_handshake.capability_flags, client_handshake.capability_flags);
    EXPECT_EQ(server_handshake.status_flags, client_handshake.status_flags);
    EXPECT_EQ(server_handshake.server_version, client_handshake.server_version);
    EXPECT_EQ(server_handshake.protocol_version, client_handshake.protocol_version);
    EXPECT_EQ(server_handshake.auth_plugin_data.substr(0, 20), client_handshake.auth_plugin_data);
    EXPECT_EQ(server_handshake.auth_plugin_name, client_handshake.auth_plugin_name);
}
