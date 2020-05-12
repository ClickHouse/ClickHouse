#include "MySQLClient.h"
#include <Core/MySQLReplication.h>

namespace DB
{
using namespace Replication;
using namespace Authentication;

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

MySQLClient::MySQLClient(const String & host_, UInt16 port_, const String & user_, const String & password_)
    : host(host_), port(port_), user(user_), password(std::move(password_))
{
    client_capability_flags = CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | CLIENT_SECURE_CONNECTION;
}

void MySQLClient::connect()
{
    if (connected)
    {
        disconnect();
    }

    const Poco::Timespan connection_timeout(10 * 1e9);
    const Poco::Timespan receive_timeout(5 * 1e9);
    const Poco::Timespan send_timeout(5 * 1e9);

    socket = std::make_unique<Poco::Net::StreamSocket>();
    address = DNSResolver::instance().resolveAddress(host, port);
    socket->connect(*address, connection_timeout);
    socket->setReceiveTimeout(receive_timeout);
    socket->setSendTimeout(send_timeout);
    socket->setNoDelay(true);
    connected = true;

    in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
    out = std::make_shared<WriteBufferFromPocoSocket>(*socket);
    packet_sender = std::make_shared<PacketSender>(*in, *out, seq);
    handshake();
}

void MySQLClient::disconnect()
{
    in = nullptr;
    out = nullptr;
    if (socket)
        socket->close();
    socket = nullptr;
    connected = false;
}

/// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
void MySQLClient::handshake()
{
    Handshake handshake;
    packet_sender->receivePacket(handshake);
    if (handshake.auth_plugin_name != mysql_native_password)
    {
        throw MySQLClientError(
            "Only support " + mysql_native_password + " auth plugin name, but got " + handshake.auth_plugin_name,
            ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    }

    Native41 native41(password, handshake.auth_plugin_data);
    String auth_plugin_data = native41.getAuthPluginData();

    HandshakeResponse handshake_response(
        client_capability_flags, max_packet_size, charset_utf8, user, "", auth_plugin_data, mysql_native_password);
    packet_sender->sendPacket<HandshakeResponse>(handshake_response, true);

    PacketResponse packet_response(client_capability_flags);
    packet_sender->receivePacket(packet_response);
    packet_sender->resetSequenceId();
    if (packet_response.getType() == PACKET_ERR)
    {
        throw MySQLClientError(packet_response.err.error_message, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    }
}

void MySQLClient::writeCommand(char command, String query)
{
    WriteCommand write_command(command, query);
    packet_sender->sendPacket<WriteCommand>(write_command, true);

    PacketResponse packet_response(client_capability_flags);
    packet_sender->receivePacket(packet_response);
    switch (packet_response.getType())
    {
        case PACKET_ERR:
            throw MySQLClientError(packet_response.err.error_message, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
        case PACKET_OK:
            break;
        default:
            break;
    }
    packet_sender->resetSequenceId();
}

void MySQLClient::registerSlaveOnMaster(UInt32 slave_id)
{
    RegisterSlave register_slave(slave_id);
    packet_sender->sendPacket<RegisterSlave>(register_slave, true);

    PacketResponse packet_response(client_capability_flags);
    packet_sender->receivePacket(packet_response);
    packet_sender->resetSequenceId();
    if (packet_response.getType() == PACKET_ERR)
        throw MySQLClientError(packet_response.err.error_message, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
}

void MySQLClient::ping()
{
    writeCommand(Command::COM_PING, "");
}

void MySQLClient::startBinlogDump(UInt32 slave_id, String replicate_db, String binlog_file_name, UInt64 binlog_pos)
{
    /// Set binlog checksum to CRC32.
    String checksum = "CRC32";
    writeCommand(Command::COM_QUERY, "SET @master_binlog_checksum = '" + checksum + "'");

    /// Set heartbeat 30s.
    UInt64 period_ns = (30 * 1e9);
    writeCommand(Command::COM_QUERY, "SET @master_heartbeat_period = " + std::to_string(period_ns));

    /// Set replication filter to master
    /// This requires MySQL version >=5.6, so results are not checked here.
    writeCommand(Command::COM_QUERY, "CHANGE REPLICATION FILTER REPLICATE_DO_DB = (" + replicate_db + ")");

    // Register slave.
    registerSlaveOnMaster(slave_id);

    binlog_pos = binlog_pos < 4 ? 4 : binlog_pos;
    BinlogDump binlog_dump(binlog_pos, binlog_file_name, slave_id);
    packet_sender->sendPacket<BinlogDump>(binlog_dump, true);
}

BinlogEventPtr MySQLClient::readOneBinlogEvent()
{
    while (true)
    {
        packet_sender->receivePacket(replication);
        return replication.readOneEvent();
    }
}

}
