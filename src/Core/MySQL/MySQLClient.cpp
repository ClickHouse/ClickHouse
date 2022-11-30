#include "MySQLClient.h"

#include <Core/MySQL/Authentication.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsProtocolText.h>
#include <Core/MySQL/PacketsReplication.h>
#include <Core/MySQL/MySQLReplication.h>
#include <Common/DNSResolver.h>
#include <Poco/String.h>


namespace DB
{
using namespace Generic;
using namespace Replication;
using namespace ProtocolText;
using namespace Authentication;
using namespace ConnectionPhase;

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

MySQLClient::MySQLClient(const String & host_, UInt16 port_, const String & user_, const String & password_)
    : host(host_), port(port_), user(user_), password(password_),
      client_capabilities(CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | CLIENT_SECURE_CONNECTION)
{
}

MySQLClient::MySQLClient(MySQLClient && other) noexcept
    : host(std::move(other.host)), port(other.port), user(std::move(other.user)), password(std::move(other.password))
    , client_capabilities(other.client_capabilities)
{
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
    packet_endpoint = MySQLProtocol::PacketEndpoint::create(*in, *out, sequence_id);

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
    sequence_id = 0;
}

/// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
void MySQLClient::handshake()
{
    Handshake handshake;
    packet_endpoint->receivePacket(handshake);
    if (handshake.auth_plugin_name != mysql_native_password)
    {
        throw Exception(
            "Only support " + mysql_native_password + " auth plugin name, but got " + handshake.auth_plugin_name,
            ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    }

    Native41 native41(password, handshake.auth_plugin_data);
    String auth_plugin_data = native41.getAuthPluginData();

    HandshakeResponse handshake_response(
        client_capabilities, MAX_PACKET_LENGTH, charset_utf8, user, "", auth_plugin_data, mysql_native_password);
    packet_endpoint->sendPacket<HandshakeResponse>(handshake_response, true);

    ResponsePacket packet_response(client_capabilities, true);
    packet_endpoint->receivePacket(packet_response);
    packet_endpoint->resetSequenceId();

    if (packet_response.getType() == PACKET_ERR)
        throw Exception(packet_response.err.error_message, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    else if (packet_response.getType() == PACKET_AUTH_SWITCH)
        throw Exception("Access denied for user " + user, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
}

void MySQLClient::writeCommand(char command, String query)
{
    WriteCommand write_command(command, query);
    packet_endpoint->sendPacket<WriteCommand>(write_command, true);

    ResponsePacket packet_response(client_capabilities);
    packet_endpoint->receivePacket(packet_response);
    switch (packet_response.getType())
    {
        case PACKET_ERR:
            throw Exception(packet_response.err.error_message, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
        case PACKET_OK:
            break;
        default:
            break;
    }
    packet_endpoint->resetSequenceId();
}

void MySQLClient::registerSlaveOnMaster(UInt32 slave_id)
{
    RegisterSlave register_slave(slave_id);
    packet_endpoint->sendPacket<RegisterSlave>(register_slave, true);

    ResponsePacket packet_response(client_capabilities);
    packet_endpoint->receivePacket(packet_response);
    packet_endpoint->resetSequenceId();
    if (packet_response.getType() == PACKET_ERR)
        throw Exception(packet_response.err.error_message, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
}

void MySQLClient::ping()
{
    writeCommand(Command::COM_PING, "");
}

void MySQLClient::setBinlogChecksum(const String & binlog_checksum)
{
    replication.setChecksumSignatureLength(Poco::toUpper(binlog_checksum) == "NONE" ? 0 : 4);
}

void MySQLClient::startBinlogDumpGTID(UInt32 slave_id, String replicate_db, std::unordered_set<String> replicate_tables, String gtid_str, const String & binlog_checksum)
{
    /// Maybe CRC32 or NONE. mysqlbinlog.cc use NONE, see its below comments:
    /// Make a notice to the server that this client is checksum-aware.
    /// It does not need the first fake Rotate necessary checksummed.
    writeCommand(Command::COM_QUERY, "SET @master_binlog_checksum = 'CRC32'");

    setBinlogChecksum(binlog_checksum);

    /// Set heartbeat 1s.
    UInt64 period_ns = (1 * 1e9);
    writeCommand(Command::COM_QUERY, "SET @master_heartbeat_period = " + std::to_string(period_ns));

    // Register slave.
    registerSlaveOnMaster(slave_id);

    /// Set GTID Sets.
    GTIDSets gtid_sets;
    gtid_sets.parse(gtid_str);
    replication.setGTIDSets(gtid_sets);

    /// Set Filter rule to replication.
    replication.setReplicateDatabase(replicate_db);
    replication.setReplicateTables(replicate_tables);

    BinlogDumpGTID binlog_dump(slave_id, gtid_sets.toPayload());
    packet_endpoint->sendPacket<BinlogDumpGTID>(binlog_dump, true);
}

BinlogEventPtr MySQLClient::readOneBinlogEvent(UInt64 milliseconds)
{
    if (packet_endpoint->tryReceivePacket(replication, milliseconds))
        return replication.readOneEvent();

    return {};
}

}
