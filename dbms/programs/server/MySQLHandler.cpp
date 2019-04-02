#include <DataStreams/copyData.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/executeQuery.h>
#include <Storages/IStorage.h>
#include <Core/MySQLProtocol.h>
#include <Core/NamesAndTypes.h>
#include <Columns/ColumnVector.h>
#include <Common/config_version.h>
#include <Common/NetException.h>
#include "MySQLHandler.h"
#include <limits>


namespace DB
{
using namespace MySQLProtocol;

namespace ErrorCodes
{
    extern const int MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES;
}

uint32_t MySQLHandler::last_connection_id = 0;


void MySQLHandler::run()
{
    connection_context = server.context();

    {
        auto in = std::make_shared<ReadBufferFromPocoSocket>(socket());
        auto out = std::make_shared<WriteBufferFromPocoSocket>(socket());
        packet_sender = PacketSender(in, out);
    }

    try
    {
        Handshake handshake(connection_id, VERSION_STRING);
        packet_sender.sendPacket<Handshake>(handshake, true);

        LOG_TRACE(log, "Sent handshake");

        auto handshake_response = packet_sender.receivePacket<HandshakeResponse>();

        LOG_DEBUG(log, "Capabilities: " << handshake_response.capability_flags
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
                                        << handshake_response.auth_plugin_name);

        capabilities = handshake_response.capability_flags;
        if (!(capabilities & CLIENT_PROTOCOL_41))
        {
            throw Exception("Required capability: CLIENT_PROTOCOL_41.", ErrorCodes::MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES);
        }
        if (!(capabilities & CLIENT_PLUGIN_AUTH))
        {
            throw Exception("Required capability: CLIENT_PLUGIN_AUTH.", ErrorCodes::MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES);
        }

        String password;
        if (handshake_response.auth_plugin_name != Authentication::ClearText)
        {

            packet_sender.sendPacket(AuthSwitchRequest(Authentication::ClearText, ""), true);
            password = packet_sender.receivePacket<NullTerminatedString>().value;
        }
        else
        {
            NullTerminatedString packet;
            packet.readPayload(std::move(handshake_response.auth_response));
            password = packet.value;
        }
        LOG_TRACE(log, "password: " << password);
        try
        {
            connection_context.setUser(handshake_response.username, password, socket().address(), "");
            connection_context.setCurrentDatabase(handshake_response.database);
            connection_context.setCurrentQueryId("");
            LOG_ERROR(log, "Authentication for user " << handshake_response.username << " succeeded.");
        }
        catch (const NetException &)
        {
            throw;
        }
        catch (const Exception & exc)
        {
            LOG_ERROR(log, "Authentication for user " << handshake_response.username << " failed.");
            packet_sender.sendPacket(ERR_Packet(exc.code(), "00000", exc.message()), true);
            throw;
        }

        OK_Packet ok_packet(0, handshake_response.capability_flags, 0, 0, 0, 0, "");
        packet_sender.sendPacket(ok_packet, true);

        while (true)
        {
            packet_sender.resetSequenceId();
            String payload = packet_sender.receivePacketPayload();
            int command = payload[0];
            LOG_DEBUG(log, "Received command: " << std::to_string(command) << ". Connection id: " << connection_id << ".");
            try
            {
                switch (command)
                {
                    case COM_QUIT:
                        return;
                    case COM_INIT_DB:
                        comInitDB(payload);
                        break;
                    case COM_QUERY:
                        comQuery(payload);
                        break;
                    case COM_FIELD_LIST:
                        comFieldList(payload);
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
                packet_sender.sendPacket(ERR_Packet(exc.code(), "00000", exc.message()), true);
            }
        }
    }
    catch (Poco::Exception & exc)
    {
        log->log(exc);
    }
}

void MySQLHandler::comInitDB(String payload)
{
    String database = payload.substr(1);
    LOG_DEBUG(log, "Setting current database to " << database);
    connection_context.setCurrentDatabase(database);
    packet_sender.sendPacket(OK_Packet(0, capabilities, 0, 0, 0, 1, ""), true);
}

void MySQLHandler::comFieldList(String payload)
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
        packet_sender.sendPacket(column_definition);
    }
    packet_sender.sendPacket(OK_Packet(0xfe, capabilities, 0, 0, 0, 0, ""), true);
}

void MySQLHandler::comPing()
{
    packet_sender.sendPacket(OK_Packet(0x0, capabilities, 0, 0, 0, 0, ""), true);
}

void MySQLHandler::comQuery(String payload)
{
    BlockIO res = executeQuery(payload.substr(1), connection_context);
    FormatSettings format_settings;
    if (res.in)
    {
        LOG_TRACE(log, "Executing query with output.");

        Block header = res.in->getHeader();
        packet_sender.sendPacket(LengthEncodedNumber(header.columns()));

        for (const ColumnWithTypeAndName & column : header.getColumnsWithTypeAndName())
        {
            ColumnDefinition column_definition(column.name, CharacterSet::binary, std::numeric_limits<uint32_t>::max(),
                                               ColumnType::MYSQL_TYPE_STRING, 0, 0);
            packet_sender.sendPacket(column_definition);

            LOG_TRACE(log, "Sent " << column.name << " column definition");
        }

        LOG_TRACE(log, "Sent columns definitions.");

        if (!(capabilities & Capability::CLIENT_DEPRECATE_EOF))
        {
            packet_sender.sendPacket(EOF_Packet(0, 0));
        }

        while (Block block = res.in->read())
        {
            size_t rows = block.rows();

            for (size_t i = 0; i < rows; i++)
            {
                ResultsetRow row_packet;
                for (ColumnWithTypeAndName & column : block)
                {
                    column.column = column.column->convertToFullColumnIfConst();

                    String column_value;
                    WriteBufferFromString ostr(column_value);

                    LOG_TRACE(log, "Sending value of type " << column.type->getName() << " of column " << column.column->getName());

                    column.type->serializeAsText(*column.column.get(), i, ostr, format_settings);
                    ostr.finish();

                    row_packet.appendColumn(std::move(column_value));
                }
                packet_sender.sendPacket(row_packet);
            }
        }

        LOG_TRACE(log, "Sent rows.");
    }

    if (capabilities & CLIENT_DEPRECATE_EOF)
    {
        packet_sender.sendPacket(OK_Packet(0xfe, capabilities, 0, 0, 0, 0, ""), true);
    }
    else
    {
        packet_sender.sendPacket(EOF_Packet(0, 0), true);
    }
}

}
