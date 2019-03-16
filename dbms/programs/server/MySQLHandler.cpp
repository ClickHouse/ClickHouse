#include <DataStreams/copyData.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
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

uint32_t MySQLHandler::last_connection_id = 0;

String MySQLHandler::readPayload()
{
    WriteBufferFromOwnString buf;

    size_t payload_length = 0;
    uint8_t packet_sequence_id;

    // packets which are larger than or equal to 16MB are splitted
    do {
        LOG_TRACE(log, "Reading from buffer");

        in->readStrict(reinterpret_cast<char *>(&payload_length), 3);

        if (payload_length > MAX_PACKET_LENGTH) {
            throw ProtocolError(Poco::format("Received packet with payload length greater than 2^24 - 1: %z.", payload_length), 0);
        }

        in->readStrict(reinterpret_cast<char *>(&packet_sequence_id), 1);

        if (packet_sequence_id != sequence_id) {
            throw ProtocolError(Poco::format("Received packet with wrong sequence-id: %d. Expected: %d.", packet_sequence_id, sequence_id), 0);
        }
        sequence_id++;

        LOG_TRACE(log, "Received packet. Sequence-id: " << static_cast<int>(packet_sequence_id) << ", payload length: " << payload_length);

        copyData(*in, static_cast<WriteBuffer &>(buf), payload_length);
    }
    while (payload_length == MAX_PACKET_LENGTH);

    return buf.str();
}

/// Converts packet to text. Useful for debugging, since packets often consist of non-printing characters.
static String packetToText(std::string_view payload) {
    String result;
    for (auto c : payload) {
        result += ' ';
        result += std::to_string(static_cast<unsigned char>(c));
    }
    return result;
}

void MySQLHandler::writePayload(std::string_view payload)
{
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
    }
    while (pos < payload.length());
    out->next();

    LOG_TRACE(log, "Packet was sent.");
}

void MySQLHandler::run() {
    sequence_id = 0;
    connection_context = server.context();

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    try
    {
        Handshake handshake(connection_id, VERSION_FULL);
        auto payload = handshake.getPayload();
        writePayload(payload);

        LOG_TRACE(log, "sent handshake");

        HandshakeResponse handshake_response;
        payload = readPayload();
        handshake_response.readPayload(payload);

        LOG_DEBUG(log, "capability_flags: " << handshake_response.capability_flags
            << "max_packet_size: %s"
            << handshake_response.max_packet_size
            << "character_set: %s"
            << handshake_response.character_set
            << "user: %s"
            << handshake_response.username
            << "auth_response length: %s"
            << handshake_response.auth_response.length()
            << "auth_response: %s"
            << handshake_response.auth_response
            << "database: %s"
            << handshake_response.database
            << "auth_plugin_name: %s"
            << handshake_response.auth_plugin_name);

        capabilities = handshake_response.capability_flags;
        if (!(capabilities & CLIENT_PROTOCOL_41)) {
            LOG_ERROR(log, "Clients without CLIENT_PROTOCOL_41 capability are not supported.");
            return;
        }

        try {
            connection_context.setUser(handshake_response.username, "", socket().address(), "");
            connection_context.setCurrentDatabase(handshake_response.database);
            connection_context.setCurrentQueryId("");
        }
        catch (const Exception & exc) {
            log->log(exc);
            writePayload(ERR_Packet(exc.code(), "00000", exc.message()).getPayload());
            return; // TODO Authentication method change
        }
        OK_Packet ok_packet(0, handshake_response.capability_flags, 0, 0, 0, 0, "");
        payload = ok_packet.getPayload();
        writePayload(payload);
        LOG_INFO(log, "sent OK_Packet");

        while (true) {
            sequence_id = 0;
            payload = readPayload();
            int command = payload[0];
            LOG_DEBUG(log, "Received command: " << std::to_string(command) << ". Connection id: " << connection_id << ".");
            try {
                switch (command) {
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
            catch (const NetException & exc) {
                log->log(exc);
                throw;
            }
            catch (const Exception & exc) {
                log->log(exc);
                writePayload(ERR_Packet(exc.code(), "00000", exc.message()).getPayload());
            }
        }
    }
    catch (Poco::Exception& exc)
    {
        log->log(exc);
    }

}

void MySQLHandler::comInitDB(const String & payload)
{
    String database = payload.substr(1);
    LOG_DEBUG(log, "Setting current database to " << database);
    connection_context.setCurrentDatabase(database);
    writePayload(OK_Packet(0, capabilities, 0, 0, 0, 1, "").getPayload());
}

void MySQLHandler::comFieldList(const String & payload) {
    ComFieldList packet;
    packet.readPayload(payload);
    StoragePtr tablePtr = connection_context.getTable(connection_context.getCurrentDatabase(), packet.table);
    for (const NameAndTypePair & column: tablePtr->getColumns().getAll())
    {
        ColumnDefinition column_definition(
            "schema", packet.table, packet.table, column.name, column.name, CharacterSet::UTF8, 100, ColumnType::MYSQL_TYPE_STRING, 0, 0
        );
        writePayload(column_definition.getPayload());
    }
    writePayload(OK_Packet(0xfe, capabilities, 0, 0, 0, 0, "").getPayload());
}

void MySQLHandler::comPing() {
    writePayload(OK_Packet(0x0, capabilities, 0, 0, 0, 0, "").getPayload());
}

void MySQLHandler::comQuery(const String & payload) {
    BlockIO res = executeQuery(payload.substr(1), connection_context);
    FormatSettings format_settings;
    if (res.in) {
        LOG_TRACE(log, "Executing query with output.");

        Block header = res.in->getHeader();
        writePayload(writeLenenc(header.columns()));

        for (const ColumnWithTypeAndName & column : header.getColumnsWithTypeAndName())
        {
            writePayload(ColumnDefinition(
                "", /// database. Apparently, addition of these fields to ColumnWithTypeAndName and changes in interpreters are needed.
                "", /// table name.
                "", /// physical table name
                column.name, /// virtual column name
                "", /// physical column name
                CharacterSet::UTF8,
                /// maximum column length which can be used for text outputting. Since query execution hasn't started, it is unknown.
                std::numeric_limits<uint32_t>::max(),
                ColumnType::MYSQL_TYPE_STRING, /// TODO
                0,
                0
            ).getPayload());

            LOG_TRACE(log, "sent " << column.name << " column definition");
        }

        LOG_TRACE(log, "Sent columns definitions.");

        while (Block block = res.in->read())
        {
            size_t rows = block.rows();
            size_t columns = block.columns();

            for (size_t i = 0; i < rows; i++) {
                String row_payload;
                for (size_t j = 0; j < columns; j++) {
                    ColumnWithTypeAndName & column = block.getByPosition(j);
                    column.column = column.column->convertToFullColumnIfConst();

                    String column_value;
                    WriteBufferFromString ostr(column_value);

                    LOG_TRACE(log, "sending value of type " << column.type->getName() << " of column " << column.column->getName());

                    column.type->serializeAsText(*column.column.get(), i, ostr, format_settings);
                    ostr.finish();

                    writeLenencStr(row_payload, column_value);
                }
                writePayload(row_payload);
            }
        }

        LOG_TRACE(log, "Sent rows.");
    }

    if (capabilities & CLIENT_DEPRECATE_EOF) {
        writePayload(OK_Packet(0xfe, capabilities, 0, 0, 0, 0, "").getPayload());
    } else {
        writePayload(EOF_Packet(0, 0).getPayload());
    }
}

}
