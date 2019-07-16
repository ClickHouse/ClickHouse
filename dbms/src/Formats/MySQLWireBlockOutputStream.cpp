#include "MySQLWireBlockOutputStream.h"
#include <Core/MySQLProtocol.h>
#include <Interpreters/ProcessList.h>
#include <iomanip>
#include <sstream>

namespace DB
{

using namespace MySQLProtocol;

MySQLWireBlockOutputStream::MySQLWireBlockOutputStream(WriteBuffer & buf, const Block & header, Context & context)
    : header(header)
    , context(context)
    , packet_sender(std::make_shared<PacketSender>(buf, context.mysql.sequence_id))
{
    packet_sender->max_packet_size = context.mysql.max_packet_size;
}

void MySQLWireBlockOutputStream::writePrefix()
{
    if (header.columns() == 0)
        return;

    packet_sender->sendPacket(LengthEncodedNumber(header.columns()));

    for (const ColumnWithTypeAndName & column : header.getColumnsWithTypeAndName())
    {
        ColumnDefinition column_definition(column.name, CharacterSet::binary, 0, ColumnType::MYSQL_TYPE_STRING, 0, 0);
        packet_sender->sendPacket(column_definition);
    }

    if (!(context.mysql.client_capabilities & Capability::CLIENT_DEPRECATE_EOF))
    {
        packet_sender->sendPacket(EOF_Packet(0, 0));
    }
}

void MySQLWireBlockOutputStream::write(const Block & block)
{
    size_t rows = block.rows();

    for (size_t i = 0; i < rows; i++)
    {
        ResultsetRow row_packet;
        for (const ColumnWithTypeAndName & column : block)
        {
            WriteBufferFromOwnString ostr;
            column.type->serializeAsText(*column.column.get(), i, ostr, format_settings);
            row_packet.appendColumn(std::move(ostr.str()));
        }
        packet_sender->sendPacket(row_packet);
    }
}

void MySQLWireBlockOutputStream::writeSuffix()
{
    QueryStatus * process_list_elem = context.getProcessListElement();
    CurrentThread::finalizePerformanceCounters();
    QueryStatusInfo info = process_list_elem->getInfo();
    size_t affected_rows = info.written_rows;

    std::stringstream human_readable_info;
    human_readable_info << std::fixed << std::setprecision(3)
        << "Read " << info.read_rows << " rows, " << formatReadableSizeWithBinarySuffix(info.read_bytes) << " in " << info.elapsed_seconds << " sec., "
        << static_cast<size_t>(info.read_rows / info.elapsed_seconds) << " rows/sec., "
        << formatReadableSizeWithBinarySuffix(info.read_bytes / info.elapsed_seconds) << "/sec.";

    if (header.columns() == 0)
        packet_sender->sendPacket(OK_Packet(0x0, context.mysql.client_capabilities, affected_rows, 0, 0, "", human_readable_info.str()), true);
    else
        if (context.mysql.client_capabilities & CLIENT_DEPRECATE_EOF)
            packet_sender->sendPacket(OK_Packet(0xfe, context.mysql.client_capabilities, affected_rows, 0, 0, "", human_readable_info.str()), true);
        else
            packet_sender->sendPacket(EOF_Packet(0, 0), true);
}

void MySQLWireBlockOutputStream::flush()
{
    packet_sender->out->next();
}

}
