#include "MySQLBlockOutputStream.h"
#include <Core/MySQLProtocol.h>


namespace DB
{

using namespace MySQLProtocol;

MySQLBlockOutputStream::MySQLBlockOutputStream(WriteBuffer & buf, const Block & header, const uint32_t capabilities, size_t & sequence_id)
    : header(header)
    , capabilities(capabilities)
    , packet_sender(new PacketSender(buf, sequence_id, "MySQLBlockOutputStream"))
{
}

void MySQLBlockOutputStream::writePrefix()
{
    if (header.columns() == 0)
        return;

    packet_sender->sendPacket(LengthEncodedNumber(header.columns()));

    for (const ColumnWithTypeAndName & column : header.getColumnsWithTypeAndName())
    {
        ColumnDefinition column_definition(column.name, CharacterSet::binary, std::numeric_limits<uint32_t>::max(),
                                           ColumnType::MYSQL_TYPE_STRING, 0, 0);
        packet_sender->sendPacket(column_definition);
    }

    if (!(capabilities & Capability::CLIENT_DEPRECATE_EOF))
    {
        packet_sender->sendPacket(EOF_Packet(0, 0));
    }
}

void MySQLBlockOutputStream::write(const Block & block)
{
    size_t rows = block.rows();

    for (size_t i = 0; i < rows; i++)
    {
        ResultsetRow row_packet;
        for (const ColumnWithTypeAndName & column : block)
        {
            String column_value;
            WriteBufferFromString ostr(column_value);
            column.type->serializeAsText(*column.column.get(), i, ostr, format_settings);
            ostr.finish();

            row_packet.appendColumn(std::move(column_value));
        }
        packet_sender->sendPacket(row_packet);
    }
}

void MySQLBlockOutputStream::writeSuffix()
{
    if (header.columns() == 0)
        packet_sender->sendPacket(OK_Packet(0x0, capabilities, 0, 0, 0, 0, ""), true);
    else
        if (capabilities & CLIENT_DEPRECATE_EOF)
            packet_sender->sendPacket(OK_Packet(0xfe, capabilities, 0, 0, 0, 0, ""), true);
        else
            packet_sender->sendPacket(EOF_Packet(0, 0), true);
}

void MySQLBlockOutputStream::flush() {
    packet_sender->out->next();
}

}
