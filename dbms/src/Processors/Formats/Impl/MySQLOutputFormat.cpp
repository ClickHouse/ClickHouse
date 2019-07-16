#include <Processors/Formats/Impl/MySQLOutputFormat.h>

#include <Core/MySQLProtocol.h>
#include <Interpreters/ProcessList.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>

#include <iomanip>
#include <sstream>

namespace DB
{

using namespace MySQLProtocol;


MySQLOutputFormat::MySQLOutputFormat(WriteBuffer & out_, const Block & header, const Context & context, const FormatSettings & settings)
    : IOutputFormat(header, out_)
    , context(context)
    , packet_sender(std::make_shared<PacketSender>(out, const_cast<uint8_t &>(context.mysql.sequence_id))) /// TODO: fix it
    , format_settings(settings)
{
}

void MySQLOutputFormat::consume(Chunk chunk)
{
    auto & header = getPort(PortKind::Main).getHeader();

    if (!initialized)
    {
        initialized = true;


        if (header.columns())
        {

            packet_sender->sendPacket(LengthEncodedNumber(header.columns()));

            for (const ColumnWithTypeAndName & column : header.getColumnsWithTypeAndName())
            {
                ColumnDefinition column_definition(column.name, CharacterSet::binary, 0, ColumnType::MYSQL_TYPE_STRING,
                                                   0, 0);
                packet_sender->sendPacket(column_definition);
            }

            if (!(context.mysql.client_capabilities & Capability::CLIENT_DEPRECATE_EOF))
            {
                packet_sender->sendPacket(EOF_Packet(0, 0));
            }
        }
    }

    size_t rows = chunk.getNumRows();
    auto & columns = chunk.getColumns();

    for (size_t i = 0; i < rows; i++)
    {
        ResultsetRow row_packet;
        for (size_t col = 0; col < columns.size(); ++col)
        {
            String column_value;
            WriteBufferFromString ostr(column_value);
            header.getByPosition(col).type->serializeAsText(*columns[col], i, ostr, format_settings);
            ostr.finish();

            row_packet.appendColumn(std::move(column_value));
        }
        packet_sender->sendPacket(row_packet);
    }
}

void MySQLOutputFormat::finalize()
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

    auto & header = getPort(PortKind::Main).getHeader();

    if (header.columns() == 0)
        packet_sender->sendPacket(OK_Packet(0x0, context.mysql.client_capabilities, affected_rows, 0, 0, "", human_readable_info.str()), true);
    else
    if (context.mysql.client_capabilities & CLIENT_DEPRECATE_EOF)
        packet_sender->sendPacket(OK_Packet(0xfe, context.mysql.client_capabilities, affected_rows, 0, 0, "", human_readable_info.str()), true);
    else
        packet_sender->sendPacket(EOF_Packet(0, 0), true);
}

void registerOutputFormatProcessorMySQLWrite(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor(
        "MySQLWire", [](WriteBuffer & buf, const Block & sample, const Context & context, const FormatSettings & settings)
        {
            return std::make_shared<MySQLOutputFormat>(buf, sample, context, settings);
        });
}

}
