#include <Processors/Formats/Impl/MySQLOutputFormat.h>

#if USE_SSL

#include <Core/MySQLProtocol.h>
#include <Interpreters/ProcessList.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>

#include <iomanip>
#include <sstream>

namespace DB
{

using namespace MySQLProtocol;


MySQLOutputFormat::MySQLOutputFormat(WriteBuffer & out_, const Block & header_, const Context & context_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_)
    , context(context_)
    , packet_sender(out, const_cast<uint8_t &>(context_.mysql.sequence_id)) /// TODO: fix it
    , format_settings(settings_)
{
    packet_sender.max_packet_size = context_.mysql.max_packet_size;
}

void MySQLOutputFormat::initialize()
{
    if (initialized)
        return;

    initialized = true;
    auto & header = getPort(PortKind::Main).getHeader();


    if (header.columns())
    {

        packet_sender.sendPacket(LengthEncodedNumber(header.columns()));

        for (const ColumnWithTypeAndName & column : header.getColumnsWithTypeAndName())
        {
            ColumnDefinition column_definition(column.name, CharacterSet::binary, 0, ColumnType::MYSQL_TYPE_STRING,
                                               0, 0);
            packet_sender.sendPacket(column_definition);
        }

        if (!(context.mysql.client_capabilities & Capability::CLIENT_DEPRECATE_EOF))
        {
            packet_sender.sendPacket(EOF_Packet(0, 0));
        }
    }
}


void MySQLOutputFormat::consume(Chunk chunk)
{
    initialize();

    auto & header = getPort(PortKind::Main).getHeader();

    size_t rows = chunk.getNumRows();
    auto & columns = chunk.getColumns();

    for (size_t i = 0; i < rows; i++)
    {
        ResultsetRow row_packet;
        for (size_t col = 0; col < columns.size(); ++col)
        {
            WriteBufferFromOwnString ostr;
            header.getByPosition(col).type->serializeAsText(*columns[col], i, ostr, format_settings);
            row_packet.appendColumn(std::move(ostr.str()));
        }
        packet_sender.sendPacket(row_packet);
    }
}

void MySQLOutputFormat::finalize()
{
    size_t affected_rows = 0;
    std::stringstream human_readable_info;
    if (QueryStatus * process_list_elem = context.getProcessListElement())
    {
        CurrentThread::finalizePerformanceCounters();
        QueryStatusInfo info = process_list_elem->getInfo();
        affected_rows = info.written_rows;
        human_readable_info << std::fixed << std::setprecision(3)
                            << "Read " << info.read_rows << " rows, " << formatReadableSizeWithBinarySuffix(info.read_bytes) << " in " << info.elapsed_seconds << " sec., "
                            << static_cast<size_t>(info.read_rows / info.elapsed_seconds) << " rows/sec., "
                            << formatReadableSizeWithBinarySuffix(info.read_bytes / info.elapsed_seconds) << "/sec.";
    }

    const auto & header = getPort(PortKind::Main).getHeader();
    if (header.columns() == 0)
        packet_sender.sendPacket(OK_Packet(0x0, context.mysql.client_capabilities, affected_rows, 0, 0, "", human_readable_info.str()), true);
    else
    if (context.mysql.client_capabilities & CLIENT_DEPRECATE_EOF)
        packet_sender.sendPacket(OK_Packet(0xfe, context.mysql.client_capabilities, affected_rows, 0, 0, "", human_readable_info.str()), true);
    else
        packet_sender.sendPacket(EOF_Packet(0, 0), true);
}

void MySQLOutputFormat::flush()
{
    packet_sender.out->next();
}

void registerOutputFormatProcessorMySQLWrite(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor(
        "MySQLWire",
        [](WriteBuffer & buf,
           const Block & sample,
           const Context & context,
           FormatFactory::WriteCallback,
           const FormatSettings & settings) { return std::make_shared<MySQLOutputFormat>(buf, sample, context, settings); });
}

}

#endif // USE_SSL
