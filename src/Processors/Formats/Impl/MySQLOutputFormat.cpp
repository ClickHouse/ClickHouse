#include <Processors/Formats/Impl/MySQLOutputFormat.h>
#include <Interpreters/ProcessList.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>

namespace DB
{

using namespace MySQLProtocol;
using namespace MySQLProtocol::Generic;
using namespace MySQLProtocol::ProtocolText;


MySQLOutputFormat::MySQLOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_)
    , format_settings(settings_)
{
}

void MySQLOutputFormat::initialize()
{
    if (initialized)
        return;

    initialized = true;
    const auto & header = getPort(PortKind::Main).getHeader();
    data_types = header.getDataTypes();

    if (header.columns())
    {
        packet_endpoint->sendPacket(LengthEncodedNumber(header.columns()));

        for (size_t i = 0; i < header.columns(); i++)
        {
            const auto & column_name = header.getColumnsWithTypeAndName()[i].name;
            packet_endpoint->sendPacket(getColumnDefinition(column_name, data_types[i]->getTypeId()));
        }

        if (!(context->mysql.client_capabilities & Capability::CLIENT_DEPRECATE_EOF))
        {
            packet_endpoint->sendPacket(EOFPacket(0, 0));
        }
    }
}


void MySQLOutputFormat::consume(Chunk chunk)
{

    initialize();

    for (size_t i = 0; i < chunk.getNumRows(); i++)
    {
        ProtocolText::ResultSetRow row_packet(data_types, chunk.getColumns(), i);
        packet_endpoint->sendPacket(row_packet);
    }
}

void MySQLOutputFormat::finalize()
{
    size_t affected_rows = 0;
    std::string human_readable_info;
    if (QueryStatus * process_list_elem = context->getProcessListElement())
    {
        CurrentThread::finalizePerformanceCounters();
        QueryStatusInfo info = process_list_elem->getInfo();
        affected_rows = info.written_rows;
        human_readable_info = fmt::format(
            "Read {} rows, {} in {} sec., {} rows/sec., {}/sec.",
            info.read_rows, ReadableSize(info.read_bytes), info.elapsed_seconds,
            static_cast<size_t>(info.read_rows / info.elapsed_seconds),
            ReadableSize(info.read_bytes / info.elapsed_seconds));
    }

    const auto & header = getPort(PortKind::Main).getHeader();
    if (header.columns() == 0)
        packet_endpoint->sendPacket(OKPacket(0x0, context->mysql.client_capabilities, affected_rows, 0, 0, "", human_readable_info), true);
    else
    if (context->mysql.client_capabilities & CLIENT_DEPRECATE_EOF)
        packet_endpoint->sendPacket(OKPacket(0xfe, context->mysql.client_capabilities, affected_rows, 0, 0, "", human_readable_info), true);
    else
        packet_endpoint->sendPacket(EOFPacket(0, 0), true);
}

void MySQLOutputFormat::flush()
{
    packet_endpoint->out->next();
}

void registerOutputFormatProcessorMySQLWire(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor(
        "MySQLWire",
        [](WriteBuffer & buf,
           const Block & sample,
           const RowOutputFormatParams &,
           const FormatSettings & settings) { return std::make_shared<MySQLOutputFormat>(buf, sample, settings); });
}

}
