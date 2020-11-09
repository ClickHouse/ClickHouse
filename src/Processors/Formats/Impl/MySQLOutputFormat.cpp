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
        packet_sender->sendPacket(LengthEncodedNumber(header.columns()));

        for (size_t i = 0; i < header.columns(); i++)
        {
            const auto & column_name = header.getColumnsWithTypeAndName()[i].name;
            packet_sender->sendPacket(getColumnDefinition(column_name, data_types[i]->getTypeId()));
        }

        if (!(context->mysql.client_capabilities & Capability::CLIENT_DEPRECATE_EOF))
        {
            packet_sender->sendPacket(EOF_Packet(0, 0));
        }
    }
}


void MySQLOutputFormat::consume(Chunk chunk)
{

    initialize();

    for (size_t i = 0; i < chunk.getNumRows(); i++)
    {
        ProtocolText::ResultsetRow row_packet(data_types, chunk.getColumns(), i);
        packet_sender->sendPacket(row_packet);
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
        packet_sender->sendPacket(OK_Packet(0x0, context->mysql.client_capabilities, affected_rows, 0, 0, "", human_readable_info), true);
    else
    if (context->mysql.client_capabilities & CLIENT_DEPRECATE_EOF)
        packet_sender->sendPacket(OK_Packet(0xfe, context->mysql.client_capabilities, affected_rows, 0, 0, "", human_readable_info), true);
    else
        packet_sender->sendPacket(EOF_Packet(0, 0), true);
}

void MySQLOutputFormat::flush()
{
    packet_sender->out->next();
}

void registerOutputFormatProcessorMySQLWire(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor(
        "MySQLWire",
        [](WriteBuffer & buf,
           const Block & sample,
           FormatFactory::WriteCallback,
           const FormatSettings & settings) { return std::make_shared<MySQLOutputFormat>(buf, sample, settings); });
}

}
