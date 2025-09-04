#include <Processors/Formats/Impl/MySQLOutputFormat.h>
#include <Common/formatReadable.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsProtocolBinary.h>
#include <Core/MySQL/PacketsProtocolText.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

#include <Processors/Port.h>

namespace DB
{

using namespace MySQLProtocol;
using namespace MySQLProtocol::Generic;
using namespace MySQLProtocol::ProtocolText;
using namespace MySQLProtocol::ProtocolBinary;

MySQLOutputFormat::MySQLOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_)
    , client_capabilities(settings_.mysql_wire.client_capabilities)
{
    /// MySQlWire is a special format that is usually used as output format for MySQL protocol connections.
    /// In this case we have a correct `sequence_id` stored in `settings_.mysql_wire`.
    /// But it's also possible to specify MySQLWire as output format for clickhouse-client or clickhouse-local.
    /// There is no `sequence_id` stored in `settings_.mysql_wire` in this case, so we create a dummy one.
    sequence_id = settings_.mysql_wire.sequence_id ? settings_.mysql_wire.sequence_id : &dummy_sequence_id;
    /// Switch between Text (COM_QUERY) and Binary (COM_EXECUTE_STMT) ResultSet
    use_binary_result_set = settings_.mysql_wire.binary_protocol;

    const auto & header = getPort(PortKind::Main).getHeader();
    data_types = header.getDataTypes();

    serializations.reserve(data_types.size());
    for (const auto & type : data_types)
        serializations.emplace_back(type->getDefaultSerialization());

    packet_endpoint = std::make_shared<MySQLProtocol::PacketEndpoint>(out, *sequence_id);
}

void MySQLOutputFormat::setContext(ContextPtr context_)
{
    context = context_;
}

void MySQLOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();

    if (header.columns())
    {
        packet_endpoint->sendPacket(LengthEncodedNumber(header.columns()), false);

        for (size_t i = 0; i < header.columns(); ++i)
        {
            const auto & column_name = header.getColumnsWithTypeAndName()[i].name;
            packet_endpoint->sendPacket(getColumnDefinition(column_name, data_types[i]), false);
        }

        if (!(client_capabilities & Capability::CLIENT_DEPRECATE_EOF) && !use_binary_result_set)
        {
            packet_endpoint->sendPacket(EOFPacket(0, 0), false);
        }
    }
}

void MySQLOutputFormat::consume(Chunk chunk)
{
    if (!use_binary_result_set)
    {
        for (size_t row = 0; row < chunk.getNumRows(); ++row)
        {
            ProtocolText::ResultSetRow row_packet(serializations, data_types, chunk.getColumns(), row);
            packet_endpoint->sendPacket(row_packet, false);
        }
    }
    else
    {
        for (size_t row = 0; row < chunk.getNumRows(); ++row)
        {
            ProtocolBinary::ResultSetRow row_packet(serializations, data_types, chunk.getColumns(), row);
            packet_endpoint->sendPacket(row_packet, false);
        }
    }

    flushImpl();
}

void MySQLOutputFormat::finalizeImpl()
{
    if (!use_binary_result_set)
    {
        size_t affected_rows = 0;
        std::string human_readable_info;
        if (QueryStatusPtr process_list_elem = getContext()->getProcessListElement())
        {
            CurrentThread::finalizePerformanceCounters();
            QueryStatusInfo info = process_list_elem->getInfo();
            affected_rows = info.written_rows;
            double elapsed_seconds = static_cast<double>(info.elapsed_microseconds) / 1000000.0;
            human_readable_info = fmt::format(
                "Read {} rows, {} in {} sec., {} rows/sec., {}/sec.",
                info.read_rows,
                ReadableSize(info.read_bytes),
                elapsed_seconds,
                static_cast<size_t>(info.read_rows / elapsed_seconds),
                ReadableSize(info.read_bytes / elapsed_seconds));
        }

        const auto & header = getPort(PortKind::Main).getHeader();
        if (header.columns() == 0)
            packet_endpoint->sendPacket(OKPacket(0x0, client_capabilities, affected_rows, 0, 0, "", human_readable_info));
        else if (client_capabilities & CLIENT_DEPRECATE_EOF)
            packet_endpoint->sendPacket(OKPacket(0xfe, client_capabilities, affected_rows, 0, 0, "", human_readable_info));
        else
            packet_endpoint->sendPacket(EOFPacket(0, 0));
    }
    else
    {
        size_t affected_rows = 0;
        if (QueryStatusPtr process_list_elem = getContext()->getProcessListElement())
        {
            CurrentThread::finalizePerformanceCounters();
            QueryStatusInfo info = process_list_elem->getInfo();
            affected_rows = info.written_rows;
        }
        if (client_capabilities & CLIENT_DEPRECATE_EOF)
            packet_endpoint->sendPacket(OKPacket(0xfe, client_capabilities, affected_rows, 0, 0, "", ""));
        else
            packet_endpoint->sendPacket(EOFPacket(0, 0));
    }
}

void MySQLOutputFormat::flushImpl()
{
    packet_endpoint->out->next();
}

void registerOutputFormatMySQLWire(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "MySQLWire",
        [](WriteBuffer & buf,
           const Block & sample,
           const FormatSettings & settings) { return std::make_shared<MySQLOutputFormat>(buf, sample, settings); });
    factory.markOutputFormatNotTTYFriendly("MySQLWire");
    factory.setContentType("MySQLWire", "application/octet-stream");
}

}
