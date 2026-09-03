#include <Processors/Formats/Impl/MySQLOutputFormat.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
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

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

namespace FailPoints
{
extern const char mysql_output_format_mid_loop_pause[];
}

MySQLOutputFormat::MySQLOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
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
    LOG_TEST(getLogger("MySQLOutputFormat"), "Consume a chunk");

    if (isCancelled())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

    if (!use_binary_result_set)
    {
        for (size_t row = 0; row < chunk.getNumRows(); ++row)
        {
            if (isCancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

            if (row == 5)
                FailPointInjection::pauseFailPoint(FailPoints::mysql_output_format_mid_loop_pause);

            ProtocolText::ResultSetRow row_packet(serializations, data_types, chunk.getColumns(), row);
            packet_endpoint->sendPacket(row_packet, false);
        }
    }
    else
    {
        for (size_t row = 0; row < chunk.getNumRows(); ++row)
        {
            if (isCancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

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
                "Read {} rows, {} in {:.3f} sec., {} rows/sec., {}/sec.",
                info.read_rows,
                ReadableSize(info.read_bytes),
                elapsed_seconds,
                static_cast<size_t>(static_cast<double>(info.read_rows) / elapsed_seconds),
                ReadableSize(static_cast<double>(info.read_bytes) / elapsed_seconds));
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

void registerOutputFormatMySQLWire(FormatFactory & factory);
void registerOutputFormatMySQLWire(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "MySQLWire",
        [](WriteBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           FormatFilterInfoPtr /*format_filter_info*/) { return std::make_shared<MySQLOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });
    factory.markOutputFormatNotTTYFriendly("MySQLWire");
    factory.setContentType("MySQLWire", "application/octet-stream");

    factory.setDocumentation("MySQLWire", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `MySQLWire` format serializes query results as a MySQL wire-protocol result set. It writes the column count and
column definitions followed by one protocol row packet for each result row and a final `EOF` or `OK` packet. The row
packets use the text protocol for normal queries and the binary protocol for prepared statements.

This is an output-only binary format intended for clients connected through ClickHouse's
[MySQL interface](/concepts/features/interfaces/mysql). The interface selects `MySQLWire` automatically and supplies
protocol state such as the client's capabilities and the packet sequence number. It's not intended for displaying or
storing query results as a standalone file.

## Example usage {#example-usage}

After enabling the MySQL interface, use a compatible client to execute a query:

```shell
mysql --protocol tcp -h 127.0.0.1 -u default -P 9004 default \
    -e "SELECT number, number * 2 AS doubled FROM numbers(3)"
```

The interface sends the result using `MySQLWire`; an explicit `FORMAT MySQLWire` clause is optional. Other explicit
output formats aren't supported over the MySQL interface.

## Format settings {#format-settings}

There are no user-configurable format settings. The MySQL interface derives the required settings from the client
handshake and the command being executed.
)DOCS_MD"});
}

}
