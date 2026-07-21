#include <Processors/Formats/Impl/PostgreSQLOutputFormat.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/ProcessList.h>

#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

PostgreSQLOutputFormat::PostgreSQLOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_)
    , format_settings(settings_)
    , message_transport(&out)
{
    // PostgreSQL uses 't' and 'f' for boolean values
    format_settings.bool_true_representation = "t";
    format_settings.bool_false_representation = "f";
}

void PostgreSQLOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();
    auto data_types = header.getDataTypes();

    if (header.columns())
    {
        VectorWithMemoryTracking<PostgreSQLProtocol::Messaging::FieldDescription> columns;
        columns.reserve(header.columns());

        for (size_t i = 0; i < header.columns(); ++i)
        {
            const auto & column_name = header.getColumnsWithTypeAndName()[i].name;
            columns.emplace_back(column_name, data_types[i]);
            serializations.emplace_back(data_types[i]->getDefaultSerialization());
        }
        message_transport.send(PostgreSQLProtocol::Messaging::RowDescription(columns));
    }
}

void PostgreSQLOutputFormat::consume(Chunk chunk)
{
    LOG_TEST(getLogger("PostgreSQLOutputFormat"), "Consume a chunk");

    /// Check for cancellation at the beginning of the loop, use throw instead of return.
    if (isCancelled())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

    for (size_t i = 0; i != chunk.getNumRows(); ++i)
    {
        /// Check for cancellation periodically, use throw instead of return.
        if (isCancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

        const Columns & columns = chunk.getColumns();
        VectorWithMemoryTracking<std::shared_ptr<PostgreSQLProtocol::Messaging::ISerializable>> row;
        row.reserve(chunk.getNumColumns());

        for (size_t j = 0; j != chunk.getNumColumns(); ++j)
        {
            if (columns[j]->isNullAt(i))
                row.push_back(std::make_shared<PostgreSQLProtocol::Messaging::NullField>());
            else
            {
                WriteBufferFromOwnString ostr;
                serializations[j]->serializeText(*columns[j], i, ostr, format_settings);
                row.push_back(std::make_shared<PostgreSQLProtocol::Messaging::StringField>(std::move(ostr.str())));
            }
        }

        message_transport.send(PostgreSQLProtocol::Messaging::DataRow(row));
    }
}

void PostgreSQLOutputFormat::flushImpl()
{
    message_transport.flush();
}

void registerOutputFormatPostgreSQLWire(FormatFactory & factory);
void registerOutputFormatPostgreSQLWire(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "PostgreSQLWire",
        [](WriteBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           FormatFilterInfoPtr /*format_filter_info*/) { return std::make_shared<PostgreSQLOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });
    factory.markOutputFormatNotTTYFriendly("PostgreSQLWire");
    factory.setContentType("PostgreSQLWire", "application/octet-stream");

    factory.setDocumentation("PostgreSQLWire", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `PostgreSQLWire` format serializes the result-set portion of the PostgreSQL wire protocol. It writes a
`RowDescription` message containing column names and types, followed by one `DataRow` message for each result row.
Values use their text representation, `NULL` values use the protocol's null-field encoding, and booleans are written as
`t` or `f`.

This is an output-only binary format intended for clients connected through ClickHouse's
[PostgreSQL interface](/concepts/features/interfaces/postgresql). The interface sets `PostgreSQLWire` as the session
default and uses it when the query doesn't include an explicit `FORMAT` clause. An explicit clause overrides the default;
other output formats don't produce a valid PostgreSQL result set. The interface writes the surrounding protocol messages,
such as authentication, command completion, and ready-for-query messages. `PostgreSQLWire` isn't intended for displaying
or storing query results as a standalone file.

## Example usage {#example-usage}

After enabling the PostgreSQL interface, use a compatible client to execute a query:

```shell
psql -p 9005 -h 127.0.0.1 -U alice -d default \
    -c "SELECT number, number % 2 = 0 AS even FROM numbers(3)"
```

Because the query doesn't specify a `FORMAT` clause, the interface sends the result using `PostgreSQLWire`.

## Format settings {#format-settings}

There are no user-configurable format settings.
)DOCS_MD"});
}

}
