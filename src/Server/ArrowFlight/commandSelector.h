#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCMessages.h>
#include <Server/ArrowFlight/FlightStatus.h>

#include <arrow/flight/Flight.pb.h>
#include <google/protobuf/any.pb.h>

#include <functional>
#include <variant>


namespace DB
{

namespace ArrowFlight
{

/// The data of a flight, encoded in the Arrow IPC format: the schema and the record batches.
using Table = ArrowIPC::EncodedStream;
using TablePtr = std::shared_ptr<const Table>;

using SchemaModifier = std::function<Result<ArrowIPC::ArrowSchema>(ArrowIPC::ArrowSchema)>;
using BlockModifier = std::function<void(ContextPtr, Block &)>;

struct SQLSet
{
    std::string sql;
    SchemaModifier schema_modifier;
    BlockModifier block_modifier;
    bool skip_query_cache = false;
};

struct CommandSelectorResult : private std::variant<SQLSet, Result<TablePtr>>
{
    // NOLINTNEXTLINE(google-explicit-constructor)
    CommandSelectorResult(const SQLSet & sql_set) : std::variant<SQLSet, Result<TablePtr>>(sql_set) {}
    // NOLINTNEXTLINE(google-explicit-constructor)
    CommandSelectorResult(const Result<TablePtr> & table) : std::variant<SQLSet, Result<TablePtr>>(table) {}
    // NOLINTNEXTLINE(google-explicit-constructor)
    CommandSelectorResult(const Status & status) : std::variant<SQLSet, Result<TablePtr>>(Result<TablePtr>(status)) {}
    // NOLINTNEXTLINE(google-explicit-constructor)
    CommandSelectorResult(TablePtr table) : std::variant<SQLSet, Result<TablePtr>>(Result<TablePtr>(std::move(table))) {}

    SQLSet * getSQLSet()
    {
        return std::get_if<SQLSet>(this);
    }

    Result<TablePtr> * getTable()
    {
        return std::get_if<Result<TablePtr>>(this);
    }
};

inline bool isArrowFlightSql(const google::protobuf::Any & any_msg)
{
    const auto & type_url = any_msg.type_url();
    const auto slash_pos = type_url.find_last_of('/');
    const auto type_name = (slash_pos == std::string::npos) ? type_url : type_url.substr(slash_pos + 1);
    return type_name.starts_with("arrow.flight.protocol.sql.");
}

inline bool cmdIsArrowFlightSql(const std::string & cmd)
{
    if (cmd.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
        return false;
    google::protobuf::Any any_msg;
    if (!any_msg.ParseFromArray(cmd.data(), static_cast<int>(cmd.size())))
        return false;
    return isArrowFlightSql(any_msg);
}

inline bool flightDescriptorIsArrowFlightSqlCommand(const arrow::flight::protocol::FlightDescriptor & descriptor)
{
    if (descriptor.type() != arrow::flight::protocol::FlightDescriptor::CMD)
        return false;
    return cmdIsArrowFlightSql(descriptor.cmd());
}

/// The format settings every Arrow Flight surface encodes with, resolved from a query context.
///
/// Arrow Flight pins the canonical Arrow mapping and follows exactly one output setting: what to do with a type
/// that has no canonical mapping. The other `output_format_arrow_*` settings are deliberately not read from the
/// context, so a Flight schema does not track `FORMAT Arrow` for the same query.
///
/// That is a conformance requirement rather than a simplification. This same conversion builds the Flight SQL
/// metadata responses, whose schemas the specification fixes - `CommandGetTables` is `catalog_name: utf8,
/// db_schema_name: utf8, table_name: utf8 not null, table_type: utf8 not null, table_schema: bytes not null` - so
/// honoring `output_format_arrow_string_as_string = 0` would answer a driver with `binary` where the specification
/// requires `utf8`, and would change the per-table schema ClickHouse advertises inside `table_schema`.
/// `output_format_arrow_date_as_uint16` is a ClickHouse backward-compatibility knob in the same way: a client
/// handed `uint16` for a `Date` has no way to tell it is a date. The schema also travels separately from the data -
/// `GetFlightInfo`, `GetSchema` and `DoGet` are distinct calls, each building its own query context from the
/// session - so every setting that can move the schema is another way for the advertised schema and the delivered
/// stream to disagree.
///
/// `output_format_arrow_unsupported_types` is the exception because `JSON`, `Dynamic`, `QBit` and
/// `AggregateFunction` have no canonical Arrow mapping at all. ClickHouse has to invent one, only the user can say
/// whether they want text or bytes, and the `clickhouse.opaque` field metadata tells the client that the column is
/// an invention rather than a native Arrow type.
FormatSettings flightFormatSettings(const ContextPtr & context);

/// The schema of the result of a query with the header `header`, serialized as Arrow Flight does, after
/// `schema_modifier`.
Result<String> flightSchema(const Block & header, const ContextPtr & context, const SchemaModifier & schema_modifier = {});

/// Encodes chunks of `header` as the data of a flight with the schema `schema` (see `flightSchema`).
TablePtr flightTable(const Block & header, const Chunks & chunks, const ContextPtr & context, const String & schema);

/// commandSelector accepts arrow flight sql command buffer and produces either resulting table
/// (and if schema_only == true then table can be empty - only schema is requested) or set of sql query - which will be executed,
/// and, if resulting table requires modification, possible schema_modifier and block_modifier - they should consistently
/// manipulate schema and blocks to produce compatible results. In case command is invalid, an error should be returned
/// in Result<TablePtr> variant - in practice, return Status::<...> - CommandSelectorResult has an implicit constructor
/// for Status.
CommandSelectorResult commandSelector(const std::string & cmd, bool schema_only = false);

}

}
