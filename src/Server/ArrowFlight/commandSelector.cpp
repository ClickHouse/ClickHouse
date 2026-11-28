#include <Server/ArrowFlight/commandSelector.h>

#include <Interpreters/Context.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Common/config_version.h>
#include <Common/quoteString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include <arrow/flight/sql/FlightSql.pb.h>

namespace DB
{

namespace ArrowFlight
{

/// The version of the Arrow format and protocols this implementation follows, which `CommandGetSqlInfo` reports
/// as `FLIGHT_SQL_SERVER_ARROW_VERSION`: that of `contrib/arrow`, where `Flight.proto` and `FlightSql.proto` come from.
static constexpr auto ARROW_FORMAT_VERSION = "25.0.0";

FormatSettings flightFormatSettings(const ContextPtr & context)
{
    /// The canonical mapping: the defaults of the native `Arrow` writer, except that strings are `Utf8`. The only
    /// setting of the context followed is `output_format_arrow_unsupported_types`, see the declaration.
    FormatSettings result = getFormatSettings(context);
    const auto unsupported_types = result.arrow.output_unsupported_types;
    result.arrow = FormatSettings{}.arrow;
    result.arrow.output_string_as_string = true;
    result.arrow.output_unsupported_types = unsupported_types;
    return result;
}

Result<String> flightSchema(const Block & header, const ContextPtr & context, const SchemaModifier & schema_modifier)
{
    String schema = ArrowIPC::serializeSchemaMessage(header.getNames(), header.getDataTypes(), flightFormatSettings(context));
    if (!schema_modifier)
        return schema;
    FLIGHT_ASSIGN_OR_RAISE(auto modified, schema_modifier(ArrowIPC::parseSchemaMessage(schema)))
    return ArrowIPC::serializeSchemaMessage(modified);
}

TablePtr flightTable(const Block & header, const Chunks & chunks, const ContextPtr & context, const String & schema)
{
    auto table = std::make_shared<Table>(ArrowIPC::encodeStream(header, chunks, flightFormatSettings(context)));
    /// The schema may differ from the one the writer derives from the header, after a schema modifier.
    table->schema = std::move(ArrowIPC::splitStream(schema).front());
    return table;
}

namespace
{

/// A record batch built buffer by buffer, in the order of the IPC format: the field nodes in pre-order, and the
/// buffers of each field after its node.
class RecordBatchBuilder
{
public:
    void node(Int64 length, Int64 null_count = 0) { nodes.emplace_back(length, null_count); }

    void buffer(const void * data, size_t size)
    {
        const size_t offset = body.size();
        body.append(static_cast<const char *>(data), size);
        body.append((8 - size % 8) % 8, '\0');
        buffers.emplace_back(static_cast<Int64>(offset), static_cast<Int64>(size));
    }

    template <typename T>
    void buffer(const VectorWithMemoryTracking<T> & values) { buffer(values.data(), values.size() * sizeof(T)); }

    void emptyBuffer() { buffers.emplace_back(static_cast<Int64>(body.size()), 0); }

    /// The offsets buffer of an empty list or string field.
    void emptyOffsets()
    {
        const Int32 zero = 0;
        buffer(&zero, sizeof(zero));
    }

    ArrowIPC::IPCMessage finish(Int64 num_rows)
    {
        namespace flatbuf = ArrowIPC::flatbuf;
        flatbuffers::FlatBufferBuilder builder;
        auto nodes_offset = builder.CreateVectorOfStructs(nodes.data(), nodes.size());
        auto buffers_offset = builder.CreateVectorOfStructs(buffers.data(), buffers.size());
        auto record_batch = flatbuf::CreateRecordBatch(builder, num_rows, nodes_offset, buffers_offset);
        auto message = flatbuf::CreateMessage(
            builder, flatbuf::MetadataVersion_V5, flatbuf::MessageHeader_RecordBatch, record_batch.Union(), static_cast<Int64>(body.size()));
        builder.Finish(message);
        ArrowIPC::IPCMessage result;
        result.header.assign(reinterpret_cast<const char *>(builder.GetBufferPointer()), builder.GetSize());
        result.body = std::move(body);
        return result;
    }

private:
    VectorWithMemoryTracking<ArrowIPC::flatbuf::FieldNode> nodes;
    VectorWithMemoryTracking<ArrowIPC::flatbuf::Buffer> buffers;
    String body;
};

ArrowIPC::ArrowField makeField(String name, ArrowIPC::TypeKind kind, bool nullable, ArrowIPC::ArrowFields children = {})
{
    ArrowIPC::ArrowField field;
    field.name = std::move(name);
    field.nullable = nullable;
    field.type.kind = kind;
    field.type.children = std::move(children);
    return field;
}

ArrowIPC::ArrowField makeIntField(String name, int bit_width, bool is_signed, bool nullable)
{
    auto field = makeField(std::move(name), ArrowIPC::TypeKind::Int, nullable);
    field.type.bit_width = bit_width;
    field.type.is_signed = is_signed;
    return field;
}

/// The schema of the result of `CommandGetSqlInfo`, which the Arrow Flight SQL specification fixes:
/// `info_name: uint32 not null, value: dense_union<string_value: utf8, bool_value: bool, bigint_value: int64,
/// int32_bitmask: int32, string_list: list<utf8>, int32_to_int32_list_map: map<int32, list<int32>>> not null`.
/// The nullability is that of the fields the Apache Arrow library built for it.
ArrowIPC::ArrowSchema sqlInfoSchema()
{
    using ArrowIPC::TypeKind;
    auto int32_list = makeField("int32_to_int32_list_map", TypeKind::Map, false, {
        makeField("entries", TypeKind::Struct, false, {
            makeIntField("key", 32, true, false),
            makeField("value", TypeKind::List, true, {makeIntField("item", 32, true, true)}),
        }),
    });
    auto value = makeField("value", TypeKind::Union, true, {
        makeField("string_value", TypeKind::Utf8, false),
        makeField("bool_value", TypeKind::Bool, false),
        makeIntField("bigint_value", 64, true, false),
        makeIntField("int32_bitmask", 32, true, false),
        makeField("string_list", TypeKind::List, false, {makeField("item", TypeKind::Utf8, true)}),
        std::move(int32_list),
    });
    value.type.union_mode = ArrowIPC::flatbuf::UnionMode_Dense;
    value.type.union_type_ids = {0, 1, 2, 3, 4, 5};

    ArrowIPC::ArrowSchema schema;
    schema.fields.push_back(makeIntField("info_name", 32, false, true));
    schema.fields.push_back(std::move(value));
    return schema;
}

}

static Result<TablePtr> commandGetSqlInfo(const arrow::flight::protocol::sql::CommandGetSqlInfo & command, bool schema_only)
{
    using SqlInfo = arrow::flight::protocol::sql::SqlInfo;

    /// The values of the union, by alternative.
    VectorWithMemoryTracking<UInt32> info_names;
    VectorWithMemoryTracking<Int8> type_ids;
    VectorWithMemoryTracking<Int32> value_offsets;
    VectorWithMemoryTracking<Int32> string_offsets{0};
    String strings;
    VectorWithMemoryTracking<bool> booleans;
    VectorWithMemoryTracking<Int32> int32_values;

    static constexpr Int8 SQL_INFO_STRING = 0;
    static constexpr Int8 SQL_INFO_BOOLEAN = 1;
    static constexpr Int8 SQL_INFO_INT32 = 3;

    auto builder_string_append = [&](auto i, const std::string & v)
    {
        info_names.push_back(static_cast<UInt32>(i));
        type_ids.push_back(SQL_INFO_STRING);
        value_offsets.push_back(static_cast<Int32>(string_offsets.size() - 1));
        strings += v;
        string_offsets.push_back(static_cast<Int32>(strings.size()));
    };

    auto builder_boolean_append = [&](auto i, bool v)
    {
        info_names.push_back(static_cast<UInt32>(i));
        type_ids.push_back(SQL_INFO_BOOLEAN);
        value_offsets.push_back(static_cast<Int32>(booleans.size()));
        booleans.push_back(v);
    };

    auto builder_int32_append = [&](auto i, int32_t v)
    {
        info_names.push_back(static_cast<UInt32>(i));
        type_ids.push_back(SQL_INFO_INT32);
        value_offsets.push_back(static_cast<Int32>(int32_values.size()));
        int32_values.push_back(v);
    };

    if (!schema_only)
    {
        #define SQL_INFO_SELECTOR(INFO_NAME, BUILDER, ARG) \
                { INFO_NAME, [&](){ BUILDER(INFO_NAME, ARG); } }

        std::unordered_map<SqlInfo, std::function<void()>> selector
        {
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_NAME, builder_string_append, "ClickHouse"),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_VERSION, builder_string_append, VERSION_STRING),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION, builder_string_append, ARROW_FORMAT_VERSION),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY, builder_boolean_append, false),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_SQL, builder_boolean_append, true),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT, builder_boolean_append, false),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION, builder_string_append, ""),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION, builder_string_append, ""),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION, builder_int32_append, arrow::flight::protocol::sql::SQL_SUPPORTED_TRANSACTION_NONE),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_CANCEL, builder_boolean_append, true),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT, builder_int32_append, 0),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT, builder_int32_append, 0)
        };
        #undef SQL_INFO_SELECTOR

        if (command.info().empty())
        {
            for (const auto & [_, builder] : selector)
                builder();
        }
        else
        {
            for (const auto & info_name : command.info())
                if (auto it = selector.find(static_cast<SqlInfo>(info_name)); it != selector.end())
                    it->second();
        }
    }

    const auto num_rows = static_cast<Int64>(info_names.size());
    const auto num_strings = static_cast<Int64>(string_offsets.size() - 1);
    const auto num_booleans = static_cast<Int64>(booleans.size());
    VectorWithMemoryTracking<UInt8> boolean_bits((booleans.size() + 7) / 8, 0);
    for (size_t i = 0; i < booleans.size(); ++i)
        if (booleans[i])
            boolean_bits[i / 8] = static_cast<UInt8>(boolean_bits[i / 8] | (1u << (i % 8)));

    RecordBatchBuilder batch;
    /// info_name
    batch.node(num_rows);
    batch.emptyBuffer();
    batch.buffer(info_names);
    /// value: a dense union has no validity bitmap, but type ids and offsets.
    batch.node(num_rows);
    batch.buffer(type_ids);
    batch.buffer(value_offsets);
    /// string_value
    batch.node(num_strings);
    batch.emptyBuffer();
    batch.buffer(string_offsets);
    batch.buffer(strings.data(), strings.size());
    /// bool_value
    batch.node(num_booleans);
    batch.emptyBuffer();
    batch.buffer(boolean_bits);
    /// bigint_value
    batch.node(0);
    batch.emptyBuffer();
    batch.emptyBuffer();
    /// int32_bitmask
    batch.node(static_cast<Int64>(int32_values.size()));
    batch.emptyBuffer();
    batch.buffer(int32_values);
    /// string_list and its item
    batch.node(0);
    batch.emptyBuffer();
    batch.emptyOffsets();
    batch.node(0);
    batch.emptyBuffer();
    batch.emptyOffsets();
    batch.emptyBuffer();
    /// int32_to_int32_list_map, its entries, the key, the value and its item
    batch.node(0);
    batch.emptyBuffer();
    batch.emptyOffsets();
    batch.node(0);
    batch.emptyBuffer();
    batch.node(0);
    batch.emptyBuffer();
    batch.emptyBuffer();
    batch.node(0);
    batch.emptyBuffer();
    batch.emptyOffsets();
    batch.node(0);
    batch.emptyBuffer();
    batch.emptyBuffer();

    auto table = std::make_shared<Table>();
    table->schema = std::move(ArrowIPC::splitStream(ArrowIPC::serializeSchemaMessage(sqlInfoSchema())).front());
    table->num_rows = static_cast<size_t>(num_rows);
    auto message = batch.finish(num_rows);
    table->num_bytes = message.body.size();
    table->batches.push_back(std::move(message));
    return TablePtr(std::move(table));
}

static SQLSet commandGetCatalogs()
{
    return {"SELECT '' AS catalog_name FROM numbers(0)", {}, {}, /* skip_query_cache = */ true};
}

static SQLSet commandGetDbSchemas(const arrow::flight::protocol::sql::CommandGetDbSchemas & command)
{
    std::string where_expression;
    if (command.has_db_schema_filter_pattern())
        where_expression = " WHERE database LIKE " + quoteString(command.db_schema_filter_pattern());

    return {"SELECT NULL::Nullable(String) AS catalog_name, name AS db_schema_name FROM system.databases" + where_expression, {}, {}, /* skip_query_cache = */ true};
}

/// Splits a formatted expression list (e.g. from `system.tables.primary_key`) into
/// individual expressions. Unlike `splitByChar(',', ...)`, this correctly handles commas
/// inside parenthesized function arguments, square-bracketed array subscripts,
/// single-quoted string literals, and backtick-quoted identifiers.
static std::vector<std::string> splitExpressionList(std::string_view s)
{
    std::vector<std::string> result;
    int depth = 0;
    bool in_single_quote = false;
    bool in_backtick = false;
    bool escape_next = false;
    size_t expr_start = 0;

    auto flush = [&](size_t end)
    {
        auto token = s.substr(expr_start, end - expr_start);
        size_t first = token.find_first_not_of(' ');
        size_t last = token.find_last_not_of(' ');
        if (first != std::string_view::npos)
            result.emplace_back(token.substr(first, last - first + 1));
    };

    for (size_t i = 0; i < s.size(); ++i)
    {
        char c = s[i];

        if (escape_next)
        {
            escape_next = false;
            continue;
        }

        if (c == '\\' && (in_single_quote || in_backtick))
        {
            escape_next = true;
            continue;
        }

        if (c == '\'' && !in_backtick)
        {
            in_single_quote = !in_single_quote;
            continue;
        }

        if (c == '`' && !in_single_quote)
        {
            in_backtick = !in_backtick;
            continue;
        }

        if (in_single_quote || in_backtick)
            continue;

        if (c == '(' || c == '[')
            ++depth;
        else if (c == ')' || c == ']')
            --depth;
        else if (c == ',' && depth == 0)
        {
            flush(i);
            expr_start = i + 1;
        }
    }

    flush(s.size());
    return result;
}

static SQLSet commandGetPrimaryKeys(const arrow::flight::protocol::sql::CommandGetPrimaryKeys & command)
{
    std::string where_expression = " WHERE" +
        (command.has_db_schema() ? (" database = " + quoteString(command.db_schema()) + " AND") : "") +
        " name = " + quoteString(command.table());

    auto sql =
        "SELECT "
            "materialize(NULL::Nullable(String)) AS catalog_name, "
            "database AS schema_name, "
            "name AS table_name, "
            "primary_key AS column_name, "
            "materialize(0::Int32) AS key_seq, "
            "materialize(NULL::Nullable(String)) AS pk_name "
        "FROM system.tables"
        + where_expression;

    auto block_modifier = [](ContextPtr, Block & block)
    {
        size_t num_rows = block.rows();
        if (num_rows == 0)
            return;

        const size_t column_name_pos = 3;
        const size_t key_seq_pos = 4;
        const size_t num_columns = block.columns();

        auto & pk_column = block.getByPosition(column_name_pos);
        auto pk_col = pk_column.column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();

        MutableColumns new_columns;
        for (size_t col = 0; col < num_columns; ++col)
            new_columns.push_back(block.getByPosition(col).column->cloneEmpty());

        for (size_t i = 0; i < num_rows; ++i)
        {
            auto expressions = splitExpressionList(pk_col->getDataAt(i));

            Int32 key_seq = 1;
            for (const auto & expr : expressions)
            {
                for (size_t col = 0; col < num_columns; ++col)
                {
                    if (col == column_name_pos)
                        new_columns[col]->insert(expr);
                    else if (col == key_seq_pos)
                        new_columns[col]->insert(key_seq);
                    else
                        new_columns[col]->insertFrom(*block.getByPosition(col).column, i);
                }
                ++key_seq;
            }
        }

        block.setColumns(std::move(new_columns));
    };

    return {sql, {}, block_modifier, /* skip_query_cache = */ true};
}

[[clang::no_destroy]] const static std::vector<std::pair<std::string, std::string>> engine_to_type =
{
    // Log tables
    {"Log", "LOG TABLE"},
    {"StripeLog", "LOG TABLE"},
    {"TinyLog", "LOG TABLE"},

    // Memory tables
    {"Buffer", "MEMORY TABLE"},
    {"Memory", "MEMORY TABLE"},
    {"Set", "MEMORY TABLE"},

    // Views
    {"View", "VIEW"},
    {"LiveView", "VIEW"},
    {"MaterializedView", "MATERIALIZED VIEW"},

    // Dictionary
    {"Dictionary", "DICTIONARY"},

    // Remote/External tables
    {"AzureBlobStorage", "REMOTE TABLE"},
    {"AzureQueue", "REMOTE TABLE"},
    {"ArrowFlight", "REMOTE TABLE"},
    {"BigQuery", "REMOTE TABLE"},
    {"DeltaLake", "REMOTE TABLE"},
    {"DeltaLakeAzure", "REMOTE TABLE"},
    {"DeltaLakeLocal", "REMOTE TABLE"},
    {"DeltaLakeS3", "REMOTE TABLE"},
    {"Distributed", "REMOTE TABLE"},
    {"Elasticsearch", "REMOTE TABLE"},
    {"GCS", "REMOTE TABLE"},
    {"HDFS", "REMOTE TABLE"},
    {"Hive", "REMOTE TABLE"},
    {"Hudi", "REMOTE TABLE"},
    {"Iceberg", "REMOTE TABLE"},
    {"IcebergAzure", "REMOTE TABLE"},
    {"IcebergHDFS", "REMOTE TABLE"},
    {"IcebergLocal", "REMOTE TABLE"},
    {"IcebergS3", "REMOTE TABLE"},
    {"Paimon", "REMOTE TABLE"},
    {"PaimonAzure", "REMOTE TABLE"},
    {"PaimonHDFS", "REMOTE TABLE"},
    {"PaimonLocal", "REMOTE TABLE"},
    {"PaimonS3", "REMOTE TABLE"},
    {"JDBC", "REMOTE TABLE"},
    {"Kafka", "REMOTE TABLE"},
    {"MaterializedPostgreSQL", "REMOTE TABLE"},
    {"MongoDB", "REMOTE TABLE"},
    {"MySQL", "REMOTE TABLE"},
    {"NATS", "REMOTE TABLE"},
    {"ODBC", "REMOTE TABLE"},
    {"OSS", "REMOTE TABLE"},
    {"PostgreSQL", "REMOTE TABLE"},
    {"Pulsar", "REMOTE TABLE"},
    {"QueryRunner", "REMOTE TABLE"},
    {"RabbitMQ", "REMOTE TABLE"},
    {"Redis", "REMOTE TABLE"},
    {"Remote", "REMOTE TABLE"},
    {"RemoteSecure", "REMOTE TABLE"},
    {"S3", "REMOTE TABLE"},
    {"S3Queue", "REMOTE TABLE"},
    {"URL", "REMOTE TABLE"},
    {"YTsaurus", "REMOTE TABLE"},

    // Regular tables (MergeTree family and others)
    {"AggregatingMergeTree", "TABLE"},
    {"Alias", "TABLE"},
    {"CoalescingMergeTree", "TABLE"},
    {"CollapsingMergeTree", "TABLE"},
    {"EmbeddedRocksDB", "TABLE"},
    {"Executable", "TABLE"},
    {"ExecutablePool", "TABLE"},
    {"GraphiteMergeTree", "TABLE"},
    {"Join", "TABLE"},
    {"KeeperMap", "TABLE"},
    {"Merge", "TABLE"},
    {"MergeTree", "TABLE"},
    {"ReplacingMergeTree", "TABLE"},
    {"ReplicatedAggregatingMergeTree", "TABLE"},
    {"ReplicatedCoalescingMergeTree", "TABLE"},
    {"ReplicatedCollapsingMergeTree", "TABLE"},
    {"ReplicatedGraphiteMergeTree", "TABLE"},
    {"ReplicatedMergeTree", "TABLE"},
    {"ReplicatedReplacingMergeTree", "TABLE"},
    {"ReplicatedSummingMergeTree", "TABLE"},
    {"ReplicatedVersionedCollapsingMergeTree", "TABLE"},
    {"SummingMergeTree", "TABLE"},
    {"VersionedCollapsingMergeTree", "TABLE"},
    {"COSN", "TABLE"},
    {"SharedAggregatingMergeTree", "TABLE"},
    {"SharedCoalescingMergeTree", "TABLE"},
    {"SharedCollapsingMergeTree", "TABLE"},
    {"SharedGraphiteMergeTree", "TABLE"},
    {"SharedJoin", "TABLE"},
    {"SharedMergeTree", "TABLE"},
    {"SharedReplacingMergeTree", "TABLE"},
    {"SharedSet", "TABLE"},
    {"SharedSummingMergeTree", "TABLE"},
    {"SharedVersionedCollapsingMergeTree", "TABLE"},

    // Special
    {"TimeSeries", "TABLE"},
    {"Null", "TABLE"},
    {"Loop", "TABLE"},
    {"SQLite", "TABLE"},
    {"File", "TABLE"},
    {"FileLog", "TABLE"},
    {"Filesystem", "TABLE"},
    {"GenerateRandom", "TABLE"},
    {"FuzzJSON", "TABLE"},
    {"FuzzQuery", "TABLE"},
    {"Obfuscate", "TABLE"},
};

const static std::string & getTableTypeMap()
{
    const static auto res = []()
    {
        auto args = std::ranges::fold_left(engine_to_type, std::pair<std::string, std::string>(),
            [](auto acc, const auto & val)
            {
                if (!acc.first.empty())
                {
                    acc.first += ", ";
                    acc.second += ", ";
                }
                acc.first += "'" + val.first + "'";
                acc.second += "'" + val.second + "'";
                return acc;
            }
        );
        return "[" + args.first + "], [" + args.second + "]";
    }();

    return res;
}

static SQLSet commandGetTables(const arrow::flight::protocol::sql::CommandGetTables & command)
{
    std::vector<std::string> where;
    if (command.has_db_schema_filter_pattern())
        where.push_back("db_schema_name LIKE " + quoteString(command.db_schema_filter_pattern()));
    if (command.has_table_name_filter_pattern())
        where.push_back("table_name LIKE " + quoteString(command.table_name_filter_pattern()));
    if (command.table_types_size())
    {
        where.push_back(
            "table_type IN [" +
            boost::algorithm::join(
                command.table_types()
                    | boost::adaptors::transformed([](const auto & table_type) { return quoteString(table_type); }),
                ", ") +
            "]"
        );
    }
    auto where_expression = where.empty() ? "" : " WHERE " + boost::algorithm::join(where, " AND ");

    if (!command.include_schema())
    {
        return {
            "SELECT "
                "catalog_name, "
                "db_schema_name, "
                "table_name, "
                "table_type "
            "FROM ("
                "SELECT "
                    "NULL::Nullable(String) AS catalog_name, "
                    "database::Nullable(String) AS db_schema_name, "
                    "table AS table_name, "
                    "transform(engine, " + getTableTypeMap() + ", 'UNKNOWN TABLE TYPE') AS table_type "
                "FROM system.tables"
            ")"
            + where_expression,
            {},
            {},
            /* skip_query_cache = */ true
        };
    }

    auto sql =
        "SELECT "
            "catalog_name, "
            "db_schema_name, "
            "table_name, "
            "table_type, "
            "table_schema "
        "FROM ("
            "SELECT "
                "NULL::Nullable(String) AS catalog_name, "
                "left.database::Nullable(String) AS db_schema_name, "
                "left.table AS table_name, "
                "transform(left.engine, " + getTableTypeMap() + ", 'UNKNOWN TABLE TYPE') AS table_type, "
                "ifNull(right.table_schema, CAST([], 'Array(Tuple(String, String))')) AS table_schema "
            "FROM system.tables AS left "
            "LEFT JOIN "
            "("
                "SELECT "
                    "database, "
                    "table, "
                    "arraySort((x, y) -> y, groupArray((name, type)), groupArray(position)) AS table_schema "
                "FROM system.columns "
                "GROUP BY "
                    "database, "
                    "table"
            ") AS right ON left.database = right.database AND left.table = right.table"
        ")"
        + where_expression;

    auto schema_modifier = [](ArrowIPC::ArrowSchema table_schema) -> Result<ArrowIPC::ArrowSchema>
    {
        /// `table_schema` holds serialized schemas, which are `bytes` in the specification.
        auto & field = table_schema.fields.at(4);
        field.type = ArrowIPC::ArrowType{};
        field.type.kind = ArrowIPC::TypeKind::Binary;
        field.custom_metadata.clear();
        return table_schema;
    };

    auto block_modifier = [](ContextPtr query_context, Block & block)
    {
        const size_t table_schema_pos = 4;
        const auto & table_schema_column = block.getByPosition(table_schema_pos);
        auto new_column = ColumnString::create();
        auto col = table_schema_column.column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
        const auto & arr = typeid_cast<const ColumnArray &>(*col);
        const auto & tuple_col = typeid_cast<const ColumnTuple &>(arr.getData());
        const auto & name_col = typeid_cast<const ColumnString &>(tuple_col.getColumn(0));
        const auto & type_col = typeid_cast<const ColumnString &>(tuple_col.getColumn(1));
        const auto format_settings = flightFormatSettings(query_context);
        for (size_t i = 0; i < col->size(); ++i)
        {
            Names names;
            DataTypes types;
            auto start = i ? arr.getOffsets()[static_cast<ssize_t>(i) - 1] : 0;
            auto end = arr.getOffsets()[i];
            for (size_t j = 0; j < end - start; ++j)
            {
                names.emplace_back(name_col.getDataAt(start + j));
                types.push_back(DataTypeFactory::instance().get(String(type_col.getDataAt(start + j))));
            }
            const auto serialized = ArrowIPC::serializeSchemaMessage(names, types, format_settings);
            new_column->insertData(serialized.data(), serialized.size());
        }

        block.getByPosition(table_schema_pos) = ColumnWithTypeAndName(
            std::move(new_column),
            std::make_shared<DataTypeString>(),
            table_schema_column.name);
    };

    return {sql, schema_modifier, block_modifier, /* skip_query_cache = */ true};
}

static SQLSet commandGetTableTypes()
{
    return {"SELECT DISTINCT transform(name, " + getTableTypeMap() + ", 'UNKNOWN TABLE TYPE') AS table_type FROM system.table_engines", {}, {}, /* skip_query_cache = */ true};
}

static CommandSelectorResult commandStatementQuery(const arrow::flight::protocol::sql::CommandStatementQuery & command)
{
    if (command.has_transaction_id())
        return Status::NotImplemented("CommandStatementQuery: transaction_id is not supported");
    if (command.query().empty())
        return Status::Invalid("CommandStatementQuery: query must not be empty");
    return SQLSet{command.query(), {}, {}};
}

static CommandSelectorResult commandStatementUpdate(const arrow::flight::protocol::sql::CommandStatementUpdate & command)
{
    if (command.has_transaction_id())
        return Status::NotImplemented("CommandStatementUpdate: transaction_id is not supported");
    if (command.query().empty())
        return Status::Invalid("CommandStatementUpdate: query must not be empty");
    return SQLSet{command.query(), {}, {}};
}

static CommandSelectorResult commandStatementIngest(const arrow::flight::protocol::sql::CommandStatementIngest & command)
{
    using CommandStatementIngest = arrow::flight::protocol::sql::CommandStatementIngest;

    if (command.has_table_definition_options())
    {
        const auto & options = command.table_definition_options();
        if (options.if_not_exist() != CommandStatementIngest::TableDefinitionOptions::TABLE_NOT_EXIST_OPTION_FAIL ||
            options.if_exists() != CommandStatementIngest::TableDefinitionOptions::TABLE_EXISTS_OPTION_APPEND)
        {
            return Status::NotImplemented("Only appending to existing tables is supported (TABLE_NOT_EXIST_OPTION_FAIL + TABLE_EXISTS_OPTION_APPEND)");
        }
    }

    if (command.has_catalog())
        return Status::NotImplemented("Catalogs are not supported.");

    if (command.temporary())
        return Status::NotImplemented("Implicit temporary tables are not supported.");

    if (command.has_transaction_id())
        return Status::NotImplemented("CommandStatementIngest: transaction_id is not supported");

    std::string schema_string;
    if (command.has_schema())
    {
        if (!isValidIdentifier(command.schema()))
            return Status::Invalid("Invalid schema name: ", command.schema());
        schema_string = backQuoteIfNeed(command.schema()) + ".";
    }

    if (!isValidIdentifier(command.table()))
        return Status::Invalid("Invalid table name: ", command.table());

    return SQLSet{"INSERT INTO " + schema_string + backQuoteIfNeed(command.table()) + " FORMAT Arrow", {}, {}};
}

static std::optional<CommandSelectorResult> commandSelectorImpl(const google::protobuf::Any & any_msg, bool schema_only)
{
    if (any_msg.Is<arrow::flight::protocol::sql::CommandGetSqlInfo>())
    {
        arrow::flight::protocol::sql::CommandGetSqlInfo command;
        if (!any_msg.UnpackTo(&command))
            return Status::SerializationError("Deserialization of sql::CommandGetSqlInfo failed.");
        return commandGetSqlInfo(command, schema_only);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetCrossReference>())
    {
        return Status::NotImplemented("sql::CommandGetCrossReference is not supported");
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetCatalogs>())
    {
        return commandGetCatalogs();
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetDbSchemas>())
    {
        arrow::flight::protocol::sql::CommandGetDbSchemas command;
        if (!any_msg.UnpackTo(&command))
            return Status::SerializationError("Deserialization of sql::CommandGetDbSchemas failed.");
        return commandGetDbSchemas(command);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetExportedKeys>())
    {
        return Status::NotImplemented("sql::CommandGetExportedKeys is not supported");
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetImportedKeys>())
    {
        return Status::NotImplemented("sql::CommandGetImportedKeys is not supported");
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetPrimaryKeys>())
    {
        arrow::flight::protocol::sql::CommandGetPrimaryKeys command;
        if (!any_msg.UnpackTo(&command))
            return Status::SerializationError("Deserialization of sql::CommandGetPrimaryKeys failed.");
        return commandGetPrimaryKeys(command);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetTables>())
    {
        arrow::flight::protocol::sql::CommandGetTables command;
        if (!any_msg.UnpackTo(&command))
            return Status::SerializationError("Deserialization of sql::CommandGetTables failed.");
        return commandGetTables(command);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetTableTypes>())
    {
        return commandGetTableTypes();
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandStatementQuery>())
    {
        arrow::flight::protocol::sql::CommandStatementQuery command;
        if (!any_msg.UnpackTo(&command))
            return Status::SerializationError("Deserialization of sql::CommandStatementQuery failed.");
        return commandStatementQuery(command);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandStatementUpdate>())
    {
        arrow::flight::protocol::sql::CommandStatementUpdate command;
        if (!any_msg.UnpackTo(&command))
            return Status::SerializationError("Deserialization of sql::CommandStatementUpdate failed.");
        return commandStatementUpdate(command);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandStatementIngest>())
    {
        using CommandStatementIngest = arrow::flight::protocol::sql::CommandStatementIngest;
        CommandStatementIngest command;
        if (!any_msg.UnpackTo(&command))
            return Status::SerializationError("Deserialization of sql::CommandStatementIngest failed.");
        return commandStatementIngest(command);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandPreparedStatementQuery>())
    {
        arrow::flight::protocol::sql::CommandPreparedStatementQuery command;
        if (!any_msg.UnpackTo(&command))
            return Status::SerializationError("Deserialization of sql::CommandPreparedStatementQuery failed.");
        if (command.prepared_statement_handle().empty())
            return Status::Invalid("CommandPreparedStatementQuery: prepared_statement_handle must not be empty");
        return SQLSet{command.prepared_statement_handle(), {}, {}};
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandPreparedStatementUpdate>())
    {
        arrow::flight::protocol::sql::CommandPreparedStatementUpdate command;
        if (!any_msg.UnpackTo(&command))
            return Status::SerializationError("Deserialization of sql::CommandPreparedStatementUpdate failed.");
        if (command.prepared_statement_handle().empty())
            return Status::Invalid("CommandPreparedStatementUpdate: prepared_statement_handle must not be empty");
        return SQLSet{command.prepared_statement_handle(), {}, {}};
    }
    else
    {
        if (isArrowFlightSql(any_msg))
            return Status::NotImplemented("Command is not implemented: ", any_msg.ShortDebugString());
    }

    return std::nullopt;
}

CommandSelectorResult commandSelector(const std::string & cmd, bool schema_only)
{
    if (cmd.empty())
        return Status::Invalid("Empty command.");
    if (cmd.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
        return Status::Invalid("Command payload is too large.");
    if (google::protobuf::Any any_msg; any_msg.ParseFromArray(cmd.data(), static_cast<int>(cmd.size())))
        if (auto result = commandSelectorImpl(any_msg, schema_only))
            return *result;
    return SQLSet{cmd, {}, {}};
}

}

}
