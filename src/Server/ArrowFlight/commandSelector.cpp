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
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include <arrow/ipc/writer.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_union.h>
#include <arrow/flight/sql/protocol_internal.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool output_format_arrow_unsupported_types_as_binary;
}

namespace ArrowFlight
{

static arrow::Result<std::shared_ptr<arrow::Table>> commandGetSqlInfo(const arrow::flight::protocol::sql::CommandGetSqlInfo & command, bool schema_only)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    auto string_builder = std::make_shared<arrow::StringBuilder>();
    auto boolean_builder = std::make_shared<arrow::BooleanBuilder>();
    auto int64_builder = std::make_shared<arrow::Int64Builder>();
    auto int32_builder = std::make_shared<arrow::Int32Builder>();

    // string_list: list<item: string> not null
    auto string_list_type = arrow::list(arrow::utf8());
    auto string_list_builder = std::make_shared<arrow::ListBuilder>(pool, std::make_shared<arrow::StringBuilder>(), string_list_type);

    // int32_to_int32_list_map: map<int32, list<item: int32>> not null
    auto value_type = arrow::list(arrow::int32());
    auto value_builder = std::make_shared<arrow::ListBuilder>(pool, std::make_shared<arrow::Int32Builder>(), value_type);
    auto int32_to_int32_list_map_type = arrow::map(arrow::int32(), value_type);
    auto int32_to_int32_list_map_builder = std::make_shared<arrow::MapBuilder>(pool, std::make_shared<arrow::Int32Builder>(), value_builder, int32_to_int32_list_map_type);

    // dense_union
    auto dense_union_type = arrow::dense_union(
        {
            std::make_shared<arrow::Field>("string_value", arrow::utf8(), false),
            std::make_shared<arrow::Field>("bool_value", arrow::boolean(), false),
            std::make_shared<arrow::Field>("bigint_value", arrow::int64(), false),
            std::make_shared<arrow::Field>("int32_bitmask", arrow::int32(), false),
            std::make_shared<arrow::Field>("string_list", string_list_type, false),
            std::make_shared<arrow::Field>("int32_to_int32_list_map", int32_to_int32_list_map_type, false)
        });

    auto dense_union_builder = std::make_shared<arrow::DenseUnionBuilder>(
        pool,
        std::vector<std::shared_ptr<arrow::ArrayBuilder>>{
            string_builder,
            boolean_builder,
            int64_builder,
            int32_builder,
            string_list_builder,
            int32_to_int32_list_map_builder
        },
        dense_union_type);

    using SqlInfo = arrow::flight::protocol::sql::SqlInfo;

    auto info_name_builder = std::make_shared<arrow::UInt32Builder>();

    static const size_t SQL_INFO_STRING = 0;
    static const size_t SQL_INFO_BOOLEAN = 1;
    static const size_t SQL_INFO_INT64 = 2;
    static const size_t SQL_INFO_INT32 = 3;

    auto builder_string_append = [&](auto i, const std::string & v)
    {
        ARROW_RETURN_NOT_OK(info_name_builder->Append(i));
        ARROW_RETURN_NOT_OK(dense_union_builder->Append(SQL_INFO_STRING));
        return string_builder->Append(v);
    };

    auto builder_boolean_append = [&](auto i, bool v)
    {
        ARROW_RETURN_NOT_OK(info_name_builder->Append(i));
        ARROW_RETURN_NOT_OK(dense_union_builder->Append(SQL_INFO_BOOLEAN));
        return boolean_builder->Append(v);
    };

    [[maybe_unused]] auto builder_int64_append = [&](auto i, int64_t v)
    {
        ARROW_RETURN_NOT_OK(info_name_builder->Append(i));
        ARROW_RETURN_NOT_OK(dense_union_builder->Append(SQL_INFO_INT64));
        return int64_builder->Append(v);
    };

    auto builder_int32_append = [&](auto i, int32_t v)
    {
        ARROW_RETURN_NOT_OK(info_name_builder->Append(i));
        ARROW_RETURN_NOT_OK(dense_union_builder->Append(SQL_INFO_INT32));
        return int32_builder->Append(v);
    };

    if (!schema_only)
    {
        #define SQL_INFO_SELECTOR(INFO_NAME, BUILDER, ARG) \
                { INFO_NAME, [&](){ return BUILDER(INFO_NAME, ARG); } }

        std::unordered_map<SqlInfo, std::function<arrow::Status()>> selector
        {
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_NAME, builder_string_append, "ClickHouse"),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_VERSION, builder_string_append, VERSION_STRING),
            SQL_INFO_SELECTOR(SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION, builder_string_append, ARROW_VERSION_STRING),
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
                ARROW_RETURN_NOT_OK(builder());
        }
        else
        {
            for (const auto & info_name : command.info())
                if (auto it = selector.find(static_cast<SqlInfo>(info_name)); it != selector.end())
                    ARROW_RETURN_NOT_OK(it->second());
        }
    }

    // Schema for table
    std::shared_ptr<arrow::Schema> table_schema = arrow::schema({
        arrow::field("info_name", arrow::uint32()),
        arrow::field("value", dense_union_type)
    });

    auto info_name = info_name_builder->Finish();
    ARROW_RETURN_NOT_OK(info_name);

    auto value = dense_union_builder->Finish();
    ARROW_RETURN_NOT_OK(value);

    return arrow::Table::Make(table_schema, {info_name.ValueUnsafe(), value.ValueUnsafe()});
}

static SQLSet commandGetCatalogs()
{
    return {"SELECT '' AS catalog_name FROM numbers(0)", {}, {}};
}

static SQLSet commandGetDbSchemas(const arrow::flight::protocol::sql::CommandGetDbSchemas & command)
{
    std::string where_expression;
    if (command.has_db_schema_filter_pattern())
        where_expression = " WHERE database LIKE " + quoteString(command.db_schema_filter_pattern());

    return {"SELECT NULL::Nullable(String) AS catalog_name, name AS db_schema_name FROM system.databases" + where_expression, {}, {}};
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
        auto pk_col = pk_column.column->convertToFullIfNeeded();

        std::vector<MutableColumnPtr> new_columns;
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

    return {sql, {}, block_modifier};
}

const static std::vector<std::pair<std::string, std::string>> engine_to_type =
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
    {"WindowView", "VIEW"},

    // Dictionary
    {"Dictionary", "DICTIONARY"},

    // Remote/External tables
    {"AzureBlobStorage", "REMOTE TABLE"},
    {"AzureQueue", "REMOTE TABLE"},
    {"ArrowFlight", "REMOTE TABLE"},
    {"DeltaLake", "REMOTE TABLE"},
    {"DeltaLakeAzure", "REMOTE TABLE"},
    {"DeltaLakeLocal", "REMOTE TABLE"},
    {"DeltaLakeS3", "REMOTE TABLE"},
    {"Distributed", "REMOTE TABLE"},
    {"GCS", "REMOTE TABLE"},
    {"HDFS", "REMOTE TABLE"},
    {"Hive", "REMOTE TABLE"},
    {"Hudi", "REMOTE TABLE"},
    {"Iceberg", "REMOTE TABLE"},
    {"IcebergAzure", "REMOTE TABLE"},
    {"IcebergHDFS", "REMOTE TABLE"},
    {"IcebergLocal", "REMOTE TABLE"},
    {"IcebergS3", "REMOTE TABLE"},
    {"JDBC", "REMOTE TABLE"},
    {"Kafka", "REMOTE TABLE"},
    {"MaterializedPostgreSQL", "REMOTE TABLE"},
    {"MongoDB", "REMOTE TABLE"},
    {"MySQL", "REMOTE TABLE"},
    {"NATS", "REMOTE TABLE"},
    {"ODBC", "REMOTE TABLE"},
    {"OSS", "REMOTE TABLE"},
    {"PostgreSQL", "REMOTE TABLE"},
    {"RabbitMQ", "REMOTE TABLE"},
    {"Redis", "REMOTE TABLE"},
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
            {}
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

    auto schema_modifier = [](std::shared_ptr<arrow::Schema> table_schema)
    {
        const auto & table_schema_field = table_schema->field(4);
        return table_schema->SetField(4, std::make_shared<arrow::Field>(table_schema_field->name(), arrow::binary(), table_schema_field->nullable()));
    };

    auto block_modifier = [](ContextPtr query_context, Block & block)
    {
        const size_t table_schema_pos = 4;
        const auto & table_schema_column = block.getByPosition(table_schema_pos);
        auto new_column = ColumnString::create();
        auto col = table_schema_column.column->convertToFullIfNeeded();
        const auto & arr = typeid_cast<const ColumnArray &>(*col);
        const auto & tuple_col = typeid_cast<const ColumnTuple &>(arr.getData());
        const auto & name_col = typeid_cast<const ColumnString &>(tuple_col.getColumn(0));
        const auto & type_col = typeid_cast<const ColumnString &>(tuple_col.getColumn(1));
        for (size_t i = 0; i < col->size(); ++i)
        {
            ColumnsWithTypeAndName table_columns;
            auto start = i ? arr.getOffsets()[i - 1] : 0;
            auto end = arr.getOffsets()[i];
            for (size_t j = 0; j < end - start; ++j)
            {
                const auto name = name_col.getDataAt(start + j);
                const auto type = type_col.getDataAt(start + j);

                auto data_type = DataTypeFactory::instance().get(String(type));
                table_columns.emplace_back(nullptr, data_type, String(name));
            }
            auto table_schema = CHColumnToArrowColumn::calculateArrowSchema(
                table_columns, "Arrow", nullptr,
                {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]});
            auto serialized_res = arrow::ipc::SerializeSchema(*table_schema, arrow::default_memory_pool());
            if (!serialized_res.ok())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to serialize Arrow schema: {}", serialized_res.status().ToString());
            const auto & serialized_buffer = serialized_res.ValueUnsafe();
            new_column->insertData(reinterpret_cast<const char *>(serialized_buffer->data()), serialized_buffer->size());
        }

        block.getByPosition(table_schema_pos) = ColumnWithTypeAndName(
            std::move(new_column),
            std::make_shared<DataTypeString>(),
            table_schema_column.name);
    };

    return {sql, schema_modifier, block_modifier};
}

static SQLSet commandGetTableTypes()
{
    return {"SELECT DISTINCT transform(name, " + getTableTypeMap() + ", 'UNKNOWN TABLE TYPE') AS table_type FROM system.table_engines", {}, {}};
}

static CommandSelectorResult commandStatementQuery(const arrow::flight::protocol::sql::CommandStatementQuery & command)
{
    if (command.query().empty())
        return arrow::Status::Invalid("CommandStatementQuery: query must not be empty");
    return SQLSet{command.query(), {}, {}};
}

static CommandSelectorResult commandStatementUpdate(const arrow::flight::protocol::sql::CommandStatementUpdate & command)
{
    if (command.query().empty())
        return arrow::Status::Invalid("CommandStatementUpdate: query must not be empty");
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
            return arrow::Status::NotImplemented("Only appending to existing tables is supported (TABLE_NOT_EXIST_OPTION_FAIL + TABLE_EXISTS_OPTION_APPEND)");
        }
    }

    if (command.has_catalog())
        return arrow::Status::NotImplemented("Catalogs are not supported.");

    if (command.temporary())
        return arrow::Status::NotImplemented("Implicit temporary tables are not supported.");

    std::string schema_string;
    if (command.has_schema())
    {
        if (!isValidIdentifier(command.schema()))
            return arrow::Status::Invalid("Invalid schema name: ", command.schema());
        schema_string = backQuoteIfNeed(command.schema()) + ".";
    }

    if (!isValidIdentifier(command.table()))
        return arrow::Status::Invalid("Invalid table name: ", command.table());

    return SQLSet{"INSERT INTO " + schema_string + backQuoteIfNeed(command.table()) + " FORMAT Arrow", {}, {}};
}

static std::optional<CommandSelectorResult> commandSelectorImpl(const google::protobuf::Any & any_msg, bool schema_only)
{
    if (any_msg.Is<arrow::flight::protocol::sql::CommandGetSqlInfo>())
    {
        arrow::flight::protocol::sql::CommandGetSqlInfo command;
        if (!any_msg.UnpackTo(&command))
            return arrow::Status::SerializationError("Deserialization of sql::CommandGetSqlInfo failed.");
        return commandGetSqlInfo(command, schema_only);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetCrossReference>())
    {
        return arrow::Status::NotImplemented("sql::CommandGetCrossReference is not supported");
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetCatalogs>())
    {
        return commandGetCatalogs();
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetDbSchemas>())
    {
        arrow::flight::protocol::sql::CommandGetDbSchemas command;
        if (!any_msg.UnpackTo(&command))
            return arrow::Status::SerializationError("Deserialization of sql::CommandGetDbSchemas failed.");
        return commandGetDbSchemas(command);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetExportedKeys>())
    {
        return arrow::Status::NotImplemented("sql::CommandGetExportedKeys is not supported");
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetImportedKeys>())
    {
        return arrow::Status::NotImplemented("sql::CommandGetImportedKeys is not supported");
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetPrimaryKeys>())
    {
        arrow::flight::protocol::sql::CommandGetPrimaryKeys command;
        if (!any_msg.UnpackTo(&command))
            return arrow::Status::SerializationError("Deserialization of sql::CommandGetPrimaryKeys failed.");
        return commandGetPrimaryKeys(command);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetTables>())
    {
        arrow::flight::protocol::sql::CommandGetTables command;
        if (!any_msg.UnpackTo(&command))
            return arrow::Status::SerializationError("Deserialization of sql::CommandGetTables failed.");
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
            return arrow::Status::SerializationError("Deserialization of sql::CommandStatementQuery failed.");
        return commandStatementQuery(command);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandStatementUpdate>())
    {
        arrow::flight::protocol::sql::CommandStatementUpdate command;
        if (!any_msg.UnpackTo(&command))
            return arrow::Status::SerializationError("Deserialization of sql::CommandStatementUpdate failed.");
        return commandStatementUpdate(command);
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandStatementIngest>())
    {
        using CommandStatementIngest = arrow::flight::protocol::sql::CommandStatementIngest;
        CommandStatementIngest command;
        if (!any_msg.UnpackTo(&command))
            return arrow::Status::SerializationError("Deserialization of sql::CommandStatementIngest failed.");
        return commandStatementIngest(command);
    }
    else
    {
        if (isArrowFlightSql(any_msg))
            return arrow::Status::NotImplemented("Command is not implemented: ", any_msg.ShortDebugString());
    }

    return std::nullopt;
}

CommandSelectorResult commandSelector(const std::string & cmd, bool schema_only)
{
    if (cmd.empty())
        return arrow::Status::Invalid("Empty command.");
    if (cmd.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
        return arrow::Status::Invalid("Command payload is too large.");
    if (google::protobuf::Any any_msg; any_msg.ParseFromArray(cmd.data(), static_cast<int>(cmd.size())))
        if (auto result = commandSelectorImpl(any_msg, schema_only))
            return *result;
    return SQLSet{cmd, {}, {}};
}

}

}
