#include <Storages/StoragePostgreSQL.h>

#if USE_LIBPQXX
#include <Processors/Sources/PostgreSQLSource.h>

#include <Common/parseAddress.h>
#include <Common/assert_cast.h>
#include <Common/parseRemoteDescription.h>
#include <Common/logger_useful.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/RemoteHostFilter.h>
#include <Common/thread_local_rng.h>

#include <Core/Settings.h>
#include <Core/PostgreSQL/PoolWithFailover.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>

#include <Formats/FormatSettings.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>

#include <Parsers/getInsertQuery.h>
#include <Parsers/ASTFunction.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Storages/StorageFactory.h>
#include <Storages/TableNameOrQuery.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>

#include <base/range.h>

#include <boost/algorithm/string/join.hpp>


namespace DB
{
namespace Setting
{
    extern const SettingsBool external_table_functions_use_nulls;
    extern const SettingsUInt64 glob_expansion_max_elements;
    extern const SettingsUInt64 postgresql_connection_attempt_timeout;
    extern const SettingsBool postgresql_connection_pool_auto_close_connection;
    extern const SettingsUInt64 postgresql_connection_pool_retries;
    extern const SettingsUInt64 postgresql_connection_pool_size;
    extern const SettingsUInt64 postgresql_connection_pool_wait_timeout;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
}

namespace
{
/// Infer the structure of a result of a user-provided query by running it with `LIMIT 0` on the PostgreSQL side.
ColumnsDescription doQueryResultStructure(pqxx::connection & connection, const String & query, bool use_nulls);
}

StoragePostgreSQL::StoragePostgreSQL(
    const StorageID & table_id_,
    postgres::PoolWithFailoverPtr pool_,
    const TableNameOrQuery & remote_table_or_query_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    const String & remote_table_schema_,
    const String & on_conflict_)
    : StorageWithCommonVirtualColumns(table_id_)
    , remote_table_or_query(remote_table_or_query_)
    , remote_table_schema(remote_table_schema_)
    , on_conflict(on_conflict_)
    , pool(std::move(pool_))
    , log(getLogger("StoragePostgreSQL (" + table_id_.getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(pool, remote_table_or_query, remote_table_schema, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StoragePostgreSQL::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

ColumnsDescription StoragePostgreSQL::getTableStructureFromData(
    const postgres::PoolWithFailoverPtr & pool_,
    const TableNameOrQuery & table_or_query,
    const String & schema,
    const ContextPtr & context_)
{
    const bool use_nulls = context_->getSettingsRef()[Setting::external_table_functions_use_nulls];
    auto connection_holder = pool_->get();

    if (table_or_query.isQuery())
        return doQueryResultStructure(connection_holder->get(), table_or_query.getQuery(), use_nulls);

    auto columns_info = fetchPostgreSQLTableStructure(
            connection_holder->get(), table_or_query.getTableName(), schema, use_nulls).physical_columns;

    if (!columns_info)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table structure not returned");

    return ColumnsDescription{columns_info->columns};
}

namespace
{

class ReadFromPostgreSQL : public SourceStepWithFilter
{
public:
    ReadFromPostgreSQL(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        SharedHeader sample_block,
        size_t max_block_size_,
        String remote_table_schema_,
        TableNameOrQuery remote_table_or_query_,
        postgres::PoolWithFailoverPtr pool_
    )
        : SourceStepWithFilter(std::move(sample_block), column_names_, query_info_, storage_snapshot_, context_)
        , logger(getLogger("ReadFromPostgreSQL"))
        , max_block_size(max_block_size_)
        , remote_table_schema(remote_table_schema_)
        , remote_table_or_query(remote_table_or_query_)
        , pool(std::move(pool_))
    {
    }

    std::string getName() const override { return "ReadFromPostgreSQL"; }

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<ReadFromPostgreSQL>(
            requiredSourceColumns(),
            query_info,
            storage_snapshot,
            context,
            getOutputHeader(),
            max_block_size,
            remote_table_schema,
            remote_table_or_query,
            pool);
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        String query;
        if (remote_table_or_query.isQuery())
        {
            /// The user-provided query is passed to PostgreSQL as is, wrapped into a subquery to project
            /// only the required columns. Predicate and LIMIT pushdown are not applied in this case, so
            /// reject any outer filter under external_table_strict_query.
            rejectOuterFilterForQueryBackedExternalSourceIfStrict(query_info, context);
            query = buildQueryForExternalDatabaseSubquery(
                remote_table_or_query.getQuery(), required_source_columns, IdentifierQuotingStyle::DoubleQuotes);
        }
        else
        {
            std::optional<size_t> transform_query_limit;
            if (limit && !filter_actions_dag)
                transform_query_limit = limit;

            /// Connection is already made to the needed database, so it should not be present in the query;
            /// remote_table_schema is empty if it is not specified, will access only table_name.
            query = transformQueryForExternalDatabase(
                query_info,
                required_source_columns,
                storage_snapshot->metadata->getColumns().getOrdinary(),
                IdentifierQuotingStyle::DoubleQuotes,
                LiteralEscapingStyle::PostgreSQL,
                remote_table_schema,
                remote_table_or_query.getTableName(),
                context,
                transform_query_limit);
        }
        LOG_TRACE(logger, "Query: {}", query);

        pipeline.init(Pipe(std::make_shared<PostgreSQLSource<>>(pool->get(), query, getOutputHeader(), max_block_size)));
    }

    LoggerPtr logger;
    size_t max_block_size;
    String remote_table_schema;
    TableNameOrQuery remote_table_or_query;
    postgres::PoolWithFailoverPtr pool;
};

}

void StoragePostgreSQL::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        WhichDataType which(column_data.type);
        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({ column_data.type, column_data.name });
    }

    auto reading = std::make_unique<ReadFromPostgreSQL>(
        column_names,
        query_info,
        storage_snapshot,
        local_context,
        std::make_shared<const Block>(sample_block),
        max_block_size,
        remote_table_schema,
        remote_table_or_query,
        pool);
    query_plan.addStep(std::move(reading));
}


class PostgreSQLSink final : public SinkToStorage
{

using Row = std::vector<std::optional<std::string>>;

public:
    explicit PostgreSQLSink(
        const StorageMetadataPtr & metadata_snapshot_,
        postgres::ConnectionHolderPtr connection_holder_,
        const String & remote_table_name_,
        const String & remote_table_schema_,
        const String & on_conflict_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , metadata_snapshot(metadata_snapshot_)
        , connection_holder(std::move(connection_holder_))
        , remote_table_name(remote_table_name_)
        , remote_table_schema(remote_table_schema_)
        , on_conflict(on_conflict_)
    {
    }

    String getName() const override { return "PostgreSQLSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        if (!inserter)
        {
            if (on_conflict.empty())
            {
                inserter = std::make_unique<StreamTo>(connection_holder->get(),
                        remote_table_schema.empty() ? pqxx::table_path({remote_table_name})
                                                    : pqxx::table_path({remote_table_schema, remote_table_name}), block.getNames());
            }
            else
            {
                inserter = std::make_unique<PreparedInsert>(connection_holder->get(), remote_table_name,
                                                            remote_table_schema, block.getColumnsWithTypeAndName(), on_conflict);
            }
        }

        const auto columns = block.getColumns();
        const size_t num_rows = block.rows();
        const size_t num_cols = block.columns();
        const auto data_types = block.getDataTypes();

        /// std::optional lets libpqxx to know if value is NULL
        std::vector<std::optional<std::string>> row(num_cols);

        for (const auto i : collections::range(0, num_rows))
        {
            for (const auto j : collections::range(0, num_cols))
            {
                if (columns[j]->isNullAt(i))
                {
                    row[j] = std::nullopt;
                }
                else
                {
                    WriteBufferFromOwnString ostr;

                    if (isArray(data_types[j]))
                    {
                        parseArray((*columns[j])[i], data_types[j], ostr);
                    }
                    else
                    {
                        data_types[j]->getDefaultSerialization()->serializeText(*columns[j], i, ostr, FormatSettings{});
                    }

                    row[j] = ostr.str();
                }
            }

            try
            {
                inserter->insert(row);
            }
            catch (const pqxx::argument_error & e)
            {
                /// libpqxx throws pqxx::argument_error when the string contains invalid UTF-8.
                /// Since pqxx::argument_error is a std::invalid_argument, which is a std::logic_error,
                /// and unhandled std::logic_error is treated as a "Logical error" with code 1001,
                /// we need to wrap it into a DB::Exception.
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot insert data into PostgreSQL: {}", e.what());
            }
        }
    }

    void onFinish() override
    {
        if (inserter)
            inserter->complete();
    }

    /// Cannot just use serializeAsText for array data type even though it converts perfectly
    /// any dimension number array into text format, because it encloses in '[]' and for postgres it must be '{}'.
    /// Check if array[...] syntax from PostgreSQL will be applicable.
    static void parseArray(const Field & array_field, const DataTypePtr & data_type, WriteBuffer & ostr)
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get());
        const auto & nested = array_type->getNestedType();
        const auto & array = array_field.safeGet<Array>();

        if (!isArray(nested))
        {
            parseArrayContent(array, data_type, ostr);
            return;
        }

        writeChar('{', ostr);

        const auto * nested_array_type = typeid_cast<const DataTypeArray *>(nested.get());
        for (auto iter = array.begin(); iter != array.end(); ++iter)
        {
            if (iter != array.begin())
                writeText(", ", ostr);

            if (!isArray(nested_array_type->getNestedType()))
            {
                parseArrayContent(iter->safeGet<Array>(), nested, ostr);
            }
            else
            {
                parseArray(*iter, nested, ostr);
            }
        }

        writeChar('}', ostr);
    }

    /// Conversion is done via column casting because with writeText(Array..) got incorrect conversion
    /// of Date and DateTime data types and it added extra quotes for values inside array.
    static void parseArrayContent(const Array & array_field, const DataTypePtr & data_type, WriteBuffer & ostr)
    {
        auto nested_type = typeid_cast<const DataTypeArray *>(data_type.get())->getNestedType();
        auto array_column = ColumnArray::create(nested_type->createColumn());
        array_column->insert(array_field);

        const IColumn & nested_column = array_column->getData();
        const auto serialization = nested_type->getDefaultSerialization();

        FormatSettings settings;
        settings.pretty.charset = FormatSettings::Pretty::Charset::ASCII;

        writeChar('{', ostr);
        for (size_t i = 0, size = array_field.size(); i < size; ++i)
        {
            if (i != 0)
                writeChar(',', ostr);

            serialization->serializeText(nested_column, i, ostr, settings);
        }
        writeChar('}', ostr);
    }

    static MutableColumnPtr createNested(DataTypePtr nested)
    {
        bool is_nullable = false;
        if (nested->isNullable())
        {
            is_nullable = true;
            nested = static_cast<const DataTypeNullable *>(nested.get())->getNestedType();
        }

        WhichDataType which(nested);
        MutableColumnPtr nested_column;
        if (which.isString() || which.isFixedString())   nested_column = ColumnString::create();
        else if (which.isInt8() || which.isInt16())      nested_column = ColumnInt16::create();
        else if (which.isUInt8() || which.isUInt16())    nested_column = ColumnUInt16::create();
        else if (which.isInt32())                        nested_column = ColumnInt32::create();
        else if (which.isUInt32())                       nested_column = ColumnUInt32::create();
        else if (which.isInt64())                        nested_column = ColumnInt64::create();
        else if (which.isUInt64())                       nested_column = ColumnUInt64::create();
        else if (which.isFloat32())                      nested_column = ColumnFloat32::create();
        else if (which.isFloat64())                      nested_column = ColumnFloat64::create();
        else if (which.isDate())                         nested_column = ColumnUInt16::create();
        else if (which.isDate32())                       nested_column = ColumnInt32::create();
        else if (which.isDateTime())                     nested_column = ColumnUInt32::create();
        else if (which.isUUID())                         nested_column = ColumnUUID::create();
        else if (which.isDateTime64())
        {
            nested_column = ColumnDecimal<DateTime64>::create(0, 6);
        }
        else if (which.isDecimal32())
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal32> *>(nested.get());
            nested_column = ColumnDecimal<Decimal32>::create(0, type->getScale());
        }
        else if (which.isDecimal64())
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal64> *>(nested.get());
            nested_column = ColumnDecimal<Decimal64>::create(0, type->getScale());
        }
        else if (which.isDecimal128())
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal128> *>(nested.get());
            nested_column = ColumnDecimal<Decimal128>::create(0, type->getScale());
        }
        else if (which.isDecimal256())
        {
            const auto & type = typeid_cast<const DataTypeDecimal<Decimal256> *>(nested.get());
            nested_column = ColumnDecimal<Decimal256>::create(0, type->getScale());
        }
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type conversion not supported");

        if (is_nullable)
            return ColumnNullable::create(std::move(nested_column), ColumnUInt8::create(nested_column->size(), static_cast<UInt8>(0)));

        return nested_column;
    }


private:
    struct Inserter
    {
        pqxx::connection & connection;
        pqxx::work tx;

        explicit Inserter(pqxx::connection & connection_)
            : connection(connection_)
            , tx(connection) {}

        virtual ~Inserter() = default;

        virtual void insert(const Row & row) = 0;
        virtual void complete() = 0;
    };

    struct StreamTo : Inserter
    {
        Names columns;
        pqxx::stream_to stream;

        StreamTo(pqxx::connection & connection_, pqxx::table_path table_, Names columns_)
            : Inserter(connection_)
            , columns(std::move(columns_))
            , stream(pqxx::stream_to::raw_table(tx, connection.quote_table(table_), connection.quote_columns(columns)))
        {
        }

        void complete() override
        {
            stream.complete();
            tx.commit();
        }

        void insert(const Row & row) override
        {
            stream.write_values(row);
        }
    };

    struct PreparedInsert : Inserter
    {
        PreparedInsert(pqxx::connection & connection_, const String & table, const String & schema,
                       const ColumnsWithTypeAndName & columns, const String & on_conflict_)
            : Inserter(connection_)
            , statement_name("insert_" + getHexUIntLowercase(thread_local_rng()))
        {
            WriteBufferFromOwnString buf;
            buf << getInsertQuery(schema, table, columns, IdentifierQuotingStyle::DoubleQuotes);
            buf << " (";
            for (size_t i = 1; i <= columns.size(); ++i)
            {
                if (i > 1)
                    buf << ", ";
                buf << "$" << i;
            }
            buf << ") ";
            buf << on_conflict_;
            connection.prepare(statement_name, buf.str());
            prepared = true;
        }

        void complete() override
        {
            connection.unprepare(statement_name);
            prepared = false;
            tx.commit();
        }

        void insert(const Row & row) override
        {
            pqxx::params params;
            params.reserve(row.size());
            params.append_multi(row);
            tx.exec_prepared(statement_name, params);
        }

        ~PreparedInsert() override
        {
            try
            {
                if (prepared)
                    connection.unprepare(statement_name);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        const String statement_name;
        bool prepared = false;
    };

    StorageMetadataPtr metadata_snapshot;
    postgres::ConnectionHolderPtr connection_holder;
    const String remote_db_name, remote_table_name, remote_table_schema, on_conflict;

    std::unique_ptr<Inserter> inserter;
};


SinkToStoragePtr StoragePostgreSQL::write(
        const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /* context */, bool /*async_insert*/)
{
    if (remote_table_or_query.isQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot write into a PostgreSQL table representing the result of a query");

    return std::make_shared<PostgreSQLSink>(metadata_snapshot, pool->get(), remote_table_or_query.getTableName(), remote_table_schema, on_conflict);
}

StoragePostgreSQL::Configuration StoragePostgreSQL::processNamedCollectionResult(const NamedCollection & named_collection, ContextPtr context_, bool require_table)
{
    StoragePostgreSQL::Configuration configuration;
    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> required_arguments = {"user", "username", "password", "database", "db"};
    if (require_table)
    {
        /// The data source is either a remote table or a query passed to PostgreSQL as is.
        if (named_collection.has("query"))
            required_arguments.insert("query");
        else
            required_arguments.insert("table");
    }

    validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(
        named_collection, required_arguments, {"schema", "on_conflict", "addresses_expr", "host", "hostname", "port", "use_table_cache"});

    configuration.addresses_expr = named_collection.getOrDefault<String>("addresses_expr", "");
    if (configuration.addresses_expr.empty())
    {
        configuration.host = named_collection.getAny<String>({"host", "hostname"});
        configuration.port = static_cast<UInt16>(named_collection.get<UInt64>("port"));
        configuration.addresses = {std::make_pair(configuration.host, configuration.port)};
    }
    else
    {
        size_t max_addresses = context_->getSettingsRef()[Setting::glob_expansion_max_elements];
        configuration.addresses = parseRemoteDescriptionForExternalDatabase(
            configuration.addresses_expr, max_addresses, 5432);
    }

    configuration.username = named_collection.getAny<String>({"username", "user"});
    configuration.password = named_collection.get<String>("password");
    configuration.database = named_collection.getAny<String>({"db", "database"});
    if (require_table)
    {
        if (named_collection.has("query"))
            configuration.table_or_query = TableNameOrQuery(TableNameOrQuery::Type::QUERY, named_collection.get<String>("query"));
        else
            configuration.table_or_query = TableNameOrQuery(TableNameOrQuery::Type::TABLE, named_collection.get<String>("table"));
    }
    configuration.schema = named_collection.getOrDefault<String>("schema", "");
    configuration.on_conflict = named_collection.getOrDefault<String>("on_conflict", "");

    return configuration;
}

StoragePostgreSQL::Configuration StoragePostgreSQL::getConfiguration(ASTs engine_args, ContextPtr context, const StorageID * table_id)
{
    StoragePostgreSQL::Configuration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context, true, nullptr, table_id))
    {
        configuration = StoragePostgreSQL::processNamedCollectionResult(*named_collection, context);
    }
    else
    {
        if (engine_args.size() < 5 || engine_args.size() > 7)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage PostgreSQL requires from 5 to 7 parameters: "
                "PostgreSQL('host:port', 'database', 'table' (or query), 'username', 'password' "
                "[, 'schema', 'ON CONFLICT ...']. Got: {}",
                engine_args.size());
        }

        /// The 3rd argument is either a table name, or a query passed to PostgreSQL as is - `(SELECT ...)` or `query('SELECT ...')`.
        auto maybe_query = tryGetExternalDatabaseQuery(
            engine_args[2], context, IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL);
        for (size_t i = 0; i < engine_args.size(); ++i)
        {
            if (i == 2 && maybe_query)
                continue;
            engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], context);
        }

        configuration.addresses_expr = checkAndGetLiteralArgument<String>(engine_args[0], "host:port");
        size_t max_addresses = context->getSettingsRef()[Setting::glob_expansion_max_elements];

        configuration.addresses = parseRemoteDescriptionForExternalDatabase(configuration.addresses_expr, max_addresses, 5432);
        if (configuration.addresses.size() == 1)
        {
            configuration.host = configuration.addresses[0].first;
            configuration.port = configuration.addresses[0].second;
        }
        configuration.database = checkAndGetLiteralArgument<String>(engine_args[1], "database");
        if (maybe_query)
            configuration.table_or_query = TableNameOrQuery(TableNameOrQuery::Type::QUERY, *maybe_query);
        else
            configuration.table_or_query = TableNameOrQuery(TableNameOrQuery::Type::TABLE, checkAndGetLiteralArgument<String>(engine_args[2], "table"));
        configuration.username = checkAndGetLiteralArgument<String>(engine_args[3], "user");
        configuration.password = checkAndGetLiteralArgument<String>(engine_args[4], "password");

        if (engine_args.size() >= 6)
            configuration.schema = checkAndGetLiteralArgument<String>(engine_args[5], "schema");
        if (engine_args.size() >= 7)
            configuration.on_conflict = checkAndGetLiteralArgument<String>(engine_args[6], "on_conflict");
    }
    for (const auto & address : configuration.addresses)
        context->getRemoteHostFilter().checkHostAndPort(address.first, toString(address.second));

    return configuration;
}


void registerStoragePostgreSQL(StorageFactory & factory);
void registerStoragePostgreSQL(StorageFactory & factory)
{
    factory.registerStorage("PostgreSQL", [](const StorageFactory::Arguments & args)
    {
        auto configuration = StoragePostgreSQL::getConfiguration(args.engine_args, args.getLocalContext(), &args.table_id);
        const auto & settings = args.getLocalContext()->getSettingsRef();
        auto pool = std::make_shared<postgres::PoolWithFailover>(
            configuration,
            settings[Setting::postgresql_connection_pool_size],
            settings[Setting::postgresql_connection_pool_wait_timeout],
            settings[Setting::postgresql_connection_pool_retries],
            settings[Setting::postgresql_connection_pool_auto_close_connection],
            settings[Setting::postgresql_connection_attempt_timeout]);

        return std::make_shared<StoragePostgreSQL>(
            args.table_id,
            std::move(pool),
            configuration.table_or_query,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
            configuration.schema,
            configuration.on_conflict);
    },
    {
        .supports_schema_inference = true,
        .source_access_type = AccessTypeObjects::Source::POSTGRES,
    },
    Documentation{
        .description = R"DOCS_MD(
The PostgreSQL engine allows `SELECT` and `INSERT` queries on data stored on a remote PostgreSQL server.

:::note
Currently, only PostgreSQL versions 12 and up are supported for the table engine.
:::

:::tip
Check out our [Managed Postgres](/docs/cloud/managed-postgres) service. Backed by NVMe storage that is physically co-located with compute, it delivers up to 10x faster performance for workloads that are disk-bound compared to alternatives using network-attached storage like EBS and allows you to replicate your Postgres data to ClickHouse using the Postgres CDC connector in ClickPipes.
:::

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = PostgreSQL({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

See a detailed description of the [CREATE TABLE](/sql-reference/statements/create/table) query.

The table structure can differ from the original PostgreSQL table structure:

- Column names should be the same as in the original PostgreSQL table, but you can use just some of these columns and in any order.
- Column types may differ from those in the original PostgreSQL table. ClickHouse tries to [cast](../../../engines/database-engines/postgresql.md#data_types-support) values to the ClickHouse data types.
- The [external_table_functions_use_nulls](/operations/settings/settings#external_table_functions_use_nulls) setting defines how to handle Nullable columns. Default value: 1. If 0, the table function does not make Nullable columns and inserts default values instead of nulls. This is also applicable for NULL values inside arrays.

**Engine Parameters**

- `host:port` — PostgreSQL server address.
- `database` — Remote database name.
- `table` — Remote table name, or a query passed to PostgreSQL as is (see [Passing a query instead of a table name](#passing-a-query)).
- `user` — PostgreSQL user.
- `password` — User password.
- `schema` — Non-default table schema. Optional.
- `on_conflict` — Conflict resolution strategy. Example: `ON CONFLICT DO NOTHING`. Optional. Note: adding this option will make insertion less efficient.

[Named collections](/operations/named-collections.md) (available since version 21.11) are recommended for production environment. Here is an example:

```xml
<named_collections>
    <postgres_creds>
        <host>localhost</host>
        <port>5432</port>
        <user>postgres</user>
        <password>****</password>
        <schema>schema1</schema>
    </postgres_creds>
</named_collections>
```

Some parameters can be overridden by key value arguments:
```sql
SELECT * FROM postgresql(postgres_creds, table='table1');
```

## Implementation details {#implementation-details}

`SELECT` queries on PostgreSQL side run as `COPY (SELECT ...) TO STDOUT` inside read-only PostgreSQL transaction with commit after each `SELECT` query.

Simple `WHERE` clauses such as `=`, `!=`, `>`, `>=`, `<`, `<=`, and `IN` are executed on the PostgreSQL server.

All joins, aggregations, sorting, `IN [ array ]` conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to PostgreSQL finishes.

## Passing a query instead of a table name {#passing-a-query}

Instead of a table name, the `table` argument can be a `SELECT` query that is passed to PostgreSQL as is. The structure of the table is inferred from the query result. The query can be written either as a subquery, or wrapped into the `query` function:

```sql
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

This is useful to push down joins, aggregations or any other processing to PostgreSQL. Such a table is read-only: `INSERT` into it is not allowed. The same syntax is supported by the [`postgresql`](/sql-reference/table-functions/postgresql) table function.

:::note
The subquery form `(SELECT ...)` is parsed by ClickHouse and re-serialized in the PostgreSQL dialect (PostgreSQL identifier quoting and string-literal escaping) before being sent to the server. It must therefore be valid ClickHouse SQL. To pass PostgreSQL-specific syntax that ClickHouse does not parse, use the `query('...')` form, whose text is sent to PostgreSQL verbatim.

Any outer `WHERE`, `LIMIT`, aggregation, etc. of the surrounding ClickHouse query is **not** pushed down into the passed query — it is applied in ClickHouse after the full query result is fetched. To restrict the data read from PostgreSQL, put the filter inside the passed query. With [`external_table_strict_query = 1`](/operations/settings/settings#external_table_strict_query) an outer filter that cannot be pushed down is rejected with an exception instead of being applied locally.
:::

`INSERT` queries on PostgreSQL side run as `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` inside PostgreSQL transaction with auto-commit after each `INSERT` statement.

PostgreSQL `Array` types are converted into ClickHouse arrays.

:::note
Be careful - in PostgreSQL an array data, created like a `type_name[]`, may contain multi-dimensional arrays of different dimensions in different table rows in same column. But in ClickHouse it is only allowed to have multidimensional arrays of the same count of dimensions in all table rows in same column.
:::

Supports multiple replicas that must be listed by `|`. For example:

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

Replicas priority for PostgreSQL dictionary source is supported. The bigger the number in map, the less the priority. The highest priority is `0`.

In the example below replica `example01-1` has the highest priority:

```xml
<postgresql>
    <port>5432</port>
    <user>clickhouse</user>
    <password>qwerty</password>
    <replica>
        <host>example01-1</host>
        <priority>1</priority>
    </replica>
    <replica>
        <host>example01-2</host>
        <priority>2</priority>
    </replica>
    <db>db_name</db>
    <table>table_name</table>
    <where>id=10</where>
    <invalidate_query>SQL_QUERY</invalidate_query>
</postgresql>
</source>
```

## Usage example {#usage-example}

### Table in PostgreSQL {#table-in-postgresql}

```text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
int_id | int_nullable | float | str  | float_nullable
--------+--------------+-------+------+----------------
       1 |              |     2 | test |
(1 row)
```

### Creating Table in ClickHouse, and connecting to  PostgreSQL table created above {#creating-table-in-clickhouse-and-connecting-to--postgresql-table-created-above}

This example uses the [PostgreSQL table engine](/engines/table-engines/integrations/postgresql.md) to connect the ClickHouse table to the PostgreSQL table and use both SELECT and INSERT statements to the PostgreSQL database:

```sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

### Inserting initial data from PostgreSQL table into ClickHouse table, using a SELECT query {#inserting-initial-data-from-postgresql-table-into-clickhouse-table-using-a-select-query}

The [postgresql table function](/sql-reference/table-functions/postgresql.md) copies the data from PostgreSQL to ClickHouse, which is often used for improving the query performance of the data by querying or performing analytics in ClickHouse rather than in PostgreSQL, or can also be used for migrating data from PostgreSQL to ClickHouse. Since we will be copying the data from PostgreSQL to ClickHouse, we will use a MergeTree table engine in ClickHouse and call it postgresql_copy:

```sql
CREATE TABLE default.postgresql_copy
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = MergeTree
ORDER BY (int_id);
```

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

### Inserting incremental data from PostgreSQL table into ClickHouse table {#inserting-incremental-data-from-postgresql-table-into-clickhouse-table}

If then performing ongoing synchronization between the PostgreSQL table and ClickHouse table after the initial insert, you can use a WHERE clause in ClickHouse to insert only data added to PostgreSQL based on a timestamp or unique sequence ID.

This would require keeping track of the max ID or timestamp previously added, such as the following:

```sql
SELECT max(`int_id`) AS maxIntID FROM default.postgresql_copy;
```

Then inserting values from PostgreSQL table greater than the max

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password')
WHERE int_id > (SELECT max(int_id) FROM default.postgresql_copy);
```

### Selecting data from the resulting ClickHouse table {#selecting-data-from-the-resulting-clickhouse-table}

```sql
SELECT * FROM postgresql_copy WHERE str IN ('test');
```

```text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

### Using non-default schema {#using-non-default-schema}

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

**See Also**

- [The `postgresql` table function](../../../sql-reference/table-functions/postgresql.md)
- [Using PostgreSQL as a dictionary source](/sql-reference/statements/create/dictionary/sources/postgresql)

## Related content {#related-content}

- Blog: [ClickHouse and PostgreSQL - a match made in data heaven - part 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
- Blog: [ClickHouse and PostgreSQL - a Match Made in Data Heaven - part 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)
)DOCS_MD",
        .syntax = "ENGINE = PostgreSQL('host:port', 'database', 'table', 'user', 'password'[, 'schema', 'on_conflict'])",
        .related = {"MySQL", "SQLite", "MaterializedPostgreSQL"}});
}

namespace
{
ColumnsDescription doQueryResultStructure(pqxx::connection & connection, const String & query, bool use_nulls)
{
    /// Run the query with `LIMIT 0` inside a read-only transaction (so this also works against read-only
    /// replicas) to obtain the result columns without fetching any data. The wrapping mirrors how the data
    /// is read later (see buildQueryForExternalDatabaseSubquery).
    pqxx::ReadTransaction tx(connection);
    pqxx::result sample{tx.exec("SELECT * FROM (" + query + ") AS __subquery LIMIT 0")};

    const auto num_columns = sample.columns();
    if (num_columns == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PostgreSQL query returned no columns: {}", query);

    /// Resolve the type names of the result columns from their type OIDs and type modifiers.
    /// Carrying the type modifier (`format_type(atttypid, atttypmod)`, exactly as the table path does) is
    /// required so that e.g. `numeric(78, 0)` is mapped to `Int256` instead of falling back to a generic
    /// `Decimal128`. `WITH ORDINALITY` together with the explicit `ORDER BY` keeps the resolved type names
    /// lined up with the result columns regardless of how the array is unnested.
    std::vector<std::string> oids;
    std::vector<std::string> type_modifiers;
    oids.reserve(num_columns);
    type_modifiers.reserve(num_columns);
    for (pqxx::row_size_type i = 0; i < num_columns; ++i)
    {
        oids.push_back(std::to_string(sample.column_type(i)));
        type_modifiers.push_back(std::to_string(sample.column_type_modifier(i)));
    }

    pqxx::result type_names{tx.exec(
        "SELECT format_type(type_oid, type_mod) FROM unnest(ARRAY[" + boost::algorithm::join(oids, ",")
        + "]::oid[], ARRAY[" + boost::algorithm::join(type_modifiers, ",")
        + "]::integer[]) WITH ORDINALITY AS t(type_oid, type_mod, ord) ORDER BY ord")};

    std::vector<String> resolved_types(num_columns);
    std::vector<pqxx::row_size_type> array_columns;
    for (pqxx::row_size_type i = 0; i < num_columns; ++i)
    {
        resolved_types[i] = type_names[i][0].as<std::string>();
        if (resolved_types[i].ends_with("[]"))
            array_columns.push_back(i);
    }

    /// PostgreSQL array type OIDs do not encode the number of dimensions, so an `integer[][]` result column
    /// looks exactly like `integer[]`. Probe the actual data with `array_ndims` (as the table path does via
    /// its `recheck_array` step) to learn the real dimensions; otherwise multidimensional arrays would be
    /// inferred as one-dimensional and fail at read time with `Got more dimensions than expected`. When the
    /// query returns no rows there is nothing to probe, and the dimensions stay at one (the result is empty
    /// anyway).
    std::vector<uint16_t> dimensions(num_columns, 1);
    if (!array_columns.empty())
    {
        /// Reference the result columns positionally through an explicit column alias list, so we do not have
        /// to rely on the (possibly duplicate or unnamed) result column names of an arbitrary query.
        std::vector<std::string> alias_columns;
        std::vector<std::string> ndims_exprs;
        alias_columns.reserve(num_columns);
        ndims_exprs.reserve(array_columns.size());
        for (pqxx::row_size_type i = 0; i < num_columns; ++i)
            alias_columns.push_back("c" + std::to_string(i));
        for (auto i : array_columns)
            ndims_exprs.push_back("array_ndims(c" + std::to_string(i) + ")");

        pqxx::result ndims_result{tx.exec(
            "SELECT " + boost::algorithm::join(ndims_exprs, ",") + " FROM (" + query + ") AS __subquery("
            + boost::algorithm::join(alias_columns, ",") + ") LIMIT 1")};

        if (!ndims_result.empty())
        {
            for (size_t j = 0; j < array_columns.size(); ++j)
            {
                const auto column = array_columns[j];
                const auto & field = ndims_result[0][static_cast<pqxx::row_size_type>(j)];

                /// `array_ndims` returns NULL when the sampled array value is NULL or empty, so the dimensions
                /// cannot be inferred from the first row. Fail early with a clear error (as the table path does
                /// in its `recheck_array` step) instead of silently inferring a one-dimensional array and then
                /// failing at read time with the confusing `Got more dimensions than expected`.
                if (field.is_null())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot infer the number of dimensions of the array in column {} ({}) of the PostgreSQL "
                        "query result, because its value in the first row is NULL or an empty array. "
                        "Make sure the first row contains a non-empty array value.",
                        column + 1, sample.column_name(column));

                dimensions[column] = static_cast<uint16_t>(field.as<int>());
            }
        }
    }

    tx.commit();

    NamesAndTypesList columns;
    auto recheck_array = []() {}; /// Dimensions are resolved explicitly above, so the recheck callback is unused.
    for (pqxx::row_size_type i = 0; i < num_columns; ++i)
    {
        auto data_type = convertPostgreSQLDataType(resolved_types[i], recheck_array, use_nulls, dimensions[i]);
        columns.emplace_back(sample.column_name(i), data_type);
    }

    return ColumnsDescription{columns};
}
}

}

#endif
