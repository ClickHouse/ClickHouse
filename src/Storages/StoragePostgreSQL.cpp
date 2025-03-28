#include "StoragePostgreSQL.h"

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

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationFixedString.h>

#include <Columns/ColumnFixedString.h>
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
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>

#include <base/range.h>

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
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StoragePostgreSQL::StoragePostgreSQL(
    const StorageID & table_id_,
    postgres::PoolWithFailoverPtr pool_,
    const String & remote_table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    const String & remote_table_schema_,
    const String & on_conflict_)
    : IStorage(table_id_)
    , remote_table_name(remote_table_name_)
    , remote_table_schema(remote_table_schema_)
    , on_conflict(on_conflict_)
    , pool(std::move(pool_))
    , log(getLogger("StoragePostgreSQL (" + table_id_.getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(pool, remote_table_name, remote_table_schema, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

ColumnsDescription StoragePostgreSQL::getTableStructureFromData(
    const postgres::PoolWithFailoverPtr & pool_,
    const String & table,
    const String & schema,
    const ContextPtr & context_)
{
    const bool use_nulls = context_->getSettingsRef()[Setting::external_table_functions_use_nulls];
    auto connection_holder = pool_->get();
    auto columns_info = fetchPostgreSQLTableStructure(
            connection_holder->get(), table, schema, use_nulls).physical_columns;

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
        Block sample_block,
        size_t max_block_size_,
        String remote_table_schema_,
        String remote_table_name_,
        postgres::ConnectionHolderPtr connection_)
        : SourceStepWithFilter(std::move(sample_block), column_names_, query_info_, storage_snapshot_, context_)
        , logger(getLogger("ReadFromPostgreSQL"))
        , max_block_size(max_block_size_)
        , remote_table_schema(remote_table_schema_)
        , remote_table_name(remote_table_name_)
        , connection(std::move(connection_))
    {
    }

    std::string getName() const override { return "ReadFromPostgreSQL"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        std::optional<size_t> transform_query_limit;
        if (limit && !filter_actions_dag)
            transform_query_limit = limit;

        /// Connection is already made to the needed database, so it should not be present in the query;
        /// remote_table_schema is empty if it is not specified, will access only table_name.
        String query = transformQueryForExternalDatabase(
            query_info,
            required_source_columns,
            storage_snapshot->metadata->getColumns().getOrdinary(),
            IdentifierQuotingStyle::DoubleQuotes,
            LiteralEscapingStyle::PostgreSQL,
            remote_table_schema,
            remote_table_name,
            context,
            transform_query_limit);
        LOG_TRACE(logger, "Query: {}", query);

        pipeline.init(Pipe(std::make_shared<PostgreSQLSource<>>(std::move(connection), query, getOutputHeader(), max_block_size)));
    }

    LoggerPtr logger;
    size_t max_block_size;
    String remote_table_schema;
    String remote_table_name;
    postgres::ConnectionHolderPtr connection;
};

}

void StoragePostgreSQL::read(
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
        sample_block,
        max_block_size,
        remote_table_schema,
        remote_table_name,
        pool->get());
    query_plan.addStep(std::move(reading));
}

class SerializationPostgreSQLFixedString : public SerializationFixedString
{
private:
    size_t size;

public:
    explicit SerializationPostgreSQLFixedString(size_t n_) : SerializationFixedString(n_), size(n_) {}

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override
    {
        const auto * nested_column = &column;
        if (column.isNullable())
        {
            const auto * column_nullable = assert_cast<const ColumnNullable *>(&column);
            nested_column = &column_nullable->getNestedColumn();
        }

        const auto column_type = nested_column->getDataType();
        const char * data = nullptr;
        if (isFixedString(column_type))
        {
            data = reinterpret_cast<const char *>(
                &assert_cast<const ColumnFixedString *>(nested_column)->getChars()[size * row_num]);
        }
        else if (isString(column_type))
            data = assert_cast<const ColumnString &>(column).getDataAt(row_num).data;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't serialize type {} as a FixedString", nested_column->getName());

        /// Write only the amount of UTF-8 characters actually used. PostgreSQL complains if we pass
        /// any \0 because it doesn't interpret them as valid UTF-8. We assume that any ending \0 is
        /// padding and replace any intermediate \0 with a whitespace. This is a trade-off we have
        /// to make since PostgreSQL doesn't accept intermediate \0's.
        std::string_view str(data, size);
        const auto end_pos = str.find_last_not_of('\0');
        auto write_size = end_pos != std::string::npos ? end_pos + 1 : size;
        str = std::string_view(data, write_size);

        /// Replace any intermediate \0 with a whitespace, allocating a new buffer only if needed.
        /// Because of SSO, new_buffer will only allocate heap memory in case the size is not small.
        std::string new_buffer;
        if (str.find('\0') != std::string::npos)
        {
            new_buffer = str;
            std::replace(new_buffer.begin(), new_buffer.end(), '\0', ' ');
            data = new_buffer.data();
        }
        writeString(data, write_size, ostr);
    }
};

static void changeSerializationForPostgreSQLFixedString(const ColumnPtr & column, DataTypePtr & data_type)
{
    /// pqxx throws an error when trying to ingest fixed-size strings padded with \0:
    /// pqxx::data_exception: Failure during '[END COPY]': ERROR:  invalid byte sequence for encoding "UTF8": 0x00
    auto non_nullable_type = removeNullable(data_type);
    auto non_nullable_column = removeNullable(column);
    if (isFixedString(non_nullable_type) && !data_type->getCustomSerialization())
    {
        auto n = assert_cast<const DataTypeFixedString &>(*non_nullable_type).getN();
        auto custom_desc = std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>(fmt::format("FixedString({})", n)), std::make_shared<SerializationPostgreSQLFixedString>(n));
        DataTypeFactory::setCustom(data_type, std::move(custom_desc));
    }
}

class PostgreSQLSink : public SinkToStorage
{

using Row = std::vector<std::optional<std::string>>;

public:
    explicit PostgreSQLSink(
        const StorageMetadataPtr & metadata_snapshot_,
        postgres::ConnectionHolderPtr connection_holder_,
        const String & remote_table_name_,
        const String & remote_table_schema_,
        const String & on_conflict_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
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
        auto data_types = block.getDataTypes();

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
                        changeSerializationForPostgreSQLFixedString(columns[j], data_types[j]);
                        data_types[j]->getDefaultSerialization()->serializeText(*columns[j], i, ostr, FormatSettings{});
                    }

                    row[j] = ostr.str();
                }
            }

            inserter->insert(row);
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
        auto array_column = ColumnArray::create(createNested(nested_type));
        array_column->insert(array_field);

        const IColumn & nested_column = array_column->getData();
        const auto parent_type = nested_type;

        FormatSettings settings;
        settings.pretty.charset = FormatSettings::Pretty::Charset::ASCII;

        if (nested_type->isNullable())
            nested_type = static_cast<const DataTypeNullable *>(nested_type.get())->getNestedType();

        /// We need to patch first the nested type in case the the parent is nullable because
        /// getDefaultSerialization() takes the serialization used for the nested type
        changeSerializationForPostgreSQLFixedString(nested_column.getPtr(), nested_type);
        const auto serialization = parent_type->getDefaultSerialization();

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
            return ColumnNullable::create(std::move(nested_column), ColumnUInt8::create(nested_column->size(), 0));

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
    return std::make_shared<PostgreSQLSink>(metadata_snapshot, pool->get(), remote_table_name, remote_table_schema, on_conflict);
}

StoragePostgreSQL::Configuration StoragePostgreSQL::processNamedCollectionResult(const NamedCollection & named_collection, ContextPtr context_, bool require_table)
{
    StoragePostgreSQL::Configuration configuration;
    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> required_arguments = {"user", "username", "password", "database", "db"};
    if (require_table)
        required_arguments.insert("table");

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
        configuration.table = named_collection.get<String>("table");
    configuration.schema = named_collection.getOrDefault<String>("schema", "");
    configuration.on_conflict = named_collection.getOrDefault<String>("on_conflict", "");

    return configuration;
}

StoragePostgreSQL::Configuration StoragePostgreSQL::getConfiguration(ASTs engine_args, ContextPtr context)
{
    StoragePostgreSQL::Configuration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
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
                "PostgreSQL('host:port', 'database', 'table', 'username', 'password' "
                "[, 'schema', 'ON CONFLICT ...']. Got: {}",
                engine_args.size());
        }

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        configuration.addresses_expr = checkAndGetLiteralArgument<String>(engine_args[0], "host:port");
        size_t max_addresses = context->getSettingsRef()[Setting::glob_expansion_max_elements];

        configuration.addresses = parseRemoteDescriptionForExternalDatabase(configuration.addresses_expr, max_addresses, 5432);
        if (configuration.addresses.size() == 1)
        {
            configuration.host = configuration.addresses[0].first;
            configuration.port = configuration.addresses[0].second;
        }
        configuration.database = checkAndGetLiteralArgument<String>(engine_args[1], "database");
        configuration.table = checkAndGetLiteralArgument<String>(engine_args[2], "table");
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


void registerStoragePostgreSQL(StorageFactory & factory)
{
    factory.registerStorage("PostgreSQL", [](const StorageFactory::Arguments & args)
    {
        auto configuration = StoragePostgreSQL::getConfiguration(args.engine_args, args.getLocalContext());
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
            configuration.table,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
            configuration.schema,
            configuration.on_conflict);
    },
    {
        .supports_schema_inference = true,
        .source_access_type = AccessType::POSTGRES,
    });
}

}

#endif
