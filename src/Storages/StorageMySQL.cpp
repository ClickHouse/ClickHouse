#include "StorageMySQL.h"

#if USE_MYSQL

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Processors/Sources/MySQLSource.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTCreateQuery.h>
#include <mysqlxx/Transaction.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <Common/RemoteHostFilter.h>
#include <Common/parseRemoteDescription.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Databases/MySQL/FetchTablesColumnsList.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 glob_expansion_max_elements;
    extern const SettingsMySQLDataTypesSupport mysql_datatypes_support_level;
    extern const SettingsUInt64 mysql_max_rows_to_insert;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
}

StorageMySQL::StorageMySQL(
    const StorageID & table_id_,
    mysqlxx::PoolWithFailover && pool_,
    const std::string & remote_database_name_,
    const std::string & remote_table_name_,
    const bool replace_query_,
    const std::string & on_duplicate_clause_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    const MySQLSettings & mysql_settings_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_database_name(remote_database_name_)
    , remote_table_name(remote_table_name_)
    , replace_query{replace_query_}
    , on_duplicate_clause{on_duplicate_clause_}
    , mysql_settings(mysql_settings_)
    , pool(std::make_shared<mysqlxx::PoolWithFailover>(pool_))
    , log(getLogger("StorageMySQL (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(*pool, remote_database_name, remote_table_name, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

ColumnsDescription StorageMySQL::getTableStructureFromData(
    mysqlxx::PoolWithFailover & pool_,
    const String & database,
    const String & table,
    const ContextPtr & context_)
{
    const auto & settings = context_->getSettingsRef();
    const auto tables_and_columns = fetchTablesColumnsList(pool_, database, {table}, settings, settings[Setting::mysql_datatypes_support_level]);

    const auto columns = tables_and_columns.find(table);
    if (columns == tables_and_columns.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "MySQL table {} doesn't exist.",
                        (database.empty() ? "" : (backQuote(database) + "." + backQuote(table))));

    return columns->second;
}

Pipe StorageMySQL::read(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info_,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names_);
    String query = transformQueryForExternalDatabase(
        query_info_,
        column_names_,
        storage_snapshot->metadata->getColumns().getOrdinary(),
        IdentifierQuotingStyle::BackticksMySQL,
        LiteralEscapingStyle::Regular,
        remote_database_name,
        remote_table_name,
        context_);
    LOG_TRACE(log, "Query: {}", query);

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);

        WhichDataType which(column_data.type);
        /// Convert enum to string.
        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({ column_data.type, column_data.name });
    }


    StreamSettings mysql_input_stream_settings(context_->getSettingsRef(),
        mysql_settings.connection_auto_close);
    return Pipe(std::make_shared<MySQLWithFailoverSource>(pool, query, sample_block, mysql_input_stream_settings));
}


class StorageMySQLSink : public SinkToStorage
{
public:
    explicit StorageMySQLSink(
        const StorageMySQL & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::string & remote_database_name_,
        const std::string & remote_table_name_,
        const mysqlxx::PoolWithFailover::Entry & entry_,
        const size_t & mysql_max_rows_to_insert)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage{storage_}
        , metadata_snapshot{metadata_snapshot_}
        , remote_database_name{remote_database_name_}
        , remote_table_name{remote_table_name_}
        , entry{entry_}
        , max_batch_rows{mysql_max_rows_to_insert}
    {
    }

    String getName() const override { return "StorageMySQLSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        auto blocks = splitBlocks(block, max_batch_rows);
        mysqlxx::Transaction trans(entry);
        try
        {
            for (const Block & batch_data : blocks)
            {
                writeBlockData(batch_data);
            }
            trans.commit();
        }
        catch (...)
        {
            trans.rollback();
            throw;
        }
    }

    void writeBlockData(const Block & block)
    {
        WriteBufferFromOwnString sqlbuf;
        sqlbuf << (storage.replace_query ? "REPLACE" : "INSERT") << " INTO ";
        if (!remote_database_name.empty())
            sqlbuf << backQuoteMySQL(remote_database_name) << ".";
        sqlbuf << backQuoteMySQL(remote_table_name);
        sqlbuf << " (" << dumpNamesWithBackQuote(block) << ") VALUES ";

        auto writer = FormatFactory::instance().getOutputFormat("Values", sqlbuf, metadata_snapshot->getSampleBlock(), storage.getContext());
        writer->write(block);

        if (!storage.on_duplicate_clause.empty())
            sqlbuf << " ON DUPLICATE KEY " << storage.on_duplicate_clause;

        sqlbuf << ";";

        auto query = this->entry->query(sqlbuf.str());
        query.execute();
    }

    Blocks splitBlocks(const Block & block, const size_t & max_rows) const
    {
        /// Avoid Excessive copy when block is small enough
        if (block.rows() <= max_rows)
            return {block};

        const size_t split_block_size = static_cast<size_t>(ceil(block.rows() * 1.0 / max_rows));
        Blocks split_blocks(split_block_size);

        for (size_t idx = 0; idx < split_block_size; ++idx)
            split_blocks[idx] = block.cloneEmpty();

        const size_t columns = block.columns();
        const size_t rows = block.rows();
        size_t offsets = 0;
        UInt64 limits = max_batch_rows;
        for (size_t idx = 0; idx < split_block_size; ++idx)
        {
            /// For last batch, limits should be the remain size
            if (idx == split_block_size - 1) limits = rows - offsets;
            for (size_t col_idx = 0; col_idx < columns; ++col_idx)
            {
                split_blocks[idx].getByPosition(col_idx).column = block.getByPosition(col_idx).column->cut(offsets, limits);
            }
            offsets += max_batch_rows;
        }

        return split_blocks;
    }

    static std::string dumpNamesWithBackQuote(const Block & block)
    {
        WriteBufferFromOwnString out;
        for (auto it = block.begin(); it != block.end(); ++it)
        {
            if (it != block.begin())
                out << ", ";
            out << backQuoteMySQL(it->name);
        }
        return out.str();
    }

private:
    const StorageMySQL & storage;
    StorageMetadataPtr metadata_snapshot;
    std::string remote_database_name;
    std::string remote_table_name;
    mysqlxx::PoolWithFailover::Entry entry;
    size_t max_batch_rows;
};


SinkToStoragePtr StorageMySQL::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    return std::make_shared<StorageMySQLSink>(
        *this,
        metadata_snapshot,
        remote_database_name,
        remote_table_name,
        pool->get(),
        local_context->getSettingsRef()[Setting::mysql_max_rows_to_insert]);
}

StorageMySQL::Configuration StorageMySQL::processNamedCollectionResult(
    const NamedCollection & named_collection, MySQLSettings & storage_settings, ContextPtr context_, bool require_table)
{
    StorageMySQL::Configuration configuration;

    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> optional_arguments = {"replace_query", "on_duplicate_clause", "addresses_expr", "host", "hostname", "port"};
    auto mysql_settings = storage_settings.all();
    for (const auto & setting : mysql_settings)
        optional_arguments.insert(setting.getName());

    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> required_arguments = {"user", "username", "password", "database", "db"};
    if (require_table)
        required_arguments.insert("table");
    validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(named_collection, required_arguments, optional_arguments);

    configuration.addresses_expr = named_collection.getOrDefault<String>("addresses_expr", "");
    if (configuration.addresses_expr.empty())
    {
        configuration.host = named_collection.getAnyOrDefault<String>({"host", "hostname"}, "");
        configuration.port = static_cast<UInt16>(named_collection.get<UInt64>("port"));
        configuration.addresses = {std::make_pair(configuration.host, configuration.port)};
    }
    else
    {
        size_t max_addresses = context_->getSettingsRef()[Setting::glob_expansion_max_elements];
        configuration.addresses = parseRemoteDescriptionForExternalDatabase(
            configuration.addresses_expr, max_addresses, 3306);
    }

    configuration.username = named_collection.getAny<String>({"username", "user"});
    configuration.password = named_collection.get<String>("password");
    configuration.database = named_collection.getAny<String>({"db", "database"});
    if (require_table)
        configuration.table = named_collection.get<String>("table");
    configuration.replace_query = named_collection.getOrDefault<UInt64>("replace_query", false);
    configuration.on_duplicate_clause = named_collection.getOrDefault<String>("on_duplicate_clause", "");

    for (const auto & setting : mysql_settings)
    {
        const auto & setting_name = setting.getName();
        if (named_collection.has(setting_name))
            storage_settings.set(setting_name, named_collection.get<String>(setting_name));
    }

    return configuration;
}

StorageMySQL::Configuration StorageMySQL::getConfiguration(ASTs engine_args, ContextPtr context_, MySQLSettings & storage_settings)
{
    StorageMySQL::Configuration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context_))
    {
        configuration = StorageMySQL::processNamedCollectionResult(*named_collection, storage_settings, context_);
    }
    else
    {
        if (engine_args.size() < 5 || engine_args.size() > 7)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage MySQL requires 5-7 parameters: "
                            "MySQL('host:port' (or 'addresses_pattern'), database, table, "
                            "'user', 'password'[, replace_query, 'on_duplicate_clause']).");

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context_);

        configuration.addresses_expr = checkAndGetLiteralArgument<String>(engine_args[0], "host:port");
        size_t max_addresses = context_->getSettingsRef()[Setting::glob_expansion_max_elements];

        configuration.addresses = parseRemoteDescriptionForExternalDatabase(configuration.addresses_expr, max_addresses, 3306);
        configuration.database = checkAndGetLiteralArgument<String>(engine_args[1], "database");
        configuration.table = checkAndGetLiteralArgument<String>(engine_args[2], "table");
        configuration.username = checkAndGetLiteralArgument<String>(engine_args[3], "username");
        configuration.password = checkAndGetLiteralArgument<String>(engine_args[4], "password");
        if (engine_args.size() >= 6)
            configuration.replace_query = checkAndGetLiteralArgument<UInt64>(engine_args[5], "replace_query");
        if (engine_args.size() == 7)
            configuration.on_duplicate_clause = checkAndGetLiteralArgument<String>(engine_args[6], "on_duplicate_clause");
    }
    for (const auto & address : configuration.addresses)
        context_->getRemoteHostFilter().checkHostAndPort(address.first, toString(address.second));
    if (configuration.replace_query && !configuration.on_duplicate_clause.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Only one of 'replace_query' and 'on_duplicate_clause' can be specified, or none of them");

    return configuration;
}


void registerStorageMySQL(StorageFactory & factory)
{
    factory.registerStorage("MySQL", [](const StorageFactory::Arguments & args)
    {
        MySQLSettings mysql_settings; /// TODO: move some arguments from the arguments to the SETTINGS.
        auto configuration = StorageMySQL::getConfiguration(args.engine_args, args.getLocalContext(), mysql_settings);

        if (args.storage_def->settings)
            mysql_settings.loadFromQuery(*args.storage_def);

        if (!mysql_settings.connection_pool_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "connection_pool_size cannot be zero.");

        mysqlxx::PoolWithFailover pool = createMySQLPoolWithFailover(configuration, mysql_settings);

        return std::make_shared<StorageMySQL>(
            args.table_id,
            std::move(pool),
            configuration.database,
            configuration.table,
            configuration.replace_query,
            configuration.on_duplicate_clause,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
            mysql_settings);
    },
    {
        .supports_settings = true,
        .supports_schema_inference = true,
        .source_access_type = AccessType::MYSQL,
    });
}

}

#endif
