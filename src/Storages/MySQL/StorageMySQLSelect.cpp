#include "config.h"
#if USE_MYSQL

#include "StorageMySQLSelect.h"

#include <Common/logger_useful.h>
#include <Common/parseRemoteDescription.h>
#include <Common/quoteString.h>
#include <Common/RemoteHostFilter.h>
#include <Core/Settings.h>
#include <DataTypes/convertMySQLDataType.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/MySQLSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/algorithm/transform.hpp>

#include <memory>


namespace DB
{

namespace Setting
{
extern const SettingsUInt64 glob_expansion_max_elements;
extern const SettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace MySQLSetting
{
extern const MySQLSettingsBool connection_auto_close;
}

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int LOGICAL_ERROR;
}

StorageMySQLSelect::StorageMySQLSelect(
    const StorageID & table_id_,
    mysqlxx::PoolWithFailover && pool_,
    const std::string & remote_database_name_,
    const std::string select_query_,
    const ColumnsDescription & columns_,
    const String & comment,
    ContextPtr context_,
    const MySQLSettings & mysql_settings_)

    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_database_name(remote_database_name_)
    , select_query(select_query_)
    , mysql_settings(std::make_unique<MySQLSettings>(mysql_settings_))
    , pool(std::make_shared<mysqlxx::PoolWithFailover>(pool_))
    , log(getLogger("StorageMySQLSelect (" + table_id_.getNameForLogs() + ")"))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = doQueryResultStructure(pool_, select_query_, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageMySQLSelect::read(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names_);
    String query_for_columns = "SELECT ";
    for (const auto & column : column_names_)
    {
        query_for_columns += backQuoteMySQL(column) + ", ";
    }
    query_for_columns = query_for_columns.substr(0, query_for_columns.size() - 2);
    query_for_columns += " FROM (" + select_query + ") AS t";
    LOG_TRACE(log, "Query: {}", query_for_columns);

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);

        WhichDataType which(column_data.type);
        /// Convert enum to string.
        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({column_data.type, column_data.name});
    }


    StreamSettings mysql_input_stream_settings(context_->getSettingsRef(), (*mysql_settings)[MySQLSetting::connection_auto_close]);
    return Pipe(std::make_shared<MySQLWithFailoverSource>(pool, query_for_columns, sample_block, mysql_input_stream_settings));
}


StorageMySQLSelect::Configuration
StorageMySQLSelect::getConfiguration(ASTs storage_args, ContextPtr context_, MySQLSettings & storage_settings)
{
    Configuration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(storage_args, context_))
    {
        ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> optional_arguments = {"addresses_expr", "host", "hostname", "port"};
        auto mysql_settings_names = storage_settings.getAllRegisteredNames();
        for (const auto & name : mysql_settings_names)
            optional_arguments.insert(name);

        ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> required_arguments = {"user", "username", "password", "database", "db", "query"};
        validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(
            *named_collection, required_arguments, optional_arguments);

        configuration.addresses_expr = named_collection->getOrDefault<String>("addresses_expr", "");
        if (configuration.addresses_expr.empty())
        {
            configuration.host = named_collection->getAnyOrDefault<String>({"host", "hostname"}, "");
            configuration.port = static_cast<UInt16>(named_collection->get<UInt64>("port"));
            configuration.addresses = {std::make_pair(configuration.host, configuration.port)};
        }
        else
        {
            size_t max_addresses = context_->getSettingsRef()[Setting::glob_expansion_max_elements];
            configuration.addresses = parseRemoteDescriptionForExternalDatabase(configuration.addresses_expr, max_addresses, 3306);
        }

        configuration.username = named_collection->getAny<String>({"username", "user"});
        configuration.password = named_collection->get<String>("password");
        configuration.database = named_collection->getAny<String>({"db", "database"});
        configuration.select_query = named_collection->get<String>("query");

        storage_settings.loadFromNamedCollection(*named_collection);

        return configuration;
    }
    if (storage_args.size() != 5)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "MySQLSelect storage requires 5 parameters: "
            "'host:port' (or 'addresses_pattern'), database, select query, user, password.");

    for (auto & storage_arg : storage_args)
        storage_arg = evaluateConstantExpressionOrIdentifierAsLiteral(storage_arg, context_);

    configuration.addresses_expr = checkAndGetLiteralArgument<String>(storage_args[0], "host:port");
    size_t max_addresses = context_->getSettingsRef()[Setting::glob_expansion_max_elements];
    configuration.addresses = parseRemoteDescriptionForExternalDatabase(configuration.addresses_expr, max_addresses, 3306);

    configuration.database = checkAndGetLiteralArgument<String>(storage_args[1], "database");
    configuration.select_query = checkAndGetLiteralArgument<String>(storage_args[2], "select_query");
    configuration.username = checkAndGetLiteralArgument<String>(storage_args[3], "username");
    configuration.password = checkAndGetLiteralArgument<String>(storage_args[4], "password");

    for (const auto & address : configuration.addresses)
        context_->getRemoteHostFilter().checkHostAndPort(address.first, toString(address.second));

    return configuration;
}

ColumnsDescription
StorageMySQLSelect::doQueryResultStructure(mysqlxx::PoolWithFailover & pool_, const String & select_query, const ContextPtr & context_)
{
    auto limited_select = "(" + select_query + ") LIMIT 0";
    auto conn = pool_.get();
    auto query = conn->query(limited_select);

    auto query_res = query.use();
    auto field_cnt = query_res.getNumFields();
    if (field_cnt == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MySQL SELECT query returned no fields");

    ColumnsDescription columns;
    const auto & settings = context_->getSettingsRef();
    for (size_t i = 0; i < field_cnt; ++i)
    {
        auto & field = query_res.getField(i);
        ColumnDescription column_description(
            query_res.getFieldName(i), convertMySQLDataType(settings[Setting::mysql_datatypes_support_level], field));
        columns.add(column_description);
    }

    // Preserve correctness of mysqlxx usage in case of force majeure related to "LIMIT 0"
    while (query_res.fetch())
    {
        // do nothing
    }
    return columns;
}
}


#endif
