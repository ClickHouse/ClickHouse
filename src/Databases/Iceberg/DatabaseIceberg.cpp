#include <Databases/Iceberg/DatabaseIceberg.h>

#if USE_AVRO
#include <Access/Common/HTTPAuthenticationScheme.h>
#include <Core/Settings.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/Iceberg/RestCatalog.h>
#include <DataTypes/DataTypeString.h>

#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/StorageNull.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

#include <Formats/FormatFactory.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDataType.h>


namespace DB
{
namespace DatabaseIcebergSetting
{
    extern const DatabaseIcebergSettingsDatabaseIcebergCatalogType catalog_type;
    extern const DatabaseIcebergSettingsString warehouse;
    extern const DatabaseIcebergSettingsString catalog_credential;
    extern const DatabaseIcebergSettingsString auth_header;
    extern const DatabaseIcebergSettingsString auth_scope;
    extern const DatabaseIcebergSettingsString storage_endpoint;
    extern const DatabaseIcebergSettingsString oauth_server_uri;
    extern const DatabaseIcebergSettingsBool vended_credentials;
}
namespace Setting
{
    extern const SettingsBool allow_experimental_database_iceberg;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
    /// Parse a string, containing at least one dot, into a two substrings:
    /// A.B.C.D.E -> A.B.C.D and E, where
    /// `A.B.C.D` is a table "namespace".
    /// `E` is a table name.
    std::pair<std::string, std::string> parseTableName(const std::string & name)
    {
        auto pos = name.rfind('.');
        if (pos == std::string::npos)
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Table cannot have empty namespace: {}", name);

        auto table_name = name.substr(pos + 1);
        auto namespace_name = name.substr(0, name.size() - table_name.size() - 1);
        return {namespace_name, table_name};
    }
}

DatabaseIceberg::DatabaseIceberg(
    const std::string & database_name_,
    const std::string & url_,
    const DatabaseIcebergSettings & settings_,
    ASTPtr database_engine_definition_)
    : IDatabase(database_name_)
    , url(url_)
    , settings(settings_)
    , database_engine_definition(database_engine_definition_)
    , log(getLogger("DatabaseIceberg(" + database_name_ + ")"))
{
    validateSettings();
}

void DatabaseIceberg::validateSettings()
{
    if (settings[DatabaseIcebergSetting::warehouse].value.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "`warehouse` setting cannot be empty. "
            "Please specify 'SETTINGS warehouse=<warehouse_name>' in the CREATE DATABASE query");
    }
}

std::shared_ptr<Iceberg::ICatalog> DatabaseIceberg::getCatalog() const
{
    if (catalog_impl)
        return catalog_impl;

    switch (settings[DatabaseIcebergSetting::catalog_type].value)
    {
        case DB::DatabaseIcebergCatalogType::REST:
        {
            catalog_impl = std::make_shared<Iceberg::RestCatalog>(
                settings[DatabaseIcebergSetting::warehouse].value,
                url,
                settings[DatabaseIcebergSetting::catalog_credential].value,
                settings[DatabaseIcebergSetting::auth_scope].value,
                settings[DatabaseIcebergSetting::auth_header],
                settings[DatabaseIcebergSetting::oauth_server_uri].value,
                Context::getGlobalContextInstance());
        }
    }
    return catalog_impl;
}

std::shared_ptr<StorageObjectStorage::Configuration> DatabaseIceberg::getConfiguration(DatabaseIcebergStorageType type) const
{
    /// TODO: add tests for azure, local storage types.

    switch (type)
    {
#if USE_AWS_S3
        case DB::DatabaseIcebergStorageType::S3:
        {
            return std::make_shared<StorageS3IcebergConfiguration>();
        }
#endif
#if USE_AZURE_BLOB_STORAGE
        case DB::DatabaseIcebergStorageType::Azure:
        {
            return std::make_shared<StorageAzureIcebergConfiguration>();
        }
#endif
#if USE_HDFS
        case DB::DatabaseIcebergStorageType::HDFS:
        {
            return std::make_shared<StorageHDFSIcebergConfiguration>();
        }
#endif
        case DB::DatabaseIcebergStorageType::Local:
        {
            return std::make_shared<StorageLocalIcebergConfiguration>();
        }
#if !USE_AWS_S3 || !USE_AZURE_BLOB_STORAGE || !USE_HDFS
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Server does not contain support for storage type {}",
                            type);
#endif
    }
}

std::string DatabaseIceberg::getStorageEndpointForTable(const Iceberg::TableMetadata & table_metadata) const
{
    auto endpoint_from_settings = settings[DatabaseIcebergSetting::storage_endpoint].value;
    if (endpoint_from_settings.empty())
        return table_metadata.getLocation();
    else
        return table_metadata.getLocationWithEndpoint(endpoint_from_settings);

}

bool DatabaseIceberg::empty() const
{
    return getCatalog()->empty();
}

bool DatabaseIceberg::isTableExist(const String & name, ContextPtr /* context_ */) const
{
    const auto [namespace_name, table_name] = parseTableName(name);
    return getCatalog()->existsTable(namespace_name, table_name);
}

StoragePtr DatabaseIceberg::tryGetTable(const String & name, ContextPtr context_) const
{
    auto catalog = getCatalog();
    auto table_metadata = Iceberg::TableMetadata().withLocation().withSchema();

    const bool with_vended_credentials = settings[DatabaseIcebergSetting::vended_credentials].value;
    if (with_vended_credentials)
        table_metadata = table_metadata.withStorageCredentials();

    auto [namespace_name, table_name] = parseTableName(name);

    if (!catalog->tryGetTableMetadata(namespace_name, table_name, table_metadata))
        return nullptr;

    /// Take database engine definition AST as base.
    ASTStorage * storage = database_engine_definition->as<ASTStorage>();
    ASTs args = storage->engine->arguments->children;

    /// Replace Iceberg Catalog endpoint with storage path endpoint of requested table.
    auto table_endpoint = getStorageEndpointForTable(table_metadata);
    args[0] = std::make_shared<ASTLiteral>(table_endpoint);

    /// We either fetch storage credentials from catalog
    /// or get storage credentials from database engine arguments
    /// in CREATE query (e.g. in `args`).
    /// Vended credentials can be disabled in catalog itself,
    /// so we have a separate setting to know whether we should even try to fetch them.
    if (with_vended_credentials && args.size() == 1)
    {
        auto storage_credentials = table_metadata.getStorageCredentials();
        if (storage_credentials)
            storage_credentials->addCredentialsToEngineArgs(args);
    }
    else if (args.size() == 1)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Either vended credentials need to be enabled "
            "or storage credentials need to be specified in database engine arguments in CREATE query");
    }

    LOG_TEST(log, "Using table endpoint: {}", args[0]->as<ASTLiteral>()->value.safeGet<String>());

    const auto columns = ColumnsDescription(table_metadata.getSchema());

    DatabaseIcebergStorageType storage_type;
    auto storage_type_from_catalog = catalog->getStorageType();
    if (storage_type_from_catalog.has_value())
        storage_type = storage_type_from_catalog.value();
    else
        storage_type = table_metadata.getStorageType();

    const auto configuration = getConfiguration(storage_type);
    auto storage_settings = std::make_unique<StorageObjectStorageSettings>();

    /// with_table_structure = false: because there will be
    /// no table structure in table definition AST.
    StorageObjectStorage::Configuration::initialize(*configuration, args, context_, /* with_table_structure */false, std::move(storage_settings));

    return std::make_shared<StorageObjectStorage>(
        configuration,
        configuration->createObjectStorage(context_, /* is_readonly */ false),
        context_,
        StorageID(getDatabaseName(), name),
        /* columns */columns,
        /* constraints */ConstraintsDescription{},
        /* comment */"",
        getFormatSettings(context_),
        LoadingStrictnessLevel::CREATE,
        /* distributed_processing */false,
        /* partition_by */nullptr,
        /* lazy_init */true);
}

DatabaseTablesIteratorPtr DatabaseIceberg::getTablesIterator(
    ContextPtr context_,
    const FilterByNameFunction & filter_by_table_name,
    bool /* skip_not_loaded */) const
{
    Tables tables;
    auto catalog = getCatalog();
    const auto iceberg_tables = catalog->getTables();

    for (const auto & table_name : iceberg_tables)
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;

        auto storage = tryGetTable(table_name, context_);
        [[maybe_unused]] bool inserted = tables.emplace(table_name, storage).second;
        chassert(inserted);
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, getDatabaseName());
}

ASTPtr DatabaseIceberg::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->setDatabase(getDatabaseName());
    create_query->set(create_query->storage, database_engine_definition);
    return create_query;
}

ASTPtr DatabaseIceberg::getCreateTableQueryImpl(
    const String & name,
    ContextPtr /* context_ */,
    bool /* throw_on_error */) const
{
    auto catalog = getCatalog();
    auto table_metadata = Iceberg::TableMetadata().withLocation().withSchema();

    const auto [namespace_name, table_name] = parseTableName(name);
    catalog->getTableMetadata(namespace_name, table_name, table_metadata);

    auto create_table_query = std::make_shared<ASTCreateQuery>();
    auto table_storage_define = database_engine_definition->clone();

    auto * storage = table_storage_define->as<ASTStorage>();
    storage->engine->kind = ASTFunction::Kind::TABLE_ENGINE;
    storage->settings = {};

    create_table_query->set(create_table_query->storage, table_storage_define);

    auto columns_declare_list = std::make_shared<ASTColumns>();
    auto columns_expression_list = std::make_shared<ASTExpressionList>();

    columns_declare_list->set(columns_declare_list->columns, columns_expression_list);
    create_table_query->set(create_table_query->columns_list, columns_declare_list);

    create_table_query->setTable(name);
    create_table_query->setDatabase(getDatabaseName());

    for (const auto & column_type_and_name : table_metadata.getSchema())
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column_type_and_name.name;
        column_declaration->type = makeASTDataType(column_type_and_name.type->getName());
        columns_expression_list->children.emplace_back(column_declaration);
    }

    auto storage_engine_arguments = storage->engine->arguments;
    if (storage_engine_arguments->children.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Unexpected number of arguments: {}",
            storage_engine_arguments->children.size());
    }

    auto table_endpoint = getStorageEndpointForTable(table_metadata);
    storage_engine_arguments->children[0] = std::make_shared<ASTLiteral>(table_endpoint);

    return create_table_query;
}

void registerDatabaseIceberg(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        if (!args.create_query.attach
            && !args.context->getSettingsRef()[Setting::allow_experimental_database_iceberg])
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "DatabaseIceberg engine is experimental. "
                            "To allow its usage, enable setting allow_experimental_database_iceberg");
        }

        const auto * database_engine_define = args.create_query.storage;
        const auto & database_engine_name = args.engine_name;

        const ASTFunction * function_define = database_engine_define->engine;
        if (!function_define->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", database_engine_name);

        ASTs & engine_args = function_define->arguments->children;
        if (engine_args.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", database_engine_name);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.context);

        const auto url = engine_args[0]->as<ASTLiteral>()->value.safeGet<String>();

        DatabaseIcebergSettings database_settings;
        if (database_engine_define->settings)
            database_settings.loadFromQuery(*database_engine_define);

        return std::make_shared<DatabaseIceberg>(
            args.database_name,
            url,
            database_settings,
            database_engine_define->clone());
    };
    factory.registerDatabase("Iceberg", create_fn, { .supports_arguments = true, .supports_settings = true });
}

}

#endif
