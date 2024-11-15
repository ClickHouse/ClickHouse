#include <Databases/Iceberg/DatabaseIceberg.h>

#if USE_AVRO
#include <Access/Common/HTTPAuthenticationScheme.h>

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
    extern const DatabaseIcebergSettingsString storage_endpoint;
    extern const DatabaseIcebergSettingsString auth_header;
    extern const DatabaseIcebergSettingsDatabaseIcebergCatalogType catalog_type;
    extern const DatabaseIcebergSettingsDatabaseIcebergStorageType storage_type;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

    void setCredentials(Poco::Net::HTTPBasicCredentials & credentials, const Poco::URI & request_uri)
    {
        const auto & user_info = request_uri.getUserInfo();
        if (!user_info.empty())
        {
            std::size_t n = user_info.find(':');
            if (n != std::string::npos)
            {
                credentials.setUsername(user_info.substr(0, n));
                credentials.setPassword(user_info.substr(n + 1));
            }
        }
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
    setCredentials(credentials, Poco::URI(url));

    const auto auth_header = settings[DatabaseIcebergSetting::auth_header].value;
    if (!auth_header.empty())
    {
        auto pos = auth_header.find(':');
        if (pos == std::string::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected format of auth header");
        headers.emplace_back(auth_header.substr(0, pos), auth_header.substr(pos + 1));
    }

}

std::unique_ptr<Iceberg::ICatalog> DatabaseIceberg::getCatalog(ContextPtr context_) const
{
    switch (settings[DatabaseIcebergSetting::catalog_type].value)
    {
        case DB::DatabaseIcebergCatalogType::REST:
        {
            return std::make_unique<Iceberg::RestCatalog>(getDatabaseName(), url, context_);
        }
    }
}

std::shared_ptr<StorageObjectStorage::Configuration> DatabaseIceberg::getConfiguration() const
{
    switch (settings[DatabaseIcebergSetting::storage_type].value)
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
                            settings[DatabaseIcebergSetting::storage_type].value);
#endif
    }
}

std::string DatabaseIceberg::getStorageEndpointForTable(const Iceberg::TableMetadata & table_metadata) const
{
    return std::filesystem::path(settings[DatabaseIcebergSetting::storage_endpoint].value)
        / table_metadata.getPath()
        / "";
}

bool DatabaseIceberg::empty() const
{
    return getCatalog(Context::getGlobalContextInstance())->empty();
}

bool DatabaseIceberg::isTableExist(const String & name, ContextPtr context_) const
{
    const auto [namespace_name, table_name] = parseTableName(name);
    return getCatalog(context_)->existsTable(namespace_name, table_name);
}

StoragePtr DatabaseIceberg::tryGetTable(const String & name, ContextPtr context_) const
{
    auto catalog = getCatalog(context_);
    auto table_metadata = Iceberg::TableMetadata().withLocation().withSchema();
    auto [namespace_name, table_name] = parseTableName(name);

    if (!catalog->tryGetTableMetadata(namespace_name, table_name, table_metadata))
        return nullptr;

    /// Take database engine definition AST as base.
    ASTStorage * storage = database_engine_definition->as<ASTStorage>();
    ASTs args = storage->engine->arguments->children;

    /// Replace Iceberg Catalog endpoint with storage path endpoint of requested table.
    auto table_endpoint = getStorageEndpointForTable(table_metadata);
    args[0] = std::make_shared<ASTLiteral>(table_endpoint);

    LOG_TEST(log, "Using table endpoint: {}", table_endpoint);

    const auto columns = ColumnsDescription(table_metadata.getSchema());
    const auto configuration = getConfiguration();

    /// with_table_structure = false: because there will be
    /// no table structure in table definition AST.
    StorageObjectStorage::Configuration::initialize(*configuration, args, context_, /* with_table_structure */false);

    return std::make_shared<StorageObjectStorage>(
        configuration,
        configuration->createObjectStorage(context_, /* is_readonly */ false),
        context_,
        StorageID(getDatabaseName(), name),
        /* columns */columns,
        /* constraints */ConstraintsDescription{},
        /* comment */"",
        getFormatSettings(context_),
        LoadingStrictnessLevel::CREATE);
}

DatabaseTablesIteratorPtr DatabaseIceberg::getTablesIterator(
    ContextPtr context_,
    const FilterByNameFunction & filter_by_table_name,
    bool /* skip_not_loaded */) const
{
    Tables tables;
    auto catalog = getCatalog(context_);
    for (const auto & table_name : catalog->getTables())
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;

        auto storage = tryGetTable(table_name, context_);
        tables.emplace(table_name, storage);
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
    ContextPtr context_,
    bool /* throw_on_error */) const
{
    auto catalog = getCatalog(context_);
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
        const auto * database_engine_define = args.create_query.storage;
        const auto & database_engine_name = args.engine_name;

        const ASTFunction * function_define = database_engine_define->engine;
        if (!function_define->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", database_engine_name);

        ASTs & engine_args = function_define->arguments->children;
        if (engine_args.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", database_engine_name);

        const size_t max_args_num = 3;
        if (engine_args.size() != max_args_num)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine must have {} arguments", max_args_num);

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
