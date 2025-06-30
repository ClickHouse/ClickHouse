#include <Databases/DataLake/DatabaseDataLake.h>
#include <Core/SettingsEnums.h>
#include <Databases/DataLake/HiveCatalog.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>

#if USE_AVRO && USE_PARQUET

#include <Access/Common/HTTPAuthenticationScheme.h>
#include <Core/Settings.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/DataLake/UnityCatalog.h>
#include <Databases/DataLake/RestCatalog.h>
#include <Databases/DataLake/GlueCatalog.h>
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
namespace DatabaseDataLakeSetting
{
    extern const DatabaseDataLakeSettingsDatabaseDataLakeCatalogType catalog_type;
    extern const DatabaseDataLakeSettingsString warehouse;
    extern const DatabaseDataLakeSettingsString catalog_credential;
    extern const DatabaseDataLakeSettingsString auth_header;
    extern const DatabaseDataLakeSettingsString auth_scope;
    extern const DatabaseDataLakeSettingsString storage_endpoint;
    extern const DatabaseDataLakeSettingsString oauth_server_uri;
    extern const DatabaseDataLakeSettingsBool vended_credentials;
    extern const DatabaseDataLakeSettingsString aws_access_key_id;
    extern const DatabaseDataLakeSettingsString aws_secret_access_key;
    extern const DatabaseDataLakeSettingsString region;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_database_iceberg;
    extern const SettingsBool allow_experimental_database_unity_catalog;
    extern const SettingsBool allow_experimental_database_glue_catalog;
    extern const SettingsBool allow_experimental_database_hms_catalog;
    extern const SettingsBool use_hive_partitioning;
}
namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
    extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
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

DatabaseDataLake::DatabaseDataLake(
    const std::string & database_name_,
    const std::string & url_,
    const DatabaseDataLakeSettings & settings_,
    ASTPtr database_engine_definition_,
    ASTPtr table_engine_definition_,
    UUID uuid)
    : IDatabase(database_name_)
    , url(url_)
    , settings(settings_)
    , database_engine_definition(database_engine_definition_)
    , table_engine_definition(table_engine_definition_)
    , log(getLogger("DatabaseDataLake(" + database_name_ + ")"))
    , db_uuid(uuid)
{
    validateSettings();
}

void DatabaseDataLake::validateSettings()
{
    if (settings[DatabaseDataLakeSetting::catalog_type].value == DB::DatabaseDataLakeCatalogType::GLUE)
    {
        if (settings[DatabaseDataLakeSetting::region].value.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "`region` setting cannot be empty for Glue Catalog. "
                "Please specify 'SETTINGS region=<region_name>' in the CREATE DATABASE query");
    }
    else if (settings[DatabaseDataLakeSetting::warehouse].value.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "`warehouse` setting cannot be empty. "
            "Please specify 'SETTINGS warehouse=<warehouse_name>' in the CREATE DATABASE query");
    }
}

std::shared_ptr<DataLake::ICatalog> DatabaseDataLake::getCatalog() const
{
    if (catalog_impl)
        return catalog_impl;

    switch (settings[DatabaseDataLakeSetting::catalog_type].value)
    {
        case DB::DatabaseDataLakeCatalogType::ICEBERG_REST:
        {
            catalog_impl = std::make_shared<DataLake::RestCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                settings[DatabaseDataLakeSetting::catalog_credential].value,
                settings[DatabaseDataLakeSetting::auth_scope].value,
                settings[DatabaseDataLakeSetting::auth_header],
                settings[DatabaseDataLakeSetting::oauth_server_uri].value,
                Context::getGlobalContextInstance());
            break;
        }
        case DB::DatabaseDataLakeCatalogType::UNITY:
        {
            catalog_impl = std::make_shared<DataLake::UnityCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                settings[DatabaseDataLakeSetting::catalog_credential].value,
                Context::getGlobalContextInstance());
            break;
        }

        case DB::DatabaseDataLakeCatalogType::GLUE:
        {
            catalog_impl = std::make_shared<DataLake::GlueCatalog>(
                settings[DatabaseDataLakeSetting::aws_access_key_id].value,
                settings[DatabaseDataLakeSetting::aws_secret_access_key].value,
                settings[DatabaseDataLakeSetting::region].value,
                url,
                Context::getGlobalContextInstance());
            break;
        }
        case DB::DatabaseDataLakeCatalogType::ICEBERG_HIVE:
        {
#if USE_HIVE
            catalog_impl = std::make_shared<DataLake::HiveCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                Context::getGlobalContextInstance());
            break;
#else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot use 'hive' database engine: ClickHouse was compiled without USE_HIVE built option");
#endif
        }
    }
    return catalog_impl;
}

std::shared_ptr<StorageObjectStorage::Configuration> DatabaseDataLake::getConfiguration(
    DatabaseDataLakeStorageType type,
    DataLakeStorageSettingsPtr storage_settings) const
{
    /// TODO: add tests for azure, local storage types.

    auto catalog = getCatalog();
    switch (catalog->getCatalogType())
    {
        case DB::DatabaseDataLakeCatalogType::ICEBERG_HIVE:
        case DatabaseDataLakeCatalogType::ICEBERG_REST:
        {
            switch (type)
            {
#if USE_AWS_S3
                case DB::DatabaseDataLakeStorageType::S3:
                {
                    return std::make_shared<StorageS3IcebergConfiguration>(storage_settings);
                }
#endif
#if USE_AZURE_BLOB_STORAGE
                case DB::DatabaseDataLakeStorageType::Azure:
                {
                    return std::make_shared<StorageAzureIcebergConfiguration>(storage_settings);
                }
#endif
#if USE_HDFS
                case DB::DatabaseDataLakeStorageType::HDFS:
                {
                    return std::make_shared<StorageHDFSIcebergConfiguration>(storage_settings);
                }
#endif
                case DB::DatabaseDataLakeStorageType::Local:
                {
                    return std::make_shared<StorageLocalIcebergConfiguration>(storage_settings);
                }
                /// Fake storage in case when catalog store not only
                /// primary-type tables (DeltaLake or Iceberg), but for
                /// examples something else like INFORMATION_SCHEMA.
                /// Such tables are unreadable, but at least we can show
                /// them in SHOW CREATE TABLE, as well we can show their
                /// schema.
                /// We use local as substitution for fake because it has 0
                /// dependencies and the most lightweight
                case DB::DatabaseDataLakeStorageType::Other:
                {
                    return std::make_shared<StorageLocalIcebergConfiguration>(storage_settings);
                }
#if !USE_AWS_S3 || !USE_AZURE_BLOB_STORAGE || !USE_HDFS
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Server does not contain support for storage type {} for Iceberg Rest catalog",
                                    type);
#endif
            }
        }
        case DatabaseDataLakeCatalogType::UNITY:
        {
            switch (type)
            {
#if USE_AWS_S3
                case DB::DatabaseDataLakeStorageType::S3:
                {
                    return std::make_shared<StorageS3DeltaLakeConfiguration>(storage_settings);
                }
#endif
                case DB::DatabaseDataLakeStorageType::Local:
                {
                    return std::make_shared<StorageLocalDeltaLakeConfiguration>(storage_settings);
                }
                /// Fake storage in case when catalog store not only
                /// primary-type tables (DeltaLake or Iceberg), but for
                /// examples something else like INFORMATION_SCHEMA.
                /// Such tables are unreadable, but at least we can show
                /// them in SHOW CREATE TABLE, as well we can show their
                /// schema.
                /// We use local as substitution for fake because it has 0
                /// dependencies and the most lightweight
                case DB::DatabaseDataLakeStorageType::Other:
                {
                    return std::make_shared<StorageLocalDeltaLakeConfiguration>(storage_settings);
                }
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Server does not contain support for storage type {} for Unity catalog",
                                    type);
            }
        }
        case DatabaseDataLakeCatalogType::GLUE:
        {
            switch (type)
            {
#if USE_AWS_S3
                case DB::DatabaseDataLakeStorageType::S3:
                {
                    return std::make_shared<StorageS3IcebergConfiguration>(storage_settings);
                }
#endif
                case DB::DatabaseDataLakeStorageType::Other:
                {
                    return std::make_shared<StorageLocalIcebergConfiguration>(storage_settings);
                }
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Server does not contain support for storage type {} for Glue catalog",
                                    type);
            }
        }
    }
}

std::string DatabaseDataLake::getStorageEndpointForTable(const DataLake::TableMetadata & table_metadata) const
{
    auto endpoint_from_settings = settings[DatabaseDataLakeSetting::storage_endpoint].value;
    if (endpoint_from_settings.empty())
        return table_metadata.getLocation();
    else
        return table_metadata.getLocationWithEndpoint(endpoint_from_settings);
}

bool DatabaseDataLake::empty() const
{
    return getCatalog()->empty();
}

bool DatabaseDataLake::isTableExist(const String & name, ContextPtr /* context_ */) const
{
    const auto [namespace_name, table_name] = parseTableName(name);
    return getCatalog()->existsTable(namespace_name, table_name);
}

StoragePtr DatabaseDataLake::tryGetTable(const String & name, ContextPtr context_)  const
{
    return tryGetTableImpl(name, context_, false, false);
}

StoragePtr DatabaseDataLake::tryGetTableImpl(const String & name, ContextPtr context_, bool lightweight, bool ignore_if_not_iceberg) const
{
    auto catalog = getCatalog();
    auto table_metadata = DataLake::TableMetadata().withSchema().withLocation().withDataLakeSpecificProperties();

    const bool with_vended_credentials = settings[DatabaseDataLakeSetting::vended_credentials].value;
    if (!lightweight && with_vended_credentials)
        table_metadata = table_metadata.withStorageCredentials();

    auto [namespace_name, table_name] = parseTableName(name);

    if (!catalog->tryGetTableMetadata(namespace_name, table_name, table_metadata))
        return nullptr;

    if (ignore_if_not_iceberg && !table_metadata.isDefaultReadableTable())
        return nullptr;

    if (!lightweight && !table_metadata.isDefaultReadableTable())
    {
        throw Exception::createRuntime(ErrorCodes::DATALAKE_DATABASE_ERROR, table_metadata.getReasonWhyTableIsUnreadable());
    }

    /// Take database engine definition AST as base.
    ASTStorage * storage = table_engine_definition->as<ASTStorage>();
    ASTs args = storage->engine->arguments->children;

    if (table_metadata.hasLocation())
    {
        /// Replace Iceberg Catalog endpoint with storage path endpoint of requested table.
        auto table_endpoint = getStorageEndpointForTable(table_metadata);
        LOG_DEBUG(log, "Table endpoint {}", table_endpoint);
        if (table_endpoint.starts_with(DataLake::FILE_PATH_PREFIX))
            table_endpoint = table_endpoint.substr(DataLake::FILE_PATH_PREFIX.length());
        if (args.empty())
            args.emplace_back(std::make_shared<ASTLiteral>(table_endpoint));
        else
            args[0] = std::make_shared<ASTLiteral>(table_endpoint);
    }

    /// We either fetch storage credentials from catalog
    /// or get storage credentials from database engine arguments
    /// in CREATE query (e.g. in `args`).
    /// Vended credentials can be disabled in catalog itself,
    /// so we have a separate setting to know whether we should even try to fetch them.
    if (args.size() == 1)
    {
        if (table_metadata.hasStorageCredentials())
        {
            LOG_DEBUG(log, "Getting credentials");
            auto storage_credentials = table_metadata.getStorageCredentials();
            if (storage_credentials)
            {
                LOG_DEBUG(log, "Has credentials");
                storage_credentials->addCredentialsToEngineArgs(args);
            }
            else
            {
                LOG_DEBUG(log, "Has no credentials");
            }
        }
        else if (!lightweight && table_metadata.requiresCredentials())
        {
            throw Exception(
               ErrorCodes::BAD_ARGUMENTS,
               "Either vended credentials need to be enabled "
               "or storage credentials need to be specified in database engine arguments in CREATE query");
        }
    }

    LOG_TEST(log, "Using table endpoint: {}", args[0]->as<ASTLiteral>()->value.safeGet<String>());

    const auto columns = ColumnsDescription(table_metadata.getSchema());

    DatabaseDataLakeStorageType storage_type = DatabaseDataLakeStorageType::Other;
    auto storage_type_from_catalog = catalog->getStorageType();
    if (storage_type_from_catalog.has_value())
    {
        storage_type = storage_type_from_catalog.value();
    }
    else
    {
        if (table_metadata.hasLocation() || !lightweight)
            storage_type = table_metadata.getStorageType();
    }

    auto storage_settings = std::make_shared<DataLakeStorageSettings>();
    storage_settings->loadFromSettingsChanges(settings.allChanged());

    if (auto table_specific_properties = table_metadata.getDataLakeSpecificProperties();
        table_specific_properties.has_value())
    {
        auto metadata_location = table_specific_properties->iceberg_metadata_file_location;
        if (!metadata_location.empty())
        {
            const auto data_location = table_metadata.getLocation();
            if (metadata_location.starts_with(data_location))
            {
                size_t remove_slash = metadata_location[data_location.size()] == '/' ? 1 : 0;
                metadata_location = metadata_location.substr(data_location.size() + remove_slash);
            }
        }

        (*storage_settings)[DB::DataLakeStorageSetting::iceberg_metadata_file_path] = metadata_location;
    }

    const auto configuration = getConfiguration(storage_type, storage_settings);

    /// HACK: Hacky-hack to enable lazy load
    ContextMutablePtr context_copy = Context::createCopy(context_);
    Settings settings_copy = context_copy->getSettingsCopy();
    settings_copy[Setting::use_hive_partitioning] = false;
    context_copy->setSettings(settings_copy);

    /// with_table_structure = false: because there will be
    /// no table structure in table definition AST.
    StorageObjectStorage::Configuration::initialize(*configuration, args, context_copy, /* with_table_structure */false);

    return std::make_shared<StorageObjectStorage>(
        configuration,
        configuration->createObjectStorage(context_copy, /* is_readonly */ false),
        context_copy,
        StorageID(getDatabaseName(), name),
        /* columns */columns,
        /* constraints */ConstraintsDescription{},
        /* comment */"",
        getFormatSettings(context_copy),
        LoadingStrictnessLevel::CREATE,
        /* distributed_processing */false,
        /* partition_by */nullptr,
        /* is_table_function */false,
        /* lazy_init */true);
}

DatabaseTablesIteratorPtr DatabaseDataLake::getTablesIterator(
    ContextPtr context_,
    const FilterByNameFunction & filter_by_table_name,
    bool skip_not_loaded) const
{
    Tables tables;
    auto catalog = getCatalog();
    const auto iceberg_tables = catalog->getTables();

    auto & pool = Context::getGlobalContextInstance()->getIcebergCatalogThreadpool();

    std::vector<std::shared_ptr<std::promise<StoragePtr>>> promises;
    std::vector<std::future<StoragePtr>> futures;
    for (const auto & table_name : iceberg_tables)
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;

        try
        {
            promises.emplace_back(std::make_shared<std::promise<StoragePtr>>());
            futures.emplace_back(promises.back()->get_future());

            pool.scheduleOrThrow(
                [this, table_name, skip_not_loaded, context_, promise=promises.back()]() mutable
                {
                    try
                    {
                        auto storage = tryGetTableImpl(table_name, context_, false, skip_not_loaded);
                        promise->set_value(storage);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log, fmt::format("Ignoring table {}", table_name));
                        promise->set_exception(std::current_exception());
                    }
                });
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to schedule task");
            pool.wait();

            throw;
        }
    }

    for (const auto & future : futures)
        future.wait();

    size_t future_index = 0;
    for (const auto & table_name : iceberg_tables)
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;

        [[maybe_unused]] bool inserted = tables.emplace(table_name, futures[future_index].get()).second;
        chassert(inserted);
        future_index++;
    }
    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, getDatabaseName());
}

DatabaseTablesIteratorPtr DatabaseDataLake::getLightweightTablesIterator(
    ContextPtr context_,
    const FilterByNameFunction & filter_by_table_name,
    bool skip_not_loaded) const
{
    Tables tables;
    auto catalog = getCatalog();
    const auto iceberg_tables = catalog->getTables();

    auto & pool = Context::getGlobalContextInstance()->getIcebergCatalogThreadpool();

    std::vector<std::shared_ptr<std::promise<StoragePtr>>> promises;
    std::vector<std::future<StoragePtr>> futures;

    for (const auto & table_name : iceberg_tables)
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;

        /// NOTE: There are one million of different ways how we can receive
        /// weird response from different catalogs. tryGetTableImpl will not
        /// throw only in case of expected errors, but sometimes we can receive
        /// completely unexpected results for some objects which can be stored
        /// in catalogs. But this function is used in SHOW TABLES query which
        /// should return at least properly described tables. That is why we
        /// have this try/catch here.
        try
        {
            promises.emplace_back(std::make_shared<std::promise<StoragePtr>>());
            futures.emplace_back(promises.back()->get_future());

            pool.scheduleOrThrow(
                [this, table_name, skip_not_loaded, context_, promise = promises.back()] mutable
                {
                    StoragePtr storage = nullptr;
                    try
                    {
                        storage = tryGetTableImpl(table_name, context_, true, skip_not_loaded);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log, fmt::format("ignoring table {}", table_name));
                    }
                    promise->set_value(storage);
                });
        }
        catch (...)
        {
            promises.back()->set_value(nullptr);
            tryLogCurrentException(log, "Failed to schedule task into pool");
        }
    }

    for (const auto & future : futures)
        future.wait();

    size_t future_index = 0;
    for (const auto & table_name : iceberg_tables)
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;

        if (auto storage_ptr = futures[future_index].get(); storage_ptr != nullptr)
        {
            [[maybe_unused]] bool inserted = tables.emplace(table_name, storage_ptr).second;
            chassert(inserted);
        }
        future_index++;
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, getDatabaseName());
}

ASTPtr DatabaseDataLake::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->setDatabase(getDatabaseName());
    create_query->set(create_query->storage, database_engine_definition);
    return create_query;
}

ASTPtr DatabaseDataLake::getCreateTableQueryImpl(
    const String & name,
    ContextPtr /* context_ */,
    bool /* throw_on_error */) const
{
    auto catalog = getCatalog();
    auto table_metadata = DataLake::TableMetadata().withLocation().withSchema();

    const auto [namespace_name, table_name] = parseTableName(name);

    if (!catalog->tryGetTableMetadata(namespace_name, table_name, table_metadata))
    {
        throw Exception(
            ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Table `{}` doesn't exist", name);
    }

    auto create_table_query = std::make_shared<ASTCreateQuery>();
    auto table_storage_define = table_engine_definition->clone();

    auto * storage = table_storage_define->as<ASTStorage>();
    storage->engine->kind = ASTFunction::Kind::TABLE_ENGINE;
    if (!table_metadata.isDefaultReadableTable())
        storage->engine->name = DataLake::FAKE_TABLE_ENGINE_NAME_FOR_UNREADABLE_TABLES;

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
        LOG_DEBUG(log, "Processing column {}", column_type_and_name.name);
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column_type_and_name.name;
        column_declaration->type = makeASTDataType(column_type_and_name.type->getName());
        columns_expression_list->children.emplace_back(column_declaration);
    }

    auto storage_engine_arguments = storage->engine->arguments;
    if (table_metadata.isDefaultReadableTable())
    {
        auto table_endpoint = getStorageEndpointForTable(table_metadata);
        if (table_endpoint.starts_with(DataLake::FILE_PATH_PREFIX))
            table_endpoint = table_endpoint.substr(DataLake::FILE_PATH_PREFIX.length());

        LOG_DEBUG(log, "Table endpoint {}", table_endpoint);
        if (storage_engine_arguments->children.empty())
            storage_engine_arguments->children.emplace_back();

        storage_engine_arguments->children[0] = std::make_shared<ASTLiteral>(table_endpoint);
    }
    else
    {
        storage_engine_arguments->children.clear();
    }

    return create_table_query;
}

void registerDatabaseDataLake(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        const auto * database_engine_define = args.create_query.storage;
        const auto & database_engine_name = args.engine_name;

        DatabaseDataLakeSettings database_settings;
        if (database_engine_define->settings)
            database_settings.loadFromQuery(*database_engine_define);

        auto catalog_type = database_settings[DB::DatabaseDataLakeSetting::catalog_type].value;
        /// Glue catalog is one per region, so it's fully identified by aws keys and region
        /// There is no URL you need to provide in constructor, even if we would want it
        ///  will be something like https://aws.amazon.com.
        ///
        ///  NOTE: it's still possible to provide endpoint argument for Glue. It's used for fake
        ///  mock glue catalog in tests only.
        bool requires_arguments = catalog_type != DatabaseDataLakeCatalogType::GLUE;

        const ASTFunction * function_define = database_engine_define->engine;

        ASTs engine_args;
        if (requires_arguments && !function_define->arguments)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", database_engine_name);
        }

        if (function_define->arguments)
        {
            engine_args = function_define->arguments->children;
            if (requires_arguments && engine_args.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", database_engine_name);
        }

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.context);

        std::string url;
        if (!engine_args.empty())
            url = engine_args[0]->as<ASTLiteral>()->value.safeGet<String>();

        auto engine_for_tables = database_engine_define->clone();
        ASTFunction * engine_func = engine_for_tables->as<ASTStorage &>().engine;
        if (engine_func->arguments == nullptr)
        {
            engine_func->arguments = std::make_shared<ASTExpressionList>();
        }

        switch (catalog_type)
        {
            case DatabaseDataLakeCatalogType::ICEBERG_REST:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_iceberg])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DatabaseDataLake with Icerberg Rest catalog is experimental. "
                                    "To allow its usage, enable setting allow_experimental_database_iceberg");
                }

                engine_func->name = "Iceberg";
                break;
            }
            case DatabaseDataLakeCatalogType::GLUE:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_glue_catalog])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DatabaseDataLake with Glue catalog is experimental. "
                                    "To allow its usage, enable setting allow_experimental_database_glue_catalog");
                }

                engine_func->name = "Iceberg";
                break;
            }
            case DatabaseDataLakeCatalogType::UNITY:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_unity_catalog])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DataLake database with Unity catalog catalog is experimental. "
                                    "To allow its usage, enable setting allow_experimental_database_unity_catalog");
                }

                engine_func->name = "DeltaLake";
                break;
            }
            case DatabaseDataLakeCatalogType::ICEBERG_HIVE:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_hms_catalog])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DatabaseDataLake with Icerberg Hive catalog is experimental. "
                                    "To allow its usage, enable setting allow_experimental_database_hms_catalog");
                }

                engine_func->name = "Iceberg";
                break;
            }
        }

        return std::make_shared<DatabaseDataLake>(
            args.database_name,
            url,
            database_settings,
            database_engine_define->clone(),
            std::move(engine_for_tables),
            args.uuid);
    };
    factory.registerDatabase("DataLakeCatalog", create_fn, { .supports_arguments = true, .supports_settings = true });
}

}

#endif
