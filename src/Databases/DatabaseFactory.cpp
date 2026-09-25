#include <filesystem>

#include <Core/Settings.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>

#if CLICKHOUSE_CLOUD
#include <Databases/DatabaseShared.h>
#endif

#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/Macros.h>
#include <Common/filesystemHelpers.h>

#if CLICKHOUSE_CLOUD
#include <Interpreters/SharedDatabaseCatalog.h>
#endif


namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool log_queries;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int CANNOT_CREATE_DATABASE;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_SETTING;
}

static void cckMetadataPathForOrdinary(const ASTCreateQuery & create, const String & metadata_path)
{
    auto default_db_disk = Context::getGlobalContextInstance()->getDatabaseDisk();

    if (!default_db_disk->isSymlinkSupported())
        return;

    const String & engine_name = create.storage->engine->name;
    const String & database_name = create.getDatabase();

    if (engine_name != "Ordinary")
        return;

    if (!default_db_disk->isSymlink(metadata_path))
        return;

    String target_path = default_db_disk->readSymlink(metadata_path);
    fs::path path_to_remove = metadata_path;
    if (path_to_remove.filename().empty())
        path_to_remove = path_to_remove.parent_path();

    /// Before 20.7 metadata/db_name.sql file might absent and Ordinary database was attached if there's metadata/db_name/ dir.
    /// Between 20.7 and 22.7 metadata/db_name.sql was created in this case as well.
    /// Since 20.7 `default` database is created with Atomic engine on the very first server run.
    /// The problem is that if server crashed during the very first run and metadata/db_name/ -> store/whatever symlink was created
    /// then it's considered as Ordinary database. And it even works somehow
    /// until background task tries to remove unused dir from store/...
    throw Exception(ErrorCodes::CANNOT_CREATE_DATABASE,
                    "Metadata directory {} for Ordinary database {} is a symbolic link to {}. "
                    "It may be a result of manual intervention, crash on very first server start or a bug. "
                    "Database cannot be attached (it's kind of protection from potential data loss). "
                    "Metadata directory must not be a symlink and must contain tables metadata files itself. "
                    "You have to resolve this manually. It can be done like this: rm {}; sudo -u clickhouse mv {} {};",
                    metadata_path, database_name, target_path,
                    quoteString(path_to_remove.string()), quoteString(target_path), quoteString(path_to_remove.string()));

}

void checkDatabaseSettingNames(
    const ASTCreateQuery & create,
    ContextPtr context,
    LoadingStrictnessLevel mode,
    bool is_metadata_replay,
    bool is_restore_from_backup)
{
    const auto * storage = create.storage;
    if (!storage || !storage->engine || !storage->settings)
        return;

    /// Plain `ATTACH` is the mode of both a stored-definition replay and a user's own `ATTACH DATABASE`, so
    /// the replay flag tells them apart; a backup holds a `CREATE DATABASE`, so its restore replays at `CREATE`.
    /// A Shared Catalog secondary re-executes the initiator's `CREATE DATABASE`, so it states nothing new.
#if CLICKHOUSE_CLOUD
    const bool is_shared_catalog_replay
        = context->getClientInfo().is_shared_catalog_internal && !SharedDatabaseCatalog::isInitialQuery(context);
#else
    const bool is_shared_catalog_replay = false;
#endif
    if (create.attach_short_syntax || (is_metadata_replay && mode >= LoadingStrictnessLevel::ATTACH) || is_restore_from_backup
        || is_shared_catalog_replay)
        return;

    /// An unregistered engine, and one that accepts no settings at all, are both reported by `validate`.
    const auto * features = DatabaseFactory::instance().tryGetDatabaseEngineFeatures(storage->engine->name);
    if (!features || !features->supports_settings)
        return;

    const Settings & query_settings = context->getSettingsRef();
    auto reject = [&](std::string_view name)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_SETTING, "Unknown setting '{}': for database engine {}", name, storage->engine->name);
    };

    /// `name = DEFAULT` is parsed into `default_settings`, not `changes`, and round-trips into the stored definition.
    for (const auto & name : storage->settings->default_settings)
        if (!features->has_builtin_setting_fn(name) && !query_settings.has(name))
            reject(name);
    /// `param_x = ...` lands in `query_parameters` with the prefix stripped, and nothing hoists a name out of it.
    for (const auto & parameter : storage->settings->query_parameters)
        reject(QUERY_PARAMETER_NAME_PREFIX + parameter.first);
}

void DatabaseFactory::validate(const ASTCreateQuery & create_query) const
{
    auto * storage = create_query.storage;

    const String & engine_name = storage->engine->name;
    const EngineFeatures & engine_features = database_engines.at(engine_name).features;

    /// Check engine may have arguments
    if (storage->engine->arguments && !engine_features.supports_arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine `{}` cannot have arguments", engine_name);

    /// Check engine may have settings
    bool has_unexpected_element = storage->engine->parameters || storage->partition_by ||
        storage->primary_key || storage->order_by ||
        storage->sample_by;
    if (has_unexpected_element || (!engine_features.supports_settings && storage->settings))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_AST,
                        "Database engine `{}` cannot have parameters, primary_key, order_by, sample_by, settings", engine_name);

    /// Check engine with table overrides
    if (create_query.table_overrides && !engine_features.supports_table_overrides)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine `{}` cannot have table overrides", engine_name);
}

DatabasePtr DatabaseFactory::get(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context, LoadingStrictnessLevel mode, bool internal, bool is_metadata_replay)
{
    const auto engine_name = create.storage->engine->name;
    /// check if the database engine is a valid one before proceeding
    if (!database_engines.contains(engine_name))
    {
        auto hints = getHints(engine_name);
        if (!hints.empty())
            throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Unknown database engine {}. Maybe you meant: {}", engine_name, toString(hints));
        throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Unknown database engine: {}", create.storage->engine->name);
    }

    /// if the engine is found (i.e. registered with the factory instance), then validate if the
    /// supplied engine arguments, settings and table overrides are valid for the engine.
    validate(create);
    cckMetadataPathForOrdinary(create, metadata_path);

    DatabasePtr impl = getImpl(create, metadata_path, context, mode, internal, is_metadata_replay);

    if (impl && context->hasQueryContext() && context->getSettingsRef()[Setting::log_queries])
        context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Database, impl->getEngineName());

    /// Attach database metadata
    if (impl && create.comment)
        impl->setDatabaseComment(create.comment->as<ASTLiteral>()->value.safeGet<String>());

    return impl;
}

void DatabaseFactory::registerDatabase(const std::string & name, CreatorFn creator_fn, EngineFeatures features, Documentation documentation)
{
    if (features.supports_settings && !features.has_builtin_setting_fn)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "DatabaseFactory: Database engine '{}' supports settings but has_builtin_setting_fn is not provided", name);
    if (!database_engines.emplace(name, Creator{std::move(creator_fn), features, std::move(documentation)}).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DatabaseFactory: the database engine name '{}' is not unique", name);
}

const DatabaseFactory::EngineFeatures * DatabaseFactory::tryGetDatabaseEngineFeatures(const String & engine_name) const
{
    auto it = database_engines.find(engine_name);
    if (it == database_engines.end())
        return nullptr;
    return &it->second.features;
}

DatabaseFactory & DatabaseFactory::instance()
{
    static DatabaseFactory db_fact;
    return db_fact;
}

bool DatabaseFactory::isDatabaseExternal(const String & engine_name) const
{
    auto it = database_engines.find(engine_name);
    if (it == database_engines.end())
        return false;
    return it->second.features.is_external;
}

DatabasePtr DatabaseFactory::getImpl(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context, LoadingStrictnessLevel mode, bool internal, bool is_metadata_replay)
{
    auto * storage = create.storage;
    const String & database_name = create.getDatabase();
    const String & engine_name = storage->engine->name;

    bool has_engine_args = false;
    if (storage->engine->arguments)
        has_engine_args = true;

    ASTs empty_engine_args;
    Arguments arguments{
        .engine_name = engine_name,
        .engine_args = has_engine_args ? storage->engine->arguments->children : empty_engine_args,
        .create_query = create,
        .database_name = database_name,
        .metadata_path = metadata_path,
        .uuid = create.uuid,
        .context = context,
        .mode = mode,
        .internal = internal,
        .is_metadata_replay = is_metadata_replay};

    // creator_fn creates and returns a DatabasePtr with the supplied arguments
    auto creator_fn = database_engines.at(engine_name).creator_fn;

    return creator_fn(arguments);
}

}
