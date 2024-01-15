#include <filesystem>

#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>
#include <Common/Macros.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int CANNOT_CREATE_DATABASE;
    extern const int LOGICAL_ERROR;
}

void cckMetadataPathForOrdinary(const ASTCreateQuery & create, const String & metadata_path)
{
    const String & engine_name = create.storage->engine->name;
    const String & database_name = create.getDatabase();

    if (engine_name != "Ordinary")
        return;

    if (!FS::isSymlink(metadata_path))
        return;

    String target_path = FS::readSymlink(metadata_path).string();
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

/// validate validates the database engine that's specified in the create query for
/// engine arguments, settings and table overrides.
void validate(const ASTCreateQuery & create_query)

{
    auto * storage = create_query.storage;

    /// Check engine may have arguments
    static const std::unordered_set<std::string_view> engines_with_arguments{"MySQL", "MaterializeMySQL", "MaterializedMySQL",
        "Lazy", "Replicated", "PostgreSQL", "MaterializedPostgreSQL", "SQLite", "Filesystem", "S3", "HDFS"};

    const String & engine_name = storage->engine->name;
    bool engine_may_have_arguments = engines_with_arguments.contains(engine_name);

    if (storage->engine->arguments && !engine_may_have_arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine `{}` cannot have arguments", engine_name);

    /// Check engine may have settings
    bool may_have_settings = endsWith(engine_name, "MySQL") || engine_name == "Replicated" || engine_name == "MaterializedPostgreSQL";
    bool has_unexpected_element = storage->engine->parameters || storage->partition_by ||
        storage->primary_key || storage->order_by ||
        storage->sample_by;
    if (has_unexpected_element || (!may_have_settings && storage->settings))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_AST,
                        "Database engine `{}` cannot have parameters, primary_key, order_by, sample_by, settings", engine_name);

    /// Check engine with table overrides
    static const std::unordered_set<std::string_view> engines_with_table_overrides{"MaterializeMySQL", "MaterializedMySQL", "MaterializedPostgreSQL"};
    if (create_query.table_overrides && !engines_with_table_overrides.contains(engine_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine `{}` cannot have table overrides", engine_name);
}

DatabasePtr DatabaseFactory::get(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context)
{
    const auto engine_name = create.storage->engine->name;
    /// check if the database engine is a valid one before proceeding
    if (!database_engines.contains(engine_name))
    {
        auto hints = getHints(engine_name);
        if (!hints.empty())
            throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Unknown database engine {}. Maybe you meant: {}", engine_name, toString(hints));
        else
            throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Unknown database engine: {}", create.storage->engine->name);
    }

    /// if the engine is found (i.e. registered with the factory instance), then validate if the
    /// supplied engine arguments, settings and table overrides are valid for the engine.
    validate(create);
    cckMetadataPathForOrdinary(create, metadata_path);

    DatabasePtr impl = getImpl(create, metadata_path, context);

    if (impl && context->hasQueryContext() && context->getSettingsRef().log_queries)
        context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Database, impl->getEngineName());

    /// Attach database metadata
    if (impl && create.comment)
        impl->setDatabaseComment(create.comment->as<ASTLiteral>()->value.safeGet<String>());

    return impl;
}

void DatabaseFactory::registerDatabase(const std::string & name, CreatorFn creator_fn)
{
    if (!database_engines.emplace(name, std::move(creator_fn)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DatabaseFactory: the database engine name '{}' is not unique", name);
}

DatabaseFactory & DatabaseFactory::instance()
{
    static DatabaseFactory db_fact;
    return db_fact;
}

DatabasePtr DatabaseFactory::getImpl(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context)
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
        .context = context};

    // creator_fn creates and returns a DatabasePtr with the supplied arguments
    auto creator_fn = database_engines.at(engine_name);

    return creator_fn(arguments);
}

}
