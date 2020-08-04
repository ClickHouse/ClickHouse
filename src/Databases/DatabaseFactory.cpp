#include <Databases/DatabaseFactory.h>

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseDictionary.h>
#include <Databases/DatabaseLazy.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/MySQL/createMySQLDatabase.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/parseAddress.h>
#include <Poco/File.h>
#include <Poco/Path.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int CANNOT_CREATE_DATABASE;
}

DatabasePtr DatabaseFactory::get(const ASTCreateQuery & create, const String & metadata_path, Context & context)
{
    bool created = false;

    try
    {
        /// Creates store/xxx/ for Atomic
        Poco::File(Poco::Path(metadata_path).makeParent()).createDirectories();
        /// Before 20.7 it's possible that .sql metadata file does not exist for some old database.
        /// In this case Ordinary database is created on server startup if the corresponding metadata directory exists.
        /// So we should remove metadata directory if database creation failed.
        created = Poco::File(metadata_path).createDirectory();
        return getImpl(create, metadata_path, context);
    }
    catch (...)
    {
        Poco::File metadata_dir(metadata_path);

        if (created && metadata_dir.exists())
            metadata_dir.remove(true);

        throw;
    }
}

template <typename ValueType>
static inline ValueType safeGetLiteralValue(const ASTPtr &ast, const String &engine_name)
{
    if (!ast || !ast->as<ASTLiteral>())
        throw Exception("Database engine " + engine_name + " requested literal argument.", ErrorCodes::BAD_ARGUMENTS);

    return ast->as<ASTLiteral>()->value.safeGet<ValueType>();
}

DatabasePtr DatabaseFactory::getImpl(const ASTCreateQuery & create, const String & metadata_path, Context & context)
{
    const auto * engine_define = create.storage;
    const String & database_name = create.database;
    const String & engine_name = engine_define->engine->name;
    const UUID & uuid = create.uuid;

    if (engine_name != "MySQL" && engine_name != "Lazy" && engine_define->engine->arguments)
        throw Exception("Database engine " + engine_name + " cannot have arguments", ErrorCodes::BAD_ARGUMENTS);

    if (engine_define->engine->parameters || engine_define->partition_by || engine_define->primary_key || engine_define->order_by ||
        engine_define->sample_by || engine_define->settings)
        throw Exception("Database engine " + engine_name + " cannot have parameters, primary_key, order_by, sample_by, settings",
                        ErrorCodes::UNKNOWN_ELEMENT_IN_AST);

    if (engine_name == "Ordinary")
        return std::make_shared<DatabaseOrdinary>(database_name, metadata_path, context);
    else if (engine_name == "Atomic")
        return std::make_shared<DatabaseAtomic>(database_name, metadata_path, uuid, context);
    else if (engine_name == "Memory")
        return std::make_shared<DatabaseMemory>(database_name, context);
    else if (engine_name == "Dictionary")
        return std::make_shared<DatabaseDictionary>(database_name, context);

#if USE_MYSQL
    else if (engine_name == "MySQL")
        return createMySQLDatabase(database_name, metadata_path, engine_define, context);
#endif

    else if (engine_name == "Lazy")
    {
        const ASTFunction * engine = engine_define->engine;

        if (!engine->arguments || engine->arguments->children.size() != 1)
            throw Exception("Lazy database require cache_expiration_time_seconds argument", ErrorCodes::BAD_ARGUMENTS);

        const auto & arguments = engine->arguments->children;

        const auto cache_expiration_time_seconds = safeGetLiteralValue<UInt64>(arguments[0], "Lazy");
        return std::make_shared<DatabaseLazy>(database_name, metadata_path, cache_expiration_time_seconds, context);
    }

    throw Exception("Unknown database engine: " + engine_name, ErrorCodes::UNKNOWN_DATABASE_ENGINE);
}

}
