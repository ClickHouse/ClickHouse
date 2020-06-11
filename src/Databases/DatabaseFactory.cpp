#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseDictionary.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseLazy.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOrdinary.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/parseAddress.h>
#include "DatabaseFactory.h"
#include <Poco/File.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#    include <Databases/DatabaseMySQL.h>
#    include <Interpreters/evaluateConstantExpression.h>
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

DatabasePtr DatabaseFactory::get(
    const String & database_name, const String & metadata_path, const ASTStorage * engine_define, Context & context)
{
    bool created = false;

    try
    {
        created = Poco::File(metadata_path).createDirectory();
        return getImpl(database_name, metadata_path, engine_define, context);
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

DatabasePtr DatabaseFactory::getImpl(
    const String & database_name, const String & metadata_path, const ASTStorage * engine_define, Context & context)
{
    String engine_name = engine_define->engine->name;

    if (engine_name != "MySQL" && engine_name != "Lazy" && engine_define->engine->arguments)
        throw Exception("Database engine " + engine_name + " cannot have arguments", ErrorCodes::BAD_ARGUMENTS);

    if (engine_define->engine->parameters || engine_define->partition_by || engine_define->primary_key || engine_define->order_by ||
        engine_define->sample_by || engine_define->settings)
        throw Exception("Database engine " + engine_name + " cannot have parameters, primary_key, order_by, sample_by, settings",
                        ErrorCodes::UNKNOWN_ELEMENT_IN_AST);

    if (engine_name == "Ordinary")
        return std::make_shared<DatabaseOrdinary>(database_name, metadata_path, context);
    else if (engine_name == "Atomic")
        return std::make_shared<DatabaseAtomic>(database_name, metadata_path, context);
    else if (engine_name == "Memory")
        return std::make_shared<DatabaseMemory>(database_name, context);
    else if (engine_name == "Dictionary")
        return std::make_shared<DatabaseDictionary>(database_name, context);

#if USE_MYSQL

    else if (engine_name == "MySQL")
    {
        const ASTFunction * engine = engine_define->engine;

        if (!engine->arguments || engine->arguments->children.size() != 4)
            throw Exception("MySQL Database require mysql_hostname, mysql_database_name, mysql_username, mysql_password arguments.",
                            ErrorCodes::BAD_ARGUMENTS);


        ASTs & arguments = engine->arguments->children;
        arguments[1] = evaluateConstantExpressionOrIdentifierAsLiteral(arguments[1], context);

        const auto & host_name_and_port = safeGetLiteralValue<String>(arguments[0], "MySQL");
        const auto & database_name_in_mysql = safeGetLiteralValue<String>(arguments[1], "MySQL");
        const auto & mysql_user_name = safeGetLiteralValue<String>(arguments[2], "MySQL");
        const auto & mysql_user_password = safeGetLiteralValue<String>(arguments[3], "MySQL");

        try
        {
            const auto & [remote_host_name, remote_port] = parseAddress(host_name_and_port, 3306);
            auto mysql_pool = mysqlxx::Pool(database_name_in_mysql, remote_host_name, mysql_user_name, mysql_user_password, remote_port);

            auto mysql_database = std::make_shared<DatabaseMySQL>(
                context, database_name, metadata_path, engine_define, database_name_in_mysql, std::move(mysql_pool));

            mysql_database->empty(); /// test database is works fine.
            return mysql_database;
        }
        catch (...)
        {
            const auto & exception_message = getCurrentExceptionMessage(true);
            throw Exception("Cannot create MySQL database, because " + exception_message, ErrorCodes::CANNOT_CREATE_DATABASE);
        }
    }

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
