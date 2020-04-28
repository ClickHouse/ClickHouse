#include <Databases/MySQL/createMySQLDatabase.h>

#if USE_MYSQL

#include <Core/Types.h>
#include <Common/parseAddress.h>
#include <Common/SettingsChanges.h>
#include <Databases/MySQL/DatabaseMySQL.h>
#include <Databases/MySQL/DatabaseMaterializeMySQL.h>

#include <mysqlxx/Pool.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/evaluateConstantExpression.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CREATE_DATABASE;
}

static inline String safeGetLiteralValue(const ASTPtr & ast)
{
    if (!ast || !ast->as<ASTLiteral>())
        throw Exception("Database engine MySQL requested literal argument.", ErrorCodes::BAD_ARGUMENTS);

    return ast->as<ASTLiteral>()->value.safeGet<String>();
}

/*static inline bool materializeMySQLDatabase(const ASTSetQuery * settings)
{
    if (!settings || settings->changes.empty())
        return false;

    for (const auto & change : settings->changes)
    {
        if (change.name == "materialize_data")
        {
            if (change.value.getType() == Field::Types::String)
                return change.value.safeGet<String>() == "true"; ///TODO: ignore case
        }

    }
    return false;
}*/

DatabasePtr createMySQLDatabase(const String & database_name, const String & metadata_path, const ASTStorage * define, Context & context)
{
    const ASTFunction * engine = define->engine;
    if (!engine->arguments || engine->arguments->children.size() != 4)
        throw Exception( "MySQL Database require mysql_hostname, mysql_database_name, mysql_username, mysql_password arguments.",
            ErrorCodes::BAD_ARGUMENTS);

    ASTs & arguments = engine->arguments->children;
    arguments[1] = evaluateConstantExpressionOrIdentifierAsLiteral(arguments[1], context);

    const auto & host_name_and_port = safeGetLiteralValue(arguments[0]);
    const auto & mysql_database_name = safeGetLiteralValue(arguments[1]);
    const auto & mysql_user_name = safeGetLiteralValue(arguments[2]);
    const auto & mysql_user_password = safeGetLiteralValue(arguments[3]);

    try
    {
        const auto & [remote_host_name, remote_port] = parseAddress(host_name_and_port, 3306);
        auto mysql_pool = mysqlxx::Pool(mysql_database_name, remote_host_name, mysql_user_name, mysql_user_password, remote_port);

        /*if (materializeMySQLDatabase(define->settings))
            return std::make_shared<DatabaseMaterializeMySQL>(
                context, database_name, metadata_path, define, mysql_database_name, std::move(mysql_pool));*/

        return std::make_shared<DatabaseMySQL>(context, database_name, metadata_path, define, mysql_database_name, std::move(mysql_pool));
    }
    catch (...)
    {
        const auto & exception_message = getCurrentExceptionMessage(true);
        throw Exception("Cannot create MySQL database, because " + exception_message, ErrorCodes::CANNOT_CREATE_DATABASE);
    }
}

}

#endif
