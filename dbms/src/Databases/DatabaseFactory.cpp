#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseDictionary.h>
#include <Databases/DatabaseMySQL.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>
#include <Common/parseAddress.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_DATABASE_ENGINE;
}

DatabasePtr DatabaseFactory::get(
    const String & database_name,
    const String & metadata_path,
    const ASTStorage * engine_define,
    Context & context)
{
    String engine_name = engine_define->engine->name;

    if (engine_name != "MySQL")
    {
        if (engine_define->engine->arguments || engine_define->engine->parameters || engine_define->partition_by ||
            engine_define->primary_key || engine_define->order_by || engine_define->sample_by || engine_define->settings)
        {
            std::stringstream ostr;
            formatAST(*engine_define, ostr, false, false);
            throw Exception("Unknown database engine: " + ostr.str(), ErrorCodes::UNKNOWN_DATABASE_ENGINE);
        }
    }

    if (engine_name == "Ordinary")
        return std::make_shared<DatabaseOrdinary>(database_name, metadata_path, context);
    else if (engine_name == "Memory")
        return std::make_shared<DatabaseMemory>(database_name);
    else if (engine_name == "Dictionary")
        return std::make_shared<DatabaseDictionary>(database_name);
    else if (engine_name == "MySQL")
    {
        ASTFunction * engine = engine_define->engine;
        const auto & arguments = engine->arguments->children;

        if (arguments.size() != 4)
            throw Exception("MySQL Database require mysql_hostname, mysql_database_name, mysql_username, mysql_password",
                            ErrorCodes::BAD_ARGUMENTS);

        const auto & mysql_host_name = arguments[0]->as<ASTLiteral>()->value.safeGet<String>();
        const auto & mysql_database_name = arguments[1]->as<ASTLiteral>()->value.safeGet<String>();
        const auto & mysql_user_name = arguments[2]->as<ASTLiteral>()->value.safeGet<String>();
        const auto & mysql_user_password = arguments[3]->as<ASTLiteral>()->value.safeGet<String>();

        auto parsed_host_port = parseAddress(mysql_host_name, 3306);
        return std::make_shared<DatabaseMySQL>(context, database_name, parsed_host_port.first, parsed_host_port.second, mysql_database_name,
            mysql_user_name, mysql_user_password);
    }


    throw Exception("Unknown database engine: " + engine_name, ErrorCodes::UNKNOWN_DATABASE_ENGINE);
}

}
