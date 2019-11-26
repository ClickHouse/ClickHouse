#include <IO/ReadBufferFromString.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterShowTablesQuery.h>
#include <Common/typeid_cast.h>
#include <iomanip>
#include <sstream>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


InterpreterShowTablesQuery::InterpreterShowTablesQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


String InterpreterShowTablesQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowTablesQuery &>();

    /// SHOW DATABASES
    if (query.databases)
        return "SELECT name FROM system.databases";

    const auto & from = query.getChild(ASTShowTablesQueryChildren::FROM);

    if (query.temporary && from)
        throw Exception("The `FROM` and `TEMPORARY` cannot be used together in `SHOW TABLES`", ErrorCodes::SYNTAX_ERROR);

    String database = from ? getIdentifierName(from) : context.getCurrentDatabase();

    /** The parameter check_database_access_rights is reset when the SHOW TABLES query is processed,
      * So that all clients can see a list of all databases and tables in them regardless of their access rights
      * to these databases.
      */
    context.assertDatabaseExists(database, false);

    std::stringstream rewritten_query;
    rewritten_query << "SELECT name FROM system.";

    if (query.dictionaries)
        rewritten_query << "dictionaries ";
    else
        rewritten_query << "tables ";

    rewritten_query << "WHERE ";

    if (query.temporary)
    {
        if (query.dictionaries)
            throw Exception("Temporary dictionaries are not possible.", ErrorCodes::SYNTAX_ERROR);
        rewritten_query << "is_temporary";
    }
    else
        rewritten_query << "database = " << std::quoted(database, '\'');

    if (const auto & like = query.getChild(ASTShowTablesQueryChildren::LIKE))
        rewritten_query << " AND name " << (query.not_like ? "NOT " : "") << "LIKE " << std::quoted(like->as<ASTLiteral>()->value.get<String>(), '\'');

    if (const auto & limit_length = query.getChild(ASTShowTablesQueryChildren::LIMIT_LENGTH))
        rewritten_query << " LIMIT " << limit_length;

    return rewritten_query.str();
}


BlockIO InterpreterShowTablesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), context, true);
}


}
