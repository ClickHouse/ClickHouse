#include <IO/ReadBufferFromString.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTIdentifier.h>
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
    const ASTShowTablesQuery & query = typeid_cast<const ASTShowTablesQuery &>(*query_ptr);

    /// SHOW DATABASES
    if (query.databases)
        return "SELECT name FROM system.databases";

    if (query.temporary && !query.from.empty())
        throw Exception("The `FROM` and `TEMPORARY` cannot be used together in `SHOW TABLES`", ErrorCodes::SYNTAX_ERROR);

    String database = query.from.empty() ? context.getCurrentDatabase() : query.from;

    /** The parameter check_database_access_rights is reset when the SHOW TABLES query is processed,
      * So that all clients can see a list of all databases and tables in them regardless of their access rights
      * to these databases.
      */
    context.assertDatabaseExists(database, false);

    std::stringstream rewritten_query;
    rewritten_query << "SELECT name FROM system.tables WHERE ";

    if (query.temporary)
        rewritten_query << "is_temporary";
    else
        rewritten_query << "database = " << std::quoted(database, '\'');

    if (!query.like.empty())
        rewritten_query << " AND name " << (query.not_like ? "NOT " : "") << "LIKE " << std::quoted(query.like, '\'');

    return rewritten_query.str();
}


BlockIO InterpreterShowTablesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), context, true);
}


}
