#include <IO/ReadBufferFromString.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
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

    /// SHOW CLUSTER/CLUSTERS 
    if (query.clusters)
    {
        std::stringstream rewritten_query;
        rewritten_query << "SELECT DISTINCT cluster FROM system.clusters";

        if (!query.like.empty())
        {
            rewritten_query << " WHERE cluster " << (query.not_like ? "NOT " : "") << "LIKE " << std::quoted(query.like, '\'');
        }

        if (query.limit_length)
            rewritten_query << " LIMIT " << query.limit_length;

        return rewritten_query.str();
    }
    else if (query.cluster)
    {
        std::stringstream rewritten_query;
        rewritten_query << "SELECT * FROM system.clusters";

        rewritten_query << " WHERE cluster = " << std::quoted(query.cluster_str, '\'');

        return rewritten_query.str();
    }

    if (query.temporary && !query.from.empty())
        throw Exception("The `FROM` and `TEMPORARY` cannot be used together in `SHOW TABLES`", ErrorCodes::SYNTAX_ERROR);

    String database = context.resolveDatabase(query.from);
    DatabaseCatalog::instance().assertDatabaseExists(database);

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

    if (!query.like.empty())
        rewritten_query << " AND name " << (query.not_like ? "NOT " : "") << "LIKE " << std::quoted(query.like, '\'');
    else if (query.where_expression)
        rewritten_query << " AND (" << query.where_expression << ")";

    if (query.limit_length)
        rewritten_query << " LIMIT " << query.limit_length;

    return rewritten_query.str();
}


BlockIO InterpreterShowTablesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), context, true);
}


}
