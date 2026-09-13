#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowFunctionsQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowFunctionsQuery.h>
#include <Common/quoteString.h>

namespace DB
{

InterpreterShowFunctionsQuery::InterpreterShowFunctionsQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}

BlockIO InterpreterShowFunctionsQuery::execute()
{
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId({});

    return executeQuery(getRewrittenQuery(), query_context, QueryFlags{ .internal = true }).second;
}

String InterpreterShowFunctionsQuery::getRewrittenQuery()
{
    constexpr const char * functions_table = "functions";

    const auto & query = query_ptr->as<ASTShowFunctionsQuery &>();

    DatabasePtr system_db = DatabaseCatalog::instance().getSystemDatabase();

    String rewritten_query = fmt::format(
        R"(
SELECT *
FROM {}.{})",
        system_db->getDatabaseName(),
        functions_table);

    if (!query.like.empty())
    {
        rewritten_query += " WHERE name ";
        rewritten_query += query.case_insensitive_like ? "ILIKE " : "LIKE ";
        rewritten_query += quoteString(query.like);
    }

    return rewritten_query;
}

void registerInterpreterShowFunctionsQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowFunctionsQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowFunctionsQuery", create_fn);
}

}
