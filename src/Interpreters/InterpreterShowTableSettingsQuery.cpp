#include <Interpreters/InterpreterShowTableSettingsQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowTableSettingsQuery.h>
#include <Common/quoteString.h>

#include <fmt/format.h>

namespace DB
{

String InterpreterShowTableSettingsQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowTableSettingsQuery &>();
    const String database = getContext()->resolveDatabase(query.database);

    /// `system.table_settings` is the statement's whole implementation; this only narrows it to one
    /// table and picks the columns worth reading in a terminal. `description` is deliberately not
    /// among them: it runs to paragraphs, and four of these columns fit a screen where five do not.
    /// Anything more - descriptions, other engines, joining `system.tables`, filtering on `source` -
    /// is a query against that table, which is why the table is the feature and this a convenience.
    String rewritten = fmt::format(
        "SELECT name, value, changed, source "
        "FROM system.table_settings "
        "WHERE database = {} AND table = {} AND alias_for = ''",
        quoteString(database), quoteString(query.table));

    if (query.changed)
        rewritten += " AND changed";

    if (query.has_like)
        rewritten += fmt::format(
            " AND name {}{} {}",
            query.not_like ? "NOT " : "",
            query.case_insensitive_like ? "ILIKE" : "LIKE",
            quoteString(query.like));

    rewritten += " ORDER BY name";
    return rewritten;
}

BlockIO InterpreterShowTableSettingsQuery::execute()
{
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");

    return executeQuery(getRewrittenQuery(), query_context, QueryFlags{ .internal = true }).second;
}

void registerInterpreterShowTableSettingsQuery(InterpreterFactory & factory);
void registerInterpreterShowTableSettingsQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowTableSettingsQuery>(args.query, args.context);
    };

    factory.registerInterpreter("InterpreterShowTableSettingsQuery", create_fn);
}

}
