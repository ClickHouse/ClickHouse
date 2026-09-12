#include <Interpreters/InterpreterShowTableSettingsQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
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
    {
        const std::string_view op = query.case_insensitive_like ? "ILIKE" : "LIKE";
        const String pattern = quoteString(query.like);

        /// The pattern is matched against the names a setting answers to, not only the one it is
        /// declared under, and the row printed is still the canonical one. `system.table_settings`
        /// carries a row per alias so that a lookup by the name you happen to know finds the
        /// setting; filtering on the canonical name alone would throw that away here and leave
        /// whoever knows only the old spelling with the empty result the alias rows exist to
        /// prevent.
        const String matches = fmt::format(
            "(name {0}{1} {2} OR name IN ("
            "SELECT alias_for FROM system.table_settings "
            "WHERE database = {3} AND table = {4} AND alias_for != '' AND name {1} {2}))",
            query.not_like ? "NOT " : "", op, pattern,
            quoteString(database), quoteString(query.table));

        /// `NOT LIKE` excludes a setting whichever of its names the pattern names, so the alias
        /// lookup is not negated with it - a setting is dropped when any name it answers to matches.
        rewritten += query.not_like
            ? fmt::format(
                " AND name NOT {0} {1} AND name NOT IN ("
                "SELECT alias_for FROM system.table_settings "
                "WHERE database = {2} AND table = {3} AND alias_for != '' AND name {0} {1})",
                op, pattern, quoteString(database), quoteString(query.table))
            : " AND " + matches;
    }

    rewritten += " ORDER BY name";
    return rewritten;
}

BlockIO InterpreterShowTableSettingsQuery::execute()
{
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");

    /// `system.table_settings` shows a data lake catalog or a remote database only when the
    /// corresponding setting allows it, and `show_data_lake_catalogs_in_system_tables` is off by
    /// default. Naming such a database explicitly is an unambiguous request for it, so enable it
    /// for this query - the same thing `InterpreterShowTablesQuery` does for `SHOW TABLES`.
    const auto & query = query_ptr->as<ASTShowTableSettingsQuery &>();
    const String database = getContext()->resolveDatabase(query.database);
    if (DatabaseCatalog::instance().isDatalakeCatalog(database))
        query_context->setSetting("show_data_lake_catalogs_in_system_tables", true);
    if (DatabaseCatalog::instance().isRemoteDatabase(database))
        query_context->setSetting("show_remote_databases_in_system_tables", true);

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
