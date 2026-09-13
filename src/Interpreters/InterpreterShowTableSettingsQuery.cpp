#include <Interpreters/InterpreterShowTableSettingsQuery.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowTableSettingsQuery.h>

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
    WriteBufferFromOwnString rewritten_query;
    rewritten_query
        << "SELECT name, value, changed, source FROM system.table_settings"
        << " WHERE database = " << DB::quote << database
        << " AND table = " << DB::quote << query.table
        << " AND alias_for = ''";

    if (query.changed)
        rewritten_query << " AND changed";

    if (query.has_like)
    {
        const std::string_view like = query.case_insensitive_like ? "ILIKE " : "LIKE ";

        /// The pattern is matched against the names a setting answers to, not only the one it is
        /// declared under, and the row printed is still the canonical one. `system.table_settings`
        /// carries a row per alias so that a lookup by the name you happen to know finds the
        /// setting; filtering on the canonical name alone would throw that away here and leave
        /// whoever knows only the old spelling with the empty result the alias rows exist to
        /// prevent. `NOT LIKE` drops a setting when any name it answers to matches.
        rewritten_query
            << " AND (name " << (query.not_like ? "NOT " : "") << like << DB::quote << query.like
            << (query.not_like ? " AND name NOT IN (" : " OR name IN (")
            << "SELECT alias_for FROM system.table_settings"
            << " WHERE database = " << DB::quote << database
            << " AND table = " << DB::quote << query.table
            << " AND alias_for != '' AND name " << like << DB::quote << query.like << "))";
    }

    rewritten_query << " ORDER BY name";
    return rewritten_query.str();
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
