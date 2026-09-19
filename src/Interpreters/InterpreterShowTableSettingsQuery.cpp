#include <Interpreters/InterpreterShowTableSettingsQuery.h>

#include <Access/Common/AccessFlags.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowTableSettingsQuery.h>

namespace DB
{

namespace
{

/// The database `system.table_settings` reports the named table under. A name without a database may be one of
/// the session's temporary tables, which that table reports with an empty `database`. As in `SHOW CREATE TABLE`,
/// a temporary table takes precedence over a table of the current database with the same name.
String resolveReportedDatabase(const ASTShowTableSettingsQuery & query, const ContextPtr & context)
{
    if (query.database.empty() && context->tryResolveStorageID(StorageID("", query.table), Context::ResolveExternal))
        return "";
    return context->resolveDatabase(query.database);
}

}

String InterpreterShowTableSettingsQuery::getRewrittenQuery(const String & database) const
{
    const auto & query = query_ptr->as<ASTShowTableSettingsQuery &>();

    /// `system.table_settings` is the statement's whole implementation; this narrows it to one table and picks
    /// the columns that fit a terminal - `description` runs to paragraphs, so it is left out. Anything more is a
    /// query against the table.
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

        /// Match the pattern against every name a setting answers to, but print the canonical row: the alias rows
        /// exist so that a lookup by an old name finds the setting. `NOT LIKE` drops a setting when any of its
        /// names matches.
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

    /// Resolved once: the access check and the rewritten query must name the same table.
    const auto & query = query_ptr->as<ASTShowTableSettingsQuery &>();
    const String database = resolveReportedDatabase(query, getContext());

    /// As `SHOW CREATE TABLE` does: a table the user may not see, or one that does not exist, is an error, not an
    /// empty result, which would read as a table with no settings. `system.table_settings` shows a table to whoever
    /// may `SHOW TABLES` it. A temporary table belongs to the session and needs no grant.
    if (!database.empty())
    {
        getContext()->checkAccess(AccessType::SHOW_TABLES, database, query.table);
        DatabaseCatalog::instance().getTable(StorageID(database, query.table), getContext());
    }

    /// `system.table_settings` shows a data lake catalog or a remote database only when the
    /// corresponding setting allows it, and `show_data_lake_catalogs_in_system_tables` is off by
    /// default. Naming such a database explicitly is an unambiguous request for it, so enable it
    /// for this query - the same thing `InterpreterShowTablesQuery` does for `SHOW TABLES`.
    if (DatabaseCatalog::instance().isDatalakeCatalog(database))
        query_context->setSetting("show_data_lake_catalogs_in_system_tables", true);
    if (DatabaseCatalog::instance().isRemoteDatabase(database))
        query_context->setSetting("show_remote_databases_in_system_tables", true);

    return executeQuery(getRewrittenQuery(database), query_context, QueryFlags{ .internal = true }).second;
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
