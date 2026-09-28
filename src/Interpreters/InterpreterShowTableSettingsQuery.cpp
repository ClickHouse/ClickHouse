#include <Interpreters/InterpreterShowTableSettingsQuery.h>

#include <Access/Common/AccessFlags.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowTableSettingsQuery.h>
#include <Storages/StorageAlias.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
}

namespace
{

/// The database `system.table_settings` reports the named table under. A name without a database may be one of the
/// session's temporary tables, reported with an empty `database`; as in `SHOW CREATE TABLE`, it takes precedence over
/// a table of the current database. Only the session's, which `system.table_settings` enumerates: external data held
/// in a query context falls through to the current database, and is reported missing rather than as settings-less.
String resolveReportedDatabase(const ASTShowTableSettingsQuery & query, const ContextPtr & context)
{
    if (query.database.empty() && context->hasSessionContext()
        && context->getSessionContext()->tryResolveStorageID(StorageID("", query.table), Context::ResolveExternal))
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
    WriteBufferFromOwnString table_filter;
    table_filter << " WHERE database = " << DB::quote << database << " AND table = " << DB::quote << query.table
                 << (query.changed ? " AND changed" : "");

    WriteBufferFromOwnString rewritten_query;
    if (query.has_like)
    {
        /// Match the pattern against every name a setting answers to, but print the declared one: alias rows exist so
        /// that a lookup by an old name finds the setting. `NOT LIKE` drops a setting when any of its names matches.
        /// All rows of a setting carry the same value, source and `changed`, so grouping them under the declared name
        /// answers in one reading of the table - a second would fetch an `S3Queue` table's settings from Keeper again.
        /// This assumes no setting is declared under another's alias, which would make the name ambiguous everywhere.
        const std::string_view like = query.case_insensitive_like ? "ILIKE " : "LIKE ";

        rewritten_query
            << "SELECT declared_name AS name, any(value) AS value, any(changed) AS changed, any(source) AS source"
            << " FROM (SELECT if(alias_for = '', name, alias_for) AS declared_name, name AS answers_to,"
            << " value, changed, source FROM system.table_settings" << table_filter.str()
            << ") GROUP BY declared_name"
            << " HAVING countIf(answers_to " << like << DB::quote << query.like << ") "
            << (query.not_like ? "= 0" : "> 0");
    }
    else
    {
        rewritten_query << "SELECT name, value, changed, source FROM system.table_settings" << table_filter.str()
                        << " AND alias_for = ''";
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

    /// The `SELECT` grant on `system.table_settings`, which the rewritten query needs for any table, first: resolving
    /// the table below can fetch it from a remote database or a data lake catalog, which a user refused the grant
    /// must not make the server do. The columns are the ones the rewritten query reads.
    getContext()->checkAccess(
        AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "table_settings",
        Strings{"database", "table", "name", "value", "changed", "source", "alias_for"});

    /// As `SHOW CREATE TABLE` does: a table the user may not see, or one that does not exist, is an error, not an
    /// empty result, which would read as a table with no settings. `system.table_settings` shows a table to whoever
    /// may `SHOW TABLES` it; a temporary table belongs to the session and needs no such grant. That is the lookup
    /// only: a table found whose settings then cannot be read is logged and skipped by the scan, as by
    /// `system.table_settings`, rather than raised with a message that may quote a secret the table states.
    if (!database.empty())
    {
        getContext()->checkAccess(AccessType::SHOW_TABLES, database, query.table);
        const auto table = DatabaseCatalog::instance().getTable(StorageID(database, query.table), getContext());

        /// An alias whose target the user may not see is an error too, in the form of the other access errors. The
        /// target is not named: `SHOW TABLES` on the alias alone does not reveal it.
        if (const auto * alias = table->as<StorageAlias>(); alias && !alias->isTargetTableGranted(getContext(), AccessType::SHOW_TABLES, {}))
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "{}: Not enough privileges. To execute this query, it's necessary to have the grant SHOW TABLES "
                "on the table {}.{} is an alias of",
                getContext()->getUserName(), backQuoteIfNeed(database), backQuoteIfNeed(query.table));
    }

    /// `system.table_settings` shows a data lake catalog or a remote database only where a setting allows it. Naming
    /// one is an unambiguous request for it, so allow it for this query, as `InterpreterShowTablesQuery` does.
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
