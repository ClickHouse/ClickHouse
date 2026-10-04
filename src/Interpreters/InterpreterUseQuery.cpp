#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Common/SettingsChanges.h>
#include <Common/SettingSource.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
    const String & new_database = query_ptr->as<ASTUseQuery &>().getDatabase();

    /// A hierarchical name (`USE a.b`) may denote the tables `b.*` of the database `a`: the access is checked for the
    /// database it denotes. When it is only a prefix of database names, it is checked as written.
    auto resolved = DatabaseCatalog::instance().resolveHierarchicalDatabase(new_database);
    getContext()->checkAccess(AccessType::SHOW_DATABASES, resolved.database_name.empty() ? new_database : resolved.database_name);
    auto session_context = getContext()->getSessionContext();

    /// `database` is a real setting that `executeQuery` applies as the documented equivalent of
    /// `USE` on every statement. Enforce its constraints here too, so that a profile which makes
    /// `database` `const` or restricts its values rejects `USE` consistently with `SET database = ...`,
    /// the HTTP `?database=...` parameter, and the `X-ClickHouse-Database` header. Check before
    /// changing the current database, so a rejected `USE` is a clean no-op.
    SettingsChanges database_change;
    database_change.setSetting("database", new_database);
    session_context->checkSettingsConstraints(database_change, SettingSource::QUERY);

    /// `setCurrentDatabase` also keeps the `database` setting in sync with the session's current
    /// database; without that, an earlier `SET database = ...` would be re-applied on the next query
    /// and silently override the database just selected by this `USE`. A query's own
    /// `SETTINGS database = ...` is applied later and still takes precedence.
    session_context->setCurrentDatabase(new_database);
    return {};
}

void registerInterpreterUseQuery(InterpreterFactory & factory);
void registerInterpreterUseQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterUseQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterUseQuery", create_fn);
}

}
