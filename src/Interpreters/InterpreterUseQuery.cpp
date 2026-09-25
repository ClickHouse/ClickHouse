#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Common/SettingsChanges.h>
#include <Common/SettingSource.h>
#include <Common/typeid_cast.h>
#include <base/find_symbols.h>
#include <Core/Settings.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_table_namespaces;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNKNOWN_DATABASE;
}

BlockIO InterpreterUseQuery::execute()
{
    const auto & use_query = query_ptr->as<ASTUseQuery &>();

    /// `USE db.ns`, the first part is the database, the rest is a namespace path
    /// A single (possibly quoted) part is always an exact database name
    const String logical_name = use_query.getDatabase();
    String database_name = logical_name;
    if (const auto * identifier = use_query.database->as<ASTIdentifier>();
        identifier && identifier->name_parts.size() > 1)
    {
        if (!getContext()->getSettingsRef()[Setting::allow_experimental_table_namespaces])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Table namespaces are an experimental feature; enable allow_experimental_table_namespaces to use `USE {}`",
                logical_name);
        database_name = identifier->name_parts[0];
    }

    getContext()->checkAccess(AccessType::SHOW_DATABASES, database_name);
    auto session_context = getContext()->getSessionContext();

    /// `database` is a real setting that `executeQuery` applies as the documented equivalent of
    /// `USE` on every statement. Enforce its constraints here too, so that a profile which makes
    /// `database` `const` or restricts its values rejects `USE` consistently with `SET database = ...`,
    /// the HTTP `?database=...` parameter, and the `X-ClickHouse-Database` header. Check before
    /// changing the current database, so a rejected `USE` is a clean no-op.
    /// The setting mirrors the logical name ("db.ns"), so that is what gets checked.
    SettingsChanges database_change;
    database_change.setSetting("database", logical_name);
    session_context->checkSettingsConstraints(database_change, SettingSource::QUERY);

    CurrentDatabaseInfo current_database_info(logical_name);

    if (current_database_info.hasTablePrefix() && !getContext()->getSettingsRef()[Setting::allow_experimental_table_namespaces])
    {
        throw Exception(
            ErrorCodes::UNKNOWN_DATABASE,
            "Table namespaces are not allowed, but {} specified. Enable 'allow_experimental_table_namespaces' setting to allow it.",
            current_database_info.getTablePrefixPart());
    }

    /// `setCurrentDatabase` also keeps the `database` setting in sync with the session's current
    /// database; without that, an earlier `SET database = ...` would be re-applied on the next query
    /// and silently override the database just selected by this `USE`. A query's own
    /// `SETTINGS database = ...` is applied later and still takes precedence.
    /// The current database stores the logical name ("db.ns"), setCurrentDatabase
    /// validates that the namespace exists and resolution folds it into table names.
    session_context->setCurrentDatabase(current_database_info);
    return {};
}

void registerInterpreterUseQuery(InterpreterFactory & factory);
void registerInterpreterUseQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterUseQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterUseQuery", create_fn, /*supports_table_namespace_scope*/ true);
}

}
