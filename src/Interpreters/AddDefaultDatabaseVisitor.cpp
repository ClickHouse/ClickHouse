#include <Interpreters/AddDefaultDatabaseVisitor.h>

#include <Common/SettingSource.h>
#include <Core/Settings.h>
#include <Core/SettingsFields.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_global_with_statement;
}

void AddDefaultDatabaseVisitor::appendSettings(SettingsChanges & changes, const ASTSelectQuery & select)
{
    const ASTPtr select_settings = select.settings();
    if (!select_settings)
        return;

    const auto * set_query = select_settings->as<ASTSetQuery>();
    if (!set_query)
        return;

    /// A clause of a view being stored may hold a query parameter, which has no value yet, so only
    /// the settings that can decide alias inheritance are taken: `profile` can name a group that
    /// sets one of the others, and `readonly` makes the constraints drop a change to any of them.
    for (const SettingChange & change : set_query->changes)
        if (change.name == "enable_global_with_statement" || change.name == "compatibility"
            || change.name == "profile" || change.name == "readonly")
            changes.push_back(change);
}

std::pair<ContextPtr, bool> AddDefaultDatabaseVisitor::scopeSettings(const ASTSelectQuery & select) const
{
    ContextPtr enclosing = scopes.empty() ? context : scopes.back().settings_context;

    SettingsChanges changes;
    appendSettings(changes, select);
    if (changes.empty())
        return {enclosing, enclosing->getSettingsRef()[Setting::enable_global_with_statement]};

    /// Applied to a context rather than searched: a repeated setting, an inner `compatibility` that
    /// reverts an outer one, and `profile` naming a group all resolve only by being applied. Clamped
    /// first, because a change the constraints drop is not in effect when the query is resolved.
    ContextMutablePtr scope_context = Context::createCopy(enclosing);
    scope_context->clampToSettingsConstraints(changes, SettingSource::QUERY);
    scope_context->applySettingsChanges(changes);
    return {scope_context, scope_context->getSettingsRef()[Setting::enable_global_with_statement]};
}

AddDefaultDatabaseVisitor::AddDefaultDatabaseVisitor(
    ContextPtr context_,
    const String & database_name_,
    bool only_replace_current_database_function_,
    bool only_replace_in_join_)
    : context(context_)
    , database_name(database_name_)
    , only_replace_current_database_function(only_replace_current_database_function_)
    , only_replace_in_join(only_replace_in_join_)
{
    if (!context->isGlobalContext())
    {
        for (const auto & [table_name, _ /* storage */] : context->getExternalTables())
        {
            external_tables.insert(table_name);
        }
    }
}

}
