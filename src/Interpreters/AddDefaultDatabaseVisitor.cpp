#include <Interpreters/AddDefaultDatabaseVisitor.h>

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

    /// A clause of a view being stored may hold a query parameter, which has no value yet, so
    /// only the settings that can decide alias inheritance are taken. `profile` can decide it
    /// too, by naming a group that sets one of the other two.
    for (const SettingChange & change : set_query->changes)
        if (change.name == "enable_global_with_statement" || change.name == "compatibility"
            || change.name == "profile")
            changes.push_back(change);
}

bool AddDefaultDatabaseVisitor::evaluateWithAliasInheritance(const SettingsChanges & changes) const
{
    if (changes.empty())
        return inherit_with_aliases;

    /// Replayed onto a context rather than searched, so that a repeated setting takes its last
    /// value, an inner `compatibility` reverts what an outer one derived, and `profile` stands for
    /// the group of settings it names.
    ContextMutablePtr replay_context = Context::createCopy(context);
    replay_context->applySettingsChanges(changes);
    return replay_context->getSettingsRef()[Setting::enable_global_with_statement];
}

AddDefaultDatabaseVisitor::AddDefaultDatabaseVisitor(
    ContextPtr context_,
    const String & database_name_,
    bool only_replace_current_database_function_,
    bool only_replace_in_join_)
    : context(context_)
    , database_name(database_name_)
    , inherit_with_aliases(context_->getSettingsRef()[Setting::enable_global_with_statement])
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
