#include <Interpreters/AddDefaultDatabaseVisitor.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_global_with_statement;
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
