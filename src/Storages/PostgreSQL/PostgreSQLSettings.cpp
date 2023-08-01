#include <Storages/PostgreSQL/PostgreSQLSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Parsers/formatAST.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(PostgreSQLSettingsTraits, LIST_OF_POSTGRESQL_SETTINGS)

void PostgreSQLSettings::loadFromQuery(const ASTSetQuery & settings_def)
{
    applyChanges(settings_def.changes);
}

void PostgreSQLSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            loadFromQuery(*storage_def.settings);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

void PostgreSQLSettings::loadFromQueryContext(ContextPtr context, ASTStorage & storage_def)
{
    if (!context->hasQueryContext())
        return;

    /*const Settings & settings = context->getQueryContext()->getSettingsRef();

    static constexpr auto setting_name = "postgresql_datatypes_support_level";
    set(setting_name, settings.postgresql_datatypes_support_level.toString());*/

    if (!storage_def.settings)
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    /*auto & changes = storage_def.settings->changes;
    if (changes.end() == std::find_if(
            changes.begin(), changes.end(),
            [](const SettingChange & c) { return c.name == setting_name; }))
    {
        changes.push_back(SettingChange{setting_name, settings.postgresql_datatypes_support_level.toString()});
    }*/
}

}
