#include <Storages/MySQL/MySQLSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Parsers/formatAST.h>
#include <Core/Field.h>
#include <Core/Settings.h>


namespace DB
{
namespace Setting
{
    extern const SettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(MySQLSettingsTraits, LIST_OF_MYSQL_SETTINGS)

void MySQLSettings::loadFromQuery(const ASTSetQuery & settings_def)
{
    applyChanges(settings_def.changes);
}

void MySQLSettings::loadFromQuery(ASTStorage & storage_def)
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

void MySQLSettings::loadFromQueryContext(ContextPtr context, ASTStorage & storage_def)
{
    if (!context->hasQueryContext())
        return;

    const Settings & settings = context->getQueryContext()->getSettingsRef();

    if (settings[Setting::mysql_datatypes_support_level].value != mysql_datatypes_support_level.value)
    {
        static constexpr auto setting_name = "mysql_datatypes_support_level";
        set(setting_name, settings[Setting::mysql_datatypes_support_level].toString());

        if (!storage_def.settings)
        {
            auto settings_ast = std::make_shared<ASTSetQuery>();
            settings_ast->is_standalone = false;
            storage_def.set(storage_def.settings, settings_ast);
        }

        auto & changes = storage_def.settings->changes;
        if (changes.end() == std::find_if(
                changes.begin(), changes.end(),
                [](const SettingChange & c) { return c.name == setting_name; }))
        {
            changes.push_back(SettingChange{setting_name, settings[Setting::mysql_datatypes_support_level].toString()});
        }
    }
}

}
