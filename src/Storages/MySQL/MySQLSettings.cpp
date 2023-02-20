#include <Storages/MySQL/MySQLSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>


namespace DB
{

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

void MySQLSettings::loadFromQueryContext(ContextPtr context)
{
    if (!context->hasQueryContext())
        return;

    const Settings & settings = context->getQueryContext()->getSettingsRef();

    if (settings.mysql_datatypes_support_level.value != mysql_datatypes_support_level.value)
        set("mysql_datatypes_support_level", settings.mysql_datatypes_support_level.toString());
}

}
