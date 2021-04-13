#include <Databases/MySQL/ConnectionMySQLSettings.h>

#include <Core/SettingsFields.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTINGS_TRAITS(ConnectionMySQLSettingsTraits, LIST_OF_CONNECTION_MYSQL_SETTINGS)

void ConnectionMySQLSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                throw Exception(e.message() + " for database " + storage_def.engine->name, ErrorCodes::BAD_ARGUMENTS);
            else
                e.rethrow();
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    SettingsChanges & changes = storage_def.settings->changes;
#define ADD_IF_ABSENT(NAME)                                                                                   \
    if (std::find_if(changes.begin(), changes.end(),                                                          \
                  [](const SettingChange & c) { return c.name == #NAME; })                                    \
            == changes.end())                                                                                 \
        changes.push_back(SettingChange{#NAME, static_cast<Field>(NAME)});

    APPLY_FOR_IMMUTABLE_CONNECTION_MYSQL_SETTINGS(ADD_IF_ABSENT)
#undef ADD_IF_ABSENT
}

void ConnectionMySQLSettings::loadFromQueryContext(ContextPtr context)
{
    if (!context->hasQueryContext())
        return;

    const Settings & settings = context->getQueryContext()->getSettingsRef();

    if (settings.mysql_datatypes_support_level.value != mysql_datatypes_support_level.value)
        set("mysql_datatypes_support_level", settings.mysql_datatypes_support_level.toString());
}


}
