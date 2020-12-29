#include <Databases/MySQL/MaterializeMySQLSettings.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(MaterializeMySQLSettingsTraits, LIST_OF_MATERIALIZE_MODE_SETTINGS)

void MaterializeMySQLSettings::loadFromQuery(ASTStorage & storage_def)
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
}

}
