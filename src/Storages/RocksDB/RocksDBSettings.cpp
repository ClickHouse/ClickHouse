#include "RocksDBSettings.h"
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(RockDBSettingsTraits, LIST_OF_ROCKSDB_SETTINGS)


void RocksDBSettings::loadFromQuery(ASTStorage & storage_def, ContextPtr /*context*/)
{
    if (storage_def.settings)
    {
        try
        {
            auto changes = storage_def.settings->changes;
            applyChanges(changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
}

std::vector<String> RocksDBSettings::getAllRegisteredNames() const
{
    std::vector<String> all_settings;
    for (const auto & setting_field : all())
        all_settings.push_back(setting_field.getName());
    return all_settings;
}
}
