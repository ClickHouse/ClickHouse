#include <Storages/StorageLogSettings.h>
#include <Disks/StoragePolicy.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
}

String getDiskName(ASTStorage & storage_def, ContextPtr context)
{
    if (storage_def.settings)
    {
        SettingsChanges changes = storage_def.settings->changes;

        const auto disk_change
            = std::find_if(changes.begin(), changes.end(), [&](const SettingChange & change) { return change.name == "disk"; });
        const auto storage_policy_change
            = std::find_if(changes.begin(), changes.end(), [&](const SettingChange & change) { return change.name == "storage_policy"; });

        if (disk_change != changes.end() && storage_policy_change != changes.end())
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE, "Could not specify `disk` and `storage_policy` at the same time for storage Log Family");

        if (disk_change != changes.end())
            return disk_change->value.safeGet<String>();

        if (storage_policy_change != changes.end())
        {
            auto policy = context->getStoragePolicy(storage_policy_change->value.safeGet<String>());
            return policy->getDisks()[0]->getName();
        }
    }

    return "default";
}

bool StorageLogSettings::hasBuiltin(std::string_view name)
{
    return name == "disk" || name == "storage_policy";
}
}
