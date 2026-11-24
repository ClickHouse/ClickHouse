#include <Storages/StorageLogSettings.h>
#include <Core/BaseSettings.h>
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

        /// Both are Strings, and there is no settings schema for the Log family to reject the value-less
        /// form `SETTINGS disk` on its own, so `safeGet` would report a `Bool` where a `String` was wanted.
        for (const auto change : {disk_change, storage_policy_change})
            if (change != changes.end() && change->shorthand)
                BaseSettingsHelpers::throwValuelessSettingIsNotBool(change->name);

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
