#include <Storages/StorageLogSettings.h>
#include <Core/BaseSettings.h>
#include <Disks/StoragePolicy.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/TableSettingsHelpers.h>

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

SettingDescriptions StorageLogSettings::enumerateEngineSettings(ContextPtr)
{
    /// `description` is a literal: `SettingDescription` keeps a view of it.
    auto describe = [](String name, String default_value, std::string_view description)
    {
        SettingDescription described;
        described.name = std::move(name);
        described.value = default_value;
        described.default_value = std::move(default_value);
        described.type = "String";
        described.comment = description;
        described.tier = SettingsTierType::PRODUCTION;
        /// Not the `Other` a `SettingDescription` starts at: these are the compiled-in defaults, and a table's own
        /// values are described by `describeTable`, which recomputes the origin from each.
        described.origin = SettingOrigin::Default;
        return described;
    };

    return {
        describe("disk", "default", "Disk the table keeps its data on. Cannot be set together with `storage_policy`."),
        describe(
            "storage_policy",
            "",
            "Storage policy whose first disk the table keeps its data on. Cannot be set together with `disk`."),
    };
}

SettingDescriptions StorageLogSettings::describeTable(const String & disk_name, const StorageID & table_id, ContextPtr context)
{
    const auto stated = getSettingsStatedInDefinition(table_id, context);
    auto settings = enumerateEngineSettings(context);
    for (auto & setting : settings)
    {
        if (setting.name == "disk")
        {
            setting.value = disk_name;
        }
        else
        {
            for (const auto & change : stated)
                if (change.name == setting.name)
                    setting.value = change.value.safeGet<String>();
        }

        /// A disk the table did not name comes from its `storage_policy`, which is what `Other` says here.
        setting.origin = setting.value == setting.default_value ? SettingOrigin::Default : SettingOrigin::Other;
    }
    return withOriginFromDefinition(std::move(settings), stated);
}
}
