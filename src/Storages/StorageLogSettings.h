#pragma once

#include <Storages/SettingDescription.h>

#include <memory>
#include <base/types.h>

namespace DB
{
    class ASTStorage;
    struct StorageID;

    String getDiskName(ASTStorage & storage_def, ContextPtr context);

    struct StorageLogSettings
    {
        static bool hasBuiltin(std::string_view name);

        /// For `system.engine_settings`. The family keeps no settings struct, so the two are described here.
        static SettingDescriptions enumerateEngineSettings(ContextPtr context);

        /// For `system.table_settings`: the two with the values a table holds. `disk_name` is the disk it keeps its
        /// data on - the one `disk` names, or the first disk of the `storage_policy`, or `default`.
        static SettingDescriptions describeTable(const String & disk_name, const StorageID & table_id, ContextPtr context);
    };
}
