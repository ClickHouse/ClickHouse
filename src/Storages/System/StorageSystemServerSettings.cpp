#include <Core/ServerSettings.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/ServerSettingColumnsParams.h>
#include <Storages/System/StorageSystemServerSettings.h>

namespace DB
{

ColumnsDescription StorageSystemServerSettings::getColumnsDescription()
{
    auto changeable_without_restart_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"No",              static_cast<Int8>(ServerSettings::ChangeableWithoutRestart::No)},
            {"IncreaseOnly",    static_cast<Int8>(ServerSettings::ChangeableWithoutRestart::IncreaseOnly)},
            {"DecreaseOnly",    static_cast<Int8>(ServerSettings::ChangeableWithoutRestart::DecreaseOnly)},
            {"Yes",             static_cast<Int8>(ServerSettings::ChangeableWithoutRestart::Yes)},
        });

    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Server setting name."},
        {"value", std::make_shared<DataTypeString>(), "Server setting value."},
        {"default", std::make_shared<DataTypeString>(), "Server setting default value."},
        {"changed", std::make_shared<DataTypeUInt8>(), "Shows whether a setting was specified in config.xml"},
        {"description", std::make_shared<DataTypeString>(), "Short server setting description."},
        {"type", std::make_shared<DataTypeString>(), "Server setting value type."},
        {"changeable_without_restart", std::move(changeable_without_restart_type), "Shows whether a setting can be changed at runtime."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."}
    };
}

void StorageSystemServerSettings::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & config = context->getConfigRef();
    ServerSettings settings;
    settings.loadSettingsFromConfig(config);

    /// Runtime-changeable and dynamically-derived values (such as `keeper_hosts`) are filled in by
    /// `dumpToSystemServerSettingsColumns` via the shared `collectChangeableServerSettings` helper.
    ServerSettingColumnsParams params{res_columns, context};
    settings.dumpToSystemServerSettingsColumns(params);
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemServerSettings) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "server_settings",
    .description = R"DOCS_MD(
Contains information about global settings for the server, which are specified in `config.xml`.
The table also includes supported nested settings with a fixed structure; dynamic sections such as lists are not included.
)DOCS_MD",
    .examples = R"DOCS_MD(
The following example shows how to get information about server settings which name contains `thread_pool`.

```sql
SELECT *
FROM system.server_settings
WHERE name LIKE '%thread_pool%'
```

```text
┌─name──────────────────────────────────────────┬─value─┬─default─┬─changed─┬─description─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─type───┬─changeable_without_restart─┬─is_obsolete─┐
│ max_thread_pool_size                          │ 10000 │ 10000   │       0 │ The maximum number of threads that could be allocated from the OS and used for query execution and background operations.                           │ UInt64 │                         No │           0 │
│ max_thread_pool_free_size                     │ 1000  │ 1000    │       0 │ The maximum number of threads that will always stay in a global thread pool once allocated and remain idle in case of insufficient number of tasks. │ UInt64 │                         No │           0 │
│ thread_pool_queue_size                        │ 10000 │ 10000   │       0 │ The maximum number of tasks that will be placed in a queue and wait for execution.                                                                  │ UInt64 │                         No │           0 │
│ max_io_thread_pool_size                       │ 100   │ 100     │       0 │ The maximum number of threads that would be used for IO operations                                                                                  │ UInt64 │                         No │           0 │
│ max_io_thread_pool_free_size                  │ 0     │ 0       │       0 │ Max free size for IO thread pool.                                                                                                                   │ UInt64 │                         No │           0 │
│ io_thread_pool_queue_size                     │ 10000 │ 10000   │       0 │ Queue size for IO thread pool.                                                                                                                      │ UInt64 │                         No │           0 │
│ max_active_parts_loading_thread_pool_size     │ 64    │ 64      │       0 │ The number of threads to load active set of data parts (Active ones) at startup.                                                                    │ UInt64 │                         No │           0 │
│ max_outdated_parts_loading_thread_pool_size   │ 32    │ 32      │       0 │ The number of threads to load inactive set of data parts (Outdated ones) at startup.                                                                │ UInt64 │                         No │           0 │
│ max_unexpected_parts_loading_thread_pool_size │ 32    │ 32      │       0 │ The number of threads to load inactive set of data parts (Unexpected ones) at startup.                                                              │ UInt64 │                         No │           0 │
│ max_parts_cleaning_thread_pool_size           │ 128   │ 128     │       0 │ The number of threads for concurrent removal of inactive data parts.                                                                                │ UInt64 │                         No │           0 │
│ max_backups_io_thread_pool_size               │ 1000  │ 1000    │       0 │ The maximum number of threads that would be used for IO operations for BACKUP queries                                                               │ UInt64 │                         No │           0 │
│ max_backups_io_thread_pool_free_size          │ 0     │ 0       │       0 │ Max free size for backups IO thread pool.                                                                                                           │ UInt64 │                         No │           0 │
│ backups_io_thread_pool_queue_size             │ 0     │ 0       │       0 │ Queue size for backups IO thread pool.                                                                                                              │ UInt64 │                         No │           0 │
└───────────────────────────────────────────────┴───────┴─────────┴─────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┴────────────────────────────┴─────────────┘

```

Using of `WHERE changed` can be useful, for example, when you want to check
whether settings in configuration files are loaded correctly and are in use.

{/* */}

```sql
SELECT * FROM system.server_settings WHERE changed AND name='max_thread_pool_size'
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [Settings](/reference/system-tables/settings)
- [Configuration Files](/concepts/features/configuration/server-config/configuration-files)
- [Server Settings](/reference/settings/server-settings/settings)
)DOCS_MD")

}
