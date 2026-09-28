#pragma once

#include <base/types.h>

namespace DB
{

/// Where a table setting's effective value came from: the `source` column of `system.table_settings`,
/// `system.engine_settings` and `system.merge_tree_settings`.
///
/// Where several sources wrote a setting, the last one is reported. The order of application is the engine's: `MergeTree`
/// applies `compatibility` before its config section, most engines apply a named collection and then the table's own
/// clause, and `S3Queue` and `AzureQueue` apply `SharedMetadata` after the definition, as
/// `docs/reference/system-tables/table_settings.mdx` explains. The declaration order is not a precedence.
enum class SettingOrigin : uint8_t
{
    /// The engine's compiled-in default. Also what `BaseSettings` stores for "nothing recorded", which is why
    /// it must stay the first value.
    Default,
    Config,           /// a server config section, e.g. <merge_tree> or <distributed>
    Compatibility,    /// rolled back to an older release's default by the `compatibility` setting
    /// A named collection the table was built from, for the settings it supplied rather than the engine arguments.
    NamedCollection,
    Definition,       /// the table's own SETTINGS clause, whether from CREATE or a later ALTER
    SharedMetadata,   /// replicated table metadata, e.g. Keeper for S3Queue and AzureQueue
    /// The engine does not report where the value came from - including a value it adjusts while it runs. Also set
    /// by enumeration for every changed setting, before a storage's override refines it. Must stay last: see below.
    Other,
};

/// `BaseSettings` records an origin in four bits. Checked here, where a value is added, by checking the last one.
static_assert(static_cast<UInt8>(SettingOrigin::Other) < 16, "a recorded origin is stored in 4 bits");
static_assert(static_cast<UInt8>(SettingOrigin::Default) == 0, "a zeroed record must mean nothing recorded");

}
