#pragma once

#include <base/types.h>

namespace DB
{

/// The origin of a table setting's effective value - where it came from. Exposed as the `source` column of
/// `system.table_settings`, `system.engine_settings` and `system.merge_tree_settings`; "origin" in the code,
/// "source" where that column is meant.
///
/// When several of them wrote a setting, the one reported is whichever wrote it last, and that order is the
/// engine's. The values are declared in the order the engines apply them - each later source overriding the
/// earlier ones, as when the table is built - and the column is an `Enum8` of them, so `ORDER BY source` sorts
/// by that precedence: keep it when adding a value. `Other` is the exception - the catch-all, set wherever an
/// engine cannot tell the source, including before any other. Most engines apply the table's own `SETTINGS`
/// clause after a config section, `compatibility` and a named collection, so `Definition` outranks those.
/// An engine that adds a source has to decide where it belongs relative to the definition - `S3Queue` and
/// `AzureQueue` put `SharedMetadata` after it, which `docs/reference/system-tables/table_settings.mdx` explains.
enum class SettingOrigin : uint8_t
{
    /// The engine's compiled-in default. Also what `BaseSettings` stores for "nothing recorded", which is why
    /// it must stay the first value.
    Default,
    Config,           /// a server config section, e.g. <merge_tree> or <distributed>
    Compatibility,    /// rolled back to an older release's default by the `compatibility` setting
    /// A named collection the table was built from, for the settings it actually supplied - not those the
    /// engine arguments overrode. Reported by the engines whose settings object records them as it loads the
    /// collection: `Kafka`, `PostgreSQL`, `MySQL`, `NATS` and `RabbitMQ`.
    NamedCollection,
    Definition,       /// the table's own SETTINGS clause, whether from CREATE or a later ALTER
    SharedMetadata,   /// replicated table metadata, e.g. Keeper for S3Queue and AzureQueue
    /// The engine does not report an origin for this setting - including a value it adjusts while it runs and
    /// does not write back. A value for that belongs before this one, in the order above. Enumeration also sets
    /// this for every setting that is merely changed, before a storage's override refines it.
    ///
    /// Staying last is what lets the `static_assert` below check the whole enum by checking this one:
    /// `BaseSettings` stores an origin in four bits, so there is room for sixteen values in all.
    Other,
};

/// Checked here, where a value is added, rather than where the four bits are allocated.
static_assert(static_cast<UInt8>(SettingOrigin::Other) < 16, "a recorded origin is stored in 4 bits");
static_assert(static_cast<UInt8>(SettingOrigin::Default) == 0, "a zeroed record must mean nothing recorded");

}
