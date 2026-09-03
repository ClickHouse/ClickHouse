#pragma once

#include <Core/SettingsTierType.h>
#include <base/types.h>

#include <string_view>
#include <vector>

namespace DB
{

/// Where the effective value of a table setting came from. Exposed as `system.table_settings.source`.
///
/// The order matters when a setting is written more than once: the sources below are applied in
/// roughly this order, and the last writer wins. A named collection is applied before the table's
/// own `SETTINGS` clause, and a config section before that, so a setting named in the definition
/// reports `Definition` even if a collection also names it.
enum class TableSettingOrigin : uint8_t
{
    Default,          /// the engine's compiled-in default
    Config,           /// a server config section, e.g. <merge_tree> or <distributed>
    Compatibility,    /// rolled back to an older release's default by the `compatibility` setting
    Definition,       /// the table's own SETTINGS clause, whether from CREATE or a later ALTER
    NamedCollection,  /// a named collection referenced in the engine arguments
    SharedMetadata,   /// replicated table metadata, e.g. Keeper for S3Queue and AzureQueue
    Runtime,          /// adjusted by the engine while it runs, and not written back to its settings
    Other,            /// the engine does not report an origin for this setting
};

std::string_view toString(TableSettingOrigin origin);

/// One setting of one table, as it is actually in effect.
///
/// `type` and `description` are `string_view` because a settings struct owns them statically. They
/// are empty, along with `default_value`, when a setting is known only from the table's `SETTINGS`
/// clause - an engine that keeps no settings struct has nothing else to report about it.
struct TableSetting
{
    String name;
    String value;
    String default_value;
    std::string_view type;
    std::string_view description;
    TableSettingOrigin origin = TableSettingOrigin::Other;
    SettingsTierType tier = SettingsTierType::PRODUCTION;
    /// Whether the engine permits `ALTER TABLE ... MODIFY SETTING` for this setting. The user's
    /// settings constraints are a separate question, applied by `system.table_settings`.
    bool alterable = true;
};

using TableSettings = std::vector<TableSetting>;

}
