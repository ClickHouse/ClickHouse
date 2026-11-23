#pragma once

#include <Interpreters/Context_fwd.h>

#include <optional>

namespace DB
{

class SettingsChanges;

/// Checks the disk settings (`disk`, `storage_policy`, `table_disk`) of a CREATE query and returns whether `disk` is set.
/// Kept out of MergeTreeSettings.cpp so that it does not depend on the Context and disk headers.
bool checkMergeTreeDiskSettings(const SettingsChanges & changes, ContextPtr context, bool is_loading_from_existing_metadata);

/// The value of `table_disk` in the settings of a CREATE query (the last one wins), if it is set.
std::optional<bool> getTableDiskSetting(const SettingsChanges & changes);

}
