#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class SettingsChanges;

/// Checks the disk settings (`disk`, `storage_policy`, `table_disk`) of a CREATE query and returns whether `disk` is set.
/// In a separate translation unit (MergeTreeSettingsDisks.cpp), so that MergeTreeSettings.cpp, which is edited for every
/// new MergeTree setting, does not depend on the headers of Context and the disks.
bool checkMergeTreeDiskSettings(const SettingsChanges & changes, ContextPtr context, bool is_loading_from_existing_metadata);

}
