#pragma once

#include <Columns/IColumn_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/SettingDescription.h>

#include <cstddef>
#include <string_view>
#include <vector>

namespace DB
{

/// The thirteen columns `system.engine_settings`, `system.merge_tree_settings` and
/// `system.table_settings` all carry, declared and written in one place so the three cannot drift:
/// `name`, `value`, `default`, `changed`, `description`, `min`, `max`, `disallowed_values`,
/// `readonly`, `type`, `is_obsolete`, `tier`, `alias_for`.
///
/// The descriptions say what is true of a *setting*, not of a *table*, because the same text has to
/// read correctly in a table describing one engine family and in one describing every engine. A
/// caller needing something more specific overrides that column with `ColumnsDescription::modify`
/// rather than declaring the whole set again.
ColumnsDescription sharedSettingColumns();

/// Writes those thirteen columns, in the order `sharedSettingColumns` declares them.
///
/// `value` is a parameter rather than read from `setting`, because `system.table_settings` reports
/// a placeholder in place of a secret the current user may not see.
///
/// The indexes continue from what the caller has already written, so a table writes its own leading
/// columns first - `engine_name`, or `database`/`table`/`engine` - and may write more of its own
/// afterwards. An empty `columns_mask` means every column is wanted, which is what a table that does
/// not override `supportsColumnsMask` gets.
void insertSharedSettingColumns(
    MutableColumns & res_columns,
    const std::vector<UInt8> & columns_mask,
    size_t & src_index,
    size_t & res_index,
    std::string_view name,
    std::string_view value,
    const SettingDescription & setting,
    std::string_view alias_for);

}
