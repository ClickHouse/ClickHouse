#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/SettingDescription.h>

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
/// read correctly in a table describing one engine family and in one describing every engine.
ColumnsDescription sharedSettingColumns();

/// Writes the rows of a settings table one column at a time, skipping the columns the query does not read.
class SettingRowWriter
{
public:
    SettingRowWriter(MutableColumns & res_columns_, const std::vector<UInt8> & columns_mask_)
        : res_columns(res_columns_), columns_mask(columns_mask_)
    {
    }

    /// Whether the query reads the next column - for a value that is work to build.
    bool wants() const { return columns_mask[src_index]; }

    /// Writes the next column of the current row, or skips it.
    void put(const Field & value)
    {
        if (wants())
            res_columns[res_index++]->insert(value);
        ++src_index;
    }

    void startRow()
    {
        src_index = 0;
        res_index = 0;
    }

private:
    MutableColumns & res_columns;
    const std::vector<UInt8> & columns_mask;
    size_t src_index = 0;
    size_t res_index = 0;
};

/// Whether a reader sees `masked_value` in place of the value: the setting holds a secret, and the reader may not
/// see secrets - as `SHOW CREATE TABLE` decides.
bool isSettingValueMasked(const SettingDescription & setting, bool show_secrets);

/// Writes the thirteen shared columns of one row.
void writeSharedSettingColumns(
    SettingRowWriter & writer, std::string_view name, const SettingDescription & setting, bool is_masked, std::string_view alias_for);

/// Writes a setting's own row and a row per alias, as `system.settings` does, so that looking a setting up by the
/// name you happen to know finds it; `alias_for` tells the rows apart. `write_leading` writes the table's own columns
/// before the shared ones, `write_trailing` those after them. Returns the number of rows written.
template <typename WriteLeading, typename WriteTrailing>
size_t writeSettingRows(
    SettingRowWriter & writer,
    const SettingDescription & setting,
    bool is_masked,
    WriteLeading && write_leading,
    WriteTrailing && write_trailing)
{
    auto write_row = [&](std::string_view name, std::string_view alias_for)
    {
        writer.startRow();
        write_leading(writer);
        writeSharedSettingColumns(writer, name, setting, is_masked, alias_for);
        write_trailing(writer);
    };

    write_row(setting.name, "");
    for (const auto alias : setting.aliases)
        write_row(alias, setting.name);
    return 1 + setting.aliases.size();
}

}
