#pragma once

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/Field.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/SettingDescription.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

/// The fourteen columns `system.engine_settings`, `system.merge_tree_settings` and
/// `system.table_settings` all carry, declared and written in one place so the three cannot drift:
/// `name`, `value`, `default`, `changed`, `source`, `description`, `min`, `max`, `disallowed_values`,
/// `readonly`, `type`, `is_obsolete`, `tier`, `alias_for`.
///
/// The descriptions say what is true of a *setting*, not of a *table*, because the same text has to
/// read correctly in a table describing one engine family and in one describing every engine.
ColumnsDescription sharedSettingColumns();

/// The type of the `source` column: every value `SettingOrigin` declares.
DataTypePtr settingOriginEnum();

/// Writes the rows of a settings table one column at a time, skipping the columns the query does not read.
class SettingRowWriter
{
public:
    /// `show_secrets` is whether this reader sees the real value of a setting that holds one, and
    /// `show_named_collection_values` whether it sees what a named collection supplied - which is the whole of a
    /// collection, not only the keys a masking rule knows, exactly as `system.named_collections` decides it.
    SettingRowWriter(
        MutableColumns & res_columns_,
        const std::vector<UInt8> & columns_mask_,
        bool show_secrets_,
        bool show_named_collection_values_)
        : res_columns(res_columns_)
        , columns_mask(columns_mask_)
        , show_secrets(show_secrets_)
        , show_named_collection_values(show_named_collection_values_)
    {
    }

    /// Whether this setting's value is reported as a placeholder rather than as it is.
    bool masks(const SettingDescription & setting) const
    {
        /// A collection's contents are secret as a whole: `SHOW CREATE TABLE` prints the collection's name rather
        /// than what it holds, and `system.named_collections` hides every key without the grant. So a value this
        /// reader could not read there must not be readable here either, whether or not a masking rule knows the
        /// name - a broker address or a database name says as much as a password does about where a table points.
        if (setting.origin == SettingOrigin::NamedCollection)
            return !show_named_collection_values;
        return !show_secrets && !setting.masked_value.empty();
    }

    /// The value as this reader may see it.
    std::string_view reportedValue(const SettingDescription & setting) const
    {
        if (!masks(setting))
            return setting.value;
        /// A collection's value has no masked form of its own: nothing of it may be shown.
        return setting.masked_value.empty() ? std::string_view{"[HIDDEN]"} : std::string_view{setting.masked_value};
    }

    /// Whether the query reads the next column - for a value that is work to build.
    bool wants() const { return columns_mask[src_index]; }

    /// Writes the next column of the current row, or skips it. A template, so that the `Field` - a copy of a string,
    /// often - is built only for a column the query reads. An empty `optional` is written as `NULL`.
    template <typename T>
    void put(const T & value)
    {
        if (wants())
        {
            if constexpr (requires { value.has_value(); })
                res_columns[res_index++]->insert(value ? Field(*value) : Field());
            else
                res_columns[res_index++]->insert(Field(value));
        }
        ++src_index;
    }

    void startRow()
    {
        src_index = 0;
        res_index = 0;
    }

    /// Every column the table declares was visited, in order: the writes and the declarations are in different
    /// places, and a missing or extra one would shift every column after it.
    void finishRow() const { chassert(src_index == columns_mask.size()); }

private:
    MutableColumns & res_columns;
    const std::vector<UInt8> & columns_mask;
    const bool show_secrets;
    const bool show_named_collection_values;
    size_t src_index = 0;
    size_t res_index = 0;
};

/// Writes the fourteen shared columns of one row.
void writeSharedSettingColumns(
    SettingRowWriter & writer, std::string_view name, const SettingDescription & setting, std::string_view alias_for);

/// Writes a setting's own row and a row per alias, as `system.settings` does, so that looking a setting up by the
/// name you happen to know finds it; `alias_for` tells the rows apart. `write_leading` writes the table's own columns
/// before the shared ones, `write_trailing` those after them. Returns the number of rows written.
template <typename WriteLeading, typename WriteTrailing>
size_t writeSettingRows(
    SettingRowWriter & writer, const SettingDescription & setting, WriteLeading && write_leading, WriteTrailing && write_trailing)
{
    auto write_row = [&](std::string_view name, std::string_view alias_for)
    {
        writer.startRow();
        write_leading(writer);
        writeSharedSettingColumns(writer, name, setting, alias_for);
        write_trailing(writer);
        writer.finishRow();
    };

    write_row(setting.name, "");
    for (const auto alias : setting.aliases)
        write_row(alias, setting.name);
    return 1 + setting.aliases.size();
}

}
