#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/SettingDescription.h>

#include <base/defines.h>

#include <functional>
#include <string_view>
#include <vector>

namespace DB
{

/// The columns `system.engine_settings`, `system.merge_tree_settings` and `system.table_settings` all carry,
/// declared and written in one place so those three cannot drift. The settings tables built on other paths -
/// `system.settings`, `system.object_storage_queue_settings`, `system.filesystem_cache_settings` - still
/// declare their own columns, and `system.object_storage_queue_settings` renders a settings struct this code
/// also renders.
///
/// The descriptions say what is true of a *setting*, not of a *table*, because the same text has to
/// read correctly in a table describing one engine family and in one describing every engine.
ColumnsDescription sharedSettingColumns();

/// Writes the rows of a settings table one column at a time, skipping the columns the query does not read.
class SettingRowWriter
{
public:
    /// Whether this reader may see the values the named collection of this name supplied. A collection is
    /// secret as a whole rather than key by key, and a grant names one collection, so the answer is per
    /// collection - `system.named_collections` decides it the same way.
    using MayShowNamedCollection = std::function<bool(const String &)>;

    /// `show_secrets` is whether this reader sees the real value of a setting that holds one. A table whose rows
    /// can never come from a named collection - one describing an engine rather than a table - leaves
    /// `may_show_named_collection` out, and nothing of a collection is shown.
    SettingRowWriter(
        MutableColumns & res_columns_,
        const std::vector<UInt8> & columns_mask_,
        bool show_secrets_,
        MayShowNamedCollection may_show_named_collection_ = {})
        : res_columns(res_columns_)
        , columns_mask(columns_mask_)
        , show_secrets(show_secrets_)
        , may_show_named_collection(std::move(may_show_named_collection_))
    {
    }

    /// Whether this setting's value is reported as a placeholder rather than as it is.
    bool masks(const SettingDescription & setting) const
    {
        /// A collection's contents are secret as a whole: `SHOW CREATE TABLE` prints the collection's name rather
        /// than what it holds, and `system.named_collections` hides every key without the grant. So a value this
        /// reader could not read there must not be readable here either, whether or not a masking rule knows the
        /// name - a broker address or a database name says as much as a password does about where a table points.
        /// The question is asked of the collection that supplied it, because that is what a grant names; a row
        /// whose collection was not recorded cannot be checked, and so is not shown.
        if (setting.origin == SettingOrigin::NamedCollection)
            return !may_show_named_collection || !may_show_named_collection(setting.named_collection);
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
    bool wants() const
    {
        /// The mask has one entry per column the table declares, so running out of it means the table wrote
        /// more columns than it declared - or that it does not override `supportsColumnsMask`, which leaves
        /// the mask empty and every read past its end.
        chassert(src_index < columns_mask.size());
        return columns_mask[src_index];
    }

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
    const MayShowNamedCollection may_show_named_collection;
    size_t src_index = 0;
    size_t res_index = 0;
};

/// Writes the shared columns of one row.
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
