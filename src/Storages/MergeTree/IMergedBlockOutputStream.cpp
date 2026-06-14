#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <DataTypes/Utils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace
{

/// Build a `SerializationInfo` for the narrowed type by reusing the matching element
/// infos from the old `SerializationInfo`. This preserves the per-element kind stack
/// (sparse vs default, etc.) and the `num_rows` / `num_defaults` statistics that the
/// reader relies on to pick the correct deserialization path.
///
/// Falls back to creating a fresh `SerializationInfo` from `new_type` if anything in
/// the structure does not match expectations (e.g. the old info is not a tuple info
/// even though `old_type` is `Tuple`, or the narrowing crosses a wrapper we don't
/// know how to unwrap).
MutableSerializationInfoPtr narrowSerializationInfo(
    const MutableSerializationInfoPtr & old_info,
    const DataTypePtr & old_type,
    const DataTypePtr & new_type)
{
    if (!old_info || !old_type)
        return new_type->createSerializationInfo({});

    /// Same type: reuse the old info as-is.
    if (new_type->equals(*old_type))
        return old_info;

    /// Tuple-to-Tuple: zip elements by name from old to new.
    if (const auto * old_tuple = typeid_cast<const DataTypeTuple *>(old_type.get()))
    {
        if (const auto * new_tuple = typeid_cast<const DataTypeTuple *>(new_type.get()))
        {
            const auto * old_info_tuple = typeid_cast<const SerializationInfoTuple *>(old_info.get());
            if (!old_info_tuple || !old_tuple->hasExplicitNames() || !new_tuple->hasExplicitNames())
                return new_type->createSerializationInfo(old_info->getSettings());

            const auto & old_names = old_tuple->getElementNames();
            const auto & old_elements = old_tuple->getElements();
            const auto & new_names = new_tuple->getElementNames();
            const auto & new_elements = new_tuple->getElements();

            std::unordered_map<String, size_t> old_name_to_index;
            for (size_t i = 0; i < old_names.size(); ++i)
                old_name_to_index.emplace(old_names[i], i);

            MutableSerializationInfos new_elem_infos;
            new_elem_infos.reserve(new_elements.size());
            for (size_t i = 0; i < new_elements.size(); ++i)
            {
                auto it = old_name_to_index.find(new_names[i]);
                if (it == old_name_to_index.end())
                {
                    /// Should not happen: narrowing only removes elements, never adds them.
                    new_elem_infos.push_back(new_elements[i]->createSerializationInfo(old_info->getSettings()));
                    continue;
                }
                const auto & old_elem_info = old_info_tuple->getElementInfo(it->second);
                new_elem_infos.push_back(narrowSerializationInfo(old_elem_info, old_elements[it->second], new_elements[i]));
            }
            return std::make_shared<SerializationInfoTuple>(std::move(new_elem_infos), Names(new_names));
        }
        /// Tuple narrowed to scalar — only happens if the original Tuple had a single named
        /// element which got fully expired. Fall through to default-info creation.
        return new_type->createSerializationInfo(old_info->getSettings());
    }

    /// Wrappers: `Array`/`Nullable`/`Map` use the base `SerializationInfo` (only `Tuple` has a
    /// dedicated subclass with per-element infos). The wrapper info carries the column-level kind
    /// (sparse vs default) and statistics, which are independent of how the inner type is shaped.
    /// So when we narrow inside an `Array(Tuple(...))` / `Map(K, Tuple(...))` / `Nullable(Tuple(...))`,
    /// the wrapper info stays valid and we hand the old wrapper info back.
    if (typeid_cast<const DataTypeArray *>(old_type.get()) && typeid_cast<const DataTypeArray *>(new_type.get()))
        return old_info;
    if (typeid_cast<const DataTypeNullable *>(old_type.get()) && typeid_cast<const DataTypeNullable *>(new_type.get()))
        return old_info;
    if (typeid_cast<const DataTypeMap *>(old_type.get()) && typeid_cast<const DataTypeMap *>(new_type.get()))
        return old_info;

    /// Anything we don't recognize: rebuild from scratch.
    return new_type->createSerializationInfo(old_info->getSettings());
}

}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
    extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
    extern const MergeTreeSettingsMergeTreeStringSerializationVersion string_serialization_version;
    extern const MergeTreeSettingsMergeTreeNullableSerializationVersion nullable_serialization_version;
    extern const MergeTreeSettingsBool propagate_types_serialization_versions_to_nested_types;
    extern const MergeTreeSettingsMergeTreeMapSerializationVersion map_serialization_version;
}

IMergedBlockOutputStream::IMergedBlockOutputStream(
    MergeTreeSettingsPtr storage_settings_,
    MutableDataPartStoragePtr data_part_storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const NamesAndTypesList & columns_list,
    bool reset_columns_)
    : storage_settings(std::move(storage_settings_))
    , metadata_snapshot(metadata_snapshot_)
    , data_part_storage(std::move(data_part_storage_))
    , reset_columns(reset_columns_)
    , info_settings
    {
        static_cast<double>((*storage_settings)[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization]),
        false,
        (*storage_settings)[MergeTreeSetting::serialization_info_version],
        (*storage_settings)[MergeTreeSetting::string_serialization_version],
        (*storage_settings)[MergeTreeSetting::nullable_serialization_version],
        (*storage_settings)[MergeTreeSetting::map_serialization_version],
        (*storage_settings)[MergeTreeSetting::propagate_types_serialization_versions_to_nested_types],
    }
    , new_serialization_infos(info_settings)
{
    if (reset_columns)
        new_serialization_infos = SerializationInfoByName(columns_list, info_settings);
}

NameSet IMergedBlockOutputStream::removeEmptyColumnsFromPart(
    const MergeTreeDataPartPtr & data_part,
    NamesAndTypesList & columns,
    const NameSet & empty_columns,
    const std::map<String, NameSet> & expired_subfields_by_column_in,
    SerializationInfoByName & serialization_infos,
    MergeTreeData::DataPart::Checksums & checksums,
    NameSet * removed_unhashed_stream_names)
{
    /// For compact part we have to override whole file with data, it's not
    /// worth it
    if ((empty_columns.empty() && expired_subfields_by_column_in.empty()) || isCompactPart(data_part))
        return {};

    /// `empty_columns` contains top-level column names whose entire contents should be
    /// removed (TTL whole-column expiry, horizontal-merge missing-column). Sanity-check
    /// against the current `columns` list so a stray name does not silently slip through.
    NameSet column_name_set;
    for (const auto & column : columns)
        column_name_set.insert(column.name);

    NameSet empty_top_columns;
    for (const auto & name : empty_columns)
    {
        if (column_name_set.contains(name))
            empty_top_columns.insert(name);
        else
            LOG_TRACE(data_part->storage.log,
                "Unknown empty column {} for part {}, ignoring",
                name, data_part->name);
    }

    /// Keep only the per-column subfield entries whose top-level column actually exists
    /// in `columns` (defensive — INSERT/merge contributors should already guarantee this).
    std::unordered_map<String, NameSet> expired_subfields_by_column;
    for (const auto & [col_name, paths] : expired_subfields_by_column_in)
    {
        if (column_name_set.contains(col_name) && !paths.empty())
            expired_subfields_by_column.emplace(col_name, paths);
    }

    if (empty_top_columns.empty() && expired_subfields_by_column.empty())
        return {};

    for (const auto & column : empty_top_columns)
        LOG_TRACE(data_part->storage.log, "Skipping expired/empty column {} for part {}", column, data_part->name);
    for (const auto & [col, subfields] : expired_subfields_by_column)
    {
        for (const auto & sub : subfields)
            LOG_TRACE(data_part->storage.log, "Pruning expired subfield {} from column {} for part {}",
                sub, col, data_part->name);
    }

    /// Compute the narrowed types for columns with subfield prunings, and promote
    /// fully-pruned columns into the top-level removal set.
    std::unordered_map<String, DataTypePtr> narrowed_types;
    for (auto & [col_name, expired_subfields] : expired_subfields_by_column)
    {
        auto column_it = std::find_if(columns.begin(), columns.end(),
            [&](const NameAndTypePair & nt) { return nt.name == col_name; });
        if (column_it == columns.end())
            continue;
        auto narrowed = narrowDataTypeByExpiredSubstreams(column_it->type, col_name, expired_subfields);
        if (!narrowed)
        {
            /// All leaves expired: promote to top-level removal.
            empty_top_columns.insert(col_name);
        }
        else if (!narrowed->equals(*column_it->type))
        {
            narrowed_types.emplace(col_name, std::move(narrowed));
        }
    }

    /// Collect counts for shared streams of different columns. Use the ORIGINAL serialization
    /// (pre-narrowing) so we count every stream that physically exists on disk.
    std::map<String, size_t> stream_counts;
    for (const auto & column : columns)
    {
        data_part->getSerialization(column.name)->enumerateStreams(
            [&](const ISerialization::SubstreamPath & substream_path)
            {
                auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column, substream_path, ".bin", checksums, data_part->storage.getSettings());
                if (stream_name)
                    ++stream_counts[*stream_name];
            });
    }

    NameSet remove_files;
    const String mrk_extension = data_part->getMarksFileExtension();

    auto mark_for_removal = [&](const String & owner_column_name, const ISerialization::SubstreamPath & substream_path)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(owner_column_name, substream_path, ".bin", checksums, data_part->storage.getSettings());
        if (stream_name && --stream_counts[*stream_name] == 0)
        {
            remove_files.emplace(*stream_name + ".bin");
            remove_files.emplace(*stream_name + mrk_extension);
            if (removed_unhashed_stream_names)
            {
                /// columns_substreams.txt records the unhashed name; compute it directly.
                ISerialization::StreamFileNameSettings stream_file_name_settings(*data_part->storage.getSettings());
                removed_unhashed_stream_names->insert(
                    ISerialization::getFileNameForStream(owner_column_name, substream_path, stream_file_name_settings));
            }
        }
    };

    /// Whole-column removal: enumerate ALL substreams of the column, mark all for removal.
    for (const auto & column_name : empty_top_columns)
    {
        auto serialization = data_part->tryGetSerialization(column_name);
        if (!serialization)
            continue;
        serialization->enumerateStreams([&](const ISerialization::SubstreamPath & path)
        {
            mark_for_removal(column_name, path);
        });
        serialization_infos.erase(column_name);
    }

    /// Subfield-only removal: enumerate substreams of the ORIGINAL type and the NARROWED
    /// type. Any stream present in the original but absent from the narrowed type — including
    /// wrapper streams like `Array.size0` or `Nullable.null` whose owning subtree was
    /// dropped entirely — is removed. Comparing by enumerated dotted path catches these
    /// wrapper streams that a literal "leaf in expired set" check would miss.
    for (const auto & [col_name, expired_subfields] : expired_subfields_by_column)
    {
        if (empty_top_columns.contains(col_name))
            continue;  /// Already handled as a whole-column removal above.

        auto serialization = data_part->tryGetSerialization(col_name);
        if (!serialization)
            continue;

        /// Collect dotted stream paths that the narrowed type still produces. The narrowed
        /// serialization must mirror the part's actual on-disk encoding for every leaf —
        /// including `String` with `with_size_stream` (which adds a `.size` substream) and
        /// per-element Sparse / Nullable wrappers. We get this by:
        ///   * pre-narrowing the old `SerializationInfo` (which keeps each element's
        ///     `KindStack` and the container's settings), then
        ///   * calling `narrowed_type->getSerialization(narrowed_info)` so `DataTypeTuple`'s
        ///     per-element `getSerialization` path wraps each kept element exactly as the
        ///     writer did.
        /// Falling back to `getSerialization(settings)` (no info) loses the per-element
        /// Sparse kind; falling back to `getDefaultSerialization()` loses both settings
        /// and Sparse. Either fallback would forget streams the part actually wrote, and
        /// we would mark them for removal.
        NameSet kept_streams;
        auto narrowed_it = narrowed_types.find(col_name);
        if (narrowed_it != narrowed_types.end())
        {
            auto old_info_it = serialization_infos.find(col_name);
            auto old_column_it = std::find_if(columns.begin(), columns.end(),
                [&](const NameAndTypePair & nt) { return nt.name == col_name; });
            DataTypePtr old_type = old_column_it != columns.end() ? old_column_it->type : nullptr;
            auto narrowed_info = (old_info_it != serialization_infos.end() && old_type)
                ? narrowSerializationInfo(old_info_it->second, old_type, narrowed_it->second)
                : MutableSerializationInfoPtr{};

            SerializationPtr narrowed_serialization;
            if (narrowed_info)
                narrowed_serialization = narrowed_it->second->getSerialization(*narrowed_info);
            else
                narrowed_serialization = narrowed_it->second->getSerialization(serialization_infos.getSettings());

            narrowed_serialization->enumerateStreams([&](const ISerialization::SubstreamPath & path)
            {
                auto subcolumn_part = ISerialization::getSubcolumnNameForStream(path);
                kept_streams.insert(subcolumn_part.empty() ? col_name : col_name + "." + subcolumn_part);
            });
        }

        serialization->enumerateStreams([&](const ISerialization::SubstreamPath & path)
        {
            /// Compute the dotted subcolumn name represented by this substream path.
            /// Example: column "data" with path -> "c2s.gold" -> dotted "data.c2s.gold";
            /// for `Array.size0` wrapper stream the subcolumn name is `arr.size0` etc.
            auto subcolumn_part = ISerialization::getSubcolumnNameForStream(path);
            String dotted = subcolumn_part.empty() ? col_name : col_name + "." + subcolumn_part;
            if (!kept_streams.contains(dotted))
                mark_for_removal(col_name, path);
        });
    }

    /// Remove files on disk and checksums
    for (auto itr = remove_files.begin(); itr != remove_files.end();)
    {
        if (checksums.files.contains(*itr))
        {
            checksums.files.erase(*itr);
            ++itr;
        }
        else /// If we have no file in checksums it doesn't exist on disk
        {
            LOG_TRACE(data_part->storage.log, "Files {} doesn't exist in checksums so it doesn't exist on disk, will not try to remove it", *itr);
            itr = remove_files.erase(itr);
        }
    }

    /// Remove fully-expired columns from the columns array.
    for (const String & empty_column_name : empty_top_columns)
    {
        auto find_func = [&empty_column_name](const auto & pair) -> bool
        {
            return pair.name == empty_column_name;
        };
        auto remove_it
            = std::find_if(columns.begin(), columns.end(), find_func);

        if (remove_it != columns.end())
            columns.erase(remove_it);
    }

    /// For columns that had only some subfields pruned, replace the entry's type with
    /// the narrowed type, and update the serialization info to match.
    for (auto & [col_name, narrowed_type] : narrowed_types)
    {
        if (empty_top_columns.contains(col_name))
            continue;
        DataTypePtr old_type;
        for (auto & nt : columns)
        {
            if (nt.name == col_name)
            {
                old_type = nt.type;
                nt.type = narrowed_type;
                break;
            }
        }
        if (auto it = serialization_infos.find(col_name); it != serialization_infos.end())
        {
            auto new_info = narrowSerializationInfo(it->second, old_type, narrowed_type);
            serialization_infos.erase(it);
            serialization_infos.emplace(col_name, std::move(new_info));
        }
    }

    return remove_files;
}

}
