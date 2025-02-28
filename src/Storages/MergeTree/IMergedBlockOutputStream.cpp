#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
}

IMergedBlockOutputStream::IMergedBlockOutputStream(
    const MergeTreeSettingsPtr & storage_settings_,
    MutableDataPartStoragePtr data_part_storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const NamesAndTypesList & columns_list,
    bool reset_columns_)
    : storage_settings(storage_settings_)
    , metadata_snapshot(metadata_snapshot_)
    , data_part_storage(data_part_storage_)
    , reset_columns(reset_columns_)
{
    if (reset_columns)
    {
        SerializationInfo::Settings info_settings =
        {
            .ratio_of_defaults_for_sparse = (*storage_settings)[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization],
            .choose_kind = false,
        };

        new_serialization_infos = SerializationInfoByName(columns_list, info_settings);
    }
}

NameSet IMergedBlockOutputStream::removeEmptyColumnsFromPart(
    const MergeTreeDataPartPtr & data_part,
    NamesAndTypesList & columns,
    SerializationInfoByName & serialization_infos,
    MergeTreeData::DataPart::Checksums & checksums)
{
    const NameSet & empty_columns = data_part->expired_columns;

    /// For compact part we have to override whole file with data, it's not
    /// worth it
    if (empty_columns.empty() || isCompactPart(data_part))
        return {};

    for (const auto & column : empty_columns)
        LOG_TRACE(data_part->storage.log, "Skipping expired/empty column {} for part {}", column, data_part->name);

    /// Collect counts for shared streams of different columns. As an example, Nested columns have shared stream with array sizes.
    std::map<String, size_t> stream_counts;
    for (const auto & column : columns)
    {
        data_part->getSerialization(column.name)->enumerateStreams(
            [&](const ISerialization::SubstreamPath & substream_path)
            {
                auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column, substream_path, checksums);
                if (stream_name)
                    ++stream_counts[*stream_name];
            });
    }

    NameSet remove_files;
    const String mrk_extension = data_part->getMarksFileExtension();
    for (const auto & column_name : empty_columns)
    {
        auto serialization = data_part->tryGetSerialization(column_name);
        if (!serialization)
            continue;

        ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column_name, substream_path, checksums);

            /// Delete files if they are no longer shared with another column.
            if (stream_name && --stream_counts[*stream_name] == 0)
            {
                remove_files.emplace(*stream_name + ".bin");
                remove_files.emplace(*stream_name + mrk_extension);
            }
        };

        serialization->enumerateStreams(callback);
        serialization_infos.erase(column_name);
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

    /// Remove columns from columns array
    for (const String & empty_column_name : empty_columns)
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

    LOG_DEBUG(getLogger("IMergedBlockOutputStream"), "remove files {}, first {}", remove_files.size(), remove_files.empty() ? "" : *remove_files.begin());

    return remove_files;
}

}
