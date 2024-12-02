#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Common/MemoryTrackerBlockerInThread.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}


Block getBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation)
{
    Block result;
    for (size_t i = 0, size = names.size(); i < size; ++i)
    {
        const auto & name = names[i];
        result.insert(i, block.getByName(name));

        /// Reorder primary key columns in advance and add them to `primary_key_columns`.
        if (permutation)
        {
            auto & column = result.getByPosition(i);
            column.column = column.column->permute(*permutation, 0);
        }
    }

    return result;
}

Block permuteBlockIfNeeded(const Block & block, const IColumn::Permutation * permutation)
{
    Block result;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        result.insert(i, block.getByPosition(i));
        if (permutation)
        {
            auto & column = result.getByPosition(i);
            column.column = column.column->permute(*permutation, 0);
        }
    }
    return result;
}

IMergeTreeDataPartWriter::IMergeTreeDataPartWriter(
    const String & data_part_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const VirtualsDescriptionPtr & virtual_columns_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : data_part_name(data_part_name_)
    , serializations(serializations_)
    , index_granularity_info(index_granularity_info_)
    , storage_settings(storage_settings_)
    , metadata_snapshot(metadata_snapshot_)
    , virtual_columns(virtual_columns_)
    , columns_list(columns_list_)
    , settings(settings_)
    , with_final_mark(settings.can_use_adaptive_granularity)
    , data_part_storage(data_part_storage_)
    , index_granularity(index_granularity_)
{
}

Columns IMergeTreeDataPartWriter::releaseIndexColumns()
{
    /// The memory for index was allocated without thread memory tracker.
    /// We need to deallocate it in shrinkToFit without memory tracker as well.
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    Columns result;
    result.reserve(index_columns.size());

    for (auto & column : index_columns)
    {
        column->shrinkToFit();
        result.push_back(std::move(column));
    }

    index_columns.clear();
    return result;
}

SerializationPtr IMergeTreeDataPartWriter::getSerialization(const String & column_name) const
{
    auto it = serializations.find(column_name);
    if (it == serializations.end())
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
            "There is no column or subcolumn {} in part {}", column_name, data_part_name);

    return it->second;
}

ASTPtr IMergeTreeDataPartWriter::getCodecDescOrDefault(const String & column_name, CompressionCodecPtr default_codec) const
{
    auto get_codec_or_default = [&](const auto & column_desc)
    {
        return column_desc.codec ? column_desc.codec : default_codec->getFullCodecDesc();
    };

    const auto & columns = metadata_snapshot->getColumns();
    if (const auto * column_desc = columns.tryGet(column_name))
        return get_codec_or_default(*column_desc);

    if (const auto * virtual_desc = virtual_columns->tryGetDescription(column_name))
        return get_codec_or_default(*virtual_desc);

    return default_codec->getFullCodecDesc();
}


IMergeTreeDataPartWriter::~IMergeTreeDataPartWriter() = default;


MergeTreeDataPartWriterPtr createMergeTreeDataPartCompactWriter(
        const String & data_part_name_,
        const String & logger_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list,
        const ColumnPositions & column_positions,
        const StorageMetadataPtr & metadata_snapshot,
        const VirtualsDescriptionPtr & virtual_columns,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const ColumnsStatistics & stats_to_recalc_,
        const String & marks_file_extension_,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity);

MergeTreeDataPartWriterPtr createMergeTreeDataPartWideWriter(
        const String & data_part_name_,
        const String & logger_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const VirtualsDescriptionPtr & virtual_columns,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const ColumnsStatistics & stats_to_recalc_,
        const String & marks_file_extension_,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity);


MergeTreeDataPartWriterPtr createMergeTreeDataPartWriter(
        MergeTreeDataPartType part_type,
        const String & data_part_name_,
        const String & logger_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list,
        const ColumnPositions & column_positions,
        const StorageMetadataPtr & metadata_snapshot,
        const VirtualsDescriptionPtr & virtual_columns,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const ColumnsStatistics & stats_to_recalc_,
        const String & marks_file_extension_,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity)
{
    if (part_type == MergeTreeDataPartType::Compact)
        return createMergeTreeDataPartCompactWriter(data_part_name_, logger_name_, serializations_, data_part_storage_,
            index_granularity_info_, storage_settings_, columns_list, column_positions, metadata_snapshot, virtual_columns, indices_to_recalc, stats_to_recalc_,
            marks_file_extension_, default_codec_, writer_settings, computed_index_granularity);
    else if (part_type == MergeTreeDataPartType::Wide)
        return createMergeTreeDataPartWideWriter(data_part_name_, logger_name_, serializations_, data_part_storage_,
            index_granularity_info_, storage_settings_, columns_list, metadata_snapshot, virtual_columns, indices_to_recalc, stats_to_recalc_,
            marks_file_extension_, default_codec_, writer_settings, computed_index_granularity);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown part type: {}", part_type.toString());
}

}
