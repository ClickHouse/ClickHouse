#include <Columns/ColumnSparse.h>
#include <Compression/CompressionFactory.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/defines.h>
#include <Common/MemoryTrackerBlockerInThread.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsString default_compression_codec;
}

Block getIndexBlockAndPermute(const Block & block, const Names & names, const IColumnPermutation * permutation)
{
    Block result;
    for (size_t i = 0, size = names.size(); i < size; ++i)
    {
        auto src_column = block.getColumnOrSubcolumnByName(names[i]);
        src_column.column = removeSpecialRepresentations(src_column.column);
        src_column.column = src_column.column->convertToFullColumnIfConst();
        result.insert(i, src_column);

        /// Reorder primary key columns in advance and add them to `primary_key_columns`.
        if (permutation)
        {
            auto & column = result.getByPosition(i);
            column.column = column.column->permute(*permutation, 0);
        }
    }

    return result;
}

Block permuteBlockIfNeeded(const Block & block, const IColumnPermutation * permutation)
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
    MergeTreeIndexGranularityPtr index_granularity_)
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
    , index_granularity(std::move(index_granularity_))
{
}

std::optional<Columns> IMergeTreeDataPartWriter::releaseIndexColumns()
{
    if (!settings.save_primary_index_in_memory)
        return {};

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

PlainMarksByName IMergeTreeDataPartWriter::releaseCachedMarks()
{
    PlainMarksByName res;
    std::swap(cached_marks, res);
    return res;
}

PlainMarksByName IMergeTreeDataPartWriter::releaseCachedIndexMarks()
{
    PlainMarksByName res;
    std::swap(cached_index_marks, res);
    return res;
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
    ASTPtr default_codec_desc = default_codec->getFullCodecDesc();

    auto default_compression_codec_mergetree_settings = (*storage_settings)[MergeTreeSetting::default_compression_codec].value;
    // Prioritize the codec from the settings over `default_codec`
    if (!default_compression_codec_mergetree_settings.empty())
        default_codec_desc = CompressionCodecFactory::instance().get(default_compression_codec_mergetree_settings)->getFullCodecDesc();

    const auto & columns = metadata_snapshot->getColumns();
    if (const auto * column_desc = columns.tryGet(column_name))
        return column_desc->codec ? column_desc->codec : default_codec_desc;

    if (const auto * virtual_desc = virtual_columns->tryGetDescription(column_name))
        return virtual_desc->codec ? virtual_desc->codec : default_codec_desc;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column name: {}", column_name);
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
        const String & marks_file_extension_,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        MergeTreeIndexGranularityPtr computed_index_granularity);

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
        const String & marks_file_extension_,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        MergeTreeIndexGranularityPtr computed_index_granularity,
        WrittenOffsetSubstreams * written_offset_substreams);

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
        const String & marks_file_extension_,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        MergeTreeIndexGranularityPtr computed_index_granularity,
        WrittenOffsetSubstreams * written_offset_substreams)
{
    if (part_type == MergeTreeDataPartType::Compact)
        return createMergeTreeDataPartCompactWriter(
            data_part_name_,
            logger_name_,
            serializations_,
            data_part_storage_,
            index_granularity_info_,
            storage_settings_,
            columns_list,
            column_positions,
            metadata_snapshot,
            virtual_columns,
            indices_to_recalc,
            marks_file_extension_,
            default_codec_,
            writer_settings,
            std::move(computed_index_granularity));
    if (part_type == MergeTreeDataPartType::Wide)
        return createMergeTreeDataPartWideWriter(
            data_part_name_,
            logger_name_,
            serializations_,
            data_part_storage_,
            index_granularity_info_,
            storage_settings_,
            columns_list,
            metadata_snapshot,
            virtual_columns,
            indices_to_recalc,
            marks_file_extension_,
            default_codec_,
            writer_settings,
            std::move(computed_index_granularity),
            written_offset_substreams);
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown part type: {}", part_type.toString());
}

}
