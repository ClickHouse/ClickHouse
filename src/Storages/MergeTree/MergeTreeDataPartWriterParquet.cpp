#include <Storages/MergeTree/MergeTreeDataPartWriterParquet.h>

#if USE_PARQUET

#include <Storages/MergeTree/MergeTreeDataPartParquet.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Formats/FormatSettings.h>
#include <IO/HashingWriteBuffer.h>
#include <Core/Block.h>
#include <algorithm>
#include <limits>

namespace DB
{

static constexpr const char * parquet_file_extension = ".parquet";

MergeTreeDataPartWriterParquet::MergeTreeDataPartWriterParquet(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    MergeTreeIndexGranularityPtr index_granularity_)
    : MergeTreeDataPartWriterOnDisk(
        data_part_name_, logger_name_, serializations_,
        data_part_storage_, index_granularity_info_, storage_settings_,
        columns_list_, metadata_snapshot_,
        indices_to_recalc_, /* marks_file_extension = */ "",
        default_codec_, settings_, std::move(index_granularity_),
        static_cast<WrittenOffsetSubstreams *>(nullptr))
{
    parquet_plain_file = getDataPartStorage().writeFile(
        MergeTreeDataPartParquet::DATA_FILE_NAME + String(parquet_file_extension),
        4096,
        settings_.query_write_settings);
    data_hashing = std::make_unique<HashingWriteBuffer>(*parquet_plain_file);

    for (const auto & column : columns_list_)
        header.insert(ColumnWithTypeAndName(column.type, column.name));

    const size_t granule_rows = index_granularity_info_.fixed_index_granularity;
    const size_t target_rows = settings.format_settings.parquet.row_group_rows;
    if (granule_rows > 0)
    {
        row_group_size_rows = target_rows / granule_rows * granule_rows;
        if (row_group_size_rows < granule_rows)
            row_group_size_rows = granule_rows;
    }
    else
        row_group_size_rows = target_rows;

    FormatSettings format_settings = settings.format_settings;
    format_settings.parquet.row_group_rows = row_group_size_rows;
    format_settings.parquet.row_group_bytes = std::numeric_limits<size_t>::max();
    format_settings.parquet.max_rows_per_page = granule_rows;
    format_settings.parquet.write_page_index = true;
    format_settings.parquet.output_compression_method = FormatSettings::ParquetCompression::NONE;
    format_settings.parquet.max_dictionary_size = 0;

    output_format = std::make_shared<ParquetBlockOutputFormat>(
        *data_hashing, std::make_shared<const Block>(header), format_settings, nullptr);
}

namespace
{

void appendBlock(Block & acc, Block && block)
{
    if (block.rows() == 0)
        return;

    if (acc.columns() == 0)
    {
        acc = std::move(block);
        return;
    }

    MutableColumns cols = acc.mutateColumns();
    for (size_t i = 0; i < cols.size(); ++i)
        cols[i]->insertRangeFrom(*block.getByPosition(i).column, 0, block.rows());
    acc.setColumns(std::move(cols));
}

Granules getGranulesToWrite(const MergeTreeIndexGranularity & index_granularity, size_t block_rows)
{
    Granules result;
    size_t current_row = 0;
    size_t current_mark = 0;
    while (current_row < block_rows)
    {
        size_t rows_in_mark = index_granularity.getMarkRows(current_mark);
        result.emplace_back(Granule{
            .start_row = current_row,
            .rows_to_write = std::min(block_rows - current_row, rows_in_mark),
            .mark_number = current_mark,
            .mark_on_start = true,
            .is_complete = (block_rows - current_row >= rows_in_mark)
        });
        current_row += result.back().rows_to_write;
        ++current_mark;
    }
    return result;
}

}

void MergeTreeDataPartWriterParquet::write(const Block & block, const IColumnPermutation * permutation, Block * /*permuted_columns_cache*/)
{
    Block result_block = block;
    prepareBlockForWriting(result_block);
    result_block = permuteBlockIfNeeded(result_block, permutation, nullptr);

    output_format->write(result_block);

    if (settings.rewrite_primary_key)
        appendBlock(primary_index_block, getIndexBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation));
    appendBlock(skip_indices_block, getIndexBlockAndPermute(block, getSkipIndicesColumns(), permutation));

    data_written = true;
}

void MergeTreeDataPartWriterParquet::fillIndexGranularity(size_t /*index_granularity_for_block*/, size_t /*rows_in_block*/)
{
}

void MergeTreeDataPartWriterParquet::finalizeIndexGranularity()
{
    output_format->finalize();
    data_hashing->next();

    for (size_t page_rows : output_format->getPageRowCounts())
        index_granularity->appendMark(page_rows);

    if (index_granularity->getMarksCount() == 0)
        return;

    auto granules = getGranulesToWrite(*index_granularity, index_granularity->getTotalRows());

    if (settings.rewrite_primary_key)
        calculateAndSerializePrimaryIndex(primary_index_block, granules);
    calculateAndSerializeSkipIndices(skip_indices_block, granules);
}

void MergeTreeDataPartWriterParquet::fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & /*checksums_to_remove*/)
{
    if (!columns_list.empty())
    {
        const String data_file = MergeTreeDataPartParquet::DATA_FILE_NAME + String(parquet_file_extension);
        checksums.files[data_file].file_size = data_hashing->count();
        checksums.files[data_file].file_hash = data_hashing->getHash();
    }

    if (settings.rewrite_primary_key)
        fillPrimaryIndexChecksums(checksums);

    fillSkipIndicesChecksums(checksums);
}

void MergeTreeDataPartWriterParquet::finish(bool sync)
{
    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(sync);
    finishSkipIndicesSerialization(sync);

    data_hashing->finalize();
    parquet_plain_file->preFinalize();
    if (sync)
        parquet_plain_file->sync();
    parquet_plain_file->finalize();
}

void MergeTreeDataPartWriterParquet::cancel() noexcept
{
    if (data_hashing)
        data_hashing->cancel();
    if (parquet_plain_file)
        parquet_plain_file->cancel();
    Base::cancel();
}

MergeTreeDataPartWriterPtr createMergeTreeDataPartParquetWriter(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list,
    const ColumnPositions & column_positions,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & writer_settings,
    MergeTreeIndexGranularityPtr computed_index_granularity);

MergeTreeDataPartWriterPtr createMergeTreeDataPartParquetWriter(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list,
    const ColumnPositions & column_positions,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & writer_settings,
    MergeTreeIndexGranularityPtr computed_index_granularity)
{
    NamesAndTypesList ordered_columns_list;
    std::copy_if(columns_list.begin(), columns_list.end(), std::back_inserter(ordered_columns_list),
        [&column_positions](const auto & column) { return column_positions.contains(column.name); });

    ordered_columns_list.sort([&column_positions](const auto & lhs, const auto & rhs)
        { return column_positions.at(lhs.name) < column_positions.at(rhs.name); });

    return std::make_unique<MergeTreeDataPartWriterParquet>(
        data_part_name_, logger_name_, serializations_, data_part_storage_,
        index_granularity_info_, storage_settings_, ordered_columns_list, metadata_snapshot,
        indices_to_recalc,
        default_codec_, writer_settings, std::move(computed_index_granularity));
}

}

#endif
