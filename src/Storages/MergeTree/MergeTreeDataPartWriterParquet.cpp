#include <Storages/MergeTree/MergeTreeDataPartWriterParquet.h>

#if USE_PARQUET

#include <Storages/MergeTree/MergeTreeDataPartParquet.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

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

    /// All parquet write knobs (compression, dictionary, bloom filter, ...) come from the query
    /// FormatSettings, mapped in one shared place; see writeOptionsFromFormatSettings.
    options = Parquet::writeOptionsFromFormatSettings(settings.format_settings);

    /// Forced invariants that the mapping must never override: the OffsetIndex is the mark index
    /// of a Parquet part, so it must always be written.
    options.write_page_index = true;
    options.write_checksums = true;

    /// Row group target from FormatSettings, rounded down to a whole number of granules.
    const size_t granule_rows = index_granularity_info.fixed_index_granularity;
    const size_t target_rows = settings.format_settings.parquet.row_group_rows;
    if (granule_rows > 0)
    {
        row_group_size_rows = target_rows / granule_rows * granule_rows;
        if (row_group_size_rows < granule_rows)
            row_group_size_rows = granule_rows;
    }
    else
        row_group_size_rows = target_rows;
}

void MergeTreeDataPartWriterParquet::flushRowGroup()
{
    Columns cols = columns_buffer.releaseColumns();
    size_t num_rows = cols.empty() ? 0 : cols[0]->size();

    Parquet::ColumnChunkWriteStates states;
    size_t i = 0;
    for (const auto & col_name_type : columns_list)
    {
        Parquet::prepareColumnForWrite(cols[i], col_name_type.type, col_name_type.name, options, &states);
        ++i;
    }

    for (auto & s : states)
    {
        Parquet::writeColumnChunkBody(s, options, format_settings, *data_hashing);
        Parquet::finalizeColumnChunkAndWriteFooter(std::move(s), file_state, *data_hashing);
    }

    Parquet::finalizeRowGroup(file_state, num_rows, options, *data_hashing);
}

namespace
{

/// Split the block into granules according to index_granularity.
Granules getGranulesToWrite(const MergeTreeIndexGranularity & index_granularity, size_t block_rows, size_t current_mark, bool last_block)
{
    if (current_mark >= index_granularity.getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Request to get granules from mark {} but index granularity size is {}",
                        current_mark, index_granularity.getMarksCount());

    Granules result;
    size_t current_row = 0;
    while (current_row < block_rows)
    {
        size_t expected_rows_in_mark = index_granularity.getMarkRows(current_mark);
        size_t rows_left_in_block = block_rows - current_row;
        if (rows_left_in_block < expected_rows_in_mark && !last_block)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Required to write {} rows, but only {} rows was written for the non last granule",
                            expected_rows_in_mark, rows_left_in_block);

        result.emplace_back(Granule{
            .start_row = current_row,
            .rows_to_write = std::min(rows_left_in_block, expected_rows_in_mark),
            .mark_number = current_mark,
            .mark_on_start = true,
            .is_complete = (rows_left_in_block >= expected_rows_in_mark)
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

    if (!file_header_written)
    {
        header = result_block.cloneEmpty();
        schema = Parquet::convertSchema(header, options, /*field_ids*/ std::nullopt);
        Parquet::writeFileHeader(file_state, *data_hashing);
        file_header_written = true;
    }

    auto granules = getGranulesToWrite(*index_granularity, result_block.rows(), getCurrentMark(), /*last_block=*/ false);
    calculateAndSerializePrimaryIndex(getIndexBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation), granules);
    calculateAndSerializeSkipIndices(getIndexBlockAndPermute(block, getSkipIndicesColumns(), permutation), granules);

    columns_buffer.add(result_block.mutateColumns());
    while (columns_buffer.size() >= row_group_size_rows)
        flushRowGroup();

    setCurrentMark(getCurrentMark() + granules.size());
    data_written = true;
}

void MergeTreeDataPartWriterParquet::fillIndexGranularity(size_t /*index_granularity_for_block*/, size_t /*rows_in_block*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeTreeDataPartWriterParquet::fillIndexGranularity is not implemented");
}

void MergeTreeDataPartWriterParquet::finalizeIndexGranularity()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeTreeDataPartWriterParquet::finalizeIndexGranularity is not implemented");
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

    String checksum_representation;
    WriteBufferFromString checksum_out(checksum_representation);
    checksums.write(checksum_out);
    checksum_out.finalize();

    if (auto status = key_value_metadata.Set(checksums_key, checksum_representation); !status.ok())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Failed to store checksums in parquet key-value metadata: {}", status.ToString());
}

void MergeTreeDataPartWriterParquet::finish(bool sync)
{
    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(sync);
    finishSkipIndicesSerialization(sync);

    std::vector<parquet::format::KeyValue> footer_metadata;
    footer_metadata.reserve(key_value_metadata.size());
    for (int64_t i = 0; i < key_value_metadata.size(); ++i)
    {
        parquet::format::KeyValue kv;
        kv.__set_key(key_value_metadata.key(i));
        kv.__set_value(key_value_metadata.value(i));
        footer_metadata.push_back(std::move(kv));
    }

    Parquet::writeFileFooter(file_state, std::move(schema), options, *data_hashing, header, footer_metadata);

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

    /// Order of writing is important in compact format
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
