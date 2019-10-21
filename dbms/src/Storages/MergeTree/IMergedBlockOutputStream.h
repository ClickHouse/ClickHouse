#pragma once

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

class IMergedBlockOutputStream : public IBlockOutputStream
{
public:
    IMergedBlockOutputStream(
        const MergeTreeDataPartPtr & data_part,
        CompressionCodecPtr codec_,
        const WriterSettings & writer_settings_,
        bool blocks_are_granules_size_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        bool can_use_adaptive_granularity_);

    using WrittenOffsetColumns = std::set<std::string>;

protected:
    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::vector<SerializationState>;

    IDataType::OutputStreamGetter createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns, bool skip_offsets);

    /// Count index_granularity for block and store in `index_granularity`
    void fillIndexGranularity(const Block & block);

    /// Write final mark to the end of column
    void writeFinalMark(
        const std::string & column_name,
        const DataTypePtr column_type,
        WrittenOffsetColumns & offset_columns,
        bool skip_offsets,
        DB::IDataType::SubstreamPath & path);

    void initSkipIndices();
    void calculateAndSerializeSkipIndices(const Block & skip_indexes_block, size_t rows);
    void finishSkipIndicesSerialization(MergeTreeData::DataPart::Checksums & checksums);
protected:
    const MergeTreeData & storage;

    SerializationStates serialization_states;
    String part_path;

    /// The offset to the first row of the block for which you want to write the index.
    size_t index_offset = 0;

    WriterSettings writer_settings;

    size_t current_mark = 0;

    /// Number of mark in data from which skip indices have to start
    /// aggregation. I.e. it's data mark number, not skip indices mark.
    size_t skip_index_data_mark = 0;

    const bool can_use_adaptive_granularity;
    const std::string marks_file_extension;
    const bool blocks_are_granules_size;

    MergeTreeIndexGranularity index_granularity;

    const bool compute_granularity;
    CompressionCodecPtr codec;

    std::vector<MergeTreeIndexPtr> skip_indices;
    std::vector<std::unique_ptr<IMergeTreeDataPartWriter::ColumnStream>> skip_indices_streams;
    MergeTreeIndexAggregators skip_indices_aggregators;
    std::vector<size_t> skip_index_filling;

    MergeTreeWriterPtr writer;

    const bool with_final_mark;
};

}
