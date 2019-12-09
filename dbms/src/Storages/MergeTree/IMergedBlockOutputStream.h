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
        const MergeTreeDataPartPtr & data_part);

    using WrittenOffsetColumns = std::set<std::string>;

    const MergeTreeIndexGranularity & getIndexGranularity()
    {
        return writer->getIndexGranularity();
    }

protected:
    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::vector<SerializationState>;

    IDataType::OutputStreamGetter createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns, bool skip_offsets);

protected:
    const MergeTreeData & storage;

    String part_path;

    /// The offset to the first row of the block for which you want to write the index.
    // size_t index_offset = 0;

    // size_t current_mark = 0;

    /// Number of mark in data from which skip indices have to start
    /// aggregation. I.e. it's data mark number, not skip indices mark.
    // size_t skip_index_data_mark = 0;

    // const bool can_use_adaptive_granularity;
    // const std::string marks_file_extension;
    // const bool blocks_are_granules_size;

    // MergeTreeIndexGranularity index_granularity;

    // const bool compute_granularity;

    // std::vector<MergeTreeIndexPtr> skip_indices;
    // std::vector<std::unique_ptr<IMergeTreeDataPartWriter::ColumnStream>> skip_indices_streams;
    // MergeTreeIndexAggregators skip_indices_aggregators;
    // std::vector<size_t> skip_index_filling;

    static Block getBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation);

    MergeTreeWriterPtr writer;

    // const bool with_final_mark;
};

}
