#pragma once

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Disks/IDisk.h>


namespace DB
{

struct StreamNameAndMark
{
    String stream_name;
    MarkInCompressedFile mark;
};

using StreamsWithMarks = std::vector<StreamNameAndMark>;
using ColumnNameToMark = std::unordered_map<String, StreamsWithMarks>;

Block getBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation);

/// Writes data part to disk in different formats.
/// Calculates and serializes primary and skip indices if needed.
class IMergeTreeDataPartWriter : private boost::noncopyable
{
public:
    IMergeTreeDataPartWriter(
        const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_list_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreeWriterSettings & settings_,
        const MergeTreeIndexGranularity & index_granularity_ = {});

    virtual ~IMergeTreeDataPartWriter();

    virtual void write(const Block & block, const IColumn::Permutation * permutation) = 0;

    virtual void finish(IMergeTreeDataPart::Checksums & checksums, bool sync) = 0;

    Columns releaseIndexColumns();
    const MergeTreeIndexGranularity & getIndexGranularity() const { return index_granularity; }

protected:
    /// Shift mark and offset to prepare read next mark.
    /// You must call it after calling write method and optionally
    ///  calling calculations of primary and skip indices.
    void next();

    size_t getCurrentMark() const { return current_mark; }
    size_t getIndexOffset() const { return index_offset; }

    const MergeTreeData::DataPartPtr data_part;
    const MergeTreeData & storage;
    const StorageMetadataPtr metadata_snapshot;
    const NamesAndTypesList columns_list;
    const MergeTreeWriterSettings settings;
    MergeTreeIndexGranularity index_granularity;
    const bool with_final_mark;

    size_t next_mark = 0;
    size_t next_index_offset = 0;

    MutableColumns index_columns;

private:
    /// Data is already written up to this mark.
    size_t current_mark = 0;
    /// The offset to the first row of the block for which you want to write the index.
    size_t index_offset = 0;
};

}
