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

struct Granule
{
    size_t start;
    size_t rows_count;
    bool mark_on_start;
    bool is_completed;
};

using Granules = std::vector<Granule>;

Granules getGranulesToWrite(const MergeTreeIndexGranularity & index_granularity, size_t block_rows, size_t current_mark, size_t rows_written_in_last_mark);

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

    size_t getCurrentMark() const { return current_mark; }
    void setCurrentMark(size_t mark) { current_mark = mark; }

    size_t getRowsWrittenInLastMark() const { return rows_written_in_last_mark; }
    void setRowsWrittenInLastMark(size_t rows_written) { rows_written_in_last_mark = rows_written; }

    const MergeTreeData::DataPartPtr data_part;
    const MergeTreeData & storage;
    const StorageMetadataPtr metadata_snapshot;
    const NamesAndTypesList columns_list;
    const MergeTreeWriterSettings settings;
    MergeTreeIndexGranularity index_granularity;
    const bool with_final_mark;

    size_t next_mark = 0;

    MutableColumns index_columns;

private:
    /// Data is already written up to this mark.
    size_t current_mark = 0;
    /// The offset to the first row of the block for which you want to write the index.
    size_t rows_written_in_last_mark = 0;
};

}
