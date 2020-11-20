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


/// Writes data part to disk in different formats.
/// Calculates and serializes primary and skip indices if needed.
class IMergeTreeDataPartWriter : private boost::noncopyable
{
public:
    IMergeTreeDataPartWriter(
        const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_list_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreeWriterSettings & settings_);

    IMergeTreeDataPartWriter(
        const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_list_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreeIndices & skip_indices_,
        const MergeTreeIndexGranularity & index_granularity_,
        const MergeTreeWriterSettings & settings_);

    virtual ~IMergeTreeDataPartWriter();

    virtual void write(
        const Block & block, const IColumn::Permutation * permutation = nullptr,
        /* Blocks with already sorted index columns */
        const Block & primary_key_block = {}, const Block & skip_indexes_block = {}) = 0;

    virtual void calculateAndSerializePrimaryIndex(const Block & /* primary_index_block */) {}
    virtual void calculateAndSerializeSkipIndices(const Block & /* skip_indexes_block */) {}

    /// Shift mark and offset to prepare read next mark.
    /// You must call it after calling write method and optionally
    ///  calling calculations of primary and skip indices.
    void next();

    virtual void initSkipIndices() {}
    virtual void initPrimaryIndex() {}

    virtual void finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync) = 0;
    virtual void finishPrimaryIndexSerialization(MergeTreeData::DataPart::Checksums & /* checksums */, bool /* sync */) {}
    virtual void finishSkipIndicesSerialization(MergeTreeData::DataPart::Checksums & /* checksums */, bool /* sync */) {}

    Columns releaseIndexColumns();
    const MergeTreeIndexGranularity & getIndexGranularity() const { return index_granularity; }
    const MergeTreeIndices & getSkipIndices() { return skip_indices; }

protected:
    size_t getCurrentMark() const { return current_mark; }
    size_t getIndexOffset() const { return index_offset; }
    /// Finishes our current unfinished mark if we have already written more rows for it
    /// than granularity in the new block. Return true if last mark actually was adjusted.
    /// Example:
    /// __|________|___. <- previous block with granularity 8 and last unfinished mark with 3 rows
    /// new_block_index_granularity = 2, so
    /// __|________|___|__|__|__|
    ///                ^ finish last unfinished mark, new marks will have granularity 2
    bool adjustLastUnfinishedMark(size_t new_block_index_granularity);

    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::unordered_map<String, SerializationState>;

    MergeTreeData::DataPartPtr data_part;
    const MergeTreeData & storage;
    const StorageMetadataPtr metadata_snapshot;
    const NamesAndTypesList columns_list;
    const MergeTreeIndices skip_indices;
    MergeTreeIndexGranularity index_granularity;
    const MergeTreeWriterSettings settings;
    const bool with_final_mark;

    size_t next_mark = 0;
    size_t next_index_offset = 0;

    /// When we were writing fresh block granularity of the last mark was adjusted
    /// See adjustLastUnfinishedMark
    bool last_granule_was_adjusted = false;

    MutableColumns index_columns;

private:
    /// Data is already written up to this mark.
    size_t current_mark = 0;
    /// The offset to the first row of the block for which you want to write the index.
    /// Or how many rows we have to write for this last unfinished mark.
    size_t index_offset = 0;
};

}
