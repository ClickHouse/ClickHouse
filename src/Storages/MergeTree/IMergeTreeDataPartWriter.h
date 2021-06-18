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

    virtual void finishDataSerialization(IMergeTreeDataPart::Checksums & checksums) = 0;
    virtual void finishPrimaryIndexSerialization(MergeTreeData::DataPart::Checksums & /* checksums */) {}
    virtual void finishSkipIndicesSerialization(MergeTreeData::DataPart::Checksums & /* checksums */) {}

    Columns releaseIndexColumns();
    const MergeTreeIndexGranularity & getIndexGranularity() const { return index_granularity; }
    const MergeTreeIndices & getSkipIndices() { return skip_indices; }

protected:
    size_t getCurrentMark() const { return current_mark; }
    size_t getIndexOffset() const { return index_offset; }

    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::unordered_map<String, SerializationState>;

    MergeTreeData::DataPartPtr data_part;
    const MergeTreeData & storage;
    StorageMetadataPtr metadata_snapshot;
    NamesAndTypesList columns_list;
    MergeTreeIndices skip_indices;
    MergeTreeIndexGranularity index_granularity;
    MergeTreeWriterSettings settings;
    bool with_final_mark;

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
