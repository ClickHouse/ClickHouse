#pragma once

#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
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
class MergeTreeDataPartWriterOnDisk : public IMergeTreeDataPartWriter
{
public:
    using WrittenOffsetColumns = std::set<std::string>;

    /// Helper class, which holds chain of buffers to write data file with marks.
    /// It is used to write: one column, skip index or all columns (in compact format).
    struct Stream
    {
        Stream(
            const String & escaped_column_name_,
            DiskPtr disk_,
            const String & data_path_,
            const std::string & data_file_extension_,
            const std::string & marks_path_,
            const std::string & marks_file_extension_,
            const CompressionCodecPtr & compression_codec_,
            size_t max_compress_block_size_,
            size_t estimated_size_,
            size_t aio_threshold_);

        String escaped_column_name;
        std::string data_file_extension;
        std::string marks_file_extension;

        /// compressed -> compressed_buf -> plain_hashing -> plain_file
        std::unique_ptr<WriteBufferFromFileBase> plain_file;
        HashingWriteBuffer plain_hashing;
        CompressedWriteBuffer compressed_buf;
        HashingWriteBuffer compressed;

        /// marks -> marks_file
        std::unique_ptr<WriteBufferFromFileBase> marks_file;
        HashingWriteBuffer marks;

        void finalize();

        void sync() const;

        void addToChecksums(IMergeTreeDataPart::Checksums & checksums);
    };

    using StreamPtr = std::unique_ptr<Stream>;

    MergeTreeDataPartWriterOnDisk(
        const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

    void calculateAndSerializePrimaryIndex(const Block & primary_index_block) final;
    void calculateAndSerializeSkipIndices(const Block & skip_indexes_block) final;

     /// Count index_granularity for block and store in `index_granularity`
    size_t computeIndexGranularity(const Block & block);
    virtual void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block);

    void initSkipIndices() final;
    void initPrimaryIndex() final;

    void finishPrimaryIndexSerialization(MergeTreeData::DataPart::Checksums & checksums) final;
    void finishSkipIndicesSerialization(MergeTreeData::DataPart::Checksums & checksums) final;

    void setWrittenOffsetColumns(WrittenOffsetColumns * written_offset_columns_)
    {
        written_offset_columns = written_offset_columns_;
    }

protected:
    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::unordered_map<String, SerializationState>;

    String part_path;
    const String marks_file_extension;
    CompressionCodecPtr default_codec;

    bool compute_granularity;
    bool need_finish_last_granule;

    /// Number of marsk in data from which skip indices have to start
    /// aggregation. I.e. it's data mark number, not skip indices mark.
    size_t skip_index_data_mark = 0;

    std::vector<StreamPtr> skip_indices_streams;
    MergeTreeIndexAggregators skip_indices_aggregators;
    std::vector<size_t> skip_index_filling;

    std::unique_ptr<WriteBufferFromFileBase> index_file_stream;
    std::unique_ptr<HashingWriteBuffer> index_stream;
    DataTypes index_types;
    /// Index columns values from the last row from the last block
    /// It's written to index file in the `writeSuffixAndFinalizePart` method
    Row last_index_row;

    bool data_written = false;
    bool primary_index_initialized = false;
    bool skip_indices_initialized = false;

    /// To correctly write Nested elements column-by-column.
    WrittenOffsetColumns * written_offset_columns = nullptr;

private:
    /// Index is already serialized up to this mark.
    size_t index_mark = 0;
};

}
