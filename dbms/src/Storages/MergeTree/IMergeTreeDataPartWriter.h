#pragma once

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <DataStreams/IBlockOutputStream.h>
// #include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>


namespace DB
{

class IMergeTreeDataPartWriter
{
public:
    using WrittenOffsetColumns = std::set<std::string>;
    using MarkWithOffset = std::pair<size_t, size_t>;

    struct ColumnStream
    {
        ColumnStream(
            const String & escaped_column_name_,
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
        WriteBufferFromFile marks_file;
        HashingWriteBuffer marks;

        void finalize();

        void sync();

        void addToChecksums(IMergeTreeDataPart::Checksums & checksums);
    };

    using ColumnStreamPtr = std::unique_ptr<ColumnStream>;
    using ColumnStreams = std::map<String, ColumnStreamPtr>;

    IMergeTreeDataPartWriter(
        const String & part_path,
        const MergeTreeData & storage,
        const NamesAndTypesList & columns_list,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc, 
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const WriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

    virtual MarkWithOffset write(
        const Block & block, const IColumn::Permutation * permutation,
        size_t from_mark, size_t offset, MergeTreeIndexGranularity & index_granularity,
        /* Blocks with already sorted index columns */
        const Block & primary_key_block = {}, const Block & skip_indexes_block = {},
        bool skip_offsets = false, const WrittenOffsetColumns & already_written_offset_columns = {}) = 0;

    virtual void finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool write_final_mark, bool sync = false) = 0;

    virtual ~IMergeTreeDataPartWriter();

    /// Count index_granularity for block and store in `index_granularity`
    void fillIndexGranularity(const Block & block);

    void initSkipIndices();
    void initPrimaryIndex();
    void calculateAndSerializePrimaryIndex(const Block & primary_index_block, size_t rows);
    void calculateAndSerializeSkipIndices(const Block & skip_indexes_block, size_t rows);
    void finishPrimaryIndexSerialization(MergeTreeData::DataPart::Checksums & checksums, bool write_final_mark);
    void finishSkipIndicesSerialization(MergeTreeData::DataPart::Checksums & checksums);
    void next();

protected:
    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::vector<SerializationState>;

    String part_path;
    const MergeTreeData & storage;
    NamesAndTypesList columns_list;
    const String marks_file_extension;

    MergeTreeIndexGranularity index_granularity;

    CompressionCodecPtr default_codec;

    bool can_use_adaptive_granularity;
    bool blocks_are_granules_size;
    bool compute_granularity;
    bool with_final_mark;

    size_t current_mark = 0;
    size_t index_offset = 0;

    size_t next_mark = 0;
    size_t next_index_offset = 0;

    /// Number of mark in data from which skip indices have to start
    /// aggregation. I.e. it's data mark number, not skip indices mark.
    size_t skip_index_data_mark = 0;

    std::vector<MergeTreeIndexPtr> skip_indices;
    std::vector<std::unique_ptr<IMergeTreeDataPartWriter::ColumnStream>> skip_indices_streams;
    MergeTreeIndexAggregators skip_indices_aggregators;
    std::vector<size_t> skip_index_filling;

    std::unique_ptr<WriteBufferFromFile> index_file_stream;
    std::unique_ptr<HashingWriteBuffer> index_stream;
    MutableColumns index_columns;
    DataTypes index_types;
    /// Index columns values from the last row from the last block
    /// It's written to index file in the `writeSuffixAndFinalizePart` method
    Row last_index_row;

    WriterSettings settings;
};

using MergeTreeWriterPtr = std::unique_ptr<IMergeTreeDataPartWriter>;

}
