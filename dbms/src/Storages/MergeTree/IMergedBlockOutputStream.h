#pragma once

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class IMergedBlockOutputStream : public IBlockOutputStream
{
public:
    IMergedBlockOutputStream(
        MergeTreeData & storage_,
        const String & part_path_,
        size_t min_compress_block_size_,
        size_t max_compress_block_size_,
        CompressionCodecPtr default_codec_,
        size_t aio_threshold_,
        bool blocks_are_granules_size_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const MergeTreeIndexGranularity & index_granularity_,
        const MergeTreeIndexGranularityInfo * index_granularity_info_ = nullptr);

    using WrittenOffsetColumns = std::set<std::string>;

protected:
    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::vector<SerializationState>;

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

        void addToChecksums(MergeTreeData::DataPart::Checksums & checksums);
    };

    using ColumnStreams = std::map<String, std::unique_ptr<ColumnStream>>;

    void addStreams(const String & path, const String & name, const IDataType & type,
                    const CompressionCodecPtr & codec, size_t estimated_size, bool skip_offsets);


    IDataType::OutputStreamGetter createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns, bool skip_offsets);

    /// Write data of one column.
    /// Return how many marks were written and
    /// how many rows were written for last mark
    std::pair<size_t, size_t> writeColumn(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        bool skip_offsets,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        size_t from_mark
    );

    /// Write single granule of one column (rows between 2 marks)
    size_t writeSingleGranule(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        bool skip_offsets,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        IDataType::SerializeBinaryBulkSettings & serialize_settings,
        size_t from_row,
        size_t number_of_rows,
        bool write_marks);

    /// Write mark for column
    void writeSingleMark(
        const String & name,
        const IDataType & type,
        WrittenOffsetColumns & offset_columns,
        bool skip_offsets,
        size_t number_of_rows,
        DB::IDataType::SubstreamPath & path);

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
    void calculateAndSerializeSkipIndices(const ColumnsWithTypeAndName & skip_indexes_columns, size_t rows);
    void finishSkipIndicesSerialization(MergeTreeData::DataPart::Checksums & checksums);
protected:
    MergeTreeData & storage;

    SerializationStates serialization_states;
    String part_path;

    ColumnStreams column_streams;

    /// The offset to the first row of the block for which you want to write the index.
    size_t index_offset = 0;

    size_t min_compress_block_size;
    size_t max_compress_block_size;

    size_t aio_threshold;

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
    std::vector<std::unique_ptr<ColumnStream>> skip_indices_streams;
    MergeTreeIndexAggregators skip_indices_aggregators;
    std::vector<size_t> skip_index_filling;

    const bool with_final_mark;
};

}
