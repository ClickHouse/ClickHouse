#pragma once

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Columns/ColumnArray.h>


namespace DB
{


class IMergedBlockOutputStream : public IBlockOutputStream
{
public:
    IMergedBlockOutputStream(
        MergeTreeData & storage_,
        size_t min_compress_block_size_,
        size_t max_compress_block_size_,
        CompressionCodecPtr default_codec_,
        size_t aio_threshold_,
        bool blocks_are_granules_size_,
        const MergeTreeIndexGranularity & index_granularity_);

    using WrittenOffsetColumns = std::set<std::string>;

protected:
    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::vector<SerializationState>;

    struct ColumnStream
    {
        ColumnStream(
            const String & escaped_column_name_,
            const String & data_path,
            const std::string & data_file_extension_,
            const std::string & marks_path,
            const std::string & marks_file_extension_,
            const CompressionCodecPtr & compression_codec,
            size_t max_compress_block_size,
            size_t estimated_size,
            size_t aio_threshold);

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

    /// Count index_granularity for block and store in `index_granularity`
    void fillIndexGranularity(const Block & block);

    MergeTreeData & storage;

    ColumnStreams column_streams;

    /// The offset to the first row of the block for which you want to write the index.
    size_t index_offset = 0;

    size_t min_compress_block_size;
    size_t max_compress_block_size;

    size_t aio_threshold;

    size_t current_mark = 0;

    const std::string marks_file_extension;
    const size_t mark_size_in_bytes;
    const bool blocks_are_granules_size;

    MergeTreeIndexGranularity index_granularity;

    const bool compute_granularity;
    CompressionCodecPtr codec;
};


/** To write one part.
  * The data refers to one partition, and is written in one part.
  */
class MergedBlockOutputStream final : public IMergedBlockOutputStream
{
public:
    MergedBlockOutputStream(
        MergeTreeData & storage_,
        String part_path_,
        const NamesAndTypesList & columns_list_,
        CompressionCodecPtr default_codec_,
        bool blocks_are_granules_size_ = false);

    MergedBlockOutputStream(
        MergeTreeData & storage_,
        String part_path_,
        const NamesAndTypesList & columns_list_,
        CompressionCodecPtr default_codec_,
        const MergeTreeData::DataPart::ColumnToSize & merged_column_to_size_,
        size_t aio_threshold_,
        bool blocks_are_granules_size_ = false);

    std::string getPartPath() const;

    Block getHeader() const override { return storage.getSampleBlock(); }

    /// If the data is pre-sorted.
    void write(const Block & block) override;

    /** If the data is not sorted, but we have previously calculated the permutation, that will sort it.
      * This method is used to save RAM, since you do not need to keep two blocks at once - the original one and the sorted one.
      */
    void writeWithPermutation(const Block & block, const IColumn::Permutation * permutation);

    void writeSuffix() override;

    /// Finilize writing part and fill inner structures
    void writeSuffixAndFinalizePart(
            MergeTreeData::MutableDataPartPtr & new_part,
            const NamesAndTypesList * total_columns_list = nullptr,
            MergeTreeData::DataPart::Checksums * additional_column_checksums = nullptr);

    const MergeTreeIndexGranularity & getIndexGranularity() const
    {
        return index_granularity;
    }

private:
    void init();

    /** If `permutation` is given, it rearranges the values in the columns when writing.
      * This is necessary to not keep the whole block in the RAM to sort it.
      */
    void writeImpl(const Block & block, const IColumn::Permutation * permutation);

private:
    NamesAndTypesList columns_list;
    SerializationStates serialization_states;
    String part_path;

    size_t rows_count = 0;

    std::unique_ptr<WriteBufferFromFile> index_file_stream;
    std::unique_ptr<HashingWriteBuffer> index_stream;
    MutableColumns index_columns;

    std::vector<std::unique_ptr<ColumnStream>> skip_indices_streams;
    MergeTreeIndexAggregators skip_indices_aggregators;
    std::vector<size_t> skip_index_filling;
};


/// Writes only those columns that are in `header`
class MergedColumnOnlyOutputStream final : public IMergedBlockOutputStream
{
public:
    /// skip_offsets: used when ALTERing columns if we know that array offsets are not altered.
    /// Pass empty 'already_written_offset_columns' first time then and pass the same object to subsequent instances of MergedColumnOnlyOutputStream
    ///  if you want to serialize elements of Nested data structure in different instances of MergedColumnOnlyOutputStream.
    MergedColumnOnlyOutputStream(
        MergeTreeData & storage_, const Block & header_, String part_path_, bool sync_,
        CompressionCodecPtr default_codec_, bool skip_offsets_,
        WrittenOffsetColumns & already_written_offset_columns,
        const MergeTreeIndexGranularity & index_granularity_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void writeSuffix() override;
    MergeTreeData::DataPart::Checksums writeSuffixAndGetChecksums();

private:
    Block header;
    SerializationStates serialization_states;
    String part_path;

    bool initialized = false;
    bool sync;
    bool skip_offsets;

    /// To correctly write Nested elements column-by-column.
    WrittenOffsetColumns & already_written_offset_columns;
};

}
