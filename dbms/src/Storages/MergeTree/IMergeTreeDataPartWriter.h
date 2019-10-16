#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class IMergeTreeDataPartWriter
{
public:
    using WrittenOffsetColumns = std::set<std::string>;

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

    virtual size_t write(
        const Block & block, size_t from_mark, size_t offset,
        /* Blocks with already sorted index columns */
        const Block & primary_key_block = {}, const Block & skip_indexes_block = {}) = 0;

    virtual std::pair<size_t, size_t> writeColumn(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        bool skip_offsets,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        size_t from_mark) = 0;

    //  /// Write single granule of one column (rows between 2 marks)
    // virtual size_t writeSingleGranule(
    //     const String & name,
    //     const IDataType & type,
    //     const IColumn & column,
    //     WrittenOffsetColumns & offset_columns,
    //     bool skip_offsets,
    //     IDataType::SerializeBinaryBulkStatePtr & serialization_state,
    //     IDataType::SerializeBinaryBulkSettings & serialize_settings,
    //     size_t from_row,
    //     size_t number_of_rows,
    //     bool write_marks) = 0;

    // /// Write mark for column
    // virtual void writeSingleMark(
    //     const String & name,
    //     const IDataType & type,
    //     WrittenOffsetColumns & offset_columns,
    //     bool skip_offsets,
    //     size_t number_of_rows,
    //     DB::IDataType::SubstreamPath & path) = 0;
protected:
    void start();

    const NamesAndTypesList & columns_list;
    IColumn::Permutation * permutation;
    bool started = false;
};

}