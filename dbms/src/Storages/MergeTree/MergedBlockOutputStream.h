#pragma once

#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>
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
        CompressionMethod compression_method_,
        size_t aio_threshold_);

protected:
    using OffsetColumns = std::set<std::string>;

    struct ColumnStream
    {
        ColumnStream(
            const String & escaped_column_name_,
            const String & data_path,
            const std::string & data_file_extension_,
            const std::string & marks_path,
            const std::string & marks_file_extension_,
            size_t max_compress_block_size,
            CompressionMethod compression_method,
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

    void addStream(const String & path, const String & name, const IDataType & type, size_t estimated_size,
        size_t level, const String & filename, bool skip_offsets);

    /// Write data of one column.
    void writeData(const String & name, const IDataType & type, const IColumn & column, OffsetColumns & offset_columns,
        size_t level, bool skip_offsets);

    MergeTreeData & storage;

    ColumnStreams column_streams;

    /// The offset to the first row of the block for which you want to write the index.
    size_t index_offset = 0;

    size_t min_compress_block_size;
    size_t max_compress_block_size;

    size_t aio_threshold;

    CompressionMethod compression_method;

private:
    /// Internal version of writeData.
    void writeDataImpl(const String & name, const IDataType & type, const IColumn & column,
        OffsetColumns & offset_columns, size_t level, bool write_array_data, bool skip_offsets);
};


/** To write one part.
  * The data refers to one partition, and is written in one part.
  */
class MergedBlockOutputStream : public IMergedBlockOutputStream
{
public:
    MergedBlockOutputStream(
        MergeTreeData & storage_,
        String part_path_,
        const NamesAndTypesList & columns_list_,
        CompressionMethod compression_method);

    MergedBlockOutputStream(
        MergeTreeData & storage_,
        String part_path_,
        const NamesAndTypesList & columns_list_,
        CompressionMethod compression_method,
        const MergeTreeData::DataPart::ColumnToSize & merged_column_to_size_,
        size_t aio_threshold_);

    std::string getPartPath() const;

    /// If the data is pre-sorted.
    void write(const Block & block) override;

    /** If the data is not sorted, but we have previously calculated the permutation, after which they will be sorted.
      * This method is used to save RAM, since you do not need to keep two blocks at once - the original one and the sorted one.
      */
    void writeWithPermutation(const Block & block, const IColumn::Permutation * permutation);

    void writeSuffix() override;

    void writeSuffixAndFinalizePart(
            MergeTreeData::MutableDataPartPtr & new_part,
            const NamesAndTypesList * total_columns_list = nullptr,
            MergeTreeData::DataPart::Checksums * additional_column_checksums = nullptr);

    /// How many marks are already written.
    size_t marksCount();

private:
    void init();

    /** If `permutation` is given, it rearranges the values in the columns when writing.
      * This is necessary to not keep the whole block in the RAM to sort it.
      */
    void writeImpl(const Block & block, const IColumn::Permutation * permutation);

private:
    NamesAndTypesList columns_list;
    String part_path;

    size_t marks_count = 0;

    std::unique_ptr<WriteBufferFromFile> index_file_stream;
    std::unique_ptr<HashingWriteBuffer> index_stream;
    MergeTreeData::DataPart::Index index_columns;
};


/// Writes only those columns that are in `block`
class MergedColumnOnlyOutputStream : public IMergedBlockOutputStream
{
public:
    MergedColumnOnlyOutputStream(
        MergeTreeData & storage_, String part_path_, bool sync_, CompressionMethod compression_method, bool skip_offsets_);

    void write(const Block & block) override;
    void writeSuffix() override;
    MergeTreeData::DataPart::Checksums writeSuffixAndGetChecksums();

private:
    String part_path;

    bool initialized = false;
    bool sync;
    bool skip_offsets;
};

}
