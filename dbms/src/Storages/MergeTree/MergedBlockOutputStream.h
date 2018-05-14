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
        CompressionSettings compression_settings_,
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
            CompressionSettings compression_settings,
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

    void addStreams(const String & path, const String & name, const IDataType & type, size_t estimated_size, bool skip_offsets);

    /// Write data of one column.
    void writeData(const String & name, const IDataType & type, const IColumn & column, OffsetColumns & offset_columns, bool skip_offsets);

    MergeTreeData & storage;

    ColumnStreams column_streams;

    /// The offset to the first row of the block for which you want to write the index.
    size_t index_offset = 0;

    size_t min_compress_block_size;
    size_t max_compress_block_size;

    size_t aio_threshold;

    CompressionSettings compression_settings;
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
        CompressionSettings compression_settings);

    MergedBlockOutputStream(
        MergeTreeData & storage_,
        String part_path_,
        const NamesAndTypesList & columns_list_,
        CompressionSettings compression_settings,
        const MergeTreeData::DataPart::ColumnToSize & merged_column_to_size_,
        size_t aio_threshold_);

    std::string getPartPath() const;

    Block getHeader() const override { return storage.getSampleBlock(); }

    /// If the data is pre-sorted.
    void write(const Block & block) override;

    /** If the data is not sorted, but we have previously calculated the permutation, that will sort it.
      * This method is used to save RAM, since you do not need to keep two blocks at once - the original one and the sorted one.
      */
    void writeWithPermutation(const Block & block, const IColumn::Permutation * permutation);

    void writeSuffix() override;

    void writeSuffixAndFinalizePart(
            MergeTreeData::MutableDataPartPtr & new_part,
            const NamesAndTypesList * total_columns_list = nullptr,
            MergeTreeData::DataPart::Checksums * additional_column_checksums = nullptr);

    /// How many rows are already written.
    size_t getRowsCount() const { return rows_count; }

private:
    void init();

    /** If `permutation` is given, it rearranges the values in the columns when writing.
      * This is necessary to not keep the whole block in the RAM to sort it.
      */
    void writeImpl(const Block & block, const IColumn::Permutation * permutation);

private:
    NamesAndTypesList columns_list;
    String part_path;

    size_t rows_count = 0;
    size_t marks_count = 0;

    std::unique_ptr<WriteBufferFromFile> index_file_stream;
    std::unique_ptr<HashingWriteBuffer> index_stream;
    MutableColumns index_columns;
};


/// Writes only those columns that are in `block`
class MergedColumnOnlyOutputStream final : public IMergedBlockOutputStream
{
public:
    /// skip_offsets: used when ALTERing columns if we know that array offsets are not altered.
    MergedColumnOnlyOutputStream(
        MergeTreeData & storage_, const Block & header_, String part_path_, bool sync_, CompressionSettings compression_settings, bool skip_offsets_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void writeSuffix() override;
    MergeTreeData::DataPart::Checksums writeSuffixAndGetChecksums();

private:
    Block header;
    String part_path;

    bool initialized = false;
    bool sync;
    bool skip_offsets;
};

}
