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
// #include <Storages/MergeTree/IMergeTreeDataPart.h>


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
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const WriterSettings & settings);

    virtual MarkWithOffset write(
        const Block & block, const IColumn::Permutation * permutation,
        size_t from_mark, size_t offset, const MergeTreeIndexGranularity & index_granularity,
        /* Blocks with already sorted index columns */
        const Block & primary_key_block = {}, const Block & skip_indexes_block = {}) = 0;

    virtual void finalize(IMergeTreeDataPart::Checksums & checksums, bool write_final_mark, bool sync = false) = 0;

    virtual ~IMergeTreeDataPartWriter();

protected:
    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::vector<SerializationState>;

    String part_path;
    const MergeTreeData & storage;
    NamesAndTypesList columns_list;
    const String marks_file_extension;

    CompressionCodecPtr default_codec;

    WriterSettings settings;
};

using MergeTreeWriterPtr = std::unique_ptr<IMergeTreeDataPartWriter>;

}
