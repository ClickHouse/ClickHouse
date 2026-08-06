#pragma once
#include <Formats/MarkInCompressedFile.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedWriteBuffer.h>

namespace DB
{

struct WriteSettings;
struct MergeTreeDataPartChecksums;

class IDataPartStorage;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

class ICompressionCodec;
using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

/// Helper class, which holds chain of buffers to write data file with marks.
/// It is used to write: one column, skip index or all columns (in compact format).
struct MergeTreeWriterStream
{
    MergeTreeWriterStream(
        const String & escaped_column_name_,
        const MutableDataPartStoragePtr & data_part_storage,
        const String & data_path_,
        const std::string & data_file_extension_,
        const std::string & marks_path_,
        const std::string & marks_file_extension_,
        const CompressionCodecPtr & compression_codec_,
        size_t max_compress_block_size_,
        const CompressionCodecPtr & marks_compression_codec_,
        size_t marks_compress_block_size_,
        const WriteSettings & query_write_settings);

    ~MergeTreeWriterStream()
    {
        plain_file.reset();
        marks_file.reset();
    }

    String escaped_column_name;
    std::string data_file_extension;
    std::string marks_file_extension;

    /// compressed_hashing -> compressor -> plain_hashing -> plain_file
    std::unique_ptr<WriteBufferFromFileBase> plain_file;
    HashingWriteBuffer plain_hashing;
    CompressedWriteBuffer compressor;
    HashingWriteBuffer compressed_hashing;

    /// marks_compressed_hashing -> marks_compressor -> marks_hashing -> marks_file
    std::unique_ptr<WriteBufferFromFileBase> marks_file;
    HashingWriteBuffer marks_hashing;
    CompressedWriteBuffer marks_compressor;
    HashingWriteBuffer marks_compressed_hashing;
    bool compress_marks;

    bool is_prefinalized = false;

    void preFinalize();
    void finalize();
    void cancel() noexcept;
    void sync() const;

    void addToChecksums(MergeTreeDataPartChecksums & checksums, bool is_compressed);
    MarkInCompressedFile getCurrentMark() const;
};

}
