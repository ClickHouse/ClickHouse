#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>

namespace DB
{

void MergeTreeWriterStream::preFinalize()
{
    /// Here the main goal is to do preFinalize calls for plain_file and marks_file
    /// Before that all hashing and compression buffers have to be finalized
    /// Otherwise some data might stuck in the buffers above plain_file and marks_file
    /// Also the order is important
    compressed_hashing.finalize();
    compressor.finalize();
    plain_hashing.finalize();

    marks_compressed_hashing.finalize();
    marks_compressor.finalize();
    marks_hashing.finalize();

    plain_file->preFinalize();
    marks_file->preFinalize();

    is_prefinalized = true;
}

void MergeTreeWriterStream::finalize()
{
    if (!is_prefinalized)
        preFinalize();

    plain_file->finalize();
    marks_file->finalize();
}

void MergeTreeWriterStream::cancel() noexcept
{
    compressed_hashing.cancel();
    compressor.cancel();
    plain_hashing.cancel();

    marks_compressed_hashing.cancel();
    marks_compressor.cancel();
    marks_hashing.cancel();

    plain_file->cancel();
    marks_file->cancel();
}

void MergeTreeWriterStream::sync() const
{
    plain_file->sync();
    marks_file->sync();
}

MergeTreeWriterStream::MergeTreeWriterStream(
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
    const WriteSettings & query_write_settings) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    plain_file(data_part_storage->writeFile(data_path_ + data_file_extension, max_compress_block_size_, query_write_settings)),
    plain_hashing(*plain_file),
    compressor(plain_hashing, compression_codec_, max_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    compressed_hashing(compressor),
    marks_file(data_part_storage->writeFile(marks_path_ + marks_file_extension, 4096, query_write_settings)),
    marks_hashing(*marks_file),
    marks_compressor(marks_hashing, marks_compression_codec_, marks_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    marks_compressed_hashing(marks_compressor),
    compress_marks(MarkType(marks_file_extension).compressed)
{
}

void MergeTreeWriterStream::addToChecksums(MergeTreeDataPartChecksums & checksums, bool is_compressed)
{
    String name = escaped_column_name;

    if (is_compressed)
    {
        checksums.files[name + data_file_extension].is_compressed = true;
        checksums.files[name + data_file_extension].uncompressed_size = compressed_hashing.count();
        checksums.files[name + data_file_extension].uncompressed_hash = compressed_hashing.getHash();
    }

    checksums.files[name + data_file_extension].file_size = plain_hashing.count();
    checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();

    if (compress_marks)
    {
        checksums.files[name + marks_file_extension].is_compressed = true;
        checksums.files[name + marks_file_extension].uncompressed_size = marks_compressed_hashing.count();
        checksums.files[name + marks_file_extension].uncompressed_hash = marks_compressed_hashing.getHash();
    }

    checksums.files[name + marks_file_extension].file_size = marks_hashing.count();
    checksums.files[name + marks_file_extension].file_hash = marks_hashing.getHash();
}

MarkInCompressedFile MergeTreeWriterStream::getCurrentMark() const
{
    return MarkInCompressedFile
    {
        .offset_in_compressed_file = plain_hashing.count(),
        .offset_in_decompressed_block = compressed_hashing.offset()
    };
}

}
