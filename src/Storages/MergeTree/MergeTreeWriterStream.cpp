#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/SizeAdaptiveSpoolBuffer.h>
#include <IO/PackedFilesWriter.h>

namespace DB
{

/// Build the bottom-of-chain buffer for one of the stream files. When the caller asked for a
/// packed archive (packed_writer non-null AND a virtual name is set), wrap the write in a
/// SizeAdaptiveSpoolBuffer that decides at write time between "stays in archive" and
/// "spills to a standalone per-file write on data_part_storage". The substream's data and
/// marks files share a coordinator so they always end up in the same layout. Otherwise
/// (column streams, or skip indices when packing is disabled) write straight to
/// data_part_storage.
static std::unique_ptr<WriteBufferFromFileBase> openStreamFile(
    const MutableDataPartStoragePtr & data_part_storage,
    PackedFilesWriter * packed_writer,
    const String & packed_virtual_name,
    const String & file_path,
    size_t buf_size,
    const WriteSettings & write_settings,
    size_t packed_spill_threshold,
    bool * coupled_spilled_flag)
{
    if (packed_writer && !packed_virtual_name.empty())
    {
        auto open_per_file = [data_part_storage, file_path, buf_size, write_settings]()
        {
            return data_part_storage->writeFile(file_path, buf_size, write_settings);
        };
        return std::make_unique<SizeAdaptiveSpoolBuffer>(
            packed_spill_threshold,
            buf_size,
            std::move(open_per_file),
            packed_writer,
            packed_virtual_name,
            write_settings,
            file_path,
            coupled_spilled_flag);
    }
    return data_part_storage->writeFile(file_path, buf_size, write_settings);
}


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
    const WriteSettings & query_write_settings,
    PackedFilesWriter * packed_writer,
    const String & packed_data_name_,
    const String & packed_marks_name_,
    size_t packed_spill_threshold_) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    is_size_adaptive(packed_writer != nullptr && (!packed_data_name_.empty() || !packed_marks_name_.empty())),
    plain_file(openStreamFile(data_part_storage, packed_writer, packed_data_name_, data_path_ + data_file_extension, max_compress_block_size_, query_write_settings, packed_spill_threshold_, &spool_coupled_spilled)),
    plain_hashing(*plain_file),
    compressor(plain_hashing, compression_codec_, max_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    compressed_hashing(compressor),
    marks_file(openStreamFile(data_part_storage, packed_writer, packed_marks_name_, marks_path_ + marks_file_extension, 4096, query_write_settings, packed_spill_threshold_, &spool_coupled_spilled)),
    marks_hashing(*marks_file),
    marks_compressor(marks_hashing, marks_compression_codec_, marks_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    marks_compressed_hashing(marks_compressor),
    compress_marks(MarkType(marks_file_extension).compressed)
{
}

bool MergeTreeWriterStream::isPacked() const
{
    /// "Packed" iff we wired the stream through the size-adaptive path AND neither the data
    /// file nor the marks file ever spilled. The shared flag tracks both files.
    return is_size_adaptive && !spool_coupled_spilled;
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
