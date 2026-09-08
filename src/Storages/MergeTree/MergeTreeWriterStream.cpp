#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <IO/PackedFilesWriter.h>
#include <IO/WriteSettings.h>
#include <Common/PODArray.h>
#include <Common/Exception.h>

#include <cstring>
#include <functional>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

namespace
{

/** Write-buffer wrapper used by skip-index streams to decide *at write time* whether the
  * substream's bytes should land inside skp_idx.packed (the bundled archive) or as a
  * standalone per-file substream on the part's data storage.
  *
  * Behaviour:
  *   - Writes accumulate into an in-memory buffer (PaddedPODArray).
  *   - As long as the accumulator stays under @spill_threshold, the substream is a packed
  *     candidate: at finalize the bytes are streamed into a fresh @packed_writer entry.
  *   - The first nextImpl() that crosses @spill_threshold spills: a real WriteBuffer is
  *     opened via @open_per_file (e.g. data_part_storage->writeFile), the buffered bytes
  *     are dumped into it, and every subsequent flush forwards directly. From then on the
  *     substream is a standalone per-file artifact and will never enter the archive.
  *
  * The wrapper sits below the per-substream hashing/compression chain in MergeTreeWriterStream.
  * Because all bytes flow through the wrapper's working_buffer on their way down, the hashing
  * buffers above see exactly the bytes that end up on disk (whether packed or per-file), so
  * the per-substream checksums are correct in either branch.
  *
  * Whether a substream ended packed or spilled is reported back through @coupled_spilled
  * (see constructor); MergeTreeWriterStream::isPacked reads that flag to pick the checksum path.
  */
class SizeAdaptiveSpoolBuffer : public WriteBufferFromFileBase
{
public:
    using OpenPerFileFunc = std::function<std::unique_ptr<WriteBufferFromFileBase>()>;

    /// @coupled_spilled: shared spill flag (lifetime owned by the caller). A substream's data
    /// file and marks file must travel together: either both are inside the archive or both are
    /// standalone per-file artifacts (the reader can't satisfy the half-and-half shape). Both
    /// buffers share one flag so the first to cross the threshold forces the other to spill on
    /// its next flush (or at finalize).
    SizeAdaptiveSpoolBuffer(
        size_t spill_threshold_,
        size_t buf_size_,
        OpenPerFileFunc open_per_file_,
        PackedFilesWriter * packed_writer_,
        const String & packed_virtual_name_,
        const WriteSettings & packed_write_settings_,
        const String & display_file_name_,
        bool & coupled_spilled_)
        : WriteBufferFromFileBase(buf_size_, nullptr, 0)
        , spill_threshold(spill_threshold_)
        , open_per_file(std::move(open_per_file_))
        , packed_writer(packed_writer_)
        , packed_virtual_name(packed_virtual_name_)
        , packed_write_settings(packed_write_settings_)
        , display_file_name(display_file_name_)
        , coupled_spilled(coupled_spilled_)
    {
    }

    ~SizeAdaptiveSpoolBuffer() override = default;

    void nextImpl() override;
    void finalizeImpl() override;
    void cancelImpl() noexcept override;
    void sync() override;
    std::string getFileName() const override { return display_file_name; }

private:
    void promoteToPerFile();
    void commitToPackedIfNeeded();

    const size_t spill_threshold;
    const OpenPerFileFunc open_per_file;
    PackedFilesWriter * const packed_writer;
    const String packed_virtual_name;
    const WriteSettings packed_write_settings;
    const String display_file_name;

    /// In-memory accumulator for the "still packable" phase.
    PaddedPODArray<UInt8> accumulator;

    /// Set once the substream has been promoted to a standalone file.
    bool spilled = false;
    std::unique_ptr<WriteBufferFromFileBase> per_file_writer;

    /// True after the accumulator has been handed to packed_writer at finalize time.
    bool committed_to_packed = false;

    /// Propagates a caller's sync() request: if not spilled, sync the packed archive when it
    /// gets written; if spilled, sync the per-file writer directly.
    bool sync_requested = false;

    /// See constructor docs. Non-owning; outlived by the owning MergeTreeWriterStream.
    bool & coupled_spilled;
};

void SizeAdaptiveSpoolBuffer::nextImpl()
{
    const size_t bytes_in_buffer = offset();
    if (bytes_in_buffer == 0)
        return;

    /// Peer (e.g. our marks file vs our data file) crossed the threshold first; we must spill
    /// too so the substream stays consistent (both packed or both per-file).
    if (!spilled && coupled_spilled)
        promoteToPerFile();

    if (spilled)
    {
        per_file_writer->write(working_buffer.begin(), bytes_in_buffer);
        return;
    }

    /// Append to the in-memory accumulator. If we exceed the threshold, promote to per-file
    /// and dump everything we have so subsequent writes flow straight through.
    const size_t old_size = accumulator.size();
    accumulator.resize(old_size + bytes_in_buffer);
    std::memcpy(accumulator.data() + old_size, working_buffer.begin(), bytes_in_buffer);

    if (accumulator.size() > spill_threshold)
        promoteToPerFile();
}

void SizeAdaptiveSpoolBuffer::promoteToPerFile()
{
    per_file_writer = open_per_file();
    per_file_writer->write(reinterpret_cast<const char *>(accumulator.data()), accumulator.size());
    /// Free the accumulator memory: from now on we'll forward bytes straight to per_file_writer.
    PaddedPODArray<UInt8>{}.swap(accumulator);
    spilled = true;
    /// Tell our peer file (data file vs marks file of the same substream) to spill as well, so
    /// the on-disk substream stays in a single layout the reader can pick up.
    coupled_spilled = true;
}

void SizeAdaptiveSpoolBuffer::commitToPackedIfNeeded()
{
    if (spilled || committed_to_packed)
        return;

    if (!packed_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SizeAdaptiveSpoolBuffer for '{}' has no packed writer at commit time", display_file_name);

    auto packed_buf = packed_writer->writeFile(packed_virtual_name, packed_write_settings);
    if (!accumulator.empty())
        packed_buf->write(reinterpret_cast<const char *>(accumulator.data()), accumulator.size());
    if (sync_requested)
        packed_buf->sync();
    packed_buf->finalize();

    PaddedPODArray<UInt8>{}.swap(accumulator);
    committed_to_packed = true;
}

void SizeAdaptiveSpoolBuffer::finalizeImpl()
{
    next();

    /// Last chance to honour a peer's late spill: if the data file spilled but the marks file
    /// had no more flushes between the spill and finalize, we still need to move marks to
    /// per-file before committing.
    if (!spilled && coupled_spilled)
        promoteToPerFile();

    if (spilled)
    {
        if (sync_requested)
            per_file_writer->sync();
        per_file_writer->finalize();
    }
    else
    {
        commitToPackedIfNeeded();
    }
}

void SizeAdaptiveSpoolBuffer::cancelImpl() noexcept
{
    if (per_file_writer)
        per_file_writer->cancel();
}

void SizeAdaptiveSpoolBuffer::sync()
{
    /// We can't sync until we know whether we're per-file or packed. Record the request and
    /// honour it at finalize time, when we know which downstream owns the bytes.
    sync_requested = true;
    if (spilled && per_file_writer)
        per_file_writer->sync();
}

}

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
    bool & coupled_spilled)
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
            coupled_spilled);
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
    const SizeAdaptivePacking & packing) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    is_size_adaptive(packing.writer != nullptr && (!packing.data_name.empty() || !packing.marks_name.empty())),
    plain_file(openStreamFile(data_part_storage, packing.writer, packing.data_name, data_path_ + data_file_extension, max_compress_block_size_, query_write_settings, packing.spill_threshold, spool_coupled_spilled)),
    plain_hashing(*plain_file),
    compressor(plain_hashing, compression_codec_, max_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    compressed_hashing(compressor),
    marks_file(openStreamFile(data_part_storage, packing.writer, packing.marks_name, marks_path_ + marks_file_extension, 4096, query_write_settings, packing.spill_threshold, spool_coupled_spilled)),
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
