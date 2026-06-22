#pragma once
#include <Formats/MarkInCompressedFile.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedWriteBuffer.h>

namespace DB
{

struct WriteSettings;
struct MergeTreeDataPartChecksums;
class PackedFilesWriter;

class IDataPartStorage;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

class ICompressionCodec;
using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

/// Helper class, which holds chain of buffers to write data file with marks.
/// It is used to write: one column, skip index or all columns (in compact format).
struct MergeTreeWriterStream
{
    /// When @packed_writer is non-null and @packed_data_name / @packed_marks_name are set,
    /// the data and marks files are routed through SizeAdaptiveSpoolBuffer wrappers. Each
    /// substream accumulates in memory while it stays under @packed_spill_threshold; if it
    /// grows past that, it is spilled to a standalone file on @data_part_storage. Substreams
    /// that don't spill end up bundled into @packed_writer at finalize. Pass @packed_writer
    /// as null to force standalone per-file writes regardless of size (e.g. compact-part
    /// column streams).
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
        const WriteSettings & query_write_settings,
        PackedFilesWriter * packed_writer = nullptr,
        const String & packed_data_name_ = {},
        const String & packed_marks_name_ = {},
        size_t packed_spill_threshold_ = 0);

    ~MergeTreeWriterStream()
    {
        plain_file.reset();
        marks_file.reset();
    }

    String escaped_column_name;
    std::string data_file_extension;
    std::string marks_file_extension;

    /// True when this stream is wired through SizeAdaptiveSpoolBuffer (skip indices with
    /// packing enabled). Decided at construction; needed because spool_coupled_spilled stays
    /// false in two unrelated cases ("never routed through spool" vs "routed but didn't
    /// spill") and isPacked() must distinguish them.
    bool is_size_adaptive = false;
    /// Shared between this substream's data and marks SizeAdaptiveSpoolBuffers, so the first
    /// to cross the spill threshold forces the other to spill too. Must be declared before
    /// plain_file / marks_file so it outlives them at destruction.
    bool spool_coupled_spilled = false;

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

    /// True iff this stream was wired through size-adaptive wrappers AND both data and marks
    /// stayed under the spill threshold (i.e. they are inside the packed archive). When false,
    /// at least one of them was spilled to a standalone on-disk file, and per-file checksum
    /// entries must be emitted.
    bool isPacked() const;
};

}
