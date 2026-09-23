#pragma once

#include <Compression/CompressedReadBufferBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <optional>

namespace DB
{

/// One column's compressed blocks of a wide `MergeTree` part, read without being decompressed.
///
/// What the GPU aggregation sends when the device is the one that decompresses: a `UInt64` column
/// that takes 1.49 GiB of link at full width occupies 195 MiB under `ZSTD(3)`.
///
/// Wide parts only. A compact part keeps every column in one `data.bin`, so a column's blocks are
/// not a run of the file and cannot be walked this way.
class MergeTreeCompressedBlockReader : private CompressedReadBufferBase
{
public:
    struct Block
    {
        /// Valid until the next `next()`: it can point into the file buffer rather than into a
        /// buffer of this reader's own.
        const char * payload;

        /// The payload alone - no checksum, no header.
        size_t compressed_bytes;

        size_t decompressed_bytes;
    };

    /// Throws when `part` is not wide, when `column` has no single stream in it, or when its file
    /// is missing.
    MergeTreeCompressedBlockReader(
        const IMergeTreeDataPart & part, const NameAndTypePair & column, const ReadSettings & read_settings);

    /// Opens the one `.bin` file of `column` in a wide part, with the same checks.
    static std::unique_ptr<ReadBufferFromFileBase>
    openColumnFile(const IMergeTreeDataPart & part, const NameAndTypePair & column, const ReadSettings & read_settings);

    /// The next block, or nothing once the column is read out.
    std::optional<Block> next();

    /// The codec the blocks read so far were written with - `CompressionMethodByte`. Only known
    /// once `next` has returned a block, since it is the block header that states it.
    std::optional<UInt8> methodByte() const { return method_byte; }

private:
    std::unique_ptr<ReadBufferFromFileBase> file;
    std::optional<UInt8> method_byte;
};

}
