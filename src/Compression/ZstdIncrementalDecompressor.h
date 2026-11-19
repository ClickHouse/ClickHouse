#pragma once

#include <cstddef>
#include <memory>


namespace DB
{

/// Decompresses one ZSTD frame block by block into a contiguous destination, so that a reader that needs only the beginning
/// of a large compressed block does not decompress the rest of it. A frame is a sequence of blocks of at most 128 KiB of
/// decompressed data that may refer to the data of the previous blocks, so all of them are decompressed into the same
/// destination one after another. While a frame is in progress, it holds a decompression context taken from the pool
/// that `CompressionCodecZSTD` uses.
class ZstdIncrementalDecompressor
{
public:
    ZstdIncrementalDecompressor();
    ~ZstdIncrementalDecompressor();

    /// The maximum size of the decompressed data of one block of a frame.
    static constexpr size_t max_block_size = 128 * 1024;

    /// Starts decompressing the frame of `source_size` bytes at `source` into `dest`, which must have room for its whole
    /// decompressed data of `dest_size` bytes. Nothing is decompressed yet. Returns false, leaving no frame in progress, if
    /// the data should be decompressed at once instead: it is not a single frame this path handles, too many frames are
    /// already in progress, or the previous frames of this decompressor were read to the end. `source` must stay valid while
    /// the frame is in progress (see `relocateSource`).
    bool start(const char * source, size_t source_size, char * dest, size_t dest_size);

    /// Decompresses the next blocks until at least `min_bytes` bytes (and at least one more block) of the destination are
    /// decompressed or the frame ends, when it is no longer in progress. Returns the number of decompressed bytes.
    size_t decompressAtLeast(size_t min_bytes);

    bool inProgress() const;
    size_t decompressedBytes() const;

    /// The frame has been copied to `new_source`.
    void relocateSource(const char * new_source);

    /// Abandons the frame in progress, if any, and returns its context to the pool.
    void reset();

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
