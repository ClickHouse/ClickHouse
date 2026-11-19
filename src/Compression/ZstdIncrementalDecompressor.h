#pragma once

#include <cstddef>
#include <memory>


namespace DB
{

/// Decompresses one ZSTD frame block by block into a contiguous destination, so that a reader that needs only the beginning
/// of a large compressed block does not decompress the rest. A frame in progress holds a context from the pool of `CompressionCodecZSTD`.
class ZstdIncrementalDecompressor
{
public:
    ZstdIncrementalDecompressor();
    ~ZstdIncrementalDecompressor();

    /// The maximum decompressed size of one block of a frame.
    static constexpr size_t max_block_size = 128 * 1024;

    /// Nothing is decompressed yet. Returns false, with no frame in progress, if the data should be decompressed at once instead.
    /// `source` must stay valid while the frame is in progress.
    bool start(const char * source, size_t source_size, char * dest, size_t dest_size);

    /// Until at least `min_bytes` bytes and one more block, or the whole frame, are decompressed. Returns the total decompressed.
    size_t decompressAtLeast(size_t min_bytes);

    bool inProgress() const;
    size_t decompressedBytes() const;

    void relocateSource(const char * new_source);

    void reset();

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
