#pragma once

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>

#include <vector>
#include <queue>

namespace DB
{

class ChunksToSquash : public ChunkInfoCloneable<ChunksToSquash>
{
public:
    ChunksToSquash() = default;
    ChunksToSquash(const ChunksToSquash & other)
    {
        chunks.reserve(other.chunks.size());
        for (const auto & chunk: other.chunks)
           chunks.push_back(chunk.clone());
    }

    std::vector<Chunk> chunks = {};
    size_t offset_first;
    size_t length_first;
    size_t offset_last;
    size_t length_last;
};

/** Merging consecutive passed blocks to specified minimum size.
  *
  * (But if one of input blocks has already at least specified size,
  *  then don't merge it with neighbours, even if neighbours are small.)
  *
  * Used to prepare blocks to adequate size for INSERT queries,
  *  because such storages as Memory, StripeLog, Log, TinyLog...
  *  store or compress data in blocks exactly as passed to it,
  *  and blocks of small size are not efficient.
  *
  * Order of data is kept.
  */

class Squashing
{
    using PendingChunks = std::deque<Chunk>;
public:
    explicit Squashing(SharedHeader header_, size_t min_block_size_rows_, size_t min_block_size_bytes_,
                        size_t max_block_size_rows_ = 0, size_t max_block_size_bytes_ = 0, bool squash_with_strict_limits_ = false);
    Squashing(Squashing && other) = default;

    void add(Chunk && input_chunk);
    Chunk generate(bool flush_if_enough_size = false);
    Chunk addAndGenerate(Chunk && input_chunk, bool flush_if_enough_size = false);
    static Chunk squash(Chunk && input_chunk, SharedHeader header);

    Chunk flush();

    void setHeader(const Block & header_) { header = std::make_shared<const Block>(header_); }
    const SharedHeader & getHeader() const { return header; }

private:

    struct CurrentData
    {
        /// To handle all min condition threshold logic
        std::vector<Chunk> chunks_ready = {}; /// Ready to be squashed
        
        size_t rows = 0;
        size_t bytes = 0;

        size_t offset_first_chunk = 0;
        size_t length_first_chunk = 0;
        size_t offset_last_chunk = 0;
        size_t length_last_chunk = 0;  /// 0 means single-chunk case (use first chunk's offset/length)

        explicit operator bool () const { return !chunks_ready.empty(); }
        size_t getRows() const { return rows; }
        size_t getBytes() const { return bytes; }
        void appendChunk(Chunk && chunk);

        size_t findLengthPending(const Chunk & chunk, size_t max_rows, size_t max_bytes, size_t offset_pending) const;
        void appendChunkSliced(Chunk && chunk, size_t len, size_t offset_pending);
    };

    CurrentData accumulated;
    PendingChunks  chunks_pending;
    SharedHeader header;

    const size_t min_block_size_rows;
    const size_t min_block_size_bytes;
    const size_t max_block_size_rows;
    const size_t max_block_size_bytes;
    size_t offset_first_chunk_pending = 0;
    const bool squash_with_strict_limits;

    static Chunk squash(std::vector<Chunk> && input_chunks, Chunk::ChunkInfoCollection && infos, SharedHeader header);
    static Chunk squash(std::vector<Chunk> && input_chunks);
    // LazyMaterializingTransform calls private method squash(std::vector<Chunk> && input_chunks)
    // that method does not handle ChunkInfos,
    // therefore it is private method to force using Squashing instance with proper arguments
    friend class LazyMaterializingTransform;

    bool oneMinReached() const;
    bool oneMinReached(size_t rows, size_t bytes) const;
    bool oneMinReached(const Chunk & chunk) const;
    bool allMinReached() const;
    bool allMinReached(size_t rows, size_t bytes) const;

    CurrentData extract();
    Chunk takeFrontPending();
    Chunk convertToChunk(CurrentData && data) const;
    Chunk generateUsingStrictBounds();
    Chunk generateUsingOneMinBound(bool flush_if_enough_size);
};

}
