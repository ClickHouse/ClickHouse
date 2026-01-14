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
public:
    explicit Squashing(SharedHeader header_, size_t min_block_size_rows_, size_t min_block_size_bytes_,
                        size_t max_block_size_rows_ = 0, size_t max_block_size_bytes_ = 0, bool squash_with_strict_limits_ = false);
    Squashing(Squashing && other) = default;

    void add(Chunk && input_chunk);
    bool canGenerate();
    Chunk generate(bool flush_if_enough_size = false);
    static Chunk squash(Chunk && input_chunk, SharedHeader header);

    Chunk flush();

    void setHeader(const Block & header_) { header = std::make_shared<const Block>(header_); }
    const SharedHeader & getHeader() const { return header; }

private:

    class AccumulatedChunks
    {
    public:
        explicit operator bool () const { return !chunks.empty(); }
        bool empty() const { return chunks.empty(); }
        size_t getRows() const { return rows; }
        size_t getBytes() const { return bytes; }
        void append(Chunk && chunk);
        void append(Chunk && chunk, size_t rows_to_add, size_t bytes_to_add);

        Chunks extract();

        std::pair<size_t,size_t> findLengthBytesPending(const Chunk & chunk, size_t max_rows, size_t max_bytes, size_t offset_pending) const;

    private:

        Chunks chunks;
        size_t rows = 0;
        size_t bytes = 0;
    };

    class PendingQueue
    {
    public:

        struct ConsumeResult
        {
            Chunk chunk;
            size_t rows;
            size_t bytes;
        };

        size_t getRows() const { return total_rows; }
        size_t getBytes() const { return total_bytes; }
        const Chunk & peekFront() const { return chunks.front(); }
        Chunk pullFront();
        void dropFront() { chunks.pop_front(); }
        void push(Chunk && chunk);
        size_t getOffset() const { return offset_first; }
        bool empty() const { return chunks.empty(); }
        std::pair<size_t, size_t> calculateConsumable(size_t max_rows, size_t max_bytes) const;
        ConsumeResult consumeUpTo(size_t max_rows, size_t max_bytes);

    private:

        std::deque<Chunk> chunks;
        size_t total_rows = 0;
        size_t total_bytes = 0;
        size_t offset_first = 0;
    };

    AccumulatedChunks accumulated;
    PendingQueue  pending;
    SharedHeader header;

    const size_t min_block_size_rows;
    const size_t min_block_size_bytes;
    const size_t max_block_size_rows;
    const size_t max_block_size_bytes;
    const bool squash_with_strict_limits;

    Chunk generateUsingStrictBounds();
    Chunk generateUsingOneMinBound(bool flush_if_enough_size);

    bool oneMinReached() const;
    bool oneMinReached(size_t rows, size_t bytes) const;
    bool oneMinReached(const Chunk & chunk) const;
    bool allMinReached() const;
    bool allMinReached(size_t rows, size_t bytes) const;
    bool oneMaxReached() const;
    bool oneMaxReached(size_t rows, size_t bytes) const;

    static Chunk squash(std::vector<Chunk> && input_chunks, Chunk::ChunkInfoCollection && infos, SharedHeader header);
    static Chunk squash(std::vector<Chunk> && input_chunks);

    AccumulatedChunks extract();
    Chunk convertToChunk();

    // LazyMaterializingTransform calls private method squash(std::vector<Chunk> && input_chunks)
    // that method does not handle ChunkInfos,
    // therefore it is private method to force using Squashing instance with proper arguments
    friend class LazyMaterializingTransform;
};

}
