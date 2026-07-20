#pragma once

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>

#include <vector>


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
    explicit Squashing(SharedHeader header_, size_t min_block_size_rows_, size_t min_block_size_bytes_);
    Squashing(Squashing && other) = default;

    Chunk add(Chunk && input_chunk, bool flush_if_enough_size = false);
    static Chunk squash(Chunk && input_chunk);
    Chunk flush();

    void setHeader(const Block & header_) { header = std::make_shared<const Block>(header_); }
    const SharedHeader & getHeader() const { return header; }

private:
    struct CurrentData
    {
        std::vector<Chunk> chunks = {};
        size_t rows = 0;
        size_t bytes = 0;

        explicit operator bool () const { return !chunks.empty(); }
        size_t getRows() const { return rows; }
        size_t getBytes() const { return bytes; }
        void add(Chunk && chunk);
    };

    const size_t min_block_size_rows;
    const size_t min_block_size_bytes;
    SharedHeader header;

    CurrentData accumulated;

    static Chunk squash(std::vector<Chunk> && input_chunks, Chunk::ChunkInfoCollection && infos);

    bool isEnoughSize() const;
    bool isEnoughSize(size_t rows, size_t bytes) const;
    bool isEnoughSize(const Chunk & chunk) const;

    CurrentData extract();

    Chunk convertToChunk(CurrentData && data) const;
};

}
