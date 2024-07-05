#pragma once

#include <vector>
#include <Core/Block.h>
#include <Processors/Chunk.h>


namespace DB
{

struct ChunksToSquash : public ChunkInfo
{
    mutable std::vector<Chunk> chunks = {};
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
    explicit Squashing(Block header_, size_t min_block_size_rows_, size_t min_block_size_bytes_);
    Squashing(Squashing && other) = default;

    Chunk add(Chunk && input_chunk);
    static Chunk squash(Chunk && input_chunk);
    Chunk flush();

    bool isDataLeft()
    {
        return !chunks_to_merge_vec.empty();
    }

    Block header;
private:
    struct CurrentSize
    {
        size_t rows = 0;
        size_t bytes = 0;
    };

    std::vector<Chunk> chunks_to_merge_vec = {};
    size_t min_block_size_rows;
    size_t min_block_size_bytes;

    CurrentSize accumulated_size;

    static const ChunksToSquash * getInfoFromChunk(const Chunk & chunk);

    static Chunk squash(std::vector<Chunk> & input_chunks);

    void expandCurrentSize(size_t rows, size_t bytes);
    void changeCurrentSize(size_t rows, size_t bytes);
    bool isEnoughSize(size_t rows, size_t bytes) const;

    Chunk convertToChunk(std::vector<Chunk> && chunks) const;
};

}
