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
    /// Conditions on rows and bytes are OR-ed. If one of them is zero, then corresponding condition is ignored.
    Squashing(size_t min_block_size_rows_, size_t min_block_size_bytes_);

    /** Add next block and possibly returns squashed block.
      * At end, you need to pass empty block. As the result for last (empty) block, you will get last Result with ready = true.
      */
    Block add(Block && block);
    Block add(const Block & block);

private:
    size_t min_block_size_rows;
    size_t min_block_size_bytes;

    Block accumulated_block;

    template <typename ReferenceType>
    Block addImpl(ReferenceType block);

    template <typename ReferenceType>
    void append(ReferenceType block);

    bool isEnoughSize(const Block & block);
    bool isEnoughSize(size_t rows, size_t bytes) const;
};

class ApplySquashing
{
public:
    explicit ApplySquashing(Block header_);

    Chunk add(Chunk && input_chunk);

private:
    Chunk accumulated_chunk;
    const Block header;

    const ChunksToSquash * getInfoFromChunk(const Chunk & chunk);

    void append(std::vector<Chunk> & input_chunks);

    bool isEnoughSize(const Block & block);
    bool isEnoughSize(size_t rows, size_t bytes) const;
};

class PlanSquashing
{
public:
    PlanSquashing(Block header_, size_t min_block_size_rows_, size_t min_block_size_bytes_);

    Chunk add(Chunk & input_chunk);
    Chunk flush();
    bool isDataLeft()
    {
        return !chunks_to_merge_vec.empty();
    }

private:
    struct CurrentSize
    {
        size_t rows = 0;
        size_t bytes = 0;
    };

    std::vector<Chunk> chunks_to_merge_vec = {};
    size_t min_block_size_rows;
    size_t min_block_size_bytes;

    const Block header;
    CurrentSize accumulated_size;

    void expandCurrentSize(size_t rows, size_t bytes);
    void changeCurrentSize(size_t rows, size_t bytes);
    bool isEnoughSize(size_t rows, size_t bytes) const;

    Chunk convertToChunk(std::vector<Chunk> && chunks);
};

}
