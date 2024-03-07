#pragma once

#include <list>
#include <memory>
#include <vector>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include "Common/MemoryTracker.h"
#include "DataTypes/Serializations/ISerialization.h"


namespace DB
{

struct ChunksToSquash : public ChunkInfo
{
    mutable std::vector<Chunk> chunks = {};
    DataTypePtr data_type = nullptr;
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
class SquashingTransform
{
public:
    /// Conditions on rows and bytes are OR-ed. If one of them is zero, then corresponding condition is ignored.
    SquashingTransform(size_t min_block_size_rows_, size_t min_block_size_bytes_);

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

class NewSquashingTransform
{
public:
    NewSquashingTransform(size_t min_block_size_rows_, size_t min_block_size_bytes_);

    Block add(Chunk && input_chunk);

private:
    size_t min_block_size_rows;
    size_t min_block_size_bytes;

    Block accumulated_block;

    template <typename ReferenceType>
    Block addImpl(ReferenceType chunk);

    template <typename ReferenceType>
    void append(ReferenceType input_chunk, DataTypePtr data_type);

    bool isEnoughSize(const Block & block);
    bool isEnoughSize(size_t rows, size_t bytes) const;
};

class BalanceTransform
{
public:
    BalanceTransform(Block header_, size_t min_block_size_rows_, size_t min_block_size_bytes_);

    Chunk add(Block && input_block);

private:
    std::vector<Chunk> chunks_to_merge_vec = {};
    size_t min_block_size_rows;
    size_t min_block_size_bytes;

    Chunk accumulated_block;
    const Block header;

    template <typename ReferenceType>
    Chunk addImpl(ReferenceType input_block);

    bool isEnoughSize(const Chunk & chunk);
    bool isEnoughSize(const std::vector<Chunk> & chunks);
    bool isEnoughSize(size_t rows, size_t bytes) const;
    void checkAndWaitMemoryAvailability(size_t bytes);
    DataTypePtr data_type = nullptr;

    MemoryTracker * memory_tracker;

    Chunk convertToChunk(std::vector<Chunk> &chunks);
};

}
