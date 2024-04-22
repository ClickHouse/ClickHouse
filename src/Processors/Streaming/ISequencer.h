#pragma once

#include <Processors/Chunk.h>

namespace DB
{

struct NeedData
{
    // empty
};

struct RecalcState
{
    // empty
};

struct Emit
{
    Chunk chunk;
};

using ExtractResult = std::variant<NeedData, RecalcState, Emit>;

/// TODO
class ISequencer
{
public:
    ISequencer() = default;
    virtual ~ISequencer() = default;

    /// TODO
    virtual void addChunk(Chunk new_chunk) = 0;

    /// TODO
    virtual ExtractResult tryExtractNext(size_t streams_count, size_t stream_index) = 0;

    /// TODO
    virtual void recalcState() = 0;
};

using SequencerPtr = std::shared_ptr<ISequencer>;

}
