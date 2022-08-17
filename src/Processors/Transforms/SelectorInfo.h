#pragma once
#include <Processors/Chunk.h>
#include <Common/PODArray.h>

namespace DB
{

/// ChunkInfo with IColumn::Selector. It is added by AddingSelectorTransform.
struct SelectorInfo : public ChunkInfo
{
    IColumn::Selector selector;
};

}
