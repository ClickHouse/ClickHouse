#pragma once

#include <Core/Streaming/CursorData.h>

#include <Processors/Chunk.h>

namespace DB
{

struct CursorInfo : public ChunkInfo
{
    static constexpr size_t INFO_SLOT = 2;
    mutable CursorDataMap cursors;

    explicit CursorInfo(CursorDataMap cursors_);
};

}
