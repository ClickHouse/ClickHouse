#pragma once

#include <Core/Streaming/CursorData.h>

#include <Processors/Chunk.h>

namespace DB
{

struct CursorInfo : public ChunkInfo
{
    static constexpr size_t info_slot = 2;
    mutable CursorDataMap cursors;

    explicit CursorInfo(CursorDataMap cursors_);
};

}
