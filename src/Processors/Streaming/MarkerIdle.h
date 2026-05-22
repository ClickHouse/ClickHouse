#pragma once

#include <Processors/Chunk.h>

namespace DB
{

struct IdleMarker : public ChunkInfoCloneable<IdleMarker>
{
};

}
