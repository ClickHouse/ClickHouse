#pragma once

#include <DataStreams/IBlockStream_fwd.h>

namespace DB
{

class Block;

void formatBlock(BlockOutputStreamPtr & out, const Block & block);

}
