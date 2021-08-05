#pragma once

#include <DataStreams/IBlockStream_fwd.h>

namespace DB
{
void formatBlock(BlockOutputStreamPtr & out, const Block & block);

}
