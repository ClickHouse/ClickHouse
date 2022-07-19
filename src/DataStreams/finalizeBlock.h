#pragma once

#include <Core/Block.h>

namespace DB
{
    /// Converts aggregate function columns with non-finalized states to final values
    void finalizeBlock(Block & block);
}
