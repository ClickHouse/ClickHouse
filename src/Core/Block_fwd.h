#pragma once

#include <list>
#include <memory>
#include <vector>

#include <Common/ListWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{
class Block;

using BlockPtr = std::shared_ptr<Block>;
using ConstBlockPtr =  std::shared_ptr<const Block>;
using Blocks = VectorWithMemoryTracking<Block>;
using BlocksList = ListWithMemoryTracking<Block>;
using BlocksPtr = std::shared_ptr<Blocks>;

using SharedHeader =  std::shared_ptr<const Block>;
using SharedHeaders = VectorWithMemoryTracking<SharedHeader>;

}
