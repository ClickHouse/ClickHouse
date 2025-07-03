#pragma once

#include <list>
#include <memory>
#include <vector>

namespace DB
{
class Block;

using BlockPtr = std::shared_ptr<Block>;
using Blocks = std::vector<Block>;
using BlocksList = std::list<Block>;
using BlocksPtr = std::shared_ptr<Blocks>;

}
