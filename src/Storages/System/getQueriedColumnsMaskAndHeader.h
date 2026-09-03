#pragma once
#include <Core/Names.h>
#include <base/types.h>

namespace DB
{

class Block;

std::pair<std::vector<UInt8>, Block> getQueriedColumnsMaskAndHeader(const Block & sample_block, const Names & column_names);

}
