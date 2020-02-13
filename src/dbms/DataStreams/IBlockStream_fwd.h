#pragma once

#include <memory>
#include <vector>

namespace DB
{

class IBlockInputStream;
class IBlockOutputStream;

using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;
using BlockOutputStreams = std::vector<BlockOutputStreamPtr>;

}
