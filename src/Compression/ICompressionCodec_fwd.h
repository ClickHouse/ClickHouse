#pragma once

#include <memory>

namespace DB
{

class ICompressionCodec;
using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

}
