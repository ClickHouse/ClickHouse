#pragma once

#include <string>
#include <unordered_map>
#include <Parsers/IAST.h>
#include <Compression/ICompressionCodec.h>

namespace DB
{
    using ColumnCodecs = std::unordered_map<std::string, CompressionCodecPtr>;
}
