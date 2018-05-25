#pragma once

#include <string>
#include <unordered_map>

#include <Compression/ICompressionCodec.h>
#include <Parsers/IAST.h>

namespace DB {

using ColumnCodecs = std::unordered_map<std::string, CodecPtr>;

}