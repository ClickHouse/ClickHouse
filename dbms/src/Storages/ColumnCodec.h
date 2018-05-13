#pragma once

#include <string>
#include <unordered_map>

#include <Parsers/IAST.h>

namespace DB {

using ColumnCodecs = std::unordered_map<std::string, std::string>;

}