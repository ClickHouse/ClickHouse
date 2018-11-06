#pragma once

#include <string>
#include <unordered_map>

#include <Parsers/IAST.h>

namespace DB
{

using ColumnComments = std::unordered_map<std::string, String>;

}
