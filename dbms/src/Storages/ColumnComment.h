#pragma once

#include <Core/Types.h>

#include <string>
#include <unordered_map>

namespace DB
{

using ColumnComments = std::unordered_map<std::string, String>;

}
