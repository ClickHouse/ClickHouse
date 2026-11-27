#pragma once

#include <string>
#include <vector>
#include <unordered_set>

namespace DB
{

using Location = std::string;
using Locations = std::vector<Location>;
using LocationSet = std::unordered_set<Location>;

}
