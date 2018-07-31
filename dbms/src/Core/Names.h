#pragma once

#include <vector>
#include <string>
#include <unordered_set>
#include <unordered_map>


namespace DB
{

using Names = std::vector<std::string>;
using NameSet = std::unordered_set<std::string>;
using NameToNameMap = std::unordered_map<std::string, std::string>;
using NameToNameSetMap = std::unordered_map<std::string, NameSet>;

}
