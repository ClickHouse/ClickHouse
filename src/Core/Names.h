#pragma once

#include <vector>
#include <string>
#include <set>
#include <unordered_set>
#include <unordered_map>


namespace DB
{

using Names = std::vector<std::string>;
using NameSet = std::unordered_set<std::string>;
using NameOrderedSet = std::set<std::string>;
using NameToNameMap = std::unordered_map<std::string, std::string>;
using NameToNameSetMap = std::unordered_map<std::string, NameSet>;
using NameToNameVector = std::vector<std::pair<std::string, std::string>>;
using NameToIndexMap = std::unordered_map<std::string, size_t>;

using NameWithAlias = std::pair<std::string, std::string>;
using NamesWithAliases = std::vector<NameWithAlias>;

}
