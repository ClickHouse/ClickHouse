#pragma once

#include <vector>
#include <string>
#include <set>
#include <unordered_set>
#include <unordered_map>

#include <Common/SetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

using Names = std::vector<std::string>;
using NameSet = std::unordered_set<std::string>;
using NameMultiSet = MultiSetWithMemoryTracking<std::string>;
using NameOrderedSet = std::set<std::string>;
using NameToNameMap = std::unordered_map<std::string, std::string>;
using NameToNameSetMap = UnorderedMapWithMemoryTracking<std::string, NameSet>;
using NameToNameVector = VectorWithMemoryTracking<std::pair<std::string, std::string>>;
using NameToIndexMap = UnorderedMapWithMemoryTracking<std::string, size_t>;

using NameWithAlias = std::pair<std::string, std::string>;
using NamesWithAliases = VectorWithMemoryTracking<NameWithAlias>;

struct NamesHash
{
    size_t operator()(const Names & column_names) const;
};
}
