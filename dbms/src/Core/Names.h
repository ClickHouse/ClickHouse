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

/// Cache position for columns in order to avoid searching in hash table.
struct NameWithPosition : public std::string
{
    static constexpr size_t INDEX_NOT_FOUND = std::numeric_limits<size_t>::max();
    size_t position = INDEX_NOT_FOUND;

    using std::string::operator=;
    using std::string::string;

    NameWithPosition() = default;
    NameWithPosition(const std::string & str) : std::string(str) {}
    NameWithPosition(std::string && str): std::string(std::move(str)) {}
};

using NamesWithPosition = std::vector<NameWithPosition>;
using NamesWithPositionSet = std::unordered_set<NameWithPosition>;

}

namespace std
{

template <>
struct hash<DB::NameWithPosition>
{
    std::size_t operator()(const DB::NameWithPosition & key) const
    {
        return hash<string>()(key);
    }
};

}
