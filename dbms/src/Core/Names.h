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
struct NameWithPosition
{
    static constexpr size_t INDEX_NOT_FOUND = std::numeric_limits<size_t>::max();

    std::string name;
    size_t position = INDEX_NOT_FOUND;

    NameWithPosition() = default;
    NameWithPosition(const NameWithPosition &) = default;
    NameWithPosition(NameWithPosition &&) = default;
    NameWithPosition(const std::string & str) : name(str) {}
    NameWithPosition(std::string && str): name(std::move(str)) {}

    NameWithPosition & operator=(const NameWithPosition &) = default;
    NameWithPosition & operator=(NameWithPosition &&) = default;
    NameWithPosition & operator=(const std::string & str) { name = str; return *this; }
    NameWithPosition & operator=(std::string && str) { name = std::move(str); return *this; }

    bool operator==(const NameWithPosition & other) const { return name == other.name; }

    operator const std::string &() const { return name; }
};

using NamesWithPosition = std::vector<NameWithPosition>;

inline std::string operator+(const std::string & lhs, const NameWithPosition & rhs) { return lhs + rhs.name; }
inline std::string operator+(const NameWithPosition & lhs, const std::string & rhs) { return lhs.name + rhs; }

}
