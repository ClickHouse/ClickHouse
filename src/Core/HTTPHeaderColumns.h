#pragma once

#include <base/types.h>
#include <unordered_map>
#include <vector>

namespace DB
{

/// One HTTP-header-to-INSERT-column mapping: the target column and the resolved
/// header value.
struct HTTPHeaderColumn
{
    String column_name;
    String value;
};

/// Ordered set of HTTP-header-to-column mappings, kept in declaration order (URL
/// params for the dynamic handler, config order for the predefined handler). A side
/// index gives O(1) first-wins insert and O(1) lookup while iteration still yields
/// declaration order, so the expanded query column list, the async batch key, and
/// the per-entry values all share one canonical order without sorting.
class HTTPHeaderColumns
{
public:
    /// The first declaration wins: a repeated column is ignored and order is preserved.
    void add(const String & column_name, const String & value)
    {
        if (!index.emplace(column_name, entries.size()).second)
            return;
        entries.push_back({column_name, value});
    }

    bool contains(const String & column_name) const { return index.contains(column_name); }

    /// Value for a column, or nullptr if the column is not mapped. The pointer is
    /// valid only until the next add() (which may reallocate); in practice all adds
    /// happen at request parse time and all lookups later, so it is never held across one.
    const String * find(const String & column_name) const
    {
        auto it = index.find(column_name);
        return it == index.end() ? nullptr : &entries[it->second].value;
    }

    bool empty() const { return entries.empty(); }
    size_t size() const { return entries.size(); }

    void reserve(size_t n)
    {
        entries.reserve(n);
        index.reserve(n);
    }

    auto begin() const { return entries.begin(); }
    auto end() const { return entries.end(); }

private:
    std::vector<HTTPHeaderColumn> entries;
    std::unordered_map<String, size_t> index;
};

}
