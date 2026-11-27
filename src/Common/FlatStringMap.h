#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>

namespace DB
{

/** An ordered key -> value sequence packed into a single buffer: copying it costs two allocations
  * regardless of the number of entries. Used by system log elements, which are deep-copied several times
  * per query and may hold hundreds of entries (a query with an old `compatibility` value changes ~600
  * settings, and a std::map of those cost ~11% of the server CPU in copies alone).
  */
struct FlatStringMap
{
    /// All keys and values concatenated, and the end offset of each of them inside `data`.
    String data;
    std::vector<UInt32> ends;

    void add(std::string_view key, std::string_view value)
    {
        data.append(key);
        ends.push_back(static_cast<UInt32>(data.size()));
        data.append(value);
        ends.push_back(static_cast<UInt32>(data.size()));
    }

    size_t size() const { return ends.size() / 2; }
    bool empty() const { return ends.empty(); }

    void reserve(size_t entries, size_t bytes)
    {
        ends.reserve(2 * entries);
        data.reserve(bytes);
    }

    /// f(std::string_view key, std::string_view value)
    template <typename F>
    void forEach(F && f) const
    {
        UInt32 begin = 0;
        for (size_t i = 0; i + 1 < ends.size(); i += 2)
        {
            f(std::string_view(data.data() + begin, ends[i] - begin),
              std::string_view(data.data() + ends[i], ends[i + 1] - ends[i]));
            begin = ends[i + 1];
        }
    }
};

}
