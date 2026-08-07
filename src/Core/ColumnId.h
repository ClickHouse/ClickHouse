#pragma once

#include <base/types.h>

#include <functional>
#include <unordered_set>
#include <utility>


namespace DB
{

/// Stable on-disk storage identifier of a column. Wraps a String, but is a distinct type so a
/// logical column name (also a String) cannot be passed where an id is expected. The ctor is
/// explicit for exactly that reason -- crossing the name/id boundary must be spelled out.
/// A default-constructed (empty) id means "no id" -- the traditional name-as-file-name behavior.
class ColumnId
{
public:
    ColumnId() = default;
    explicit ColumnId(String value_) : id(std::move(value_)) {}

    const String & value() const { return id; }
    bool empty() const { return id.empty(); }

    bool operator==(const ColumnId & other) const { return id == other.id; }
    bool operator!=(const ColumnId & other) const { return id != other.id; }

private:
    String id;
};

}

namespace std
{
    template <>
    struct hash<DB::ColumnId>
    {
        size_t operator()(const DB::ColumnId & column_id) const { return hash<String>()(column_id.value()); }
    };
}

namespace DB
{

using ColumnIdSet = std::unordered_set<ColumnId>;

}
