#pragma once

#include <base/types.h>

#include <functional>
#include <unordered_set>
#include <utility>


namespace DB
{

/// Stable on-disk storage identifier of a column. A distinct type, and an explicit ctor, so that a
/// logical column name (also a String) cannot cross into id space without being spelled out.
/// Empty means "no id" -- the traditional name-as-file-name behavior.
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
