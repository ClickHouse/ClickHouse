#pragma once

#include <Common/SipHash.h>
#include <base/types.h>
#include <unordered_set>

namespace DB
{

/// Represents dependency from other column.
/// Used to determine, which columns we have to read, if we want to update some other column.
/// Necessary, because table can have some dependencies, which requires several columns for calculation.
struct ColumnDependency
{
    enum Kind : UInt8
    {
        /// Exists any skip index, that requires @column_name
        SKIP_INDEX,

        /// Exists any projection, that requires @column_name
        PROJECTION,

        /// Exists any TTL expression, that requires @column_name
        TTL_EXPRESSION,

        /// TTL is set for @column_name.
        TTL_TARGET
    };

    ColumnDependency(const String & column_name_, Kind kind_)
        : column_name(column_name_), kind(kind_) {}

    String column_name;
    Kind kind;

    bool isReadOnly() const
    {
        return kind == SKIP_INDEX || kind == PROJECTION || kind == TTL_EXPRESSION;
    }

    bool operator==(const ColumnDependency & other) const
    {
        return kind == other.kind && column_name == other.column_name;
    }

    struct Hash
    {
        UInt64 operator()(const ColumnDependency & dependency) const
        {
            SipHash hash;
            hash.update(dependency.column_name);
            hash.update(dependency.kind);
            return hash.get64();
        }
    };
};

using ColumnDependencies = std::unordered_set<ColumnDependency, ColumnDependency::Hash>;

}
