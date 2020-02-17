#pragma once

#include <Common/SipHash.h>
#include <Core/Types.h>
#include <unordered_set>

namespace DB
{

struct ColumnDependency
{
    enum Kind
    {
        SKIP_INDEX,
        TTL_EXPRESSION,
        TTL_TARGET
    };

    ColumnDependency(const String & column_name_, Kind kind_)
        : column_name(column_name_), kind(kind_) {}

    String column_name;
    Kind kind;

    bool isReadOnly() const
    {
        return kind == SKIP_INDEX || kind == TTL_EXPRESSION;
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
