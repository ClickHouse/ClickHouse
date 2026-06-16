#pragma once

#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitors.h>
#include <Core/Field.h>
#include <base/types.h>

#include <cstdint>
#include <vector>

namespace DB
{

/// Captures the scoring function name and literal parameters
/// parsed from the table function's argument AST.
struct ScoredSearchDescriptor
{
    /// Groups scoring functions for storage-side dispatch.
    /// Names are globally unique across families; the storage
    /// layer validates `family` after `get()` returns.
    enum class Family : uint8_t
    {
        Text,
        Hybrid,
    };

    String name;                 /// "bm25" | ...
    std::vector<Field> params;   /// validated literal params
    Family family;

    /// For query-condition cache keys. Concatenates `name` with the
    /// parenthesised list of params if any.
    String toString() const
    {
        if (params.empty())
            return name;

        String result = name;
        result += '(';
        for (size_t i = 0; i < params.size(); ++i)
        {
            if (i != 0)
                result += ", ";
            result += applyVisitor(FieldVisitorToString(), params[i]);
        }
        result += ')';
        return result;
    }
};

}
