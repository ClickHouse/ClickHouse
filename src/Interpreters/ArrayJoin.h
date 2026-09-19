#pragma once
#include <Core/Names.h>

namespace DB
{

struct ArrayJoin
{
    Names columns;
    bool is_left = false;

    /// Maps analyzer-generated ARRAY JOIN column identifiers to the corresponding input
    /// expression result names. It is only needed by query-plan optimizations that insert
    /// expressions below `ArrayJoinStep`; execution itself uses `columns`.
    NameToNameMap source_columns;
};

}
