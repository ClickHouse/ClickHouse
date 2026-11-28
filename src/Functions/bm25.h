#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Storages/MergeTree/BM25Kernel.h>

namespace DB
{

/// Validates the arguments of `bm25([k1[, b]])` (constant numbers, `k1 >= 0`, `0 <= b <= 1`)
/// and returns the parameters with the defaults filled in. Shared by the function's type
/// resolution and by the query planner rewrite that replaces the function.
BM25Params parseBM25FunctionArguments(const ColumnsWithTypeAndName & arguments);

}
