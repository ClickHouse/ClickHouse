#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.h>

#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>

#include <optional>
#include <vector>

namespace DB::GPU
{

/// A predicate compiled for the device, and the columns it reads in the order the program's
/// `PushColumn` instructions number them.
struct CompiledGPUFilter
{
    GPUFilterProgram program;
    std::vector<NameAndTypePair> columns;
};

/** Compiles the predicate `filter_column_name` of `dag` into a program for the device, or answers
  * nothing and says in `refusal` why it cannot.
  *
  * What compiles: the comparisons, `and`, `or` and `not` over non-nullable integer and float
  * columns and constants, and a column standing for itself as ClickHouse reads one in a `WHERE`.
  * An integer compares with a float only when the integer is a constant a double holds exactly,
  * and then as that double.
  */
std::optional<CompiledGPUFilter> compileGPUFilter(const ActionsDAG & dag, const String & filter_column_name, String & refusal);

}

#endif
