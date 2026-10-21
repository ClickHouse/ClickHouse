#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.cuh>

#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>

#include <optional>
#include <vector>

namespace DB::GPU
{

struct CompiledGPUFilter
{
    GPUFilterProgram program;
    std::vector<NameAndTypePair> columns;
};

std::optional<CompiledGPUFilter> compileGPUFilter(const ActionsDAG & dag, const String & filter_column_name, String & refusal);

/// Of output `output_name` of `dag`: a `Float64` that the device computes from `Float64` columns and numeric constants
/// by `plus`, `minus`, `multiply` and `divide`, as ClickHouse computes it, an operation at a time.
std::optional<CompiledGPUFilter> compileGPUExpression(const ActionsDAG & dag, const String & output_name, String & refusal);

}

#endif
