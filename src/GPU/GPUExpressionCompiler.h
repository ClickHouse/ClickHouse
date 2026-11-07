#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUExpression.cuh>

#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB::GPU
{

struct GPUKernel;

struct CompiledGPUExpression
{
    GPUExpression expression;
    std::vector<NameAndTypePair> columns;
    DataTypePtr result_type;
    std::shared_ptr<const GPUKernel> kernel;
};

std::optional<CompiledGPUExpression> compileGPUFilter(const ActionsDAG & dag, const String & filter_column_name, String & refusal);

std::optional<CompiledGPUExpression> compileGPUExpression(const ActionsDAG & dag, const String & output_name, String & refusal);

}

#endif
