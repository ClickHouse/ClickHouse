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

/// A kernel loaded on the device, see `GPUExpressionCompiler.cpp`.
struct GPUKernel;

/// An expression of `ActionsDAG` compiled into a kernel of the device, or nothing with `refusal` saying why the device
/// does not evaluate it. A row is computed by the code that the functions of ClickHouse compile into for the CPU, see
/// `IFunction::compileImpl` and `CompileDAG`, which LLVM emits as PTX instead: the device computes what ClickHouse
/// computes, and an expression of a function that does not compile, or of a column that is `Nullable`, is refused.
struct CompiledGPUExpression
{
    GPUExpression expression;
    /// What the kernel takes as its column `i`, the inputs of the expression.
    std::vector<NameAndTypePair> columns;
    /// Keeps the kernel of `expression` loaded for as long as this is kept: whatever runs it keeps it until it no longer
    /// does. The compiler keeps a bounded number of the kernels besides, which it unloads once nothing else keeps them.
    std::shared_ptr<const GPUKernel> kernel;
};

/// Of output `filter_column_name` of `dag`: a predicate that holds where it is not zero.
std::optional<CompiledGPUExpression> compileGPUFilter(const ActionsDAG & dag, const String & filter_column_name, String & refusal);

/// Of output `output_name` of `dag`: a `Float64`.
std::optional<CompiledGPUExpression> compileGPUExpression(const ActionsDAG & dag, const String & output_name, String & refusal);

}

#endif
