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

struct CompiledGPUFilter
{
    GPUFilterProgram program;
    std::vector<NameAndTypePair> columns;
};

std::optional<CompiledGPUFilter> compileGPUFilter(const ActionsDAG & dag, const String & filter_column_name, String & refusal);

}

#endif
