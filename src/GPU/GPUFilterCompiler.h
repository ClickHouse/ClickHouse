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

    /// `__applyFilter(id, key)`: whether the key, column `key_column` of `columns`, is among the keys of runtime filter
    /// `filter_id`, which the device tells with a set of them, see `DeviceKeySet`. The program loads it as its column
    /// `columns.size() + i` for membership `i`, a `UInt8`, which whoever runs the program computes before it.
    struct Membership
    {
        String filter_id;
        uint32_t key_column;
    };
    std::vector<Membership> memberships;
};

/// With `with_memberships`, `__applyFilter` compiles into a membership, see `CompiledGPUFilter::Membership`; without, it
/// is refused, as whoever runs the program has no set of keys for it.
std::optional<CompiledGPUFilter> compileGPUFilter(const ActionsDAG & dag, const String & filter_column_name, String & refusal, bool with_memberships = false);

/// Of output `output_name` of `dag`: a `Float64` that the device computes from `Float64` columns and numeric constants
/// by `plus`, `minus`, `multiply` and `divide`, as ClickHouse computes it, an operation at a time.
std::optional<CompiledGPUFilter> compileGPUExpression(const ActionsDAG & dag, const String & output_name, String & refusal);

}

#endif
