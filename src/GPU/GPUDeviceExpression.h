#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUDeviceColumns.h>
#include <GPU/GPUTypes.cuh>

#include <Core/Block.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{
class ExpressionActions;
}

namespace DB::GPU
{

/** The actions of an `ExpressionTransform` over columns on the device, see `DeviceChunkInfo`: a column of the output is
  * one of the input, under its own name or another, or a `Float64` the device computes from `Float64` columns of the
  * input by `compileGPUExpression`. The columns of the input that the output leaves out are dropped.
  */
class DeviceExpression
{
public:
    /// Nothing where some column of `output_header` is neither of these.
    static std::unique_ptr<DeviceExpression> tryCompile(const ExpressionActions & actions, const Block & input_header, const Block & output_header);

    std::shared_ptr<const GPUDeviceColumns> execute(const GPUDeviceColumns & input) const;

private:
    struct Output
    {
        /// Of a column of the input, its position there.
        std::optional<size_t> input_position;
        /// Of a computed column, the program and the positions in the input of the columns it loads.
        std::optional<GPUDerivedValue> derived;
        std::vector<size_t> derived_inputs;
    };

    std::vector<Output> outputs;
};

}

#endif
