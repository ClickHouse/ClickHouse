#pragma once

#include <GPU/IGroupBy.h>

#include <memory>

namespace DB::GPU
{

class RecordGroupBy final : public IGroupBy
{
public:
    RecordGroupBy(GPUSpan<GPUElementType> key_element_types_, GPUSpan<GPUGroupByValue> values_);

    ~RecordGroupBy() override;

    double addBatch(
        GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, GPUSpan<DeviceColumnView> filter_columns, const GPUFilterProgram * filter)
        override;

    size_t finalize() override;

    void copyGroupsOut(GPUSpan<HostColumnView> keys, GPUSpan<HostColumnView> values) override;

private:
    struct State;
    std::unique_ptr<State> state;
};

}
