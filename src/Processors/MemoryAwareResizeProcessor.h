#pragma once

#include <queue>
#include <unordered_map>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/Transforms/InsertMemoryThrottle.h>

namespace DB
{

/// ResizeProcessor variant that activates or deactivates output ports based on
/// the current memory pressure reported by an InsertMemoryThrottle
/// Per-chunk memory cost is learned from the chunks passing through this processor
class MemoryAwareResizeProcessor final : public IProcessor
{
public:
    MemoryAwareResizeProcessor(
        SharedHeader header,
        size_t num_inputs,
        size_t num_outputs,
        InsertMemoryThrottlePtr throttle_);

    String getName() const override { return "MemoryAwareResize"; }

    Status prepare(const UpdatedInputPorts &, const UpdatedOutputPorts &) override;

private:
    InsertMemoryThrottlePtr throttle;

    size_t allowed_outputs;
    static constexpr size_t min_outputs = 1;

    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    std::queue<UInt64> waiting_outputs;
    std::queue<UInt64> inputs_with_data;
    bool initialized = false;
    bool is_reading_started = false;

    enum class OutputStatus : uint8_t
    {
        NotActive,
        NeedData,
        Finished,
    };

    enum class InputStatus : uint8_t
    {
        NotActive,
        HasData,
        Finished,
    };

    struct InputPortWithStatus
    {
        InputPort * port;
        InputStatus status;
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        OutputStatus status;
    };

    std::vector<InputPortWithStatus> input_ports;
    std::vector<OutputPortWithStatus> output_ports;
    std::unordered_map<const InputPort *, UInt64> input_port_index;
    std::unordered_map<const OutputPort *, UInt64> output_port_index;

    void updateAllowedOutputs();
};

}
