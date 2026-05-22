#pragma once

#include <Core/Block_fwd.h>
#include <Core/Field.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <deque>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

/// K -> Y watermark calibrator with per-output queues.
///
/// Data chunks are routed to the output whose pending-chunk queue is currently shortest.
/// Watermark / idle markers are absorbed into per-input slots; once every still-open input has
/// published a marker since the last broadcast, the merger computes `min(latest_watermark[i])`
/// over non-idle slots and appends the broadcast marker to every still-open output queue
/// (or an `IdleMarker` if every still-open slot is idle).
class WatermarkMerger final : public IProcessor
{
    struct InputState
    {
        bool idle = false;
        std::optional<Field> pending_watermark;
    };

    struct OutputState
    {
        std::deque<Chunk> queue;
    };

    void handleOutputUpdate(OutputPort * output, OutputState & state);
    void handleInputUpdate(InputPort * input, InputState & state);
    void broadcastAlignedMarker();
    size_t getPendingQueuesCount() const;

public:
    WatermarkMerger(SharedHeader header, size_t num_inputs, size_t num_outputs);

    String getName() const override { return "WatermarkMerger"; }
    Status prepare(const UpdatedInputPorts & updated_input_ports, const UpdatedOutputPorts & updated_output_ports) override;

private:
    const LoggerPtr log;

    std::unordered_map<OutputPort *, OutputState> outputs_state;
    std::unordered_map<InputPort *, InputState> inputs_state;

    std::unordered_set<OutputPort *> finished_outputs;
    std::unordered_set<InputPort *> finished_inputs;
    std::unordered_set<InputPort *> marked_inputs;

    bool initialized = false;
    bool marked_state_reported = false;
};

}
