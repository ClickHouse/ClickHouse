#pragma once

#include <Common/Logger.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/DequeWithMemoryTracking.h>

#include <Core/Block_fwd.h>
#include <Core/Field.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <deque>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

/// N -> M watermark-aware stream resize.
class CalibrateWatermarksProcessor final : public IProcessor
{
    struct InputState
    {
        bool idle = false;
        Field watermark;
    };

    struct OutputState
    {
        DequeWithMemoryTracking<Chunk> queue;
    };

    void handleOutputUpdate(OutputPort * output, OutputState & state);
    void handleInputUpdate(InputPort * input, InputState & state);

    size_t getPendingQueuesCount() const;
    void broadcastMarker(const Chunk & marker);
    void mergeInputStates();

public:
    CalibrateWatermarksProcessor(SharedHeader header, size_t num_inputs, size_t num_outputs, Field initial_watermark);

    String getName() const override { return "CalibrateWatermarks"; }
    Status prepare(const UpdatedInputPorts & updated_input_ports, const UpdatedOutputPorts & updated_output_ports) override;

private:
    const LoggerPtr log;

    /// State information.
    UnorderedMapWithMemoryTracking<OutputPort *, OutputState> outputs_state;
    UnorderedMapWithMemoryTracking<InputPort *, InputState> inputs_state;
    bool initialized = false;

    /// Runtime information.
    UnorderedSetWithMemoryTracking<OutputPort *> finished_outputs;
    UnorderedSetWithMemoryTracking<InputPort *> finished_inputs;
    Field last_emitted_watermark;
    bool emitted_idle = false;
};

}
