#pragma once

#include <Processors/IProcessor.h>
#include <QueryPipeline/SizeLimits.h>

#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Enforce size limits on the combined sets of parallel final `DISTINCT` transforms.
/// Each input keeps its corresponding output so downstream steps can reuse the disjoint streams.
/// A `BREAK` limit emits the chunk that reaches the limit before closing every input, including
/// partitions that emit no rows and may otherwise keep reading.
/// With `input_is_distinct`, no `DISTINCT` transforms precede it because the input rows are already distinct. Then it
/// counts the rows it passes and checks only the limit on rows.
class DistinctLimitsCheckingTransform final : public IProcessor
{
public:
    DistinctLimitsCheckingTransform(
        const SharedHeader & header, const SizeLimits & size_limits_, size_t num_streams, bool input_is_distinct_ = false);

    String getName() const override { return "DistinctLimitsCheckingTransform"; }

    Status prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs) override;
    Status prepare() override;

private:
    struct PortPair
    {
        InputPort & input;
        OutputPort & output;
        bool is_finished = false;
    };

    Status preparePair(PortPair & pair);

    VectorWithMemoryTracking<PortPair> port_pairs;
    UnorderedMapWithMemoryTracking<const Port *, PortPair *> port_to_pair;
    size_t num_finished_port_pairs = 0;
    const SizeLimits size_limits;
    const bool input_is_distinct;
    UInt64 rows = 0;
    bool limit_reached = false;
};

}
