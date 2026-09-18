#pragma once

#include <Processors/IProcessor.h>
#include <QueryPipeline/SizeLimits.h>

#include <unordered_map>

namespace DB
{

/// Enforce size limits on the combined sets of parallel final `DISTINCT` transforms.
/// Each input keeps its corresponding output so downstream steps can reuse the disjoint streams.
/// A `BREAK` limit emits the chunk that reaches the limit before closing every input, including
/// partitions that emit no rows and may otherwise keep reading.
class DistinctLimitTransform final : public IProcessor
{
public:
    DistinctLimitTransform(const SharedHeader & header, const SizeLimits & size_limits_, size_t num_streams);

    String getName() const override { return "DistinctLimitTransform"; }

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

    std::vector<PortPair> port_pairs;
    std::unordered_map<const Port *, PortPair *> port_to_pair;
    size_t num_finished_port_pairs = 0;
    const SizeLimits size_limits;
    UInt64 rows = 0;
    bool limit_reached = false;
};

}
