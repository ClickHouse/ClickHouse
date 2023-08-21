#pragma once
#include <Processors/IProcessor.h>

namespace DB
{

/// Processor with N inputs and N outputs. Only moves data from i-th input to i-th output as is.
/// Some ports are delayed. Delayed ports are processed after other outputs are all finished.
/// Data between ports is not mixed. It is important because this processor can be used before MergingSortedTransform.
/// Delayed ports are appeared after joins, when some non-matched data need to be processed at the end.
class DelayedPortsProcessor final : public IProcessor
{
public:
    DelayedPortsProcessor(const Block & header, size_t num_ports, const PortNumbers & delayed_ports, bool assert_main_ports_empty = false);

    String getName() const override { return "DelayedPorts"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:

    struct PortsPair
    {
        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_delayed = false;
        bool is_finished = false;
    };

    std::vector<PortsPair> port_pairs;
    const size_t num_delayed_ports;
    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    size_t num_finished_main_inputs = 0;

    std::vector<size_t> output_to_pair;
    bool are_inputs_initialized = false;

    bool processPair(PortsPair & pair);
    void finishPair(PortsPair & pair);
    bool shouldSkipDelayed() const;
};

}
