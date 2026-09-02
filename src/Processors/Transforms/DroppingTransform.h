#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <vector>

namespace DB
{

/** Consumes all output ports of a source pipeline: the data streams plus the optional
  * totals and extremes streams. Forwards the data streams unchanged (1:1 pass-through)
  * and drops (discards) the totals and extremes streams.
  *
  * Unlike a childless `NullSink` attached to the totals/extremes ports, this processor has
  * data outputs that continue down the pipeline, so it is not a childless node. As a result
  * `ExecutingGraph::initializeExecution` does not seed it, which avoids prematurely pulling
  * an unbuilt materialized-CTE read.
  *
  * Port order in the constructor: first the `num_streams` data inputs, then (if present) the
  * totals input, then (if present) the extremes input. Outputs are the `num_streams` data
  * outputs. This ordering is required by `Pipe::addTransform`, which connects the pipe's data
  * ports to the first inputs and identifies the totals/extremes inputs by pointer.
  */
class DroppingTransform : public IProcessor
{
public:
    DroppingTransform(SharedHeader header, size_t num_streams_, bool has_totals, bool has_extremes);

    String getName() const override { return "DroppingTransform"; }

    Status prepare() override;

    InputPort * getTotalsPort() { return totals_input; }
    InputPort * getExtremesPort() { return extremes_input; }

private:
    size_t num_streams;

    std::vector<InputPort *> data_inputs;
    std::vector<OutputPort *> data_outputs;

    InputPort * totals_input = nullptr;
    InputPort * extremes_input = nullptr;
};

}
