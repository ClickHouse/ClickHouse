#pragma once

#include <Processors/IProcessor.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

/// DelayedSource delays pipeline calculation until it starts execution.
/// It accepts callback which creates a new pipe.
///
/// First time when DelayedSource's main output port needs data, callback is called.
/// Then, DelayedSource expands pipeline: adds new inputs and connects pipe with it.
/// Then, DelayedSource just move data from inputs to outputs until finished.
///
/// It main output port of DelayedSource is never needed, callback won't be called.
class DelayedSource : public IProcessor
{
public:
    using Creator = std::function<QueryPipelineBuilder()>;

    DelayedSource(const Block & header, Creator processors_creator, bool add_totals_port, bool add_extremes_port);
    String getName() const override { return "Delayed"; }

    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

    OutputPort & getPort() { return *main; }
    OutputPort * getTotalsPort() { return totals; }
    OutputPort * getExtremesPort() { return extremes; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit.swap(counter); }
    void setRowsBeforeAggregationCounter(RowsBeforeStepCounterPtr counter) override { rows_before_aggregation.swap(counter); }

private:
    QueryPlanResourceHolder resources;
    Creator creator;
    Processors processors;
    RowsBeforeStepCounterPtr rows_before_limit;
    RowsBeforeStepCounterPtr rows_before_aggregation;

    /// Outputs for DelayedSource.
    OutputPort * main = nullptr;
    OutputPort * totals = nullptr;
    OutputPort * extremes = nullptr;

    /// Outputs from returned pipe.
    OutputPort * main_output = nullptr;
    OutputPort * totals_output = nullptr;
    OutputPort * extremes_output = nullptr;
};

/// Creates pipe from DelayedSource.
Pipe createDelayedPipe(const Block & header, DelayedSource::Creator processors_creator, bool add_totals_port, bool add_extremes_port);

}
