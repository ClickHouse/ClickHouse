#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Pipe.h>

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
    using Creator = std::function<Pipe()>;

    DelayedSource(const Block & header, Creator processors_creator);
    String getName() const override { return "Delayed"; }

    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

    enum PortKind { Main = 0, Totals = 1, Extremes = 2 };
    OutputPort & getPort(PortKind kind) { return *std::next(outputs.begin(), kind); }

private:
    Creator creator;
    Processors processors;

    /// Outputs from returned pipe.
    OutputPort * main_output = nullptr;
    OutputPort * totals_output = nullptr;
    OutputPort * extremes_output = nullptr;
};

/// Creates pipe from DelayedSource.
Pipe createDelayedPipe(const Block & header, DelayedSource::Creator processors_creator);

}
