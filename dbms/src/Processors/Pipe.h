#pragma once
#include <Processors/IProcessor.h>
#include <Processors/Sources/SourceWithProgress.h>

namespace DB
{

class Pipe;
using Pipes = std::vector<Pipe>;

/// Pipe is a set of processors which represents the part of pipeline with single output.
/// All processors in pipe are connected. All ports are connected except the output one.
class Pipe
{
public:
    /// Create from source. It must have no input ports and single output.
    explicit Pipe(ProcessorPtr source);
    /// Connect several pipes together with specified transform.
    /// Transform must have the number of inputs equals to the number of pipes. And single output.
    /// Will connect pipes outputs with transform inputs automatically.
    Pipe(Pipes && pipes, ProcessorPtr transform);

    Pipe(const Pipe & other) = delete;
    Pipe(Pipe && other) = default;

    Pipe & operator=(const Pipe & other) = delete;
    Pipe & operator=(Pipe && other) = default;

    OutputPort & getPort() const { return *output_port; }
    const Block & getHeader() const { return output_port->getHeader(); }

    /// Add transform to pipe. It must have single input and single output (is checked).
    /// Input will be connected with current output port, output port will be updated.
    void addSimpleTransform(ProcessorPtr transform);

    Processors detachProcessors() && { return std::move(processors); }

    /// Specify quotas and limits for every ISourceWithProgress.
    void setLimits(const SourceWithProgress::LocalLimits & limits);
    void setQuota(const std::shared_ptr<QuotaContext> & quota);

    /// Set information about preferred executor number for sources.
    void pinSources(size_t executor_number);

    void enableQuota();

    void setTotalsPort(OutputPort * totals_) { totals = totals_; }
    OutputPort * getTotalsPort() const { return totals; }

private:
    Processors processors;
    OutputPort * output_port = nullptr;
    OutputPort * totals = nullptr;
};

}
