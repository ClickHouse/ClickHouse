#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

class PartialResultTransform : public IProcessor
{
public:
    PartialResultTransform(const Block & header, UInt64 partial_result_limit_, UInt64 partial_result_duration_ms_);
    PartialResultTransform(const Block & input_header, const Block & output_header, UInt64 partial_result_limit_, UInt64 partial_result_duration_ms_);

    String getName() const override { return "PartialResultTransform"; }

    Status prepare() override;
    void work() override;

    bool isPartialResultProcessor() const override { return true; }

protected:
    enum class SnaphotStatus
    {
        NotReady, // Waiting for data from the previous partial result processor or awaiting a timer before creating the snapshot.
        Ready,    // Current partial result processor has received a snapshot from the processor in the main pipeline.
        Stopped,  // The processor from the main pipeline has started sending data, and the pipeline for partial results should use data from the next processors of the main pipeline.
    };

    struct ShaphotResult
    {
        Chunk chunk;
        SnaphotStatus snapshot_status;
    };

    InputPort & input;
    OutputPort & output;

    UInt64 partial_result_limit;
    UInt64 partial_result_duration_ms;

    ShaphotResult partial_result = {{}, SnaphotStatus::NotReady};

    bool finished_getting_snapshots = false;

    virtual void transformPartialResult(Chunk & /*chunk*/) {}
    virtual ShaphotResult getRealProcessorSnapshot() { return {{}, SnaphotStatus::Stopped}; }

private:
    Stopwatch watch;
};

}
