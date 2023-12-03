#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

/// Processors of this type are used to construct an auxiliary pipeline with processors corresponding to those in the main pipeline.
/// These processors work in two modes:
/// 1) Creating a snapshot of the corresponding processor from the main pipeline once per partial_result_duration_ms (period in milliseconds), and then sending the snapshot through the partial result pipeline.
/// 2) Transforming small blocks of data in the same way as the original processor and sending the transformed data through the partial result pipeline.
/// All processors of this type rely on the invariant that a new block from the previous processor of the partial result pipeline overwrites information about the previous block of the same previous processor.
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

    virtual void transformPartialResult(Chunk & /*chunk*/) = 0;
    virtual ShaphotResult getRealProcessorSnapshot() = 0; // { return {{}, SnaphotStatus::Stopped}; }

private:
    Stopwatch watch;
};

}
