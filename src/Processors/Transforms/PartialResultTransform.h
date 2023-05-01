#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

class PartialResultTransform : public IProcessor
{
public:
    PartialResultTransform(const Block & header, UInt64 partial_result_limit_, UInt64 partial_result_duration_ms_);

    String getName() const override { return "PartialResultTransform"; }

    Status prepare() override;
    void work() override;

    bool isPartialResultProcessor() const override { return true; }

protected:
    enum class SnaphotStatus
    {
        NotReady,
        Ready,
        Stopped,
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
