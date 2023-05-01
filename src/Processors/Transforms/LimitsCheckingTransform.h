#pragma once
#include <Access/EnabledQuota.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Poco/Timespan.h>
#include <Common/Stopwatch.h>

#include <QueryPipeline/StreamLocalLimits.h>

namespace DB
{

/// Information for profiling.
struct ProcessorProfileInfo
{
    bool started = false;
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};    /// Time with waiting time

    size_t rows = 0;
    size_t blocks = 0;
    size_t bytes = 0;

    void update(const Chunk & block);
};

class LimitsCheckingTransform : public ISimpleTransform
{
public:

    LimitsCheckingTransform(const Block & header_, StreamLocalLimits limits_);

    String getName() const override { return "LimitsCheckingTransform"; }

    void setQuota(const std::shared_ptr<const EnabledQuota> & quota_) { quota = quota_; }

protected:
    void transform(Chunk & chunk) override;

    bool supportPartialResultProcessor() const override { return true; }
    ProcessorPtr getPartialResultProcessor(ProcessorPtr current_processor, UInt64 partial_result_limit, UInt64 partial_result_duration_ms) override;

private:
    StreamLocalLimits limits;

    std::shared_ptr<const EnabledQuota> quota;
    UInt64 prev_elapsed = 0;

    ProcessorProfileInfo info;

    bool checkTimeLimit();
    void checkQuota(Chunk & chunk);
};

}
