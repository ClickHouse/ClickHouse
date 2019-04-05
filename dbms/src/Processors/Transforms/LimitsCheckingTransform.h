#pragma once
#include <Processors/ISimpleTransform.h>
#include <DataStreams/SizeLimits.h>
#include <Poco/Timespan.h>
#include <Interpreters/ProcessList.h>

#include <DataStreams/IBlockOutputStream.h>

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

    using LocalLimits = IBlockInputStream::LocalLimits;
    using LimitsMode = IBlockInputStream::LimitsMode;

    /// LIMITS_CURRENT
    LimitsCheckingTransform(const Block & header, LocalLimits limits);
    /// LIMITS_TOTAL
    LimitsCheckingTransform(const Block & header, LocalLimits limits, QueryStatus * process_list_elem);

    String getName() const override { return "LimitsCheckingTransform"; }

    void setQuota(QuotaForIntervals & quota_) { quota = &quota_; }

protected:
    void transform(Chunk & chunk) override;

private:
    LocalLimits limits;
    QueryStatus * process_list_elem = nullptr;

    QuotaForIntervals * quota = nullptr;
    double prev_elapsed = 0;

    ProcessorProfileInfo info;

    bool checkTimeLimit();
    void checkQuota(Chunk & chunk);
};

}
