#pragma once
#include <Processors/ISimpleTransform.h>
#include <DataStreams/SizeLimits.h>
#include <Poco/Timespan.h>
#include <Interpreters/ProcessList.h>

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

    /** What limitations and quotas should be checked.
      * LIMITS_CURRENT - checks amount of data read by current stream only (BlockStreamProfileInfo is used for check).
      *  Currently it is used in root streams to check max_result_{rows,bytes} limits.
      * LIMITS_TOTAL - checks total amount of read data from leaf streams (i.e. data read from disk and remote servers).
      *  It is checks max_{rows,bytes}_to_read in progress handler and use info from ProcessListElement::progress_in for this.
      *  Currently this check is performed only in leaf streams.
      */
    enum LimitsMode
    {
        LIMITS_CURRENT,
        LIMITS_TOTAL,
    };

    /// It is a subset of limitations from Limits.
    struct LocalLimits
    {
        SizeLimits size_limits;

        Poco::Timespan max_execution_time = 0;
        OverflowMode timeout_overflow_mode = OverflowMode::THROW;

        /// in rows per second
        size_t min_execution_speed = 0;
        /// Verify that the speed is not too low after the specified time has elapsed.
        Poco::Timespan timeout_before_checking_execution_speed = 0;
    };

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
    LimitsMode mode = LIMITS_CURRENT;
    QueryStatus * process_list_elem = nullptr;

    QuotaForIntervals * quota = nullptr;
    double prev_elapsed = 0;

    ProcessorProfileInfo info;

    bool checkTimeLimit();
};

}
