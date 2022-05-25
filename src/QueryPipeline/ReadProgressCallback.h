#pragma once
#include <Common/Stopwatch.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <IO/Progress.h>

namespace DB
{

class QueryStatus;
class EnabledQuota;

class ReadProgressCallback
{
public:
    void setLimits(const StreamLocalLimits & limits_) { limits = limits_; }
    void setLeafLimits(const SizeLimits & leaf_limits_) {leaf_limits = leaf_limits_; }
    void setQuota(const std::shared_ptr<const EnabledQuota> & quota_) { quota = quota_; }
    void setProcessListElement(QueryStatus * elem);
    void setProgressCallback(const ProgressCallback & callback) { progress_callback = callback; }
    void addTotalRowsApprox(size_t value) { total_rows_approx += value; }

    /// Skip updating profile events.
    /// For merges in mutations it may need special logic, it's done inside ProgressCallback.
    void disableProfileEventUpdate() { update_profile_events = false; }

    bool onProgress(uint64_t read_rows, uint64_t read_bytes);

private:
    StreamLocalLimits limits;
    SizeLimits leaf_limits;
    std::shared_ptr<const EnabledQuota> quota;
    ProgressCallback progress_callback;
    QueryStatus * process_list_elem = nullptr;

    /// The approximate total number of rows to read. For progress bar.
    size_t total_rows_approx = 0;

    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};    /// Time with waiting time.
    /// According to total_stopwatch in microseconds.
    UInt64 last_profile_events_update_time = 0;

    bool update_profile_events = true;
};

}
