#pragma once
#include <Common/Stopwatch.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <IO/Progress.h>
#include <mutex>


namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class EnabledQuota;

struct StorageLimits;
using StorageLimitsList = std::list<StorageLimits>;


class ReadProgressCallback
{
public:
    void setQuota(const std::shared_ptr<const EnabledQuota> & quota_) { quota = quota_; }
    void setProcessListElement(QueryStatusPtr elem);
    void setProgressCallback(const ProgressCallback & callback) { progress_callback = callback; }
    void addTotalRowsApprox(size_t value) { total_rows_approx += value; }
    void addTotalBytes(size_t value) { total_bytes += value; }

    /// Skip updating profile events.
    /// For merges in mutations it may need special logic, it's done inside ProgressCallback.
    void disableProfileEventUpdate() { update_profile_events = false; }

    bool onProgress(uint64_t read_rows, uint64_t read_bytes, const StorageLimitsList & storage_limits);

private:
    std::shared_ptr<const EnabledQuota> quota;
    ProgressCallback progress_callback;
    QueryStatusPtr process_list_elem;

    /// The approximate total number of rows to read. For progress bar.
    std::atomic_size_t total_rows_approx = 0;
    /// The total number of bytes to read. For progress bar.
    std::atomic_size_t total_bytes = 0;

    Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};  /// Including waiting time

    bool update_profile_events = true;
};

}
