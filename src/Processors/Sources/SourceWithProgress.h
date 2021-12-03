#pragma once
#include <Processors/ISource.h>
#include <DataStreams/IBlockInputStream.h>
#include <Common/Stopwatch.h>
#include <DataStreams/StreamLocalLimits.h>

namespace DB
{

/// Adds progress to ISource.
/// This class takes care of limits, quotas, callback on progress and updating performance counters for current thread.
class ISourceWithProgress : public ISource
{
public:
    using ISource::ISource;

    /// Set limitations that checked on each chunk.
    virtual void setLimits(const StreamLocalLimits & limits_) = 0;

    /// Set limitations that checked on each chunk for distributed queries on leaf nodes.
    virtual void setLeafLimits(const SizeLimits & leaf_limits_) = 0;

    /// Set the quota. If you set a quota on the amount of raw data,
    /// then you should also set mode = LIMITS_TOTAL to LocalLimits with setLimits.
    virtual void setQuota(const std::shared_ptr<const EnabledQuota> & quota_) = 0;

    /// Set the pointer to the process list item.
    /// General information about the resources spent on the request will be written into it.
    /// Based on this information, the quota and some restrictions will be checked.
    /// This information will also be available in the SHOW PROCESSLIST request.
    virtual void setProcessListElement(QueryStatus * elem) = 0;

    /// Set the execution progress bar callback.
    /// It is called after each chunk.
    /// The function takes the number of rows in the last chunk, the number of bytes in the last chunk.
    /// Note that the callback can be called from different threads.
    virtual void setProgressCallback(const ProgressCallback & callback) = 0;

    /// Set the approximate total number of rows to read.
    virtual void addTotalRowsApprox(size_t value) = 0;
};

/// Implementation for ISourceWithProgress
class SourceWithProgress : public ISourceWithProgress
{
public:
    using ISourceWithProgress::ISourceWithProgress;
    /// If enable_auto_progress flag is set, progress() will be automatically called on each generated chunk.
    SourceWithProgress(Block header, bool enable_auto_progress);

    void setLimits(const StreamLocalLimits & limits_) final { limits = limits_; }
    void setLeafLimits(const SizeLimits & leaf_limits_) final {leaf_limits = leaf_limits_; }
    void setQuota(const std::shared_ptr<const EnabledQuota> & quota_) final { quota = quota_; }
    void setProcessListElement(QueryStatus * elem) final { process_list_elem = elem; }
    void setProgressCallback(const ProgressCallback & callback) final { progress_callback = callback; }
    void addTotalRowsApprox(size_t value) final { total_rows_approx += value; }

protected:
    /// Call this method to provide information about progress.
    void progress(const Progress & value);

    void work() override;

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

    /// This flag checks if progress() was manually called at generate() call.
    /// If not, it will be called for chunk after generate() was finished.
    bool was_progress_called = false;

    /// If enabled, progress() will be automatically called on each generated chunk.
    bool auto_progress = true;
};

}
