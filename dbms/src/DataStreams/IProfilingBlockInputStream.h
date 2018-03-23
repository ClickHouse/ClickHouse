#pragma once

#include <IO/Progress.h>

#include <DataStreams/BlockStreamProfileInfo.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/SizeLimits.h>

#include <Interpreters/SettingsCommon.h>

#include <atomic>


namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

class QuotaForIntervals;
class ProcessListElement;
class IProfilingBlockInputStream;

using ProfilingBlockInputStreamPtr = std::shared_ptr<IProfilingBlockInputStream>;


/** Watches out at how the source of the blocks works.
  * Lets you get information for profiling:
  *  rows per second, blocks per second, megabytes per second, etc.
  * Allows you to stop reading data (in nested sources).
  */
class IProfilingBlockInputStream : public IBlockInputStream
{
    friend struct BlockStreamProfileInfo;

public:
    IProfilingBlockInputStream();

    Block read() override final;

    /** The default implementation calls readPrefixImpl() on itself, and then readPrefix() recursively for all children.
      * There are cases when you do not want `readPrefix` of children to be called synchronously, in this function,
      *  but you want them to be called, for example, in separate threads (for parallel initialization of children).
      * Then overload `readPrefix` function.
      */
    void readPrefix() override;

    /** The default implementation calls recursively readSuffix() on all children, and then readSuffixImpl() on itself.
      * If this stream calls read() in children in a separate thread, this behavior is usually incorrect:
      * readSuffix() of the child can not be called at the moment when the same child's read() is executed in another thread.
      * In this case, you need to override this method so that readSuffix() in children is called, for example, after connecting streams.
      */
    void readSuffix() override;

    /// Get information about execution speed.
    const BlockStreamProfileInfo & getProfileInfo() const { return info; }

    /** Get "total" values.
      * The default implementation takes them from itself or from the first child source in which they are.
      * The overridden method can perform some calculations. For example, apply an expression to the `totals` of the child source.
      * There can be no total values - then an empty block is returned.
      *
      * Call this method only after all the data has been retrieved with `read`,
      *  otherwise there will be problems if any data at the same time is computed in another thread.
      */
    virtual Block getTotals();

    /// The same for minimums and maximums.
    Block getExtremes();


    /** Set the execution progress bar callback.
      * The callback is passed to all child sources.
      * By default, it is called for leaf sources, after each block.
      * (But this can be overridden in the progress() method)
      * The function takes the number of rows in the last block, the number of bytes in the last block.
      * Note that the callback can be called from different threads.
      */
    void setProgressCallback(const ProgressCallback & callback);


    /** In this method:
      * - the progress callback is called;
      * - the status of the query execution in ProcessList is updated;
      * - checks restrictions and quotas that should be checked not within the same source,
      *   but over the total amount of resources spent in all sources at once (information in the ProcessList).
      */
    virtual void progress(const Progress & value)
    {
        /// The data for progress is taken from leaf sources.
        if (children.empty())
            progressImpl(value);
    }

    void progressImpl(const Progress & value);


    /** Set the pointer to the process list item.
      * It is passed to all child sources.
      * General information about the resources spent on the request will be written into it.
      * Based on this information, the quota and some restrictions will be checked.
      * This information will also be available in the SHOW PROCESSLIST request.
      */
    void setProcessListElement(ProcessListElement * elem);

    /** Set the approximate total number of rows to read.
      */
    void addTotalRowsApprox(size_t value) { total_rows_approx += value; }


    /** Ask to abort the receipt of data as soon as possible.
      * By default - just sets the flag is_cancelled and asks that all children be interrupted.
      * This function can be called several times, including simultaneously from different threads.
      * Have two modes:
      *  with kill = false only is_cancelled is set - streams will stop silently with returning some processed data.
      *  with kill = true also is_killed set - queries will stop with exception.
      */
    virtual void cancel(bool kill);

    /** Do you want to abort the receipt of data.
     */
    bool isCancelled() const
    {
        return is_cancelled.load(std::memory_order_seq_cst);
    }

    bool isCancelledOrThrowIfKilled() const
    {
        if (!isCancelled())
            return false;
        if (is_killed)
            throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);
        return true;
    }

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
        LimitsMode mode = LIMITS_CURRENT;

        SizeLimits size_limits;

        Poco::Timespan max_execution_time = 0;
        OverflowMode timeout_overflow_mode = OverflowMode::THROW;

        /// in rows per second
        size_t min_execution_speed = 0;
        /// Verify that the speed is not too low after the specified time has elapsed.
        Poco::Timespan timeout_before_checking_execution_speed = 0;
    };

    /** Set limitations that checked on each block. */
    void setLimits(const LocalLimits & limits_)
    {
        limits = limits_;
    }

    const LocalLimits & getLimits() const
    {
        return limits;
    }

    /** Set the quota. If you set a quota on the amount of raw data,
      * then you should also set mode = LIMITS_TOTAL to LocalLimits with setLimits.
      */
    void setQuota(QuotaForIntervals & quota_)
    {
        quota = &quota_;
    }

    /// Enable calculation of minimums and maximums by the result columns.
    void enableExtremes() { enabled_extremes = true; }

protected:
    BlockStreamProfileInfo info;
    std::atomic<bool> is_cancelled{false};
    bool is_killed{false};
    ProgressCallback progress_callback;
    ProcessListElement * process_list_elem = nullptr;

    /// Additional information that can be generated during the work process.

    /// Total values during aggregation.
    Block totals;
    /// Minimums and maximums. The first row of the block - minimums, the second - the maximums.
    Block extremes;


    void addChild(BlockInputStreamPtr & child)
    {
        std::lock_guard lock(children_mutex);
        children.push_back(child);
    }

private:
    bool enabled_extremes = false;

    /// The limit on the number of rows/bytes has been exceeded, and you need to stop execution on the next `read` call, as if the thread has run out.
    bool limit_exceeded_need_break = false;

    /// Limitations and quotas.

    LocalLimits limits;

    QuotaForIntervals * quota = nullptr;    /// If nullptr - the quota is not used.
    double prev_elapsed = 0;

    /// The approximate total number of rows to read. For progress bar.
    size_t total_rows_approx = 0;

    /// The successors must implement this function.
    virtual Block readImpl() = 0;

    /// Here you can do a preliminary initialization.
    virtual void readPrefixImpl() {}

    /// Here you need to do a finalization, which can lead to an exception.
    virtual void readSuffixImpl() {}

    void updateExtremes(Block & block);

    /** Check constraints and quotas.
      * But only those that can be tested within each separate source.
      */
    bool checkTimeLimits();
    void checkQuota(Block & block);


    template <typename F>
    void forEachProfilingChild(F && f)
    {
        std::lock_guard lock(children_mutex);
        for (auto & child : children)
            if (IProfilingBlockInputStream * p_child = dynamic_cast<IProfilingBlockInputStream *>(child.get()))
                if (f(*p_child))
                    return;
    }
};

}
