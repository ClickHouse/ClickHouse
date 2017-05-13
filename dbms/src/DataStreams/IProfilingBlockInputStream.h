#pragma once

#include <Core/Progress.h>

#include <Interpreters/Limits.h>

#include <DataStreams/BlockStreamProfileInfo.h>
#include <DataStreams/IBlockInputStream.h>

#include <atomic>

namespace DB
{

class QuotaForIntervals;
struct ProcessListElement;
class IProfilingBlockInputStream;

using ProfilingBlockInputStreamPtr = std::shared_ptr<IProfilingBlockInputStream>;


/** Watches out at how the source of the blocks works.
  * Lets you get information for profiling:
  *  rows per second, blocks per second, megabytes per second, etc.
  * Allows you to stop reading data (in nested sources).
  */
class IProfilingBlockInputStream : public IBlockInputStream
{
public:
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
    virtual const Block & getTotals();

    /// The same for minimums and maximums.
    const Block & getExtremes() const;


    /** Set the execution progress bar callback.
      * The callback is passed to all child sources.
      * By default, it is called for leaf sources, after each block.
      * (But this can be overridden in the progress() method)
      * The function takes the number of rows in the last block, the number of bytes in the last block.
      * Note that the callback can be called from different threads.
      */
    void setProgressCallback(ProgressCallback callback);


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
    void setTotalRowsApprox(size_t value) { total_rows_approx = value; }


    /** Ask to abort the receipt of data as soon as possible.
      * By default - just sets the flag is_cancelled and asks that all children be interrupted.
      * This function can be called several times, including simultaneously from different threads.
      */
    virtual void cancel();

    /** Do you want to abort the receipt of data.
     */
    bool isCancelled() const
    {
        return is_cancelled.load(std::memory_order_seq_cst);
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

        /// If it is zero, corresponding limit check isn't performed.
        size_t max_rows_to_read = 0;
        size_t max_bytes_to_read = 0;
        OverflowMode read_overflow_mode = OverflowMode::THROW;

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
    ProgressCallback progress_callback;
    ProcessListElement * process_list_elem = nullptr;

    bool enabled_extremes = false;

    /// Additional information that can be generated during the work process.

    /// Total values during aggregation.
    Block totals;
    /// Minimums and maximums. The first row of the block - minimums, the second - the maximums.
    Block extremes;
    /// The approximate total number of rows to read. For progress bar.
    size_t total_rows_approx = 0;
    /// Information about the approximate total number of rows is collected in the parent source.
    bool collected_total_rows_approx = false;

    /// The limit on the number of rows/bytes has been exceeded, and you need to stop execution on the next `read` call, as if the thread has run out.
    bool limit_exceeded_need_break = false;

    /// Limitations and quotas.

    LocalLimits limits;

    QuotaForIntervals * quota = nullptr;    /// If nullptr - the quota is not used.
    double prev_elapsed = 0;

    /// The heirs must implement this function.
    virtual Block readImpl() = 0;

    /// Here you can do a preliminary initialization.
    virtual void readPrefixImpl() {}

    /// Here you need to do a finalization, which can lead to an exception.
    virtual void readSuffixImpl() {}

    void updateExtremes(Block & block);

    /** Check constraints and quotas.
      * But only those that can be tested within each separate source.
      */
    bool checkLimits();
    void checkQuota(Block & block);

    /// Gather information about the approximate total number of rows from all children.
    void collectTotalRowsApprox();

    /** Send information about the approximate total number of rows to the progress bar.
      * It is done so that sending occurs only in the upper source.
      */
    void collectAndSendTotalRowsApprox();
};

}
