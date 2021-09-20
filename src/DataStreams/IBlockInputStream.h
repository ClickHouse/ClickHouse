#pragma once

#include <Core/Block.h>
#include <DataStreams/BlockStreamProfileInfo.h>
#include <DataStreams/ExecutionSpeedLimits.h>
#include <DataStreams/SizeLimits.h>
#include <DataStreams/StreamLocalLimits.h>
#include <IO/Progress.h>
#include <Storages/TableLockHolder.h>
#include <Common/TypePromotion.h>

#include <atomic>
#include <shared_mutex>


namespace DB
{

namespace ErrorCodes
{
}

class ProcessListElement;
class EnabledQuota;
class QueryStatus;
struct SortColumnDescription;
using SortDescription = std::vector<SortColumnDescription>;

/** Callback to track the progress of the query.
  * Used in IBlockInputStream and Context.
  * The function takes the number of rows in the last block, the number of bytes in the last block.
  * Note that the callback can be called from different threads.
  */
using ProgressCallback = std::function<void(const Progress & progress)>;


/** The stream interface for reading data by blocks from the database.
  * Relational operations are supposed to be done also as implementations of this interface.
  * Watches out at how the source of the blocks works.
  * Lets you get information for profiling: rows per second, blocks per second, megabytes per second, etc.
  * Allows you to stop reading data (in nested sources).
  */
class IBlockInputStream : public TypePromotion<IBlockInputStream>
{
    friend struct BlockStreamProfileInfo;

public:
    IBlockInputStream() { info.parent = this; }
    virtual ~IBlockInputStream() {}

    IBlockInputStream(const IBlockInputStream &) = delete;
    IBlockInputStream & operator=(const IBlockInputStream &) = delete;

    /// To output the data stream transformation tree (query execution plan).
    virtual String getName() const = 0;

    /** Get data structure of the stream in a form of "header" block (it is also called "sample block").
      * Header block contains column names, data types, columns of size 0. Constant columns must have corresponding values.
      * It is guaranteed that method "read" returns blocks of exactly that structure.
      */
    virtual Block getHeader() const = 0;

    virtual const BlockMissingValues & getMissingValues() const
    {
        static const BlockMissingValues none;
        return none;
    }

    /** Read next block.
      * If there are no more blocks, return an empty block (for which operator `bool` returns false).
      * NOTE: Only one thread can read from one instance of IBlockInputStream simultaneously.
      * This also applies for readPrefix, readSuffix.
      */
    Block read();

    /** Read something before starting all data or after the end of all data.
      * In the `readSuffix` function, you can implement a finalization that can lead to an exception.
      * readPrefix() must be called before the first call to read().
      * readSuffix() should be called after read() returns an empty block, or after a call to cancel(), but not during read() execution.
      */

    /** The default implementation calls readPrefixImpl() on itself, and then readPrefix() recursively for all children.
      * There are cases when you do not want `readPrefix` of children to be called synchronously, in this function,
      *  but you want them to be called, for example, in separate threads (for parallel initialization of children).
      * Then overload `readPrefix` function.
      */
    virtual void readPrefix();

    /** The default implementation calls recursively readSuffix() on all children, and then readSuffixImpl() on itself.
      * If this stream calls read() in children in a separate thread, this behavior is usually incorrect:
      * readSuffix() of the child can not be called at the moment when the same child's read() is executed in another thread.
      * In this case, you need to override this method so that readSuffix() in children is called, for example, after connecting streams.
      */
    virtual void readSuffix();

    /// Must be called before `read()` and `readPrefix()`.
    void dumpTree(WriteBuffer & ostr, size_t indent = 0, size_t multiplier = 1) const;

    /** Check the depth of the pipeline.
      * If max_depth is specified and the `depth` is greater - throw an exception.
      * Must be called before `read()` and `readPrefix()`.
      */
    size_t checkDepth(size_t max_depth) const { return checkDepthImpl(max_depth, max_depth); }

    /// Do not allow to change the table while the blocks stream and its children are alive.
    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }

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
    virtual Block getExtremes();


    /** Set the execution progress bar callback.
      * The callback is passed to all child sources.
      * By default, it is called for leaf sources, after each block.
      * (But this can be overridden in the progress() method)
      * The function takes the number of rows in the last block, the number of bytes in the last block.
      * Note that the callback can be called from different threads.
      */
    virtual void setProgressCallback(const ProgressCallback & callback);


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
    virtual void setProcessListElement(QueryStatus * elem);

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

    bool isCancelled() const;
    bool isCancelledOrThrowIfKilled() const;

    /** Set limitations that checked on each block. */
    virtual void setLimits(const StreamLocalLimits & limits_)
    {
        limits = limits_;
    }

    const StreamLocalLimits & getLimits() const
    {
        return limits;
    }

    /** Set the quota. If you set a quota on the amount of raw data,
      * then you should also set mode = LIMITS_TOTAL to LocalLimits with setLimits.
      */
    virtual void setQuota(const std::shared_ptr<const EnabledQuota> & new_quota)
    {
        quota = new_quota;
    }

    /// Enable calculation of minimums and maximums by the result columns.
    void enableExtremes() { enabled_extremes = true; }

protected:
    /// Order is important: `table_locks` must be destroyed after `children` so that tables from
    /// which child streams read are protected by the locks during the lifetime of the child streams.
    std::vector<TableLockHolder> table_locks;

    BlockInputStreams children;
    std::shared_mutex children_mutex;

    BlockStreamProfileInfo info;
    std::atomic<bool> is_cancelled{false};
    std::atomic<bool> is_killed{false};
    ProgressCallback progress_callback;
    QueryStatus * process_list_elem = nullptr;
    /// According to total_stopwatch in microseconds
    UInt64 last_profile_events_update_time = 0;

    /// Additional information that can be generated during the work process.

    /// Total values during aggregation.
    Block totals;
    /// Minimums and maximums. The first row of the block - minimums, the second - the maximums.
    Block extremes;


    void addChild(const BlockInputStreamPtr & child)
    {
        std::unique_lock lock(children_mutex);
        children.push_back(child);
    }

    /** Check limits.
      * But only those that can be checked within each separate stream.
      */
    bool checkTimeLimit() const;

#ifndef NDEBUG
    bool read_prefix_is_called = false;
    bool read_suffix_is_called = false;
#endif

private:
    bool enabled_extremes = false;

    /// The limit on the number of rows/bytes has been exceeded, and you need to stop execution on the next `read` call, as if the thread has run out.
    bool limit_exceeded_need_break = false;

    /// Limitations and quotas.

    StreamLocalLimits limits;

    std::shared_ptr<const EnabledQuota> quota;    /// If nullptr - the quota is not used.
    UInt64 prev_elapsed = 0;

    /// The approximate total number of rows to read. For progress bar.
    size_t total_rows_approx = 0;

    /// Derived classes must implement this function.
    virtual Block readImpl() = 0;

    /// Here you can do a preliminary initialization.
    virtual void readPrefixImpl() {}

    /// Here you need to do a finalization, which can lead to an exception.
    virtual void readSuffixImpl() {}

    void updateExtremes(Block & block);

    /** Check quotas.
      * But only those that can be checked within each separate stream.
      */
    void checkQuota(Block & block);

    size_t checkDepthImpl(size_t max_depth, size_t level) const;

    /// Get text with names of this source and the entire subtree.
    String getTreeID() const;

    template <typename F>
    void forEachChild(F && f)
    {
        /// NOTE: Acquire a read lock, therefore f() should be thread safe
        std::shared_lock lock(children_mutex);

        // Reduce lock scope and avoid recursive locking since that is undefined for shared_mutex.
        const auto children_copy = children;
        lock.unlock();

        for (auto & child : children_copy)
            if (f(*child))
                return;
    }

};

}
