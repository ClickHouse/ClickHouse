#pragma once

#include <Core/Block_fwd.h>
#include <IO/Progress.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Common/Stopwatch.h>

namespace DB
{

class Block;
class WriteBuffer;
class IFramingFormat;

/** Output format have three inputs and no outputs. It writes data from WriteBuffer.
  *
  * First input is for main resultset, second is for "totals" and third is for "extremes".
  * It's not necessarily to connect "totals" or "extremes" ports (they may remain dangling).
  *
  * Data from input ports are pulled in order: first, from main input, then totals, then extremes.
  *
  * By default, data for "totals" and "extremes" is ignored.
  */
class IOutputFormat : public IProcessor
{
public:
    enum PortKind { Main = 0, Totals = 1, Extremes = 2 };

    IOutputFormat(SharedHeader header_, WriteBuffer & out_);

    Status prepare() override;
    void work() override;

    void flush();
    void setAutoFlush() { auto_flush = true; }

    /// Value for rows_before_limit_at_least field.
    virtual void setRowsBeforeLimit(size_t /*rows_before_limit*/) {}

    /// Counter to calculate rows_before_limit_at_least in processors pipeline.
    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_counter.swap(counter); }

    /// Value for rows_before_aggregation field.
    virtual void setRowsBeforeAggregation(size_t /*rows_before_aggregation*/) {}

    /// Counter to calculate rows_before_aggregation in processors pipeline.
    void setRowsBeforeAggregationCounter(RowsBeforeStepCounterPtr counter) override { rows_before_aggregation_counter.swap(counter); }

    /// Notify about progress. Method could be called from different threads.
    /// Passed values are deltas, that must be summarized.
    virtual void onProgress(const Progress & progress);

    /// Hand the final progress - carrying the final counters (`result_rows` / `result_bytes` /
    /// `memory_usage`) computed after the query finished - to the framing format (see `setFraming`),
    /// which writes it as the last `progress` packet of its own (deferred) finalization, after the
    /// trailing logs and profile events are drained, so a successful framed stream really ends with
    /// it. For a pulling query the output format is finalized by the pipeline before those counters
    /// are known, so `onProgress` would drop the update (it writes only while the format is not
    /// finalized); this bypasses that guard. No-op when there is no framing. The passed value is a
    /// delta, added to the accumulated progress. Must be called before the framing format is
    /// finalized (`getFraming()->finalize()`).
    void writeFinalProgress(const Progress & progress);

    /// Set initial progress values on initialization of the format, before it starts writing the data.
    void setProgress(Progress progress);

    InputPort & getPort(PortKind kind);

    /// Compatibility with old interface.
    /// TODO: separate formats and processors.

    void write(const Block & block);

    void finalize();

    virtual bool expectMaterializedColumns() const { return true; }
    virtual bool supportsSpecialSerializationKinds() const { return false; }

    void setTotals(const Block & totals);
    void setExtremes(const Block & extremes);

    virtual bool supportsWritingException() const { return false; }
    virtual void setException(const String & /*exception_message*/) {}

    /// A framing format (see IFramingFormat.h) multiplexes the formatted data along with auxiliary
    /// packets (progress, logs, profile events, exceptions) in the output stream. The format must
    /// have been created over the framing format's payload buffer. When set, the format notifies
    /// the framing format on packet boundaries, and progress is routed to the framing format
    /// instead of the `writeProgress` method. Not compatible with parallel formatting.
    /// `for_exception` attaches the framing to serialize only an exception packet, so the deferred
    /// totals/extremes check (which matters only for data packets) is skipped.
    void setFraming(const std::shared_ptr<IFramingFormat> & framing_, bool for_exception = false);
    const std::shared_ptr<IFramingFormat> & getFraming() const { return framing; }

    /// By default the output format finalizes the framing format (which writes the trailing logs,
    /// profile events and the exception packet, then closes the stream) as part of its own
    /// finalization. Deferring it lets the caller finalize the framing format later - after the
    /// query-finish logging - so those trailing server logs are captured too. When deferred, the
    /// caller is responsible for calling `getFraming()->finalize()`.
    void deferFramingFinalize() { framing_finalize_deferred = true; }

    size_t getResultRows() const { return result_rows; }
    size_t getResultBytes() const { return result_bytes; }

    void doNotWritePrefix() { need_write_prefix = false; }

    void resetFormatter()
    {
        need_write_prefix = true;
        need_write_suffix = true;
        finalized = false;
        resetFormatterImpl();
    }

    /// Reset the statistics watch to a specific point in time
    /// If set to not running it will stop on the call (elapsed = now() - given start)
    void setStartTime(UInt64 start, bool is_running)
    {
        statistics.watch = Stopwatch(CLOCK_MONOTONIC, start, true);
        if (!is_running)
            statistics.watch.stop();
    }

    void writePrefixIfNeeded()
    {
        if (need_write_prefix)
        {
            writePrefix();
            need_write_prefix = false;
        }
    }

    void setProgressWriteFrequencyMicroseconds(size_t value)
    {
        progress_write_frequency_us = value;
    }

    /// Derived classes can use some wrappers around out WriteBuffer
    /// and can override this method to return wrapper
    /// that should be used in its derived classes.
    virtual WriteBuffer * getWriteBufferPtr() { return &out; }

protected:
    friend class ParallelFormattingOutputFormat;

    void writeSuffixIfNeeded()
    {
        if (need_write_suffix)
        {
            writeSuffix();
            need_write_suffix = false;
        }
    }

    void finalizeUnlocked();

    virtual void flushImpl();

    virtual void consume(Chunk) = 0;
    virtual void consumeTotals(Chunk) {}
    virtual void consumeExtremes(Chunk) {}
    virtual void finalizeImpl() {}
    virtual void finalizeBuffers() {}
    virtual void writePrefix() {}
    virtual void writeSuffix() {}
    virtual void resetFormatterImpl() {}

    /// If the method writeProgress is non-empty.
    virtual bool writesProgressConcurrently() const
    {
        return false;
    }

    /// This method could be called from another thread,
    /// but will be serialized with other writing methods using the writing_mutex.
    virtual void writeProgress(const Progress &) {}

    /// Methods-helpers for parallel formatting.

    /// Set the number of rows that was already read in
    /// parallel formatting before creating this formatter.
    void setRowsReadBefore(size_t first_row_number_)
    {
        rows_read_before = first_row_number_;
        onRowsReadBeforeUpdate();
    }

    size_t getRowsReadBefore() const { return rows_read_before; }

    /// Update state according to new rows_read_before.
    virtual void onRowsReadBeforeUpdate() {}

    /// Some formats outputs some statistics after the data,
    /// in parallel formatting we collect these statistics outside the
    /// underling format and then set it to format before finalizing.
    struct Statistics
    {
        Stopwatch watch;
        Progress progress;
        bool applied_limit = false;
        size_t rows_before_limit = 0;
        bool applied_aggregation = false;
        size_t rows_before_aggregation = 0;
        Chunk totals;
        Chunk extremes;
    };

    /// In some formats the way we print extremes depends on
    /// were totals printed or not. In this case in parallel formatting
    /// we should notify underling format if totals were printed.
    void setTotalsAreWritten() { are_totals_written = true; }
    bool areTotalsWritten() const { return are_totals_written; }

    /// Return true if format saves totals and extremes in consumeTotals/consumeExtremes and
    /// outputs them in finalize() method.
    virtual bool areTotalsAndExtremesUsedInFinalize() const { return false; }

    WriteBuffer & out;

    Chunk current_chunk;
    PortKind current_block_kind = PortKind::Main;
    bool has_input = false;
    bool finished = false;
    bool finalized = false;
    bool framing_finalize_deferred = false;
    /// The framing was attached only to serialize an exception packet (see `setFraming`'s
    /// `for_exception`). In this case the real output format must not write anything into the payload
    /// buffer on finalization, so that no empty format skeleton leaks as a `data` packet.
    bool framing_exception_only = false;

    /// Flush data on each consumed chunk. This is intended for interactive applications to output data as soon as it's ready.
    bool auto_flush = false;

    bool need_write_prefix  = true;
    bool need_write_suffix = true;

    RowsBeforeStepCounterPtr rows_before_limit_counter;
    RowsBeforeStepCounterPtr rows_before_aggregation_counter;

    Statistics statistics;
    std::atomic_bool has_progress_update_to_write = false;

    /// To serialize the calls to writeProgress (which could be called from another thread) and other writing methods.
    std::mutex writing_mutex;

    std::shared_ptr<IFramingFormat> framing;

private:
    /// Write the postponed progress update (to the framing format if it is set), under the writing mutex.
    void writeProgressIfNeededUnlocked();

    size_t rows_read_before = 0;
    bool are_totals_written = false;

    /// Counters for consumed chunks. Are used for QueryLog.
    size_t result_rows = 0;
    size_t result_bytes = 0;

    UInt64 progress_write_frequency_us = 0;
    std::atomic<UInt64> prev_progress_write_ns = 0;
};

}
