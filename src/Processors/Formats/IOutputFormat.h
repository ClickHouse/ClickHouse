#pragma once

#include <string>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <IO/Progress.h>
#include <Common/Stopwatch.h>

namespace DB
{

class WriteBuffer;

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

    IOutputFormat(const Block & header_, WriteBuffer & out_);

    Status prepare() override;
    void work() override;

    /// Flush output buffers if any.
    virtual void flush();

    void setAutoFlush() { auto_flush = true; }

    /// Value for rows_before_limit_at_least field.
    virtual void setRowsBeforeLimit(size_t /*rows_before_limit*/) {}

    /// Counter to calculate rows_before_limit_at_least in processors pipeline.
    void setRowsBeforeLimitCounter(RowsBeforeLimitCounterPtr counter) { rows_before_limit_counter.swap(counter); }

    /// Notify about progress. Method could be called from different threads.
    /// Passed value are delta, that must be summarized.
    virtual void onProgress(const Progress & /*progress*/) {}

    /// Content-Type to set when sending HTTP response.
    virtual std::string getContentType() const { return "text/plain; charset=UTF-8"; }

    InputPort & getPort(PortKind kind) { return *std::next(inputs.begin(), kind); }

    /// Compatibility with old interface.
    /// TODO: separate formats and processors.

    void write(const Block & block);

    void finalize();

    virtual bool expectMaterializedColumns() const { return true; }

    void setTotals(const Block & totals)
    {
        writeSuffixIfNot();
        consumeTotals(Chunk(totals.getColumns(), totals.rows()));
        are_totals_written = true;
    }
    void setExtremes(const Block & extremes)
    {
        writeSuffixIfNot();
        consumeExtremes(Chunk(extremes.getColumns(), extremes.rows()));
    }

    size_t getResultRows() const { return result_rows; }
    size_t getResultBytes() const { return result_bytes; }

    void doNotWritePrefix() { need_write_prefix = false; }

protected:
    friend class ParallelFormattingOutputFormat;

    virtual void consume(Chunk) = 0;
    virtual void consumeTotals(Chunk) {}
    virtual void consumeExtremes(Chunk) {}
    virtual void finalizeImpl() {}
    virtual void writePrefix() {}
    virtual void writeSuffix() {}

    void writePrefixIfNot()
    {
        if (need_write_prefix)
        {
            writePrefix();
            need_write_prefix = false;
        }
    }

    void writeSuffixIfNot()
    {
        if (need_write_suffix)
        {
            writeSuffix();
            need_write_suffix = false;
        }
    }

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
        Chunk totals;
        Chunk extremes;
    };

    void setOutsideStatistics(Statistics statistics_) { statistics = std::make_shared<Statistics>(std::move(statistics_)); }
    std::shared_ptr<Statistics> getOutsideStatistics() const { return statistics; }

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

    /// Flush data on each consumed chunk. This is intended for interactive applications to output data as soon as it's ready.
    bool auto_flush = false;

    bool need_write_prefix  = true;
    bool need_write_suffix = true;

    RowsBeforeLimitCounterPtr rows_before_limit_counter;

private:
    size_t rows_read_before = 0;
    std::shared_ptr<Statistics> statistics = nullptr;
    bool are_totals_written = false;

    /// Counters for consumed chunks. Are used for QueryLog.
    size_t result_rows = 0;
    size_t result_bytes = 0;
};
}
