#pragma once

#include <string>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <IO/Progress.h>
#include <Common/Stopwatch.h>
#include <iostream>

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

    void setFirstRowNumber(size_t first_row_number_)
    {
        first_row_number = first_row_number_;
        onFirstRowNumberUpdate();
    }

    struct Statistics
    {
        Stopwatch watch;
        Progress progress;
        bool applied_limit = false;
        size_t rows_before_limit = 0;
    };

    void setOutsideStatistics(const Statistics & statistics) { outside_statistics = statistics; }

    void setTotalsAreWritten() { are_totals_written = true; }

    bool areTotalsWritten() const { return are_totals_written; }

protected:
    friend class ParallelFormattingOutputFormat;

    virtual void consume(Chunk) = 0;
    virtual void consumeTotals(Chunk) {}
    virtual void consumeExtremes(Chunk) {}
    virtual void finalizeImpl() {}
    virtual void writePrefix() {}
    virtual void writeSuffix() {}

    virtual void onFirstRowNumberUpdate() {}

    size_t getFirstRowNumber() const { return first_row_number; }
    std::optional<Statistics> getOutsideStatistics() const { return outside_statistics; }

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
    size_t first_row_number = 0;
    std::optional<Statistics> outside_statistics = std::nullopt;
    bool are_totals_written = false;

    /// Counters for consumed chunks. Are used for QueryLog.
    size_t result_rows = 0;
    size_t result_bytes = 0;
};
}
