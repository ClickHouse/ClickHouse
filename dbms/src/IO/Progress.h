#pragma once

#include <atomic>
#include <cstddef>
#include <common/Types.h>

#include <Core/Defines.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;


/// See Progress.
struct ProgressValues
{
    size_t rows;
    size_t bytes;
    size_t total_rows;

    void read(ReadBuffer & in, UInt64 server_revision);
    void write(WriteBuffer & out, UInt64 client_revision) const;
    void writeJSON(WriteBuffer & out) const;
};


/** Progress of query execution.
  * Values, transferred over network are deltas - how much was done after previously sent value.
  * The same struct is also used for summarized values.
  */
struct Progress
{
    std::atomic<size_t> rows {0};        /// Rows (source) processed.
    std::atomic<size_t> bytes {0};       /// Bytes (uncompressed, source) processed.

    /** How much rows must be processed, in total, approximately. Non-zero value is sent when there is information about some new part of job.
      * Received values must be summed to get estimate of total rows to process.
      * Used for rendering progress bar on client.
      */
    std::atomic<size_t> total_rows {0};

    Progress() {}
    Progress(size_t rows_, size_t bytes_, size_t total_rows_ = 0)
        : rows(rows_), bytes(bytes_), total_rows(total_rows_) {}

    void read(ReadBuffer & in, UInt64 server_revision);
    void write(WriteBuffer & out, UInt64 client_revision) const;

    /// Progress in JSON format (single line, without whitespaces) is used in HTTP headers.
    void writeJSON(WriteBuffer & out) const;

    /// Each value separately is changed atomically (but not whole object).
    bool incrementPiecewiseAtomically(const Progress & rhs)
    {
        rows += rhs.rows;
        bytes += rhs.bytes;
        total_rows += rhs.total_rows;

        return rhs.rows ? true : false;
    }

    void reset()
    {
        rows = 0;
        bytes = 0;
        total_rows = 0;
    }

    ProgressValues getValues() const
    {
        ProgressValues res;

        res.rows = rows.load(std::memory_order_relaxed);
        res.bytes = bytes.load(std::memory_order_relaxed);
        res.total_rows = total_rows.load(std::memory_order_relaxed);

        return res;
    }

    ProgressValues fetchAndResetPiecewiseAtomically()
    {
        ProgressValues res;

        res.rows = rows.fetch_and(0);
        res.bytes = bytes.fetch_and(0);
        res.total_rows = total_rows.fetch_and(0);

        return res;
    }

    Progress & operator=(Progress && other)
    {
        rows = other.rows.load(std::memory_order_relaxed);
        bytes = other.bytes.load(std::memory_order_relaxed);
        total_rows = other.total_rows.load(std::memory_order_relaxed);

        return *this;
    }

    Progress(Progress && other)
    {
        *this = std::move(other);
    }
};


}
