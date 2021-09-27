#pragma once

#include <atomic>
#include <cstddef>
#include <common/types.h>

#include <Core/Defines.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// See Progress.
struct ProgressValues
{
    size_t read_rows;
    size_t read_bytes;
    size_t total_rows_to_read;
    size_t written_rows;
    size_t written_bytes;

    void read(ReadBuffer & in, UInt64 server_revision);
    void write(WriteBuffer & out, UInt64 client_revision) const;
    void writeJSON(WriteBuffer & out) const;
};

struct ReadProgress
{
    size_t read_rows;
    size_t read_bytes;
    size_t total_rows_to_read;

    ReadProgress(size_t read_rows_, size_t read_bytes_, size_t total_rows_to_read_ = 0)
        : read_rows(read_rows_), read_bytes(read_bytes_), total_rows_to_read(total_rows_to_read_) {}
};

struct WriteProgress
{
    size_t written_rows;
    size_t written_bytes;

    WriteProgress(size_t written_rows_, size_t written_bytes_)
        : written_rows(written_rows_), written_bytes(written_bytes_) {}
};

/** Progress of query execution.
  * Values, transferred over network are deltas - how much was done after previously sent value.
  * The same struct is also used for summarized values.
  */
struct Progress
{
    std::atomic<size_t> read_rows {0};        /// Rows (source) processed.
    std::atomic<size_t> read_bytes {0};       /// Bytes (uncompressed, source) processed.

    /** How much rows must be processed, in total, approximately. Non-zero value is sent when there is information about some new part of job.
      * Received values must be summed to get estimate of total rows to process.
      * Used for rendering progress bar on client.
      */
    std::atomic<size_t> total_rows_to_read {0};


    std::atomic<size_t> written_rows {0};
    std::atomic<size_t> written_bytes {0};

    Progress() {}
    Progress(size_t read_rows_, size_t read_bytes_, size_t total_rows_to_read_ = 0)
        : read_rows(read_rows_), read_bytes(read_bytes_), total_rows_to_read(total_rows_to_read_) {}
    Progress(ReadProgress read_progress)
        : read_rows(read_progress.read_rows), read_bytes(read_progress.read_bytes), total_rows_to_read(read_progress.total_rows_to_read) {}
    Progress(WriteProgress write_progress)
        : written_rows(write_progress.written_rows), written_bytes(write_progress.written_bytes)  {}

    void read(ReadBuffer & in, UInt64 server_revision);
    void write(WriteBuffer & out, UInt64 client_revision) const;
    /// Progress in JSON format (single line, without whitespaces) is used in HTTP headers.
    void writeJSON(WriteBuffer & out) const;

    /// Each value separately is changed atomically (but not whole object).
    bool incrementPiecewiseAtomically(const Progress & rhs)
    {
        read_rows += rhs.read_rows;
        read_bytes += rhs.read_bytes;
        total_rows_to_read += rhs.total_rows_to_read;
        written_rows += rhs.written_rows;
        written_bytes += rhs.written_bytes;

        return rhs.read_rows || rhs.written_rows ? true : false;
    }

    void reset()
    {
        read_rows = 0;
        read_bytes = 0;
        total_rows_to_read = 0;
        written_rows = 0;
        written_bytes = 0;
    }

    ProgressValues getValues() const
    {
        ProgressValues res;

        res.read_rows = read_rows.load(std::memory_order_relaxed);
        res.read_bytes = read_bytes.load(std::memory_order_relaxed);
        res.total_rows_to_read = total_rows_to_read.load(std::memory_order_relaxed);
        res.written_rows = written_rows.load(std::memory_order_relaxed);
        res.written_bytes = written_bytes.load(std::memory_order_relaxed);

        return res;
    }

    ProgressValues fetchAndResetPiecewiseAtomically()
    {
        ProgressValues res;

        res.read_rows = read_rows.fetch_and(0);
        res.read_bytes = read_bytes.fetch_and(0);
        res.total_rows_to_read = total_rows_to_read.fetch_and(0);
        res.written_rows = written_rows.fetch_and(0);
        res.written_bytes = written_bytes.fetch_and(0);

        return res;
    }

    Progress & operator=(Progress && other)
    {
        read_rows = other.read_rows.load(std::memory_order_relaxed);
        read_bytes = other.read_bytes.load(std::memory_order_relaxed);
        total_rows_to_read = other.total_rows_to_read.load(std::memory_order_relaxed);
        written_rows = other.written_rows.load(std::memory_order_relaxed);
        written_bytes = other.written_bytes.load(std::memory_order_relaxed);

        return *this;
    }

    Progress(Progress && other)
    {
        *this = std::move(other);
    }
};

}
