#pragma once

#include <atomic>
#include <functional>
#include <optional>
#include <base/types.h>

#include <Core/Defines.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// See Progress.
struct ProgressValues
{
    UInt64 read_rows = 0;
    UInt64 read_bytes = 0;

    UInt64 total_rows_to_read = 0;
    UInt64 total_bytes_to_read = 0;

    UInt64 written_rows = 0;
    UInt64 written_bytes = 0;

    UInt64 result_rows = 0;
    UInt64 result_bytes = 0;

    UInt64 elapsed_ns = 0;

    Int64 memory_usage = 0;

    /// See `Progress::rows_before_limit_at_least`.
    UInt64 rows_before_limit_at_least = 0;
    bool has_rows_before_limit_at_least = false;

    void read(ReadBuffer & in, UInt64 server_revision);
    void write(WriteBuffer & out, UInt64 client_revision) const;

    /// If `verbose`, zero values are written too, and so is `rows_before_limit_at_least` (if it is set),
    /// which belongs only to the final summary of a query.
    void writeJSON(WriteBuffer & out, bool verbose) const;
};

struct ReadProgress
{
    UInt64 read_rows = 0;
    UInt64 read_bytes = 0;
    UInt64 total_rows_to_read = 0;
    UInt64 total_bytes_to_read = 0;

    ReadProgress(UInt64 read_rows_, UInt64 read_bytes_, UInt64 total_rows_to_read_ = 0, UInt64 total_bytes_to_read_ = 0)
        : read_rows(read_rows_), read_bytes(read_bytes_), total_rows_to_read(total_rows_to_read_), total_bytes_to_read(total_bytes_to_read_) {}
};

struct WriteProgress
{
    UInt64 written_rows = 0;
    UInt64 written_bytes = 0;

    WriteProgress(UInt64 written_rows_, UInt64 written_bytes_)
        : written_rows(written_rows_), written_bytes(written_bytes_) {}
};

struct ResultProgress
{
    UInt64 result_rows = 0;
    UInt64 result_bytes = 0;
    Int64 memory_usage = 0;

    /// Set only for queries with a limit, see `Progress::rows_before_limit_at_least`.
    std::optional<UInt64> rows_before_limit_at_least;

    ResultProgress(UInt64 result_rows_, UInt64 result_bytes_, Int64 memory_usage_)
        : result_rows(result_rows_), result_bytes(result_bytes_), memory_usage(memory_usage_) {}
};

struct FileProgress
{
    /// Here read_bytes (raw bytes) - do not equal ReadProgress::read_bytes, which are calculated according to column types.
    UInt64 read_bytes = 0;
    UInt64 total_bytes_to_read = 0;

    explicit FileProgress(UInt64 read_bytes_, UInt64 total_bytes_to_read_ = 0) : read_bytes(read_bytes_), total_bytes_to_read(total_bytes_to_read_) {}
};


/** Progress of query execution.
  * Values, transferred over network are deltas - how much was done after previously sent value.
  * The same struct is also used for summarized values.
  */
struct Progress
{
    std::atomic<UInt64> read_rows {0};        /// Rows (source) processed.
    std::atomic<UInt64> read_bytes {0};       /// Bytes (uncompressed, source) processed.

    /** How much rows/bytes must be processed, in total, approximately. Non-zero value is sent when there is information about
      * some new part of job. Received values must be summed to get estimate of total rows to process.
      */
    std::atomic<UInt64> total_rows_to_read {0};
    std::atomic<UInt64> total_bytes_to_read {0};

    std::atomic<UInt64> written_rows {0};
    std::atomic<UInt64> written_bytes {0};

    std::atomic<UInt64> result_rows {0};
    std::atomic<UInt64> result_bytes {0};

    std::atomic<UInt64> elapsed_ns {0};

    std::atomic<Int64> memory_usage {0};

    /// The same value as `rows_before_limit_at_least` in the `JSON` output formats. Unlike the other fields, it is not
    /// an increment: it is reported once, when the query has finished, and only if the query has a limit (as indicated
    /// by `has_rows_before_limit_at_least`). It is not sent over the native protocol, which has its own `ProfileInfo` packet
    /// for it, and it is written only in the `Verbose` JSON, that is, in the `X-ClickHouse-Summary` HTTP header.
    std::atomic<UInt64> rows_before_limit_at_least {0};
    std::atomic<bool> has_rows_before_limit_at_least {false};

    Progress() = default;

    Progress(UInt64 read_rows_, UInt64 read_bytes_, UInt64 total_rows_to_read_ = 0, UInt64 total_bytes_to_read_ = 0)
        : read_rows(read_rows_), read_bytes(read_bytes_), total_rows_to_read(total_rows_to_read_), total_bytes_to_read(total_bytes_to_read_) {}

    explicit Progress(ReadProgress read_progress)
        : read_rows(read_progress.read_rows), read_bytes(read_progress.read_bytes), total_rows_to_read(read_progress.total_rows_to_read) {}

    explicit Progress(WriteProgress write_progress)
        : written_rows(write_progress.written_rows), written_bytes(write_progress.written_bytes) {}

    explicit Progress(ResultProgress result_progress)
        : result_rows(result_progress.result_rows)
        , result_bytes(result_progress.result_bytes)
        , memory_usage(result_progress.memory_usage)
        , rows_before_limit_at_least(result_progress.rows_before_limit_at_least.value_or(0))
        , has_rows_before_limit_at_least(result_progress.rows_before_limit_at_least.has_value())
    {
    }

    explicit Progress(FileProgress file_progress)
        : read_bytes(file_progress.read_bytes), total_bytes_to_read(file_progress.total_bytes_to_read) {}

    void read(ReadBuffer & in, UInt64 server_revision);

    void write(WriteBuffer & out, UInt64 client_revision) const;

    enum class DisplayMode
    {
        Verbose,  // Display zero values and `rows_before_limit_at_least`. Needed for X-ClickHouse-Summary
        Minimal   // Do not write zero values. Needed to send less data for frequent progress updates (X-ClickHouse-Progress)
    };

    /// Progress in JSON format (single line, without whitespaces) is used in HTTP headers.
    void writeJSON(WriteBuffer & out, DisplayMode mode) const;

    /// Each value separately is changed atomically (but not whole object).
    /// `rows_before_limit_at_least` is not incremented, but replaced, if it is set in `rhs`.
    bool incrementPiecewiseAtomically(const Progress & rhs);

    void incrementElapsedNs(UInt64 elapsed_ns_);

    bool empty() const;

    void reset();

    ProgressValues getValues() const;

    ProgressValues fetchValuesAndResetPiecewiseAtomically();

    Progress fetchAndResetPiecewiseAtomically();

    Progress & operator=(Progress && other) noexcept;

    Progress(Progress && other) noexcept
    {
        *this = std::move(other);
    }
};


/** Callback to track the progress of the query.
  * Used in QueryPipeline and Context.
  * The function takes the number of rows in the last block, the number of bytes in the last block.
  * Note that the callback can be called from different threads.
  */
using ProgressCallback = std::function<void(const Progress & progress)>;

}
