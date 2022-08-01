#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <base/types.h>

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
    size_t total_bytes_to_read;

    size_t written_rows;
    size_t written_bytes;

    size_t result_rows;
    size_t result_bytes;

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

struct ResultProgress
{
    size_t result_rows;
    size_t result_bytes;

    ResultProgress(size_t result_rows_, size_t result_bytes_)
        : result_rows(result_rows_), result_bytes(result_bytes_) {}
};

struct FileProgress
{
    /// Here read_bytes (raw bytes) - do not equal ReadProgress::read_bytes, which are calculated according to column types.
    size_t read_bytes;
    size_t total_bytes_to_read;

    explicit FileProgress(size_t read_bytes_, size_t total_bytes_to_read_ = 0) : read_bytes(read_bytes_), total_bytes_to_read(total_bytes_to_read_) {}
};


/** Progress of query execution.
  * Values, transferred over network are deltas - how much was done after previously sent value.
  * The same struct is also used for summarized values.
  */
struct Progress
{
    std::atomic<size_t> read_rows {0};        /// Rows (source) processed.
    std::atomic<size_t> read_bytes {0};       /// Bytes (uncompressed, source) processed.

    /** How much rows/bytes must be processed, in total, approximately. Non-zero value is sent when there is information about
      * some new part of job. Received values must be summed to get estimate of total rows to process.
      */
    std::atomic<size_t> total_rows_to_read {0};
    std::atomic<size_t> total_bytes_to_read {0};

    std::atomic<size_t> written_rows {0};
    std::atomic<size_t> written_bytes {0};

    std::atomic<size_t> result_rows {0};
    std::atomic<size_t> result_bytes {0};

    Progress() = default;

    Progress(size_t read_rows_, size_t read_bytes_, size_t total_rows_to_read_ = 0)
        : read_rows(read_rows_), read_bytes(read_bytes_), total_rows_to_read(total_rows_to_read_) {}

    explicit Progress(ReadProgress read_progress)
        : read_rows(read_progress.read_rows), read_bytes(read_progress.read_bytes), total_rows_to_read(read_progress.total_rows_to_read) {}

    explicit Progress(WriteProgress write_progress)
        : written_rows(write_progress.written_rows), written_bytes(write_progress.written_bytes) {}

    explicit Progress(ResultProgress result_progress)
        : result_rows(result_progress.result_rows), result_bytes(result_progress.result_bytes) {}

    explicit Progress(FileProgress file_progress)
        : read_bytes(file_progress.read_bytes), total_bytes_to_read(file_progress.total_bytes_to_read) {}

    void read(ReadBuffer & in, UInt64 server_revision);

    void write(WriteBuffer & out, UInt64 client_revision) const;

    /// Progress in JSON format (single line, without whitespaces) is used in HTTP headers.
    void writeJSON(WriteBuffer & out) const;

    /// Each value separately is changed atomically (but not whole object).
    bool incrementPiecewiseAtomically(const Progress & rhs);

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
