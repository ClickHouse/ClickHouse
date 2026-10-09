#include <Storages/OdpsArrowReader.h>

#if USE_ODPS_TUNNEL && USE_ODPS_ARROW

#include <Storages/OdpsRecordReader.h>

#include <algorithm>
#include <condition_variable>
#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int QUERY_WAS_CANCELLED;
}

AutoReconnectArrowReader::AutoReconnectArrowReader(
    DownloadFactory download_factory_,
    UInt64 start_,
    UInt64 count_,
    UInt64 batch_rows_,
    UInt64 raw_size_,
    const std::vector<std::string> & columns_,
    const apsara::odps::sdk::CompressOption & compress_,
    UInt64 max_retries_,
    UInt64 retry_initial_backoff_ms_,
    UInt64 retry_max_backoff_ms_,
    UInt64 retry_max_elapsed_ms_,
    std::function<bool()> is_cancelled_,
    Poco::Logger * log_)
    : download_factory(std::move(download_factory_))
    , start(start_)
    , count(count_)
    , batch_rows(batch_rows_)
    , raw_size(raw_size_)
    , columns(columns_)
    , compress(compress_)
    , max_retries(max_retries_)
    , retry_initial_backoff_ms(retry_initial_backoff_ms_)
    , retry_max_backoff_ms(retry_max_backoff_ms_)
    , retry_max_elapsed_ms(retry_max_elapsed_ms_)
    , is_cancelled(std::move(is_cancelled_))
    , log(log_)
{
    /// Mirrors `AutoReconnectRecordReader`: open eagerly, so a broken session
    /// fails the query at source construction instead of at the first read.
    /// The query-scoped owner remains responsible for session completion.
    openReaderWithRetry();
}

void AutoReconnectArrowReader::openReader()
{
    /// Replays the download session with the same downloadId from the last
    /// fully consumed batch (the same breakpoint strategy as the row-based
    /// wrapper). Constructing the SDK reader performs a session Reload request
    /// internally, so this may throw `OdpsTunnelException`.
    if (!download)
        download = download_factory();
    reader = download->OpenBufferArrowReader(
        start + total_read_rows,
        count - total_read_rows,
        batch_rows,
        raw_size,
        columns,
        compress,
        /*disable_modified_check=*/ false);
}

void AutoReconnectArrowReader::openReaderWithRetry()
{
    while (!reader)
    {
        if (is_cancelled && is_cancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute Arrow read was cancelled");

        const auto attempt_started = std::chrono::steady_clock::now();
        try
        {
            openReader();
            if (is_cancelled && is_cancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute Arrow read was cancelled");
        }
        catch (const apsara::odps::sdk::OdpsException & ex)
        {
            if (is_cancelled && is_cancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute Arrow read was cancelled");
            if (classifyOdpsReadError(ex) != OdpsReadErrorKind::Retryable)
                throw OdpsReadException(OdpsReadErrorKind::ArrowStream, ex);

            reader.reset();
            download.reset();
            prepareRetry(ex, attempt_started);
        }
    }
}

bool AutoReconnectArrowReader::read(std::shared_ptr<arrow::RecordBatch> & batch)
{
    while (true)
    {
        if (is_cancelled && is_cancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute Arrow read was cancelled");

        /// All requested rows have been consumed: normal EOF, no request issued.
        if (total_read_rows >= count)
            return false;

        auto attempt_started = std::chrono::steady_clock::now();
        try
        {
            if (!reader)
                openReaderWithRetry();

            attempt_started = std::chrono::steady_clock::now();
            std::shared_ptr<arrow::RecordBatch> result = reader->Read();
            if (is_cancelled && is_cancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute Arrow read was cancelled");

            if (!result)
            {
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA,
                    "MaxCompute Arrow reader reached EOF after {} rows, expected exactly {}",
                    total_read_rows,
                    count);
            }

            const UInt64 batch_row_count = static_cast<UInt64>(result->num_rows());
            if (batch_row_count == 0)
            {
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA,
                    "MaxCompute Arrow reader returned an empty batch before completing the requested range");
            }
            if (batch_row_count > count - total_read_rows)
            {
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA,
                    "MaxCompute Arrow reader returned {} rows beyond the expected range",
                    batch_row_count - (count - total_read_rows));
            }

            /// A batch was fully received: advance the breakpoint, mirroring
            /// the per-record accounting of the row-based wrapper.
            total_read_rows += batch_row_count;
            batch = std::move(result);
            return true;
        }
        catch (const apsara::odps::sdk::OdpsException & ex)
        {
            if (is_cancelled && is_cancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute Arrow read was cancelled");
            if (classifyOdpsReadError(ex) != OdpsReadErrorKind::Retryable)
                throw OdpsReadException(OdpsReadErrorKind::ArrowStream, ex);

            reader.reset();
            download.reset();
            prepareRetry(ex, attempt_started);
        }
    }
}

void AutoReconnectArrowReader::prepareRetry(
    const apsara::odps::sdk::OdpsException & ex,
    std::chrono::steady_clock::time_point attempt_started)
{
    const UInt64 attempt_elapsed_ms = static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - attempt_started).count());
    retry_elapsed_ms += std::min(attempt_elapsed_ms, retry_max_elapsed_ms - retry_elapsed_ms);
    if (retry_count >= max_retries || retry_elapsed_ms >= retry_max_elapsed_ms)
        throw OdpsReadException(OdpsReadErrorKind::ArrowStream, ex);

    ++retry_count;
    UInt64 backoff_ms = retry_initial_backoff_ms;
    for (UInt64 i = 1; i < retry_count && backoff_ms < retry_max_backoff_ms; ++i)
        backoff_ms = backoff_ms > retry_max_backoff_ms / 2 ? retry_max_backoff_ms : backoff_ms * 2;
    backoff_ms = std::min(backoff_ms, retry_max_elapsed_ms - retry_elapsed_ms);

    LOG_WARNING(
        log,
        "MaxCompute Arrow read failed with retryable error code '{}'; reopening from row {} after {} ms (retry {}/{})",
        ex.GetErrorCode(),
        start + total_read_rows,
        backoff_ms,
        retry_count,
        max_retries);

    std::mutex wait_mutex;
    std::condition_variable wait_condition;
    std::unique_lock wait_lock(wait_mutex);
    const auto wait_started = std::chrono::steady_clock::now();
    UInt64 waited_ms = 0;
    while (waited_ms < backoff_ms)
    {
        if (is_cancelled && is_cancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute Arrow read was cancelled while waiting to retry");

        wait_condition.wait_for(wait_lock, std::chrono::milliseconds(std::min<UInt64>(50, backoff_ms - waited_ms)));
        waited_ms = static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - wait_started).count());
    }
    retry_elapsed_ms += std::min(waited_ms, retry_max_elapsed_ms - retry_elapsed_ms);
    if (is_cancelled && is_cancelled())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute Arrow read was cancelled while waiting to retry");
    if (retry_elapsed_ms >= retry_max_elapsed_ms)
        throw OdpsReadException(OdpsReadErrorKind::ArrowStream, ex);
}

void AutoReconnectArrowReader::close()
{
    /// `IBufferArrowRecordReader` has no `Close`: every `Read` opens and closes
    /// its own HTTP connection inside the SDK.
    reader.reset();
    download.reset();
}

}

#endif
