#include <Storages/OdpsRecordReader.h>

#if USE_ODPS_TUNNEL

#include <odps_clickhouse_adapter.h>

#include <algorithm>
#include <array>
#include <condition_variable>
#include <mutex>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int QUERY_WAS_CANCELLED;
}

OdpsReadErrorKind classifyOdpsReadError(const apsara::odps::sdk::OdpsException & ex)
{
    const String error_code = ex.GetErrorCode();
    if (error_code == apsara::odps::sdk::CONNECTION_ERROR
        || error_code == apsara::odps::sdk::REQUEST_TIMEOUT
        || error_code == apsara::odps::sdk::FLOW_EXCEEDED)
        return OdpsReadErrorKind::Retryable;

    /// The SDK converts an interrupted body into these parser failures instead
    /// of preserving the transport error. Retrying is safe because readers are
    /// reopened only from a row or batch boundary that ClickHouse has already
    /// delivered. Deliberate checksum mismatch messages are excluded.
    if (error_code == apsara::odps::sdk::INTERNAL_ERROR)
    {
        const String error_message = ex.GetErrorMsg();
        static constexpr std::array<std::string_view, 9> interrupted_stream_messages{
            "Read tag error, maybe EOF reached",
            "Read metrics string failed",
            "Read record crc error",
            "Read crccrc error",
            "Read int64 failed",
            "Read string failed",
            "Read bool failed",
            "Read double failed",
            "Read type TimeStamp error"};
        if (std::any_of(interrupted_stream_messages.begin(), interrupted_stream_messages.end(),
                [&](std::string_view message)
                {
                    return error_message.starts_with(message);
                }))
            return OdpsReadErrorKind::Retryable;
    }

    const String exception_message = ex.ToString();
    if (exception_message == "Connection Timeout"
        || apsara::odps::sdk::clickhouse::isArrowDeserializeError(exception_message))
        return OdpsReadErrorKind::Retryable;

    return OdpsReadErrorKind::Unrecoverable;
}

OdpsReadException::OdpsReadException(
    OdpsReadErrorKind kind_,
    const apsara::odps::sdk::OdpsException & cause)
    : Exception(ErrorCodes::NETWORK_ERROR, "ODPS tunnel read error: {}", cause.ToString())
    , error_kind(kind_)
{
}

AutoReconnectRecordReader::AutoReconnectRecordReader(
    DownloadFactory download_factory_,
    UInt64 start_,
    UInt64 count_,
    const std::vector<std::string> & columns_,
    bool compress_,
    UInt64 max_retries_,
    UInt64 retry_initial_backoff_ms_,
    UInt64 retry_max_backoff_ms_,
    UInt64 retry_max_elapsed_ms_,
    std::function<bool()> is_cancelled_,
    Poco::Logger * log_)
    : download_factory(std::move(download_factory_))
    , start(start_)
    , count(count_)
    , columns(columns_)
    , compress(compress_)
    , max_retries(max_retries_)
    , retry_initial_backoff_ms(retry_initial_backoff_ms_)
    , retry_max_backoff_ms(retry_max_backoff_ms_)
    , retry_max_elapsed_ms(retry_max_elapsed_ms_)
    , is_cancelled(std::move(is_cancelled_))
    , log(log_)
{
    openReaderWithRetry();
}

void AutoReconnectRecordReader::openReader()
{
    if (!download)
        download = download_factory();
    reader = download->OpenReader(start + total_read_rows, count - total_read_rows, columns, compress);
}

void AutoReconnectRecordReader::openReaderWithRetry()
{
    while (!reader)
    {
        if (is_cancelled && is_cancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute read was cancelled");

        const auto attempt_started = std::chrono::steady_clock::now();
        try
        {
            openReader();
            if (is_cancelled && is_cancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute read was cancelled");
        }
        catch (const apsara::odps::sdk::OdpsException & ex)
        {
            if (is_cancelled && is_cancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute read was cancelled");
            if (classifyOdpsReadError(ex) != OdpsReadErrorKind::Retryable)
                throw OdpsReadException(OdpsReadErrorKind::Unrecoverable, ex);

            reader.reset();
            download.reset();
            prepareRetry(ex, attempt_started);
        }
    }
}

bool AutoReconnectRecordReader::read(apsara::odps::sdk::ODPSTableRecord & record)
{
    while (true)
    {
        if (is_cancelled && is_cancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute read was cancelled");
        if (eof_verified)
            return false;

        auto attempt_started = std::chrono::steady_clock::now();
        try
        {
            if (!reader)
                openReaderWithRetry();

            attempt_started = std::chrono::steady_clock::now();
            const bool has_record = reader->Read(record);
            if (is_cancelled && is_cancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute read was cancelled");
            if (!has_record)
            {
                if (total_read_rows != count)
                {
                    throw Exception(
                        ErrorCodes::CANNOT_READ_ALL_DATA,
                        "MaxCompute reader reached EOF after {} rows, expected exactly {}",
                        total_read_rows,
                        count);
                }
                eof_verified = true;
                return false;
            }

            if (total_read_rows == count)
            {
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA,
                    "MaxCompute reader returned more than the expected {} rows",
                    count);
            }

            ++total_read_rows;
            return true;
        }
        catch (const apsara::odps::sdk::OdpsException & ex)
        {
            if (is_cancelled && is_cancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute read was cancelled");
            auto error_kind = classifyOdpsReadError(ex);
            if (error_kind != OdpsReadErrorKind::Retryable)
                throw OdpsReadException(error_kind, ex);

            reader.reset();
            download.reset();
            prepareRetry(ex, attempt_started);
        }
    }
}

void AutoReconnectRecordReader::prepareRetry(
    const apsara::odps::sdk::OdpsException & ex,
    std::chrono::steady_clock::time_point attempt_started)
{
    const UInt64 attempt_elapsed_ms = static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - attempt_started).count());
    retry_elapsed_ms += std::min(attempt_elapsed_ms, retry_max_elapsed_ms - retry_elapsed_ms);
    if (retry_count >= max_retries || retry_elapsed_ms >= retry_max_elapsed_ms)
        throw OdpsReadException(OdpsReadErrorKind::Unrecoverable, ex);

    ++retry_count;
    UInt64 backoff_ms = retry_initial_backoff_ms;
    for (UInt64 i = 1; i < retry_count && backoff_ms < retry_max_backoff_ms; ++i)
        backoff_ms = backoff_ms > retry_max_backoff_ms / 2 ? retry_max_backoff_ms : backoff_ms * 2;
    backoff_ms = std::min(backoff_ms, retry_max_elapsed_ms - retry_elapsed_ms);

    LOG_WARNING(
        log,
        "MaxCompute read failed with retryable error code '{}'; reopening from row {} after {} ms (retry {}/{})",
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
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute read was cancelled while waiting to retry");

        wait_condition.wait_for(wait_lock, std::chrono::milliseconds(std::min<UInt64>(50, backoff_ms - waited_ms)));
        waited_ms = static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - wait_started).count());
    }
    retry_elapsed_ms += std::min(waited_ms, retry_max_elapsed_ms - retry_elapsed_ms);
    if (is_cancelled && is_cancelled())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute read was cancelled while waiting to retry");
    if (retry_elapsed_ms >= retry_max_elapsed_ms)
        throw OdpsReadException(OdpsReadErrorKind::Unrecoverable, ex);
}

void AutoReconnectRecordReader::close()
{
    if (reader)
    {
        reader->Close();
        reader.reset();
    }

    download.reset();
}

apsara::odps::sdk::IODPSTableSchema * AutoReconnectRecordReader::getSchema()
{
    return reader->GetSchema();
}

apsara::odps::sdk::ODPSTableRecordPtr AutoReconnectRecordReader::createBufferRecord()
{
    return reader->CreateBufferRecord();
}

}

#endif
