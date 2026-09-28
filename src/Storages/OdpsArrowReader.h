#pragma once
#include "config.h"

#if USE_ODPS_TUNNEL && USE_ODPS_ARROW

#include <odps_tunnel.h>
#include <Common/logger_useful.h>
#include <Core/Types.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace DB
{

/// Columnar (Arrow IPC) counterpart of `AutoReconnectRecordReader`.
///
/// Wraps `IBufferArrowRecordReader`: the SDK reader issues one HTTP request per
/// batch, so the 900s idle timeout of the row-based long connection does not
/// apply. Tunnel errors are retried by reopening the reader from the last fully
/// consumed batch. Each attempt reloads a fresh download handle with the same
/// query-scoped download ID and therefore keeps the same snapshot.
///
/// The `IDownload` handle is borrowed from the query-scoped session. `close`
/// releases the reader handle but never completes the shared session.
class AutoReconnectArrowReader
{
public:
    using DownloadFactory = std::function<apsara::odps::sdk::IDownloadPtr()>;

    /// `batch_rows_` is the server-side per-batch row limit (bufferRecordCount),
    /// `raw_size_` the per-batch byte limit (rawSize, 0 = unlimited).
    AutoReconnectArrowReader(
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
        Poco::Logger * log_);

    /// Read one record batch. Returns false on EOF (no exception for EOF).
    /// On tunnel errors, automatically reconnects from the breakpoint and
    /// retries within the configured retry count and cumulative elapsed-time budgets.
    bool read(std::shared_ptr<arrow::RecordBatch> & batch);

    /// Release the borrowed reader handle. Safe to call multiple times.
    void close();

    UInt64 totalReadRows() const { return total_read_rows; }

private:
    void openReader();
    void openReaderWithRetry();
    void prepareRetry(
        const apsara::odps::sdk::OdpsException & ex,
        std::chrono::steady_clock::time_point attempt_started);

    DownloadFactory download_factory;
    apsara::odps::sdk::IDownloadPtr download;
    apsara::odps::sdk::IBufferArrowRecordReaderPtr reader;
    UInt64 start;
    UInt64 count;
    UInt64 batch_rows;
    UInt64 raw_size;
    std::vector<std::string> columns;
    apsara::odps::sdk::CompressOption compress;
    UInt64 max_retries;
    UInt64 retry_initial_backoff_ms;
    UInt64 retry_max_backoff_ms;
    UInt64 retry_max_elapsed_ms;
    std::function<bool()> is_cancelled;
    UInt64 retry_count = 0;
    UInt64 retry_elapsed_ms = 0;
    UInt64 total_read_rows = 0;
    Poco::Logger * log;
};

}

#endif
