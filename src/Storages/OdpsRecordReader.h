#pragma once
#include "config.h"

#if USE_ODPS_TUNNEL

#include <odps_tunnel.h>
#include <Common/Exception.h>
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

/// Classification of ODPS tunnel read errors.
/// Used to replace fragile string-based error detection with a structured enum.
enum class OdpsReadErrorKind
{
    /// Transport or throttling error that can be retried from a confirmed row boundary.
    Retryable,
    /// All other tunnel errors (non-recoverable).
    Unrecoverable,
    /// Unrecoverable failure of the Arrow columnar stream after exhausting
    /// reconnection attempts (see AutoReconnectArrowReader).
    ArrowStream,
};

/// Classify an `OdpsException` using the SDK error code. The SDK throws the
/// base exception for some HTTP failures and reports interrupted streams as
/// parser errors, so both forms are normalized here. CRC mismatches, schema
/// errors, and authorization failures are never retried.
OdpsReadErrorKind classifyOdpsReadError(const apsara::odps::sdk::OdpsException & ex);

/// Wrapper around `OdpsException` with a classified error kind.
/// Provides structured error handling instead of string comparison at call sites.
class OdpsReadException : public Exception
{
public:
    OdpsReadException(OdpsReadErrorKind kind_, const apsara::odps::sdk::OdpsException & cause);
    OdpsReadErrorKind kind() const { return error_kind; }

private:
    OdpsReadErrorKind error_kind;
};

/// Wraps `IRecordReader` with bounded reconnection for retryable transport,
/// timeout, and throttling errors. The elapsed-time budget counts failed SDK
/// attempts and retry backoff, but not healthy scan time.
/// Returns false on EOF instead of throwing an exception.
///
/// This class owns only a reloaded download handle and its record reader. The
/// query-scoped owner controls the download session lifecycle.
class AutoReconnectRecordReader
{
public:
    using DownloadFactory = std::function<apsara::odps::sdk::IDownloadPtr()>;

    AutoReconnectRecordReader(
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
        Poco::Logger * log_);

    /// Read one record. Returns false on EOF (no exception thrown for EOF).
    /// On a retryable SDK error, reconnects from the confirmed row boundary.
    /// On unrecoverable errors, throws `OdpsReadException`.
    bool read(apsara::odps::sdk::ODPSTableRecord & record);

    /// Close the borrowed reader handle. The query-scoped session owner is
    /// solely responsible for completing the download session.
    /// Safe to call multiple times. A close failure is propagated on the
    /// explicit success path and logged by the source destructor on cleanup.
    void close();

    /// Accessors used during initialization (called once before any `read`).
    apsara::odps::sdk::IODPSTableSchema * getSchema();
    apsara::odps::sdk::ODPSTableRecordPtr createBufferRecord();

    UInt64 totalReadRows() const { return total_read_rows; }

private:
    void openReader();
    void openReaderWithRetry();
    void prepareRetry(
        const apsara::odps::sdk::OdpsException & ex,
        std::chrono::steady_clock::time_point attempt_started);

    DownloadFactory download_factory;
    apsara::odps::sdk::IDownloadPtr download;
    apsara::odps::sdk::IRecordReaderPtr reader;
    UInt64 start;
    UInt64 count;
    std::vector<std::string> columns;
    bool compress;
    UInt64 max_retries;
    UInt64 retry_initial_backoff_ms;
    UInt64 retry_max_backoff_ms;
    UInt64 retry_max_elapsed_ms;
    std::function<bool()> is_cancelled;
    UInt64 retry_count = 0;
    UInt64 retry_elapsed_ms = 0;
    UInt64 total_read_rows = 0;
    bool eof_verified = false;
    Poco::Logger * log;
};

}

#endif
