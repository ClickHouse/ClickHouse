#pragma once

#include "config.h"

#if USE_ODPS_TUNNEL

#include <odps_tunnel.h>

#include <Core/Types.h>

#include <functional>
#include <memory>
#include <mutex>
#include <utility>

namespace DB
{

struct MaxComputeConnectionConfiguration
{
    String tunnel_endpoint;
    String odps_endpoint;
    String project;
    String table;
    String partition_spec;
    String access_key_id;
    String access_key_secret;
    String sts_token;
    String quota_name;
    UInt64 connect_timeout_ms = 10000;
    UInt64 request_timeout_ms = 300000;
    std::function<void(const String &)> endpoint_validator;
    std::function<void(const String &)> resolved_endpoint_observer;
    std::function<bool()> cancellation_checker;
};

/// Owns the one Tunnel download session used by a query. Sources only receive
/// reloaded handles and never call the session-level `Complete` operation.
class MaxComputeReadSession final
{
public:
    explicit MaxComputeReadSession(
        MaxComputeConnectionConfiguration configuration_,
        const String & download_id = {});
    ~MaxComputeReadSession();

    MaxComputeReadSession(const MaxComputeReadSession &) = delete;
    MaxComputeReadSession & operator=(const MaxComputeReadSession &) = delete;

    UInt64 getRecordCount() const { return record_count; }
    const String & getDownloadId() const { return download_id; }
    apsara::odps::sdk::IODPSTableSchema * getSchema() const;

    apsara::odps::sdk::IDownloadPtr createReaderDownload() const;

    void initializeTasks(size_t task_count_, UInt64 expected_rows_);
    void finishTask(UInt64 expected_rows, UInt64 actual_rows);
    void finalize();

private:
    void finalizeNoThrow() noexcept;

    const MaxComputeConnectionConfiguration configuration;
    mutable std::mutex mutex;
    apsara::odps::sdk::IDownloadPtr owner_download;
    String download_id;
    UInt64 record_count = 0;
    size_t task_count = 0;
    size_t finished_tasks = 0;
    UInt64 expected_rows = 0;
    UInt64 finished_rows = 0;
    bool tasks_initialized = false;
    bool finalized = false;
};

using MaxComputeReadSessionPtr = std::shared_ptr<MaxComputeReadSession>;

}

#endif
