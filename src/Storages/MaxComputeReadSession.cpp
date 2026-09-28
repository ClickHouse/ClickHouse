#include <Storages/MaxComputeReadSession.h>

#if USE_ODPS_TUNNEL

#include <odps_clickhouse_adapter.h>

#include <Common/Exception.h>
#include <Common/config_version.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace
{
    int timeoutSeconds(UInt64 milliseconds)
    {
        const UInt64 seconds = std::max<UInt64>(1, milliseconds / 1000 + (milliseconds % 1000 != 0));
        return static_cast<int>(std::min<UInt64>(seconds, std::numeric_limits<int>::max()));
    }

    void throwIfCancelled(const MaxComputeConnectionConfiguration & configuration)
    {
        if (configuration.cancellation_checker && configuration.cancellation_checker())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "MaxCompute download session creation was cancelled");
    }

    apsara::odps::sdk::Configuration makeSDKConfiguration(const MaxComputeConnectionConfiguration & configuration)
    {
        apsara::odps::sdk::Configuration result;
        result.SetAccount(apsara::odps::sdk::Account{
            apsara::odps::sdk::ACCOUNT_ALIYUN,
            configuration.access_key_id,
            configuration.access_key_secret});
        if (!configuration.sts_token.empty())
            result.SetStsToken(apsara::odps::sdk::StsToken(configuration.sts_token));
        result.SetTunnelQuotaName(configuration.quota_name);
        result.SetSocketConnectTimeout(timeoutSeconds(configuration.connect_timeout_ms));
        result.SetSocketTimeout(timeoutSeconds(configuration.request_timeout_ms));
        result.SetUserAgent(apsara::odps::sdk::UserAgent{
            configuration.project + "_" + configuration.table,
            VERSION_STRING});
        return result;
    }

    MaxComputeConnectionConfiguration resolveTunnelConfiguration(MaxComputeConnectionConfiguration configuration)
    {
        throwIfCancelled(configuration);
        if (configuration.odps_endpoint.empty())
        {
            if (configuration.endpoint_validator)
                configuration.endpoint_validator(configuration.tunnel_endpoint);
            if (configuration.resolved_endpoint_observer)
                configuration.resolved_endpoint_observer(configuration.tunnel_endpoint);
            return configuration;
        }

        if (configuration.endpoint_validator)
            configuration.endpoint_validator(configuration.odps_endpoint);
        auto routing_configuration = makeSDKConfiguration(configuration);
        routing_configuration.SetEndpoint(configuration.odps_endpoint);

        /// Resolve routing while the request still targets the already validated
        /// ODPS endpoint. The returned Tunnel endpoint is validated by ClickHouse
        /// before the SDK is allowed to access it. The resolved endpoint is kept
        /// in the query-scoped configuration, so reader handles do not reroute.
        configuration.tunnel_endpoint = apsara::odps::sdk::clickhouse::resolveTunnelEndpoint(
            routing_configuration,
            configuration.project);
        if (configuration.endpoint_validator)
            configuration.endpoint_validator(configuration.tunnel_endpoint);
        if (configuration.resolved_endpoint_observer)
            configuration.resolved_endpoint_observer(configuration.tunnel_endpoint);
        configuration.odps_endpoint.clear();
        throwIfCancelled(configuration);
        return configuration;
    }

    apsara::odps::sdk::IDownloadPtr createTunnelDownload(
        const MaxComputeConnectionConfiguration & configuration,
        const String & download_id)
    {
        throwIfCancelled(configuration);
        if (configuration.endpoint_validator)
            configuration.endpoint_validator(configuration.tunnel_endpoint);
        auto sdk_configuration = makeSDKConfiguration(configuration);
        sdk_configuration.SetTunnelEndpoint(configuration.tunnel_endpoint);

        apsara::odps::sdk::OdpsTunnel tunnel;
        tunnel.Init(sdk_configuration);
        try
        {
            auto download = tunnel.CreateDownload(
                configuration.project,
                configuration.table,
                configuration.partition_spec,
                download_id);
            throwIfCancelled(configuration);
            return download;
        }
        catch (const apsara::odps::sdk::OdpsException &)
        {
            throwIfCancelled(configuration);
            throw;
        }
    }
}

MaxComputeReadSession::MaxComputeReadSession(
    MaxComputeConnectionConfiguration configuration_,
    const String & download_id_)
    : configuration(resolveTunnelConfiguration(std::move(configuration_)))
    , owner_download(createTunnelDownload(configuration, download_id_))
    , download_id(owner_download->GetDownloadId())
    , record_count(owner_download->GetRecordCount())
{
}

MaxComputeReadSession::~MaxComputeReadSession()
{
    finalizeNoThrow();
}

apsara::odps::sdk::IODPSTableSchema * MaxComputeReadSession::getSchema() const
{
    std::lock_guard lock(mutex);
    if (!owner_download)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaxCompute session was finalized before its schema was consumed");
    return owner_download->GetSchema();
}

apsara::odps::sdk::IDownloadPtr MaxComputeReadSession::createReaderDownload() const
{
    {
        std::lock_guard lock(mutex);
        if (finalized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot open a reader for a finalized MaxCompute session");
    }

    return createTunnelDownload(configuration, download_id);
}

void MaxComputeReadSession::initializeTasks(size_t task_count_, UInt64 expected_rows_)
{
    std::lock_guard lock(mutex);
    if (tasks_initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaxCompute session tasks were initialized more than once");

    tasks_initialized = true;
    task_count = task_count_;
    expected_rows = expected_rows_;
}

void MaxComputeReadSession::finishTask(UInt64 task_expected_rows, UInt64 task_actual_rows)
{
    if (task_expected_rows != task_actual_rows)
    {
        throw Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "MaxCompute task returned {} rows, expected exactly {}",
            task_actual_rows,
            task_expected_rows);
    }

    bool should_finalize = false;
    {
        std::lock_guard lock(mutex);
        if (!tasks_initialized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MaxCompute task finished before task initialization");
        if (finished_tasks >= task_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MaxCompute task was reported as finished more than once");

        ++finished_tasks;
        finished_rows += task_actual_rows;
        should_finalize = finished_tasks == task_count;

        if (should_finalize && finished_rows != expected_rows)
        {
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "MaxCompute query returned {} rows, expected exactly {}",
                finished_rows,
                expected_rows);
        }
    }

    if (should_finalize)
        finalize();
}

void MaxComputeReadSession::finalize()
{
    apsara::odps::sdk::IDownloadPtr download;
    {
        std::lock_guard lock(mutex);
        if (finalized)
            return;
        finalized = true;
        download = std::move(owner_download);
    }

    if (download)
        download->Complete();
}

void MaxComputeReadSession::finalizeNoThrow() noexcept
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException("MaxComputeReadSession", "Failed to finalize MaxCompute download session");
    }
}

}

#endif
