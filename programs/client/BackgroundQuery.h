#pragma once

#include <Client/ConnectionParameters.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context_fwd.h>
#include <IO/CompressionMethod.h>
#include <base/types.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

class WriteBuffer;

/// Owns the independent client sessions used by interactive background jobs.
/// A terminal job is retained, together with its spooled output, until it is
/// consumed by foreground() or the manager is destroyed.
class BackgroundQueryManager
{
public:
    using JobId = UInt64;

    enum class State : UInt8
    {
        Starting,
        Running,
        Succeeded,
        Failed,
        Cancelled,
    };

    struct Snapshot
    {
        Snapshot(
            ContextPtr context_,
            const ConnectionParameters & connection_parameters_,
            const Poco::Util::AbstractConfiguration & configuration_,
            String default_database_,
            size_t max_client_network_bandwidth_ = 0,
            String client_local_timezone_ = {},
            String default_output_format_ = {},
            bool is_default_format_ = true,
            CompressionMethod default_output_compression_method_ = CompressionMethod::None,
            bool has_vertical_output_suffix_ = false,
            bool inline_insert_data_ = false,
            bool allow_merge_tree_settings_ = false,
            QueryProcessingStage::Enum query_processing_stage_ = QueryProcessingStage::Enum::Complete,
            ClientInfo::QueryKind query_kind_ = ClientInfo::QueryKind::INITIAL_QUERY);

        ContextPtr context;
        ConnectionParameters connection_parameters;
        Poco::AutoPtr<Poco::Util::AbstractConfiguration> configuration;
        String default_database;
        size_t max_client_network_bandwidth = 0;
        String client_local_timezone;
        String default_output_format;
        bool is_default_format = true;
        CompressionMethod default_output_compression_method = CompressionMethod::None;
        bool has_vertical_output_suffix = false;
        bool inline_insert_data = false;
        bool allow_merge_tree_settings = false;
        QueryProcessingStage::Enum query_processing_stage = QueryProcessingStage::Enum::Complete;
        ClientInfo::QueryKind query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    };

    struct JobInfo
    {
        JobId id = 0;
        String query_id;
        String query;
        State state = State::Starting;
        std::chrono::milliseconds elapsed{0};
        UInt64 spool_bytes = 0;
        String output_format;
        bool output_is_tty_friendly = true;
        String error;
    };

    enum class ForegroundStatus : UInt8
    {
        NotFound,
        Running,
        UnsafeOutput,
        Replayed,
    };

    struct ForegroundResult
    {
        ForegroundStatus status = ForegroundStatus::NotFound;
        std::optional<JobInfo> job;
    };

    enum class CancelStatus : UInt8
    {
        NotFound,
        Requested,
        Discarded,
    };

    BackgroundQueryManager();
    ~BackgroundQueryManager();

    BackgroundQueryManager(const BackgroundQueryManager &) = delete;
    BackgroundQueryManager & operator=(const BackgroundQueryManager &) = delete;

    /// Starts one query using a context copy and a dedicated TCP connection.
    JobId start(String query, String display_query, Snapshot snapshot);

    std::vector<JobInfo> list() const;
    std::optional<JobInfo> get(JobId id) const;

    /// Never waits for an active query. For a terminal query, replays its
    /// result and diagnostic spools and then removes it from the manager.
    ForegroundResult foreground(JobId id, WriteBuffer & output, WriteBuffer & diagnostics, bool allow_unsafe_output = false);

    /// Requests cancellation and returns without waiting for a running worker.
    /// A terminal job is removed immediately and its spools are discarded.
    CancelStatus cancel(JobId id);

    /// Cancels and joins every retained job. Safe to call more than once.
    void shutdown();

    static std::string_view stateName(State state);

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
