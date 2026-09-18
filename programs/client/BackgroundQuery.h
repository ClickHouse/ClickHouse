#pragma once

#include <Client/ConnectionParameters.h>
#include <Client/IServerConnection.h>
#include <Core/QueryProcessingStage.h>
#include <IO/CompressionMethod.h>
#include <IO/Progress.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>
#include <Common/Exception.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <atomic>
#include <chrono>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

class WriteBuffer;

/// Owns client sessions and spooled output retained for interactive background jobs.
class BackgroundQueryManager
{
public:
    using JobId = UInt64;

    /// Hidden handle for an attached query that must not consume a public job ID.
    class AttachedHandle
    {
    public:
        AttachedHandle() = default;
        explicit operator bool() const { return value != 0; }
        bool operator==(const AttachedHandle &) const = default;

    private:
        explicit AttachedHandle(UInt64 value_) : value(value_) { }

        UInt64 value = 0;
        friend class BackgroundQueryManager;
    };

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
        struct Metrics
        {
            ProgressValues progress;
            double cpu_usage = 0;
            bool cpu_usage_available = false;
            bool cpu_usage_is_average = false;
            UInt64 memory_usage = 0;
            UInt64 max_host_memory_usage = 0;
            Int64 peak_memory_usage = -1;
            UInt64 temporary_data_on_disk = 0;
            UInt64 max_host_temporary_data_on_disk = 0;
        };

        JobId id = 0;
        String query_id;
        String query;
        State state = State::Starting;
        std::chrono::milliseconds elapsed{0};
        UInt64 spool_bytes = 0;
        String output_format;
        bool output_is_tty_friendly = true;
        String error;
        Metrics metrics;
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

    enum class AttachedWaitStatus : UInt8
    {
        Running,
        DetachAcknowledged,
        Terminal,
    };

    struct AttachedResult
    {
        ServerConnectionPtr connection;
        bool connection_needs_resynchronization = false;
        std::unique_ptr<Exception> server_exception;
        std::unique_ptr<Exception> client_exception;
        JobInfo job;
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

    /// Starts on the caller's connection and output until detachment is acknowledged.
    /// Keep `foreground_output` alive until terminal completion or promotion.
    AttachedHandle startAttached(
        String query,
        String display_query,
        Snapshot snapshot,
        ServerConnectionPtr & connection,
        WriteBuffer & foreground_output,
        std::ostream & foreground_output_stream,
        std::ostream & foreground_error_stream,
        String server_logs_file,
        UInt64 server_revision,
        String server_version,
        bool stdout_is_a_tty,
        bool stderr_is_a_tty,
        uint16_t terminal_width,
        int foreground_stderr_fd,
        int foreground_tty_fd,
        bool render_progress,
        bool render_progress_table,
        bool progress_table_toggle_enabled,
        const std::atomic_bool & progress_table_toggle_on);

    /// Wait at most `timeout` for detachment to be acknowledged or for the
    /// attached query to become terminal.
    AttachedWaitStatus waitAttached(AttachedHandle handle, std::chrono::milliseconds timeout) const;

    /// Requests a nonblocking redirect; waitAttached() reports its acknowledgement.
    void requestDetach(AttachedHandle handle);

    /// Move an acknowledged detached query into the public job list. Returns
    /// nullopt if the worker has not acknowledged the handoff yet.
    std::optional<JobId> promoteDetached(AttachedHandle handle);

    /// Request cancellation of a non-terminal attached query. Returns false if
    /// the handle does not exist or the query is already terminal.
    bool cancelAttached(AttachedHandle handle);

    /// Exceptional-path cleanup for a handle which the caller can no longer
    /// supervise. Cancels and joins the worker before removing it.
    void discardAttached(AttachedHandle handle) noexcept;

    /// Consume a terminal query which never detached, replay its summary and
    /// diagnostics, and return its connection to the interactive client.
    AttachedResult collectAttached(AttachedHandle handle, WriteBuffer & output, WriteBuffer & diagnostics);

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
