#pragma once

#include <Client/ConnectionParameters.h>
#include <Client/IServerConnection.h>
#include <Common/Exception.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context_fwd.h>
#include <IO/CompressionMethod.h>
#include <base/types.h>

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

/// Owns the independent client sessions used by interactive background jobs.
/// A terminal job is retained, together with its spooled output, until it is
/// consumed by foreground() or the manager is destroyed.
class BackgroundQueryManager
{
public:
    using JobId = UInt64;

    /// Identifies a foreground query while it is still attached. Attached
    /// handles live in a separate namespace from public job IDs: a query which
    /// finishes in the foreground must not consume a number visible in \jobs.
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

    /** Start a foreground query on a worker, transferring the caller's current
      * connection to it. Query result bytes initially go directly to
      * `foreground_output`. If requestDetach() is acknowledged at a safe point
      * in the worker's receive loop, subsequent result bytes go to the job's
      * anonymous spool and the job is assigned a public ID.
      *
      * The caller must keep `foreground_output` alive until the query either
      * becomes terminal or promoteDetached() returns a public job ID.
      */
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

    /// Ask the worker to redirect future result bytes to its spool. The request
    /// is nonblocking; waitAttached() reports when the receive loop has
    /// acknowledged the handoff.
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
