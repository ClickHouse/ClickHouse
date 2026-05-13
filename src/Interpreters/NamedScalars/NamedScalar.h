#pragma once

#include <Interpreters/NamedScalars/INamedScalarValueBackend.h>
#include <Interpreters/NamedScalars/NamedScalarDefinitionParse.h>
#include <Interpreters/NamedScalars/NamedScalarValueCodec.h>
#include <Interpreters/Context_fwd.h>

#include <Common/Logger.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>

namespace DB
{

class NamedScalar;
class QueryStatus;
using NamedScalarPtr = std::shared_ptr<NamedScalar>;

/// Per-scalar lifecycle owner.
///
/// What it owns:
///   * a parsed, immutable definition (UUID + expression + definer +
///     optional refresh period);
///   * the current cached value;
///   * a single refresh task that evaluates SELECT under the DEFINER
///     and publishes via the value backend;
///   * an atomic reload flag flipped by the backend's value-watch
///     callback (peer published a fresh value) and drained at task entry.
///
/// What it does NOT own:
///   * SQL parsing - manager builds ParsedDefinition via
///     NamedScalarDefinitionParse and hands it in;
///   * definition persistence - manager's definition_store does that;
///   * UUID minting - manager extracts/generates the UUID before construction.
///
/// Definition lifecycle: definitions are immutable. CREATE OR REPLACE
/// produces a NEW NamedScalar with a NEW UUID; the manager swaps it
/// into the catalog map and shuts down the old one. There is no
/// in-place reconcile path here.
///
/// Concurrency:
///   * `current_value_mutex` guards the cached value (RW lock; readers
///     copy out a shared_ptr and drop the lock immediately).
///   * `publish_mutex` serialises every publish: refresh task body,
///     watch-driven reload, initial evaluate. Holds across encode/store
///     and the post-publish current_value swap.
///   * `refresh_mutex` guards the refresh runtime + the task holder.
///     Task holders are always moved out from under it before
///     destruction (joining-while-locked deadlocks against the body's
///     own exit path).
///   * `live` + task destruction in shutdown() fences DROP / OR REPLACE
///     against in-flight refresh: the holder's destructor joins, the
///     task body checks `live` and returns early on subsequent runs.
class NamedScalar : public std::enable_shared_from_this<NamedScalar>
{
public:
    struct RefreshSnapshot
    {
        bool refreshable = false;
        UInt64 period_seconds = 0;
        std::optional<std::chrono::system_clock::time_point> next_refresh_time;
        std::optional<std::chrono::system_clock::time_point> refresh_started_at;
        UInt64 consecutive_failures = 0;
    };

    struct CurrentValue
    {
        Field value;
        DataTypePtr type;
        bool is_valid = false;
    };

    struct Info
    {
        std::optional<CurrentValue> value;
        RefreshSnapshot refresh;
        std::chrono::system_clock::time_point loading_start_time;
        std::optional<std::chrono::system_clock::time_point> last_refresh_time;
        std::optional<std::chrono::system_clock::time_point> last_success_time;
        ASTPtr expression;
        String definer;
        String last_refresh_hostname;
        String last_error;
        String last_error_type;
    };

    NamedScalar(ParsedDefinition definition, INamedScalarValueBackend & value_backend);
    ~NamedScalar();

    NamedScalar(const NamedScalar &) = delete;
    NamedScalar & operator=(const NamedScalar &) = delete;

    /// Initial-value paths. Manager calls exactly one of these before
    /// `start()`:
    ///   * `evaluateAndStoreValue` - local CREATE: run SELECT under
    ///     DEFINER, encode, publish via backend lease. Throws on any
    ///     failure (missing definer, lease unavailable, SELECT error,
    ///     publish race lost) - caller rolls back the CREATE.
    ///   * `loadValueFromBackend` - restart-from-store / peer-CREATE
    ///     seen via watch: read the existing blob and arm the data-watch.
    ///     Returns silently if the blob is missing (refresh task will
    ///     populate). Logs and reschedules the refresh task on transient
    ///     backend errors; never throws past the catch.
    ///     Also used internally on watch-fire to drain `reload_requested`.
    void evaluateAndStoreValue(const ContextPtr & context);
    void loadValueFromBackend();

    /// Wire the refresh task and schedule it. After `start()` the
    /// scalar runs autonomously: refresh on schedule, reload on
    /// backend value-watch fire.
    void start(const ContextPtr & context);

    /// Fence DROP/OR-REPLACE against in-flight tasks. The caller's
    /// subsequent durable removal happens only after any running
    /// publish has returned. Must run on a non-schedule-pool thread.
    void shutdown();

    /// Throws NAMED_SCALAR_NOT_REFRESHABLE if no REFRESH clause.
    void requestRefreshNow();
    void setRefreshPaused(bool paused);
    bool isRefreshable() const;

    /// Refresh-body in-flight cancellation hooks (set by executeScalarSelect
    /// around the running PipelineExecutor; cancelled from shutdown()).
    void setInFlight(std::weak_ptr<QueryStatus> handle) const;
    void clearInFlight() const;
    void cancelInFlight();

    /// Non-throwing peek for callers that have their own fallback
    /// (`getNamedScalarOrDefault`, `system.named_scalars`).
    std::optional<CurrentValue> tryGetValue() const;

    /// Snapshot for system.named_scalars.
    Info getInfo() const;

    const String & getName() const { return definition.name; }
    const String & getUUID() const { return definition.uuid; }

private:
    using StoredValuePtr = std::shared_ptr<const StoredValue>;

    struct RefreshRuntime
    {
        explicit RefreshRuntime(UInt64 period_seconds_) : period_seconds(period_seconds_) {}

        UInt64 period_seconds = 0;
        /// Schedule anchor; next tick fires at last_completed + period.
        std::chrono::sys_seconds last_completed_timeslot;
        /// Bumped on failure, reset on success. Surfaced via
        /// system.named_scalars; not used for refresh scheduling.
        UInt64 consecutive_failures = 0;
        std::chrono::system_clock::time_point next_refresh_time;
        std::optional<std::chrono::system_clock::time_point> refresh_started_at;
        bool paused = false;
        bool out_of_schedule_refresh_requested = false;
    };

    struct RefreshRunResult
    {
        bool published = false;
        String error_message;
        String error_type;
    };

    struct EvaluatedSnapshot
    {
        StoredValuePtr value;
        String payload;
    };

    enum class RefreshActionKind : UInt8
    {
        /// Nothing to do this tick (populate-only with already-populated
        /// value, or refresh paused without override).
        Skip,
        /// Run a refresh evaluation now (out-of-schedule or due).
        EvaluateNow,
        /// Wait until `when` then re-enter the task.
        WaitUntil,
    };

    struct RefreshAction
    {
        RefreshActionKind kind = RefreshActionKind::Skip;
        std::chrono::system_clock::time_point when;
        std::chrono::sys_seconds timeslot;
        bool out_of_schedule = false;
    };

    StoredValuePtr loadCurrentValue() const;
    void storeCurrentValue(StoredValuePtr ptr);

    /// Build definer context, run SELECT, wrap into StoredValue, encode
    /// to wire-format and check size. Throws on definer-missing,
    /// SELECT failure, or oversize blob.
    EvaluatedSnapshot evaluateAndEncode(const ContextPtr & context) const;

    RefreshSnapshot snapshotRefresh() const;
    void deactivate();

    /// Backend value-watch fire. Sets `reload_requested` and schedules
    /// the refresh task. Cheap enough to run on the Keeper IO thread.
    void onValueChanged();

    /// Refresh task body. Drains reload_requested, then decides + executes.
    void runTask(const ContextPtr & context);

    /// Decide what to do under refresh_mutex; returns a value object the
    /// caller acts on without holding any lock.
    RefreshAction decideRefreshAction();

    /// Acquire lease, evaluate, publish, record outcome. Re-enters
    /// refresh_mutex internally; never holds it across I/O.
    void executeRefreshAction(const ContextPtr & context, RefreshAction action);

    std::optional<NamedScalarRefreshLease> acquireLeaseOrWait(std::chrono::sys_seconds timeslot);
    RefreshRunResult evaluateAndPublishUnderLease(
        const ContextPtr & context,
        StoredValuePtr previous,
        NamedScalarRefreshLease & refresh_lease);

    /// Create the refresh task. Caller holds `refresh_mutex`. Wired
    /// exactly once by `start()`; never re-wired in this scalar's life.
    void wireTaskLocked(const ContextPtr & context);

    void planNextRefreshLocked(std::chrono::system_clock::time_point now,
                               bool out_of_schedule,
                               std::chrono::system_clock::time_point & when,
                               std::chrono::sys_seconds & timeslot);
    void recordSkipByPeerLocked(std::chrono::sys_seconds timeslot);
    void recordRefreshOutcomeLocked(bool succeeded,
                                    bool out_of_schedule,
                                    std::chrono::sys_seconds timeslot);

    const ParsedDefinition definition;
    INamedScalarValueBackend & value_backend;

    mutable std::shared_mutex current_value_mutex;
    StoredValuePtr current_value;

    std::atomic<bool> live{true};

    /// Set by the backend value-watch callback (peer wrote a fresh value).
    /// Drained by `runTask` at entry: if set, reload from the backend
    /// before doing the regular refresh-due check.
    std::atomic<bool> reload_requested{false};

    /// True iff a Keeper data-watch on the value znode is currently
    /// registered. Set when we register, cleared by the watch callback
    /// when it fires (Keeper watches are one-shot). Spurious false ⇒
    /// next loadValueFromBackend re-arms; spurious true is impossible
    /// because we never set it after the registration completes.
    std::atomic<bool> value_watch_active{false};

    mutable std::mutex publish_mutex;

    mutable std::mutex refresh_mutex;
    std::optional<RefreshRuntime> refresh;
    BackgroundSchedulePoolTaskHolder task;

    /// Handle to the in-flight refresh query, if any. Set while the
    /// refresh body executes; cleared on completion. Used by
    /// cancelInFlight() to interrupt a running SELECT on shutdown /
    /// DROP / OR REPLACE.
    mutable std::mutex in_flight_mutex;
    mutable std::weak_ptr<QueryStatus> in_flight_query;
};

}
