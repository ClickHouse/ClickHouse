#include <Interpreters/NamedScalars/NamedScalar.h>
#include <Interpreters/NamedScalars/NamedScalarsManager.h>

#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Columns/IColumn.h>
#include <Common/CurrentMetrics.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <base/scope_guard.h>
#include <base/getFQDNOrHostName.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Block.h>

#include <DataTypes/IDataType.h>

#include <Common/CurrentThread.h>
#include <Common/QueryScope.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/executeQuery.h>

#include <Parsers/ASTFunction.h>

#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>
#include <utility>

namespace ProfileEvents
{
    extern const Event NamedScalarRefreshAttempts;
    extern const Event NamedScalarRefreshSuccesses;
    extern const Event NamedScalarRefreshFailures;
    extern const Event NamedScalarRefreshSkippedByPeer;
    extern const Event NamedScalarRefreshDurationMicroseconds;
}

namespace DB
{

constexpr size_t REFRESH_LEASE_BUSY_RETRY_MS = 5000;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
    extern const int NAMED_SCALAR_NOT_REFRESHABLE;
}

namespace
{

void assertNoNamedScalarAccess(const ASTPtr & expression)
{
    static constexpr std::string_view forbidden_names[] = {
        "getNamedScalar",
        "getNamedScalarOrDefault",
    };
    if (!expression)
        return;

    if (const auto * function = expression->as<ASTFunction>())
    {
        for (const auto & forbidden : forbidden_names)
        {
            if (function->name == forbidden)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Named scalar definition cannot reference function {}()",
                    function->name);
        }
    }

    for (const auto & child : expression->children)
        assertNoNamedScalarAccess(child);
}

ContextMutablePtr createEvaluationContext(const ContextPtr & context)
{
    auto eval_context = Context::createCopy(context);
    eval_context->makeQueryContext();
    eval_context->setCurrentQueryId({});
    eval_context->setProcessListElement({});
    eval_context->setInternalQuery(true);
    return eval_context;
}

ContextMutablePtr buildDefinerEvalContext(const ContextPtr & global_context, const String & name, const String & definer)
{
    if (definer.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "named scalar '{}': missing definer in persisted definition",
            name);

    auto & access_control = global_context->getAccessControl();
    const UUID definer_id = access_control.getID<User>(definer);

    auto ctx = Context::createCopy(global_context);
    ctx->makeQueryContext();
    ctx->setInternalQuery(true);
    ctx->setUser(definer_id);
    return ctx;
}

std::pair<Field, DataTypePtr> executeScalarSelect(
    const ASTPtr & select_query,
    const ContextMutablePtr & context,
    const NamedScalar * owner)
{
    /// Route through executeQuery so the refresh body shows up in
    /// system.processes (KILL QUERY support) and system.query_log
    /// (audit / perf), and so the running PipelineExecutor honors
    /// QueryStatus::cancelQuery() driven by NamedScalar::cancelInFlight().
    /// QueryScope attaches a per-thread ThreadGroup that ProcessList::insert
    /// requires. Synchronous CREATE-time evaluation runs on a client thread
    /// that already has a group; only the background-pool refresh path
    /// needs us to attach.
    std::optional<QueryScope> query_scope;
    if (!CurrentThread::getGroup())
        query_scope.emplace(QueryScope::create(context));
    auto io = executeQuery(select_query->formatWithSecretsOneLine(), context, QueryFlags{.internal = true}).second;

    /// Publish the in-flight QueryStatus before pulling — cancellation
    /// triggers between executeQuery and the first pull must reach this
    /// query. Owner is null only on the synchronous CREATE-time eval,
    /// where there is no running NamedScalar to attach to.
    if (owner)
    {
        if (auto status = context->getProcessListElement())
            owner->setInFlight(status);
    }
    SCOPE_EXIT({ if (owner) owner->clearInFlight(); });

    /// Pull the result and finalize the BlockIO so the QueryFinish/Exception
    /// row is written to system.query_log (the finish_callbacks are wired
    /// inside executeQuery; they fire from io.onFinish / onException).
    Block block;
    Block next_block;
    try
    {
        PullingPipelineExecutor executor(io.pipeline);
        while (block.rows() == 0 && executor.pull(block)) {}

        if (block.rows() == 0)
            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar query returned empty result");
        if (block.rows() != 1)
            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar query returned more than one row");

        while (next_block.rows() == 0 && executor.pull(next_block)) {}
        if (next_block.rows() != 0)
            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar query returned more than one row");

        block = materializeBlock(block);
        if (block.columns() != 1)
            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar query returned more than one column");
    }
    catch (...)
    {
        io.onException();
        throw;
    }
    io.onFinish();

    const auto & column_with_type = block.getByPosition(0);
    Field field;
    column_with_type.column->get(0, field);
    return {std::move(field), column_with_type.type};
}

struct EvaluatedExpression { Field value; DataTypePtr type; };

EvaluatedExpression evaluateExpression(
    const ASTPtr & expression,
    const ContextPtr & context,
    const NamedScalar * owner)
{
    assertNoNamedScalarAccess(expression);
    auto eval_context = createEvaluationContext(context);
    auto [value, type] = executeScalarSelect(expression, eval_context, owner);
    return {std::move(value), std::move(type)};
}

std::chrono::system_clock::time_point nextRefreshTime(
    UInt64 period_seconds,
    std::chrono::system_clock::time_point last_completed)
{
    return last_completed + std::chrono::seconds(period_seconds);
}

}

NamedScalar::NamedScalar(ParsedDefinition definition_, INamedScalarValueBackend & value_backend_)
    : definition(std::move(definition_))
    , value_backend(value_backend_)
{
    if (definition.refresh_period_seconds)
        refresh.emplace(*definition.refresh_period_seconds);
}

NamedScalar::~NamedScalar()
{
    /// Move the task holder out from under refresh_mutex; its destructor
    /// (deactivate + join) must not run while holding a catalog mutex.
    BackgroundSchedulePoolTaskHolder local_task;
    {
        std::lock_guard lock(refresh_mutex);
        local_task = std::move(task);
    }
}

NamedScalar::StoredValuePtr NamedScalar::loadCurrentValue() const
{
    std::shared_lock lock(current_value_mutex);
    return current_value;
}

void NamedScalar::storeCurrentValue(NamedScalar::StoredValuePtr ptr)
{
    std::unique_lock lock(current_value_mutex);
    current_value = std::move(ptr);
}

NamedScalar::EvaluatedSnapshot NamedScalar::evaluateAndEncode(const ContextPtr & context) const
{
    auto eval_context = buildDefinerEvalContext(context, definition.name, definition.definer);

    auto evaluated = evaluateExpression(definition.expression, eval_context, this);

    auto value = StoredValue::fromEvaluationSuccess(
        evaluated.type,
        std::move(evaluated.value),
        getFQDNOrHostName(),
        std::chrono::system_clock::now());
    String payload = encodeNamedScalarValueAndCheckSize(*value, context);

    return {std::move(value), std::move(payload)};
}

void NamedScalar::evaluateAndStoreValue(const ContextPtr & context)
{
    auto lease = value_backend.tryAcquireRefreshLease(definition.name, definition.uuid);
    if (!lease)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Could not acquire refresh lease for named scalar '{}' "
            "(another node may be creating the same scalar concurrently)",
            definition.name);

    auto snapshot = evaluateAndEncode(context);

    std::lock_guard publish_lock(publish_mutex);
    if (lease->publish(snapshot.payload) != RefreshPublishResult::Published)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Could not publish initial value for named scalar '{}' (lease publish lost the race)",
            definition.name);

    storeCurrentValue(std::move(snapshot.value));
}

void NamedScalar::start(const ContextPtr & context)
{
    /// Soft-bail if already shut down. Reachable when a watcher reconcile
    /// installs a replacement (and shuts down this scalar) between our
    /// caller's `swapScalar` and `start()` — expected race in the watcher
    /// path, not a bug. The replacement's own `start()` will run and
    /// own the slot.
    if (!live.load(std::memory_order_acquire))
        return;
    {
        std::lock_guard refresh_lock(refresh_mutex);
        if (refresh)
        {
            /// Seed the schedule anchor from the most recent successful
            /// publish so refresh-on-restart lands at the next aligned
            /// tick instead of immediately. If we have no value yet,
            /// anchor to "now" so the first tick fires after one period.
            auto value = loadCurrentValue();
            auto seed = (value && value->is_valid())
                ? value->last_successful_update_time
                : std::chrono::system_clock::now();
            refresh->last_completed_timeslot = std::chrono::floor<std::chrono::seconds>(seed);
            refresh->next_refresh_time = nextRefreshTime(refresh->period_seconds, refresh->last_completed_timeslot);
        }
        wireTaskLocked(context);
        task->schedule();
    }

    /// Arm the backend value watch (idempotent re-arm if Manager already
    /// called loadValueFromBackend). Without this, a local-CREATE'd
    /// scalar would not pick up subsequent peer publishes against its
    /// own UUID until its first scheduled refresh tick.
    if (value_backend.supportsValueWatches())
        loadValueFromBackend();
}

void NamedScalar::onValueChanged()
{
    /// Runs on the Keeper IO thread. Must remain non-blocking: flag the
    /// reload, kick the refresh task to drain it.
    reload_requested.store(true, std::memory_order_release);
    std::lock_guard refresh_lock(refresh_mutex);
    if (task && live.load(std::memory_order_acquire))
        task->schedule();
}

void NamedScalar::shutdown()
{
    live.store(false);
    /// Interrupt the running SELECT (if any) before joining the task,
    /// so the holder destructor doesn't block on the definer's
    /// max_execution_time.
    cancelInFlight();
    deactivate();
}

void NamedScalar::setInFlight(std::weak_ptr<QueryStatus> handle) const
{
    std::lock_guard lock(in_flight_mutex);
    /// Invariant: BackgroundSchedulePoolTaskInfo::execute holds exec_mutex
    /// across the whole function() call, so a scalar's task never runs
    /// concurrently with itself. The synchronous CREATE-time eval doesn't
    /// reach setInFlight (owner is null on that path).
    chassert(in_flight_query.expired() && "setInFlight called while previous refresh is still in flight");
    in_flight_query = std::move(handle);
}

void NamedScalar::clearInFlight() const
{
    std::lock_guard lock(in_flight_mutex);
    in_flight_query.reset();
}

void NamedScalar::cancelInFlight()
{
    std::shared_ptr<QueryStatus> status;
    {
        std::lock_guard lock(in_flight_mutex);
        status = in_flight_query.lock();
    }
    if (status)
        status->cancelQuery(CancelReason::CANCELLED_BY_USER);
}

void NamedScalar::deactivate()
{
    BackgroundSchedulePoolTaskHolder old;
    {
        std::lock_guard refresh_lock(refresh_mutex);
        old = std::move(task);
    }
}

void NamedScalar::requestRefreshNow()
{
    std::lock_guard refresh_lock(refresh_mutex);
    if (!refresh)
        throw Exception(
            ErrorCodes::NAMED_SCALAR_NOT_REFRESHABLE,
            "named scalar '{}' is not refreshable",
            definition.name);
    refresh->out_of_schedule_refresh_requested = true;
    if (task)
        task->schedule();
}

void NamedScalar::setRefreshPaused(bool paused)
{
    std::lock_guard refresh_lock(refresh_mutex);
    if (!refresh)
        return;
    refresh->paused = paused;
    if (!paused && task)
        task->schedule();
}

bool NamedScalar::isRefreshable() const
{
    std::lock_guard refresh_lock(refresh_mutex);
    return refresh.has_value();
}

std::optional<NamedScalar::CurrentValue> NamedScalar::tryGetValue() const
{
    auto value = loadCurrentValue();
    if (!value || !value->has_value())
        return std::nullopt;
    return CurrentValue{
        .value = value->value,
        .type = value->type,
        .is_valid = value->is_valid(),
    };
}

NamedScalar::Info NamedScalar::getInfo() const
{
    Info info;
    info.refresh = snapshotRefresh();
    info.loading_start_time = definition.load_time;
    info.expression = definition.expression;
    info.definer = definition.definer;

    auto value = loadCurrentValue();
    if (value)
    {
        if (value->has_value())
        {
            info.value = CurrentValue{
                .value = value->value,
                .type = value->type,
                .is_valid = value->is_valid(),
            };
        }
        info.last_refresh_time = value->last_update_time;
        if (value->last_successful_update_time.time_since_epoch().count() > 0)
            info.last_success_time = value->last_successful_update_time;
        info.last_refresh_hostname = value->last_update_hostname;
        info.last_error = value->last_error;
        info.last_error_type = value->last_error_type;
    }
    return info;
}

NamedScalar::RefreshSnapshot NamedScalar::snapshotRefresh() const
{
    std::lock_guard refresh_lock(refresh_mutex);
    RefreshSnapshot snap;
    if (refresh)
    {
        snap.refreshable = true;
        snap.period_seconds = refresh->period_seconds;
        snap.next_refresh_time = refresh->next_refresh_time;
        snap.refresh_started_at = refresh->refresh_started_at;
        snap.consecutive_failures = refresh->consecutive_failures;
    }
    return snap;
}

void NamedScalar::wireTaskLocked(const ContextPtr & context)
{
    chassert(!task);  // Wired exactly once by start(); never re-wired.

    /// The single task handles refresh ticks AND drains `reload_requested`.
    /// Even non-refreshable scalars get one because shared-cache scalars
    /// still need to react to peer-driven value-watch fires.
    const auto storage_id = StorageID("", fmt::format("named_scalar.{}", definition.name));

    std::weak_ptr<NamedScalar> weak_self = weak_from_this();
    task = context->getNamedScalarRefreshPool().createTask(storage_id, "NamedScalarRefresh",
        [context, weak_self]
        {
            if (auto self = weak_self.lock())
                self->runTask(context);
        });
}

void NamedScalar::planNextRefreshLocked(
    std::chrono::system_clock::time_point now,
    bool out_of_schedule,
    std::chrono::system_clock::time_point & when,
    std::chrono::sys_seconds & timeslot)
{
    /// Equal-cadence schedule: next tick is `last_completed + period`.
    const auto last_completed = std::chrono::sys_seconds(refresh->last_completed_timeslot);
    when = nextRefreshTime(refresh->period_seconds, last_completed);
    timeslot = std::chrono::floor<std::chrono::seconds>(when);
    if (out_of_schedule)
        when = now;
    refresh->next_refresh_time = when;
}

void NamedScalar::recordSkipByPeerLocked(std::chrono::sys_seconds timeslot)
{
    /// Mark this tick completed locally so we advance to the NEXT tick;
    /// without this, losing the per-tick lock would loop us straight
    /// back into the same timeslot, defeating one-leader-per-tick.
    refresh->last_completed_timeslot = timeslot;
}

void NamedScalar::recordRefreshOutcomeLocked(
    bool succeeded,
    bool out_of_schedule,
    std::chrono::sys_seconds timeslot)
{
    if (succeeded)
        refresh->consecutive_failures = 0;
    else
        refresh->consecutive_failures += 1;

    /// Advance schedule anchor on success OR failure - otherwise a
    /// failure leaves `now >= next` and the next tick fires immediately,
    /// producing a tight retry loop. Manual SYSTEM REFRESH (out_of_schedule)
    /// is additive: it does NOT advance the anchor, so the next
    /// scheduled tick still happens.
    if (!out_of_schedule)
        refresh->last_completed_timeslot = timeslot;
}

void NamedScalar::loadValueFromBackend()
{
    try
    {
        if (!live.load(std::memory_order_acquire))
            return;

        std::optional<String> value_blob;
        if (value_watch_active.load(std::memory_order_acquire))
        {
            /// A data-watch is already registered for our value znode; just
            /// refresh our snapshot without re-arming. Saves a Keeper round
            /// trip on the redundant start()-after-loadValueFromBackend
            /// path used by the watch-driven peer-CREATE / restart flows.
            value_blob = value_backend.readValueBlob(definition.uuid);
        }
        else
        {
            /// Claim the right to arm BEFORE registering. If the callback
            /// races with us and clears the flag while we're inside
            /// readValueBlobAndWatch, the next loadValueFromBackend will
            /// re-arm. Setting the flag AFTER would leave us with the flag
            /// true and no watch (callback already fired and cleared).
            /// Rollback on throw.
            value_watch_active.store(true, std::memory_order_release);
            try
            {
                value_blob = value_backend.readValueBlobAndWatch(
                    definition.uuid,
                    [weak_self = weak_from_this()]
                    {
                        if (auto self = weak_self.lock())
                        {
                            self->value_watch_active.store(false, std::memory_order_release);
                            self->onValueChanged();
                        }
                    });
            }
            catch (...)
            {
                value_watch_active.store(false, std::memory_order_release);
                throw;
            }
        }

        if (!value_blob)
            return;

        auto snapshot = tryDecodeNamedScalarValueBlob(*value_blob, definition.name, getLogger("NamedScalar"));
        if (!snapshot)
            return;
        auto value = std::make_shared<StoredValue>(*snapshot);

        std::lock_guard publish_lock(publish_mutex);
        if (live.load(std::memory_order_acquire))
            storeCurrentValue(std::move(value));
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("NamedScalar"),
            fmt::format("reloading persisted value for named scalar '{}'", definition.name));
        reload_requested.store(true, std::memory_order_release);
        std::lock_guard refresh_lock(refresh_mutex);
        if (task && live.load(std::memory_order_acquire))
            task->scheduleAfter(1000);
    }
}

std::optional<NamedScalarRefreshLease> NamedScalar::acquireLeaseOrWait(std::chrono::sys_seconds timeslot)
{
    /// Treat transient store errors as "unavailable": retry without
    /// advancing the schedule anchor. A missing lease means a peer won
    /// the refresh race for this tick.
    try
    {
        auto lease = value_backend.tryAcquireRefreshLease(definition.name, definition.uuid);
        if (lease)
            return lease;
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("NamedScalar"),
            fmt::format("named scalar '{}' refresh: acquiring refresh lease threw, retrying in 1s", definition.name));
        std::lock_guard refresh_lock(refresh_mutex);
        if (task && live.load(std::memory_order_acquire))
            task->scheduleAfter(1000);
        return std::nullopt;
    }

    ProfileEvents::increment(ProfileEvents::NamedScalarRefreshSkippedByPeer);
    {
        std::lock_guard refresh_lock(refresh_mutex);
        if (refresh)
            recordSkipByPeerLocked(timeslot);
    }

    /// Bounded fallback so a peer dying with the ephemeral lock does not
    /// leave this replica stale until much later operator action.
    size_t retry_ms = REFRESH_LEASE_BUSY_RETRY_MS;
    if (definition.refresh_period_seconds)
        retry_ms = std::min(
            retry_ms,
            static_cast<size_t>(std::chrono::milliseconds(std::chrono::seconds(*definition.refresh_period_seconds)).count()));

    std::lock_guard refresh_lock(refresh_mutex);
    if (task && live.load(std::memory_order_acquire))
        task->scheduleAfter(retry_ms);
    return std::nullopt;
}

NamedScalar::RefreshRunResult NamedScalar::evaluateAndPublishUnderLease(
    const ContextPtr & context,
    StoredValuePtr previous,
    NamedScalarRefreshLease & refresh_lease)
{
    RefreshRunResult result;

    try
    {
        auto snapshot = evaluateAndEncode(context);

        std::lock_guard publish_lock(publish_mutex);
        if (!live.load(std::memory_order_acquire))
            return result;

        switch (refresh_lease.publish(snapshot.payload))
        {
            case RefreshPublishResult::Published:
                storeCurrentValue(std::move(snapshot.value));
                result.published = true;
                break;
            case RefreshPublishResult::Diverged:
                /// Peer OR REPLACE / DROP. Watcher will install a fresh
                /// scalar; this run is a no-op (empty error_message marks
                /// it as such, so we don't bump consecutive_failures).
                break;
        }
    }
    catch (...)
    {
        result.error_message = getCurrentExceptionMessage(false);
        result.error_type = ErrorCodes::getName(getCurrentExceptionCode());

        LOG_ERROR(getLogger("NamedScalar"),
            "named scalar '{}' refresh failed [{}]: {}",
            definition.name, result.error_type, result.error_message);

        auto failure = StoredValue::fromEvaluationFailure(
            previous.get(), result.error_message, result.error_type, getFQDNOrHostName(),
            std::chrono::system_clock::now());

        std::lock_guard publish_lock(publish_mutex);
        if (!live.load(std::memory_order_acquire))
            return result;

        try
        {
            const String payload = encodeNamedScalarValueAndCheckSize(*failure, context);
            switch (refresh_lease.publish(payload))
            {
                case RefreshPublishResult::Published:
                    storeCurrentValue(failure);
                    break;
                case RefreshPublishResult::Diverged:
                    /// Peer divergence: pretend the failure didn't happen.
                    result.error_message.clear();
                    result.error_type.clear();
                    break;
            }
        }
        catch (...)
        {
            storeCurrentValue(failure);
            tryLogCurrentException(getLogger("NamedScalar"),
                fmt::format("persisting failure marker for '{}'", definition.name));
        }
    }

    return result;
}

NamedScalar::RefreshAction NamedScalar::decideRefreshAction()
{
    auto value_at_start = loadCurrentValue();
    const bool needs_population = !(value_at_start && value_at_start->has_value());

    std::lock_guard refresh_lock(refresh_mutex);
    const bool populate_only = !refresh;

    /// `populate_only && !needs_population` is reachable for non-refreshable
    /// shared-cache scalars whose initial value is already published by a
    /// peer (we read it via loadValueFromBackend at start). Nothing to do.
    if (populate_only && !needs_population)
        return {RefreshActionKind::Skip, {}, {}, false};

    /// Manual SYSTEM REFRESH bypasses `paused`.
    if (!populate_only && !needs_population && refresh->paused
        && !refresh->out_of_schedule_refresh_requested)
    {
        const auto delay = std::chrono::milliseconds(std::chrono::seconds(refresh->period_seconds));
        return {RefreshActionKind::WaitUntil, std::chrono::system_clock::now() + delay, {}, false};
    }

    const auto now = std::chrono::system_clock::now();
    bool out_of_schedule = true;
    std::chrono::system_clock::time_point when;
    std::chrono::sys_seconds timeslot = std::chrono::floor<std::chrono::seconds>(now);

    if (!populate_only)
    {
        if (needs_population)
        {
            out_of_schedule = true;
            refresh->out_of_schedule_refresh_requested = false;
        }
        else
        {
            out_of_schedule = refresh->out_of_schedule_refresh_requested;
            refresh->out_of_schedule_refresh_requested = false;
            planNextRefreshLocked(now, out_of_schedule, when, timeslot);
        }
    }

    if (!populate_only && !needs_population && !out_of_schedule && now < when)
        return {RefreshActionKind::WaitUntil, when, timeslot, false};

    return {RefreshActionKind::EvaluateNow, when, timeslot, out_of_schedule};
}

void NamedScalar::executeRefreshAction(const ContextPtr & context, RefreshAction action)
{
    if (action.kind == RefreshActionKind::Skip)
        return;

    if (action.kind == RefreshActionKind::WaitUntil)
    {
        auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
            action.when - std::chrono::system_clock::now());
        size_t delay_ms = delay.count() > 0 ? static_cast<size_t>(delay.count()) : 0;
        std::lock_guard refresh_lock(refresh_mutex);
        if (task && live.load(std::memory_order_acquire))
            task->scheduleAfter(delay_ms);
        return;
    }

    chassert(action.kind == RefreshActionKind::EvaluateNow);

    std::optional<NamedScalarRefreshLease> refresh_lease = acquireLeaseOrWait(action.timeslot);
    if (!refresh_lease)
        return;

    ProfileEvents::increment(ProfileEvents::NamedScalarRefreshAttempts);
    const auto refresh_body_started = std::chrono::steady_clock::now();

    {
        std::lock_guard refresh_lock(refresh_mutex);
        if (refresh)
            refresh->refresh_started_at = std::chrono::system_clock::now();
    }

    auto value_at_start = loadCurrentValue();
    RefreshRunResult result = evaluateAndPublishUnderLease(context, value_at_start, *refresh_lease);

    ProfileEvents::increment(ProfileEvents::NamedScalarRefreshDurationMicroseconds,
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - refresh_body_started).count());

    std::lock_guard refresh_lock(refresh_mutex);
    if (!refresh)
    {
        if (!result.published && task && live.load(std::memory_order_acquire))
            task->scheduleAfter(1000);
        return;
    }
    refresh->refresh_started_at.reset();

    /// Peer divergence (no-op outcome): don't bump consecutive_failures.
    /// Back off so we don't tight-loop while a peer DROP+watcher reconcile
    /// is still propagating (acquire lease → Diverged → schedule()).
    if (!result.published && result.error_message.empty())
    {
        if (task && live.load(std::memory_order_acquire))
            task->scheduleAfter(REFRESH_LEASE_BUSY_RETRY_MS);
        return;
    }

    ProfileEvents::increment(result.published
        ? ProfileEvents::NamedScalarRefreshSuccesses
        : ProfileEvents::NamedScalarRefreshFailures);
    recordRefreshOutcomeLocked(result.published, action.out_of_schedule, action.timeslot);

    if (task && live.load(std::memory_order_acquire))
        task->schedule();
}

void NamedScalar::runTask(const ContextPtr & context)
{
    if (!live.load(std::memory_order_acquire))
        return;

    /// Drain any pending backend value-watch fire BEFORE deciding whether
    /// to refresh: a peer's fresh value should be visible to the refresh
    /// logic so it can skip an evaluation that's no longer needed.
    if (reload_requested.exchange(false, std::memory_order_acq_rel))
        loadValueFromBackend();

    executeRefreshAction(context, decideRefreshAction());
}

}
