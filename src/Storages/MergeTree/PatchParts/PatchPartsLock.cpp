#include <Storages/MergeTree/PatchParts/PatchPartsLock.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Analyzer/Utils.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <filesystem>
#include <boost/algorithm/string/join.hpp>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Interpreters/ProcessList.h>
#include <thread>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event PatchesAcquireLockTries;
    extern const Event PatchesAcquireLockBadVersionRetries;
    extern const Event PatchesAcquireLockMicroseconds;
}

namespace DB
{

namespace FailPoints
{
    extern const char patch_parts_lock_pause_before_cas[];
}

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUpdateParallelMode update_parallel_mode;
}

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace
{

constexpr Int64 max_wait_chunk_ms = 3000;
constexpr Int64 bad_version_backoff_ms = 50;

/// Milliseconds left of the timeout, or zero if it is exhausted. Elapsed time is subtracted in the
/// millisecond domain, so a huge timeout cannot overflow the way a chrono deadline would.
Int64 getRemainingMs(const Stopwatch & watch, Int64 timeout_ms)
{
    return std::max<Int64>(timeout_ms - static_cast<Int64>(watch.elapsedMilliseconds()), 0);
}

/// Calls `wait_chunk` with a slice of the remaining time until it reports success, polling query
/// cancellation in between. Returns false once the timeout is exhausted. `wait_chunk` must not
/// discard the state it waits on between calls: a Poco::Event watch is one-shot, and a timed out
/// wait deregisters nothing.
template <typename WaitChunk>
bool waitInterruptibly(const ContextPtr & context, const Stopwatch & watch, Int64 timeout_ms, WaitChunk && wait_chunk)
{
    auto query_status = context->getProcessListElementSafe();

    for (auto remaining_ms = getRemainingMs(watch, timeout_ms); remaining_ms > 0; remaining_ms = getRemainingMs(watch, timeout_ms))
    {
        if (query_status)
            query_status->checkTimeLimit();

        if (wait_chunk(std::min(max_wait_chunk_ms, remaining_ms)))
            return true;
    }

    /// The last chunk is not followed by another iteration's check, so a cancellation that arrived
    /// while waiting in it would otherwise be reported as the lock timing out.
    if (query_status)
        query_status->checkTimeLimit();

    return false;
}

void waitInterruptibly(const ContextPtr & context, Poco::Event & event, const Stopwatch & watch, Int64 timeout_ms)
{
    waitInterruptibly(context, watch, timeout_ms, [&](Int64 chunk_ms) { return event.tryWait(chunk_ms); });
}

zkutil::EphemeralNodeHolderPtr getLockForSyncMode(
    const ContextPtr & context,
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & zookeeper_path)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PatchesAcquireLockMicroseconds);

    auto lock_path = fs::path(zookeeper_path) / "lightweight_updates" / "lock";
    auto lock_acquire_timeout = context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds();
    Stopwatch acquire_watch;

    /// The first attempt is unconditional, so an uncontended update still succeeds at zero timeout.
    for (size_t num_try = 0;; ++num_try)
    {
        ProfileEvents::increment(ProfileEvents::PatchesAcquireLockTries);
        LOG_TRACE(getLogger("getLockForSyncMode"), "Trying to get lock (try: {}, path: {}) for lightweight update", num_try, lock_path.string());
        auto code = zookeeper->tryCreate(lock_path, "", zkutil::CreateMode::Ephemeral);

        if (code == Coordination::Error::ZOK)
        {
            LOG_TRACE(getLogger("getLockForSyncMode"), "Got lock (try: {}, path: {}) for lightweight update", num_try, lock_path.string());
            return zkutil::EphemeralNodeHolder::existing(lock_path, *zookeeper);
        }

        if (code != Coordination::Error::ZNODEEXISTS)
            throw zkutil::KeeperException::fromPath(code, lock_path);

        auto remaining_ms = getRemainingMs(acquire_watch, lock_acquire_timeout);
        if (remaining_ms == 0)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                "Failed to get lock in {} ms with {} tries for lightweight update in sync mode",
                lock_acquire_timeout, num_try + 1);

        /// The watch is one-shot, so a fresh event is required for every wait.
        auto lock_event = std::make_shared<Poco::Event>();
        if (zookeeper->exists(lock_path, nullptr, lock_event))
            waitInterruptibly(context, *lock_event, acquire_watch, lock_acquire_timeout);
    }
}

zkutil::EphemeralNodeHolderPtr getLockForAutoMode(
    const MutationCommands & commands,
    const ContextPtr & context,
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & zookeeper_path)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PatchesAcquireLockMicroseconds);

    auto affected_columns = getUpdateAffectedColumns(commands, context);
    auto in_progress_path = fs::path(zookeeper_path) / "lightweight_updates" / "in_progress";
    auto lock_acquire_timeout = context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds();
    auto affected_columns_str = affected_columns.toString();

    Coordination::Stat parent_stat;
    Stopwatch acquire_watch;

    auto throw_timeout = [&](size_t num_tries)
    {
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
            "Failed to get lock in {} ms with {} tries for lightweight update in auto mode",
            lock_acquire_timeout, num_tries);
    };

    /// The first attempt is unconditional, so an uncontended update still succeeds at zero timeout.
    for (size_t num_try = 0;; ++num_try)
    {
        ProfileEvents::increment(ProfileEvents::PatchesAcquireLockTries);
        LOG_TRACE(getLogger("getLockForAutoMode"), "Trying to get lock (try: {}, path: {}) for lightweight update", num_try, in_progress_path.string());

        auto in_progress_ids = zookeeper->getChildren(in_progress_path, &parent_stat);

        Names multiget_paths;
        multiget_paths.reserve(in_progress_ids.size());

        for (const auto & id : in_progress_ids)
            multiget_paths.push_back(in_progress_path / id);

        auto contents = zookeeper->tryGet(multiget_paths);
        String conflicting_path;

        for (size_t i = 0; i < contents.size(); ++i)
        {
            /// Update has already finished and node was removed.
            if (contents[i].error == Coordination::Error::ZNONODE)
                continue;

            if (contents[i].error != Coordination::Error::ZOK)
                throw zkutil::KeeperException::fromPath(contents[i].error, multiget_paths[i]);

            UpdateAffectedColumns in_progress_affected;
            in_progress_affected.fromString(contents[i].data);

            if (in_progress_affected.hasConflict(affected_columns))
            {
                conflicting_path = multiget_paths[i];
                break;
            }
        }

        if (!conflicting_path.empty())
        {
            LOG_TRACE(getLogger("getLockForAutoMode"), "Columns required for lightweight update are being updated by another query, will try one more time");

            auto remaining_ms = getRemainingMs(acquire_watch, lock_acquire_timeout);
            if (remaining_ms == 0)
                throw_timeout(num_try + 1);

            /// Watch the conflicting update's own node: the parent directory changes on every
            /// concurrent update, so its watch fires without this query being able to progress.
            /// The watch is one-shot, so a fresh event is required for every wait.
            auto conflict_event = std::make_shared<Poco::Event>();
            String conflicting_data;

            /// The node may be gone already, in which case the event would never be set.
            if (zookeeper->tryGet(conflicting_path, conflicting_data, nullptr, conflict_event))
                waitInterruptibly(context, *conflict_event, acquire_watch, lock_acquire_timeout);

            continue;
        }

        Coordination::Requests ops;
        ops.push_back(zkutil::makeCreateRequest(in_progress_path / "update-", affected_columns_str, zkutil::CreateMode::EphemeralSequential));
        ops.push_back(zkutil::makeSetRequest(in_progress_path, "", parent_stat.version));

        /// `parent_stat.version` was read above, so anything committing here makes the request below
        /// fail with ZBADVERSION.
        FailPointInjection::pauseFailPoint(FailPoints::patch_parts_lock_pause_before_cas);

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);

        if (code == Coordination::Error::ZBADVERSION)
        {
            LOG_TRACE(getLogger("getLockForAutoMode"), "Lightweight update has been committed by another replica, will try one more time");

            auto remaining_ms = getRemainingMs(acquire_watch, lock_acquire_timeout);
            if (remaining_ms == 0)
                throw_timeout(num_try + 1);

            /// Another update committed, so there is no node this query could watch. Back off a
            /// fixed amount instead, otherwise the retry spins on Keeper for the whole timeout.
            if (auto query_status = context->getProcessListElementSafe())
                query_status->checkTimeLimit();

            ProfileEvents::increment(ProfileEvents::PatchesAcquireLockBadVersionRetries);
            std::this_thread::sleep_for(std::chrono::milliseconds(std::min(bad_version_backoff_ms, remaining_ms)));
            continue;
        }

        zkutil::KeeperMultiException::check(code, ops, responses);

        const auto & created_path = dynamic_cast<const Coordination::CreateResponse &>(*responses.front()).path_created;
        LOG_TRACE(getLogger("getLockForAutoMode"), "Got lock (try: {}, path: {}) for lightweight update", num_try, created_path);
        return zkutil::EphemeralNodeHolder::existing(created_path, *zookeeper);
    }
}

}

bool UpdateAffectedColumns::hasConflict(const UpdateAffectedColumns & other) const
{
    for (const auto & column : updated)
    {
        if (other.used.contains(column))
            return true;
    }

    for (const auto & column : other.updated)
    {
        if (used.contains(column))
            return true;
    }

    return false;
}

String UpdateAffectedColumns::toString() const
{
    WriteBufferFromOwnString out;
    out << "format version: " << VERSION << "\n";

    auto write_columns = [&](const auto & columns, const char * suffix)
    {
        out << columns.size() << " " << suffix << "\n";

        for (const auto & column : columns)
        {
            writeBackQuotedString(column, out);
            writeChar('\n', out);
        }
    };

    write_columns(used, "used columns:");
    write_columns(updated, "updated columns:");

    return out.str();
}

void UpdateAffectedColumns::fromString(const String & str)
{
    ReadBufferFromString in(str);

    size_t version = 0;
    in >> "format version: " >> version >> "\n";
    if (version != VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown version of affected columns serializaiton: {}", version);

    auto read_columns = [&](auto & columns, const char * suffix)
    {
        size_t count = 0;
        in >> count >> " " >> suffix >> "\n";

        String column_name;
        for (size_t i = 0; i < count; ++i)
        {
            readBackQuotedString(column_name, in);
            assertChar('\n', in);
            columns.insert(column_name);
        }
    };

    read_columns(used, "used columns:");
    read_columns(updated, "updated columns:");
}

void UpdateAffectedColumnsWithCounters::add(const UpdateAffectedColumns & other)
{
    for (const auto & column : other.used)
        ++used[column];

    for (const auto & column : other.updated)
        ++updated[column];
}

void UpdateAffectedColumnsWithCounters::remove(const UpdateAffectedColumns & other)
{
    auto remove_from_map = [](auto & map, const auto & vec)
    {
        for (const auto & column : vec)
        {
            auto it = map.find(column);
            if (it == map.end())
            {
                LOG_FATAL(getLogger("UpdateAffectedColumnsWithCounters"),
                    "Cannot remove column {} from map. The state of affected columns became inconsistent", column);
                std::terminate();
            }

            if (--it->second == 0)
                map.erase(it);
        }
    };

    remove_from_map(used, other.used);
    remove_from_map(updated, other.updated);
}

bool UpdateAffectedColumnsWithCounters::hasConflict(const UpdateAffectedColumns & other) const
{
    for (const auto & [column, _] : updated)
    {
        if (other.used.contains(column))
            return true;
    }

    for (const auto & column : other.updated)
    {
        if (used.contains(column))
            return true;
    }

    return false;
}

void PlainLightweightUpdatesSync::lockColumns(
    const ContextPtr & context, const UpdateAffectedColumns & affected_columns, Int64 timeout_ms)
{
    auto no_conflict = [&] { return !in_progress_columns.hasConflict(affected_columns); };

    std::unique_lock lock(in_progress_mutex);
    Stopwatch acquire_watch;

    /// The first check is unconditional, so an uncontended update still succeeds at zero timeout.
    if (!no_conflict())
    {
        auto wait_chunk = [&](Int64 chunk_ms)
        {
            return in_progress_cv.wait_for(lock, std::chrono::milliseconds(chunk_ms), no_conflict);
        };

        if (!waitInterruptibly(context, acquire_watch, timeout_ms, wait_chunk))
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                "Failed to get lock in {} ms for lightweight update with auto mode", timeout_ms);
    }

    in_progress_columns.add(affected_columns);
}

bool PlainLightweightUpdatesSync::lockSyncMutex(
    const ContextPtr & context, std::unique_lock<std::timed_mutex> & sync_lock, Int64 timeout_ms)
{
    Stopwatch acquire_watch;

    /// The first attempt is unconditional, so an uncontended update still succeeds at zero timeout.
    if (sync_lock.try_lock())
        return true;

    auto wait_chunk = [&](Int64 chunk_ms) { return sync_lock.try_lock_for(std::chrono::milliseconds(chunk_ms)); };
    return waitInterruptibly(context, acquire_watch, timeout_ms, wait_chunk);
}

void PlainLightweightUpdatesSync::releaseColumns(const UpdateAffectedColumns & affected_columns)
{
    std::lock_guard lock(in_progress_mutex);
    in_progress_columns.remove(affected_columns);
    in_progress_cv.notify_one();
}

PlainLightweightUpdateLock::~PlainLightweightUpdateLock()
{
    if (lightweight_updates_sync)
        lightweight_updates_sync->releaseColumns(affected_columns);
}

UpdateAffectedColumns getUpdateAffectedColumns(const MutationCommands & commands, const ContextPtr & context)
{
    UpdateAffectedColumns res;

    for (const auto & command : commands)
    {
        auto alter = command.ast();
        if (!alter)
            continue;

        /// The predicate and assignment expressions were re-parsed from the serialized mutation command,
        /// so their set operations (UNION/INTERSECT/EXCEPT) are not normalized yet. Normalize them before
        /// building the query tree, as `executeQuery` does, otherwise the analyzer rejects them.
        ASTPtr predicate(alter->predicate);
        if (predicate)
            normalizeSetOperations(predicate, context);

        auto query_tree = buildQueryTree(predicate, context);
        auto identifiers = collectIdentifiersFullNames(query_tree);
        std::move(identifiers.begin(), identifiers.end(), std::inserter(res.used, res.used.end()));

        if (!alter->update_assignments)
            continue;

        for (const auto & child : alter->update_assignments->children)
        {
            const auto & assignment = child->as<ASTAssignment &>();
            res.updated.insert(assignment.column_name);

            ASTPtr assignment_expression = assignment.expression();
            normalizeSetOperations(assignment_expression, context);

            query_tree = buildQueryTree(assignment_expression, context);
            identifiers = collectIdentifiersFullNames(query_tree);
            std::move(identifiers.begin(), identifiers.end(), std::inserter(res.used, res.used.end()));
        }
    }

    return res;
}

void LightweightUpdateHolderInKeeper::reset()
{
    partition_block_numbers.reset();
    lock.reset();
}

zkutil::EphemeralNodeHolderPtr getLockForLightweightUpdateInKeeper(
    const MutationCommands & commands,
    const ContextPtr & context,
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & zookeeper_path)
{
    auto parallel_mode = context->getSettingsRef()[Setting::update_parallel_mode];

    if (parallel_mode == UpdateParallelMode::SYNC)
        return getLockForSyncMode(context, zookeeper, zookeeper_path);

    if (parallel_mode == UpdateParallelMode::AUTO)
        return getLockForAutoMode(commands, context, zookeeper, zookeeper_path);

    return {};
}

}
