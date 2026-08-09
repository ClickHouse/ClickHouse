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
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event PatchesAcquireLockTries;
    extern const Event PatchesAcquireLockMicroseconds;
}

namespace DB
{

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

using LockClock = std::chrono::steady_clock;

/// Milliseconds left until the deadline, or zero if it has passed.
Int64 getRemainingMs(LockClock::time_point deadline)
{
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - LockClock::now()).count();
    return std::max<Int64>(remaining, 0);
}

zkutil::EphemeralNodeHolderPtr getLockForSyncMode(
    const ContextPtr & context,
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & zookeeper_path)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PatchesAcquireLockMicroseconds);

    auto lock_path = fs::path(zookeeper_path) / "lightweight_updates" / "lock";
    auto lock_acquire_timeout = context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds();
    auto deadline = LockClock::now() + std::chrono::milliseconds(lock_acquire_timeout);

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

        auto remaining_ms = getRemainingMs(deadline);
        if (remaining_ms == 0)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                "Failed to get lock in {} ms with {} tries for lightweight update in sync mode",
                lock_acquire_timeout, num_try + 1);

        /// The watch is one-shot, so a fresh event is required for every wait.
        auto lock_event = std::make_shared<Poco::Event>();
        if (zookeeper->exists(lock_path, nullptr, lock_event))
            lock_event->tryWait(remaining_ms);
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
    auto deadline = LockClock::now() + std::chrono::milliseconds(lock_acquire_timeout);

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

            auto remaining_ms = getRemainingMs(deadline);
            if (remaining_ms == 0)
                throw_timeout(num_try + 1);

            /// Watch the conflicting update's own node: the parent directory changes on every
            /// concurrent update, so its watch fires without this query being able to progress.
            /// The watch is one-shot, so a fresh event is required for every wait.
            auto conflict_event = std::make_shared<Poco::Event>();
            String conflicting_data;

            /// The node may be gone already, in which case the event would never be set.
            if (zookeeper->tryGet(conflicting_path, conflicting_data, nullptr, conflict_event))
                conflict_event->tryWait(remaining_ms);

            continue;
        }

        Coordination::Requests ops;
        ops.push_back(zkutil::makeCreateRequest(in_progress_path / "update-", affected_columns_str, zkutil::CreateMode::EphemeralSequential));
        ops.push_back(zkutil::makeSetRequest(in_progress_path, "", parent_stat.version));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);

        if (code == Coordination::Error::ZBADVERSION)
        {
            LOG_TRACE(getLogger("getLockForAutoMode"), "Lightweight update has been committed by another replica, will try one more time");

            if (getRemainingMs(deadline) == 0)
                throw_timeout(num_try + 1);

            /// Another update committed, so there is no conflict of this query to wait for.
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

void PlainLightweightUpdatesSync::lockColumns(const UpdateAffectedColumns & affected_columns, size_t timeout_ms)
{
    std::unique_lock lock(in_progress_mutex);

    bool res = in_progress_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&]
    {
        return !in_progress_columns.hasConflict(affected_columns);
    });

    if (!res)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Failed to get lock in {} ms for lightwegiht update with auto mode", timeout_ms);

    in_progress_columns.add(affected_columns);
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
