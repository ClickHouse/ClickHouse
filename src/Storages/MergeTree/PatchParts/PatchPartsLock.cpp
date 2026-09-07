#include <Storages/MergeTree/PatchParts/PatchPartsLock.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Analyzer/Utils.h>
#include <Analyzer/QueryTreeBuilder.h>
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

constexpr size_t max_tries = 100;

zkutil::EphemeralNodeHolderPtr getLockForSyncMode(
    const ContextPtr & context,
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & zookeeper_path)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PatchesAcquireLockMicroseconds);

    auto lock_path = fs::path(zookeeper_path) / "lightweight_updates" / "lock";
    auto lock_event = std::make_shared<Poco::Event>();
    auto lock_acquire_timeout = context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds();

    for (size_t i = 0; i < max_tries; ++i)
    {
        ProfileEvents::increment(ProfileEvents::PatchesAcquireLockTries);
        LOG_TRACE(getLogger("getLockForSyncMode"), "Trying to get lock (try: {}, path: {}) for lightweight update", i, lock_path.string());
        auto code = zookeeper->tryCreate(lock_path, "", zkutil::CreateMode::Ephemeral);

        if (code == Coordination::Error::ZOK)
        {
            LOG_TRACE(getLogger("getLockForSyncMode"), "Got lock (try: {}, path: {}) for lightweight update", i, lock_path.string());
            return zkutil::EphemeralNodeHolder::existing(lock_path, *zookeeper);
        }

        if (code != Coordination::Error::ZNODEEXISTS)
            throw zkutil::KeeperException::fromPath(code, lock_path);

        if (zookeeper->exists(lock_path, nullptr, lock_event))
            lock_event->tryWait(lock_acquire_timeout);
    }

    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Failed to get lock with {} tries for lightwegiht update in sync mode", max_tries);
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
    auto in_progress_event = std::make_shared<Poco::Event>();

    for (size_t num_try = 0; num_try < max_tries; ++num_try)
    {
        ProfileEvents::increment(ProfileEvents::PatchesAcquireLockTries);
        LOG_TRACE(getLogger("getLockForAutoMode"), "Trying to get lock (try: {}, path: {}) for lightweight update", num_try, in_progress_path.string());

        auto in_progress_ids = zookeeper->getChildren(in_progress_path, &parent_stat, in_progress_event);

        Names multiget_paths;
        multiget_paths.reserve(in_progress_ids.size());

        for (const auto & id : in_progress_ids)
            multiget_paths.push_back(in_progress_path / id);

        auto contents = zookeeper->tryGet(multiget_paths);
        bool has_dependency_in_progress = false;

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
                has_dependency_in_progress = true;
                break;
            }
        }

        if (has_dependency_in_progress)
        {
            LOG_TRACE(getLogger("getLockForAutoMode"), "Columns required for lightweight update are being updated by another query, will try one more time");
            in_progress_event->tryWait(lock_acquire_timeout);
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
            in_progress_event->tryWait(lock_acquire_timeout);
            continue;
        }

        zkutil::KeeperMultiException::check(code, ops, responses);

        const auto & created_path = dynamic_cast<const Coordination::CreateResponse &>(*responses.front()).path_created;
        LOG_TRACE(getLogger("getLockForAutoMode"), "Got lock (try: {}, path: {}) for lightweight update", num_try, created_path);
        return zkutil::EphemeralNodeHolder::existing(created_path, *zookeeper);
    }

    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Failed to get lock with {} tries for lightwegiht update in auto mode", max_tries);
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

    size_t version;
    in >> "format version: " >> version >> "\n";
    if (version != VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown version of affected columns serializaiton: {}", version);

    auto read_columns = [&](auto & columns, const char * suffix)
    {
        size_t count;
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
        auto query_tree = buildQueryTree(command.predicate, context);
        auto identifiers = collectIdentifiersFullNames(query_tree);
        std::move(identifiers.begin(), identifiers.end(), std::inserter(res.used, res.used.end()));

        for (const auto & [name, ast] : command.column_to_update_expression)
        {
            res.updated.insert(name);

            query_tree = buildQueryTree(ast, context);
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
