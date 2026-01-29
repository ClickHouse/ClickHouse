#include <config.h>

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonStreamState.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int REPLICA_IS_ALREADY_ACTIVE;
}

PaimonStreamState::PaimonStreamState(
    zkutil::ZooKeeperPtr keeper_,
    const String & keeper_path_,
    const String & replica_name_,
    LoggerPtr log_)
    : keeper(std::move(keeper_))
    , keeper_path(keeper_path_)
    , replica_name(replica_name_)
    , fs_keeper_path(keeper_path_)
    , log(log_)
{
    if (!keeper)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PaimonStreamState requires a valid Keeper instance");
}

bool PaimonStreamState::needsNewKeeper() const
{
    std::lock_guard lock(mutex);
    return !keeper || keeper->expired();
}

void PaimonStreamState::setKeeper(zkutil::ZooKeeperPtr keeper_)
{
    std::lock_guard lock(mutex);
    keeper = std::move(keeper_);
    replica_is_active_node = nullptr;
    is_active = false;
}

std::optional<Int64> PaimonStreamState::getCommittedSnapshotId() const
{
    auto value = readFromKeeper(fs_keeper_path / COMMITTED_SNAPSHOT_NODE);
    if (!value)
        return std::nullopt;

    return parse<Int64>(*value);
}

void PaimonStreamState::acquireProcessingLock()
{
    std::lock_guard lock(mutex);

    const auto processing_lock_path = fs_keeper_path / PROCESSING_LOCK_NODE;
    try
    {
        keeper->create(processing_lock_path, replica_name, zkutil::CreateMode::Ephemeral);
        LOG_DEBUG(log, "Acquired processing lock at {}", processing_lock_path.string());
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
            throw Exception(
                ErrorCodes::REPLICA_IS_ALREADY_ACTIVE,
                "Another incremental read is in progress (processing lock exists at {})",
                processing_lock_path.string());
        throw;
    }
}

void PaimonStreamState::releaseProcessingLock()
{
    std::lock_guard lock(mutex);
    removeProcessingLock();
}

void PaimonStreamState::initializeKeeperNodes()
{
    std::lock_guard lock(mutex);

    LOG_DEBUG(log, "Initializing Paimon stream state in Keeper at {}", keeper_path);

    keeper->createAncestors(keeper_path);

    Coordination::Requests ops;

    // Create root path if not exists
    if (!keeper->exists(keeper_path))
        ops.emplace_back(zkutil::makeCreateRequest(keeper_path, "", zkutil::CreateMode::Persistent));

    // Create replicas directory
    auto replicas_path = fs_keeper_path / REPLICAS_NODE;
    if (!keeper->exists(replicas_path))
        ops.emplace_back(zkutil::makeCreateRequest(replicas_path, "", zkutil::CreateMode::Persistent));

    // Create this replica's directory
    auto replica_path = replicas_path / replica_name;
    if (!keeper->exists(replica_path))
        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "", zkutil::CreateMode::Persistent));

    if (!ops.empty())
    {
        Coordination::Responses responses;
        auto code = keeper->tryMulti(ops, responses);
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }
    }

    LOG_INFO(log, "Paimon stream state initialized at {}", keeper_path);
}

bool PaimonStreamState::activate()
{
    std::lock_guard lock(mutex);

    if (is_active && !keeper->expired())
    {
        LOG_TRACE(log, "Paimon replica {} already active", replica_name);
        return true;
    }

    auto replica_path = fs_keeper_path / REPLICAS_NODE / replica_name;
    auto is_active_path = replica_path / IS_ACTIVE_NODE;

    try
    {
        // Try to delete existing ephemeral node if it belongs to us (stale session)
        keeper->tryRemove(is_active_path, -1);

        // Create new ephemeral node
        keeper->create(is_active_path, replica_name, zkutil::CreateMode::Ephemeral);
        replica_is_active_node = zkutil::EphemeralNodeHolder::existing(is_active_path, *keeper);
        is_active = true;

        LOG_INFO(log, "Paimon replica {} activated", replica_name);
        return true;
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_WARNING(log, "Paimon replica {} appears to be already active. Another instance may be running.", replica_name);
            return false;
        }
        throw;
    }
}

void PaimonStreamState::deactivate()
{
    std::lock_guard lock(mutex);

    replica_is_active_node = nullptr;
    is_active = false;

    LOG_INFO(log, "Paimon replica {} deactivated", replica_name);
}

void PaimonStreamState::setCommittedSnapshot(Int64 snapshot_id)
{
    std::lock_guard lock(mutex);

    LOG_DEBUG(log, "Committing snapshot {} to Keeper", snapshot_id);

    auto committed_path = fs_keeper_path / COMMITTED_SNAPSHOT_NODE;

    Coordination::Requests ops;

    // Update or create committed snapshot node
    keeper->checkExistsAndGetCreateAncestorsOps(committed_path, ops);
    if (keeper->exists(committed_path))
        ops.emplace_back(zkutil::makeSetRequest(committed_path, toString(snapshot_id), -1));
    else
        ops.emplace_back(zkutil::makeCreateRequest(committed_path, toString(snapshot_id), zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    auto code = keeper->tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK)
        zkutil::KeeperMultiException::check(code, ops, responses);

    LOG_INFO(log, "Snapshot {} committed successfully", snapshot_id);
}

void PaimonStreamState::writeToKeeper(const std::filesystem::path & path, const String & value)
{
    Coordination::Requests ops;
    keeper->checkExistsAndGetCreateAncestorsOps(path, ops);

    if (keeper->exists(path))
        ops.emplace_back(zkutil::makeSetRequest(path, value, -1));
    else
        ops.emplace_back(zkutil::makeCreateRequest(path, value, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    auto code = keeper->tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK)
        zkutil::KeeperMultiException::check(code, ops, responses);
}

std::optional<String> PaimonStreamState::readFromKeeper(const std::filesystem::path & path) const
{
    std::lock_guard lock(mutex);

    String result;
    if (!keeper->tryGet(path, result))
        return std::nullopt;

    return result;
}

void PaimonStreamState::removeProcessingLock()
{
    auto processing_lock_path = fs_keeper_path / PROCESSING_LOCK_NODE;
    keeper->tryRemove(processing_lock_path, -1);
}

}


#endif

