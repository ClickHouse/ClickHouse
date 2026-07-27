#include <config.h>

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonStreamState.h>
#include <Core/Field.h>
#include <Core/ServerUUID.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
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
    , active_node_identifier(Field(ServerUUID::get()).dump())
{
    if (!keeper)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PaimonStreamState requires a valid Keeper instance");
}

PaimonStreamState::~PaimonStreamState()
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::~PaimonStreamState");
    replica_is_active_node = nullptr;
}

bool PaimonStreamState::needsNewKeeper() const
{
    std::lock_guard lock(mutex);
    return !keeper || keeper->expired();
}

void PaimonStreamState::setKeeper(zkutil::ZooKeeperPtr keeper_)
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::setKeeper");
    std::lock_guard lock(mutex);
    replica_is_active_node = nullptr;
    keeper = std::move(keeper_);
    is_active = false;
}

std::optional<Int64> PaimonStreamState::getCommittedSnapshotId() const
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::getCommittedSnapshotId");
    auto value = readFromKeeper(fs_keeper_path / COMMITTED_SNAPSHOT_NODE);
    if (!value)
        return std::nullopt;

    return parse<Int64>(*value);
}

std::optional<Int64> PaimonStreamState::getCommittedTableIdentity() const
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::getCommittedTableIdentity");
    auto value = readFromKeeper(fs_keeper_path / COMMITTED_TABLE_IDENTITY_NODE);
    if (!value)
        return std::nullopt;

    return parse<Int64>(*value);
}

void PaimonStreamState::acquireProcessingLock()
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::acquireProcessingLock");
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
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::releaseProcessingLock");
    std::lock_guard lock(mutex);
    removeProcessingLock();
}

void PaimonStreamState::initializeKeeperNodes()
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::initializeKeeperNodes");
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
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::activate");
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
        /// Check whether an existing is_active node belongs to this server.
        /// Only reclaim it when the stored identifier matches ours (stale
        /// session from the same server). If it belongs to another server,
        /// refuse to activate to avoid stealing another replica's marker.
        Coordination::Stat stat;
        String existing_identifier;
        if (keeper->tryGet(is_active_path, existing_identifier, &stat))
        {
            if (existing_identifier == active_node_identifier)
            {
                /// Stale node from our previous session — safe to reclaim.
                /// Use versioned delete (CAS) to guard against TOCTOU races.
                auto remove_code = keeper->tryRemove(is_active_path, stat.version);
                if (remove_code != Coordination::Error::ZOK)
                {
                    LOG_WARNING(log, "Failed to remove stale is_active node at {} (code: {}). "
                        "Will retry on next attempt.", is_active_path.string(), remove_code);
                    return false;
                }
                LOG_INFO(log, "Removed stale is_active node from previous session at {}", is_active_path.string());
            }
            else
            {
                LOG_WARNING(log, "Paimon replica {} is_active node belongs to another server instance "
                    "(expected: {}, found: {}). Refusing to activate.",
                    replica_name, active_node_identifier, existing_identifier);
                return false;
            }
        }

        /// Create new ephemeral node with our server identifier.
        keeper->create(is_active_path, active_node_identifier, zkutil::CreateMode::Ephemeral);
        replica_is_active_node = zkutil::EphemeralNodeHolder::existing(is_active_path, *keeper);
        is_active = true;

        LOG_INFO(log, "Paimon replica {} activated with identifier {}", replica_name, active_node_identifier);
        return true;
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_WARNING(log, "Paimon replica {} is_active node was created by another session "
                "between our check and create. Will retry on next attempt.", replica_name);
            return false;
        }
        throw;
    }
}

void PaimonStreamState::deactivate()
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::deactivate");
    std::lock_guard lock(mutex);

    replica_is_active_node = nullptr;
    is_active = false;

    LOG_INFO(log, "Paimon replica {} deactivated", replica_name);
}

void PaimonStreamState::setCommittedSnapshot(Int64 snapshot_id, Int64 table_identity)
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::setCommittedSnapshot");
    std::lock_guard lock(mutex);

    LOG_DEBUG(log, "Committing snapshot {} (table identity {}) to Keeper", snapshot_id, table_identity);

    auto committed_path = fs_keeper_path / COMMITTED_SNAPSHOT_NODE;
    auto identity_path = fs_keeper_path / COMMITTED_TABLE_IDENTITY_NODE;

    Coordination::Requests ops;

    // Update or create committed snapshot node
    keeper->checkExistsAndGetCreateAncestorsOps(committed_path, ops);
    if (keeper->exists(committed_path))
        ops.emplace_back(zkutil::makeSetRequest(committed_path, toString(snapshot_id), -1));
    else
        ops.emplace_back(zkutil::makeCreateRequest(committed_path, toString(snapshot_id), zkutil::CreateMode::Persistent));

    /// The watermark is a bare snapshot id, and snapshot numbering restarts from 1 in a table that
    /// an external DROP + re-CREATE laid out at the same path, so the id alone does not say which
    /// table generation it belongs to.  Record the generation marker in the same transaction, so a
    /// watermark is never observable without the identity it was committed for.  `table_identity`
    /// is 0 when the identity was not latched at create time (see PaimonMetadata::validateTableIdentity);
    /// nothing is known to record then, and the node is left as is.
    if (table_identity != 0)
    {
        keeper->checkExistsAndGetCreateAncestorsOps(identity_path, ops);
        if (keeper->exists(identity_path))
            ops.emplace_back(zkutil::makeSetRequest(identity_path, toString(table_identity), -1));
        else
            ops.emplace_back(zkutil::makeCreateRequest(identity_path, toString(table_identity), zkutil::CreateMode::Persistent));
    }

    Coordination::Responses responses;
    auto code = keeper->tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK)
        zkutil::KeeperMultiException::check(code, ops, responses);

    LOG_INFO(log, "Snapshot {} committed successfully", snapshot_id);
}

void PaimonStreamState::writeToKeeper(const std::filesystem::path & path, const String & value)
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::writeToKeeper");
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
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::readFromKeeper");
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
    LOG_DEBUG(log, "Released processing lock at {}", processing_lock_path.string());
}

}


#endif

