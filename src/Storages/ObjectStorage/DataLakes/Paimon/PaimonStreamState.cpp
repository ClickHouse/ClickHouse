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
extern const int INVALID_STATE;
extern const int REPLICA_IS_ALREADY_ACTIVE;
}

PaimonProcessingLock::PaimonProcessingLock(
    zkutil::ZooKeeperPtr keeper_,
    std::filesystem::path path_,
    String token_,
    Int64 session_id_,
    Int32 version_)
    : keeper(std::move(keeper_))
    , path(std::move(path_))
    , token(std::move(token_))
    , session_id(session_id_)
    , version(version_)
{
}

PaimonProcessingLock::~PaimonProcessingLock()
{
    auto component_guard = Coordination::setCurrentComponent("PaimonProcessingLock::~PaimonProcessingLock");
    auto log = getLogger("PaimonProcessingLock");

    try
    {
        if (keeper->expired())
        {
            /// There is no live session left to remove it through, and removing it through
            /// any other session would delete whoever holds the lock now. Keeper drops the
            /// node itself once it expires the session server-side.
            LOG_DEBUG(log, "Not releasing the processing lock at {}: the session that acquired it has expired, "
                "Keeper will drop the node when that session times out", path.string());
            return;
        }

        Coordination::Stat stat;
        String current_token;
        if (!keeper->tryGet(path.string(), current_token, &stat))
        {
            LOG_WARNING(log, "Processing lock at {} is already gone, nothing to release", path.string());
            return;
        }

        /// The ephemeral owner is the session id that created the node, so it cannot
        /// be forged. Refuse to touch a lock somebody else is holding.
        if (stat.ephemeralOwner != session_id || current_token != token)
        {
            LOG_WARNING(log, "Refusing to release the processing lock at {}: it is held by another consumer "
                "(expected token '{}' owned by session {}, found token '{}' owned by session {})",
                path.string(), token, session_id, current_token, stat.ephemeralOwner);
            return;
        }

        auto code = keeper->tryRemove(path.string(), stat.version);
        if (code != Coordination::Error::ZOK)
            LOG_WARNING(log, "Failed to release the processing lock at {}: {}", path.string(), code);
        else
            LOG_DEBUG(log, "Released processing lock at {}", path.string());
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to release the Paimon processing lock at " + path.string());
    }
}

void PaimonProcessingLock::addFenceOps(Coordination::Requests & ops) const
{
    /// Together with tryMulti(..., check_session_valid=true) this makes the
    /// transaction fail unless we still own the lock: the session check rejects a
    /// holder whose session was replaced, and the version check rejects a lock node
    /// that was removed and recreated by somebody else.
    ops.emplace_back(zkutil::makeCheckRequest(path.string(), version));
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

PaimonProcessingLockPtr PaimonStreamState::acquireProcessingLock()
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::acquireProcessingLock");
    std::lock_guard lock(mutex);

    const auto processing_lock_path = fs_keeper_path / PROCESSING_LOCK_NODE;
    /// Pin the session we are acquiring with: the `keeper` member may be replaced by
    /// a concurrent query, and the lock must stay tied to the session that owns it.
    auto lock_keeper = keeper;
    const Int64 session_id = lock_keeper->getClientID();
    const String token = fmt::format("{}/{}/{}", replica_name, active_node_identifier, session_id);

    /// Create the node and stamp it in one transaction. The stamp exists because the node's
    /// version is what fences later commits, and a node freshly created by another consumer
    /// also sits at the default version 0 - ours has to leave it, or a check op cannot tell
    /// the two apart.
    ///
    /// This has to be atomic. As two round trips, a hardware error on the second one would
    /// leave an ephemeral lock nobody can clean up: the error finalizes this client, so there
    /// is no live session left to remove the node through, and the node itself survives until
    /// the server expires the session - blocking every incremental read on this table until
    /// then. One transaction has no such intermediate state.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(processing_lock_path, token, zkutil::CreateMode::Ephemeral));
    ops.emplace_back(zkutil::makeSetRequest(processing_lock_path, token, /*version=*/0));

    Coordination::Responses responses;
    auto code = lock_keeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZNODEEXISTS)
        throw Exception(
            ErrorCodes::REPLICA_IS_ALREADY_ACTIVE,
            "Another incremental read is in progress (processing lock exists at {})",
            processing_lock_path.string());
    if (code != Coordination::Error::ZOK)
        zkutil::KeeperMultiException::check(code, ops, responses);

    const Int32 version = dynamic_cast<const Coordination::SetResponse &>(*responses[1]).stat.version;

    LOG_DEBUG(log, "Acquired processing lock at {} with token '{}' (version {})",
        processing_lock_path.string(), token, version);

    return std::make_unique<PaimonProcessingLock>(
        std::move(lock_keeper), processing_lock_path, token, session_id, version);
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

void PaimonStreamState::setCommittedSnapshot(const PaimonProcessingLock & processing_lock, Int64 snapshot_id)
{
    auto component_guard = Coordination::setCurrentComponent("PaimonStreamState::setCommittedSnapshot");
    std::lock_guard lock(mutex);

    LOG_DEBUG(log, "Committing snapshot {} to Keeper", snapshot_id);

    /// Commit through the session that acquired the lock. Using the `keeper` member
    /// here would let a read whose session died borrow a session established later
    /// by a concurrent query and commit over another consumer's progress.
    auto & lock_keeper = processing_lock.getKeeper();
    const auto committed_path = fs_keeper_path / COMMITTED_SNAPSHOT_NODE;

    Coordination::Requests ops;
    processing_lock.addFenceOps(ops);
    const size_t first_watermark_op = ops.size();

    Coordination::Stat stat;
    String current_value;
    if (lock_keeper.tryGet(committed_path, current_value, &stat))
    {
        const auto current_snapshot_id = parse<Int64>(current_value);
        if (snapshot_id <= current_snapshot_id)
            throw Exception(
                ErrorCodes::INVALID_STATE,
                "Refusing to move the Paimon committed snapshot backwards at {}: it is already {}, "
                "attempted to set it to {}. Another consumer advanced it while this read was in progress.",
                committed_path.string(), current_snapshot_id, snapshot_id);

        ops.emplace_back(zkutil::makeSetRequest(committed_path, toString(snapshot_id), stat.version));
    }
    else
    {
        lock_keeper.checkExistsAndGetCreateAncestorsOps(committed_path, ops);
        ops.emplace_back(zkutil::makeCreateRequest(committed_path, toString(snapshot_id), zkutil::CreateMode::Persistent));
    }

    Coordination::Responses responses;
    auto code = lock_keeper.tryMulti(ops, responses, /*check_session_valid=*/true);
    if (code != Coordination::Error::ZOK)
    {
        if (code == Coordination::Error::ZSESSIONMOVED)
            throw Exception(
                ErrorCodes::INVALID_STATE,
                "Refusing to advance the Paimon committed snapshot to {}: the Keeper session that acquired the "
                "processing lock at {} is no longer the current one, so this read no longer holds the lock.",
                snapshot_id, processing_lock.getPath().string());

        if (Coordination::isUserError(code) && !responses.empty())
        {
            const size_t failed_op = zkutil::getFailedOpIndex(code, responses);
            if (failed_op < first_watermark_op)
                throw Exception(
                    ErrorCodes::INVALID_STATE,
                    "Refusing to advance the Paimon committed snapshot to {}: the processing lock at {} is no longer "
                    "held by this read (it was removed or taken over by another consumer).",
                    snapshot_id, processing_lock.getPath().string());

            if (code == Coordination::Error::ZBADVERSION || code == Coordination::Error::ZNODEEXISTS)
                throw Exception(
                    ErrorCodes::INVALID_STATE,
                    "Refusing to advance the Paimon committed snapshot to {}: the watermark at {} was modified "
                    "concurrently by another consumer.",
                    snapshot_id, committed_path.string());
        }

        zkutil::KeeperMultiException::check(code, ops, responses);
    }

    LOG_INFO(log, "Snapshot {} committed successfully", snapshot_id);
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

}


#endif
