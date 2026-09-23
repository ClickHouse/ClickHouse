/// NOLINTBEGIN(clang-analyzer-optin.core.EnumCastOutOfRange)

#include <algorithm>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <boost/algorithm/string.hpp>
#include <Poco/SHA1Engine.h>

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <base/hex.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/SharedLockGuard.h>
#include <Common/StringHashForHeterogeneousLookup.h>
#include <Common/thread_local_rng.h>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperReconfiguration.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperStorageImpl.h>
#include <Coordination/KeeperMemNodesStorage.h>

#include <limits>
#include <shared_mutex>
#include <base/defines.h>

namespace ProfileEvents
{
    extern const Event KeeperWatchesTriggered;
    extern const Event KeeperWatchTriggeredNodeCreated;
    extern const Event KeeperWatchTriggeredNodeDeleted;
    extern const Event KeeperWatchTriggeredNodeDataChanged;
    extern const Event KeeperWatchTriggeredNodeChildrenChanged;
}

namespace CurrentMetrics
{
    extern const Metric KeeperTTLNodes;
    extern const Metric KeeperContainerNodes;
}


namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 log_slow_cpu_threshold_ms;
    extern const CoordinationSettingsBool check_node_acl_on_remove;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char keeper_leader_sets_invalid_digest[];
}

namespace
{
String getSHA1(const String & userdata)
{
    Poco::SHA1Engine engine;
    engine.update(userdata);
    const auto & digest_id = engine.digest();
    return String{digest_id.begin(), digest_id.end()};
}

void incrementTriggeredWatchProfileEvent(Coordination::Event event_type, size_t count)
{
    if (count == 0)
        return;
    ProfileEvents::increment(ProfileEvents::KeeperWatchesTriggered, count);
    switch (event_type)
    {
        case Coordination::Event::CREATED:
            ProfileEvents::increment(ProfileEvents::KeeperWatchTriggeredNodeCreated, count);
            break;
        case Coordination::Event::DELETED:
            ProfileEvents::increment(ProfileEvents::KeeperWatchTriggeredNodeDeleted, count);
            break;
        case Coordination::Event::CHANGED:
            ProfileEvents::increment(ProfileEvents::KeeperWatchTriggeredNodeDataChanged, count);
            break;
        case Coordination::Event::CHILD:
            ProfileEvents::increment(ProfileEvents::KeeperWatchTriggeredNodeChildrenChanged, count);
            break;
        default:
            break;
    }
}

KeeperResponsesForSessions processWatchesImplBase(
    std::string_view path,
    KeeperStorage::Watches & watches,
    KeeperStorage::Watches & list_watches,
    KeeperStorage::SessionAndWatcher & sessions_and_watchers,
    Coordination::Event event_type,
    bool should_delete)
{
    KeeperResponsesForSessions result;

    auto watch_it = watches.find(path);
    if (watch_it != watches.end())
    {
        std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_response = std::make_shared<Coordination::ZooKeeperWatchResponse>();
        watch_response->path = std::string{path};
        watch_response->xid = Coordination::WATCH_XID;
        watch_response->zxid = -1;
        watch_response->type = event_type;
        watch_response->state = Coordination::State::CONNECTED;
        for (auto watcher_session : watch_it->second)
        {
            if (should_delete)
            {
                [[maybe_unused]] auto erased = sessions_and_watchers[watcher_session].erase(
                    KeeperStorage::WatchInfo{.path = path, .type = KeeperStorage::WatchType::WATCH});
                chassert(erased);
            }
            result.push_back(KeeperResponseForSession{watcher_session, watch_response});
        }
        incrementTriggeredWatchProfileEvent(event_type, watch_it->second.size());

        if (should_delete)
            watches.erase(watch_it);
    }

    auto parent_path = Coordination::parentNodePath(path);

    std::vector<std::string_view> paths_to_check_for_list_watches;
    if (event_type == Coordination::Event::CREATED)
    {
        paths_to_check_for_list_watches.push_back(parent_path); /// Trigger list watches for parent
    }
    else if (event_type == Coordination::Event::DELETED)
    {
        paths_to_check_for_list_watches.push_back(path); /// Trigger both list watches for this path
        paths_to_check_for_list_watches.push_back(parent_path); /// And for parent path
    }

    /// CHANGED event never trigger list watches

    for (const auto & path_to_check : paths_to_check_for_list_watches)
    {
        watch_it = list_watches.find(path_to_check);
        if (watch_it != list_watches.end())
        {
            std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_list_response
                = std::make_shared<Coordination::ZooKeeperWatchResponse>();
            watch_list_response->path = path_to_check;
            watch_list_response->xid = Coordination::WATCH_XID;
            watch_list_response->zxid = -1;
            if (path_to_check == parent_path)
                watch_list_response->type = Coordination::Event::CHILD;
            else
                watch_list_response->type = Coordination::Event::DELETED;
            watch_list_response->state = Coordination::State::CONNECTED;
            for (auto watcher_session : watch_it->second)
            {
                if (should_delete)
                {
                    [[maybe_unused]] auto erased = sessions_and_watchers[watcher_session].erase(
                        KeeperStorage::WatchInfo{.path = String(path_to_check), .type = KeeperStorage::WatchType::LIST_WATCH});
                    chassert(erased);
                }
                result.push_back(KeeperResponseForSession{watcher_session, watch_list_response});
            }
            incrementTriggeredWatchProfileEvent(
                static_cast<Coordination::Event>(watch_list_response->type),
                watch_it->second.size());

            if (should_delete)
                list_watches.erase(watch_it);
        }
    }

    return result;
}

}

void unregisterEphemeralPath(KeeperStorage::Ephemerals & ephemerals, int64_t session_id, const std::string & path, bool throw_if_missing)
{
    auto ephemerals_it = ephemerals.find(session_id);
    if (ephemerals_it == ephemerals.end())
    {
        if (throw_if_missing)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Session {} is expected to have ephemeral paths but no path is registered", session_id);

        return;
    }

    if (auto erased = ephemerals_it->second.erase(path); !erased && throw_if_missing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session {} is missing ephemeral path {}", session_id, path);

    if (ephemerals_it->second.empty())
        ephemerals.erase(ephemerals_it);
}

std::pair<KeeperResponsesForSessions, Int64> KeeperStorage::processWatchesImpl(
    std::string_view path,
    Coordination::Event event_type)
{
    KeeperResponsesForSessions result;

    auto process_non_persistent_watches = processWatchesImplBase(path, watches, list_watches, sessions_and_watchers, event_type, /*should_delete=*/true);
    auto process_persistent_watches = processWatchesImplBase(path, persistent_watches, persistent_list_watches, sessions_and_watchers, event_type, /*should_delete=*/false);

    if (!persistent_recursive_watches.empty())
    {
        std::string_view current_path = path;
        while (true)
        {
            auto watch_it = persistent_recursive_watches.find(current_path);
            if (watch_it != persistent_recursive_watches.end())
            {
                std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_list_response
                    = std::make_shared<Coordination::ZooKeeperWatchResponse>();
                watch_list_response->path = current_path;
                watch_list_response->xid = Coordination::WATCH_XID;
                watch_list_response->zxid = -1;
                watch_list_response->type = event_type;
                watch_list_response->state = Coordination::State::CONNECTED;
                for (auto watcher_session : watch_it->second)
                    result.push_back(KeeperResponseForSession{watcher_session, watch_list_response});
                incrementTriggeredWatchProfileEvent(event_type, watch_it->second.size());
            }

            if (current_path == "/")
                break;

            current_path = Coordination::parentNodePath(current_path);
        }
    }

    result.reserve(process_non_persistent_watches.size() + process_persistent_watches.size());
    std::ranges::copy(process_non_persistent_watches, std::back_inserter(result));
    std::ranges::copy(process_persistent_watches, std::back_inserter(result));

    return {result, process_non_persistent_watches.size()};
}

KeeperStorage::~KeeperStorage() = default;

KeeperStorage::KeeperStorage(
    int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_)
    : keeper_context(keeper_context_), superdigest(superdigest_), session_expiry_queue(tick_time_ms)
{

}

std::unique_ptr<KeeperStorage> KeeperStorage::create(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_, bool initialize_system_nodes)
{
    std::unique_ptr<KeeperStorage> res = std::make_unique<KeeperMemoryStorage>(tick_time_ms, superdigest_, keeper_context_);
    if (initialize_system_nodes)
        res->initializeSystemNodes();
    return res;
}

void KeeperStorage::initializeSystemNodes()
{
    if (initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage system nodes initialized twice");

    // insert root system path if it isn't already inserted
    nodes_storage->addCommittedNodeIfNotExists(
        "/", /*stats=*/{}, /*data=*/ "", /*update_parent_num_children=*/ false, &nodes_digest);
    nodes_storage->addCommittedNodeIfNotExists(
        keeper_system_path, /*stats=*/{}, /*data=*/ "", /*update_parent_num_children=*/ true, &nodes_digest);

    // insert child system nodes
    for (const auto & [path, data] : keeper_context->getSystemNodesWithData())
    {
        chassert(path.starts_with(keeper_system_path));

        /// Don't update digest and parent num_children (keep it at 0) to keep digest constant,
        /// independent of server version and configuration.
        nodes_storage->addCommittedNodeIfNotExists(
            path, /*stats=*/{}, data, /*update_parent_num_children=*/ false, /*out_digest=*/ nullptr);
    }

    initialized = true;
}

void KeeperStorage::loadFromSnapshot(KeeperSnapshotReader & reader)
{
    reader.readMetadata();
    reader.readACLMapAndNodeCount();

    bool recalculate_digest = reader.nodes_digest == 0 && keeper_context->digestEnabled();
    nodes_digest = reader.nodes_digest;

    nodes_storage->loadNodesFromSnapshot(reader, this, recalculate_digest ? &nodes_digest : nullptr);

    acl_map = std::move(reader.acl_map);
    zxid = reader.commit_zxid;
    old_snapshot_zxid = reader.old_snapshot_zxid;
    session_id_counter = reader.session_id_counter;

    reader.readSessionsAndClusterConfig(*this);

    initializeSystemNodes();
}

bool KeeperStorage::UncommittedState::hasACL(int64_t session_id, bool committed, std::function<bool(const AuthID &)> predicate) const
{
    const auto check_auth = [&](const auto & auth_ids)
    {
        for (const auto & auth : auth_ids)
        {
            using TAuth = std::remove_cvref_t<decltype(auth)>;

            const AuthID * auth_ptr = nullptr;
            if constexpr (std::same_as<TAuth, AuthID>)
                auth_ptr = &auth;
            else
                auth_ptr = auth.second.get();

            if (predicate(*auth_ptr))
                return true;
        }
        return false;
    };

    const auto check_session = [&](const auto & session_and_auth_map)
    {
        if (auto auth_it = session_and_auth_map.find(session_id); auth_it != session_and_auth_map.end())
            return check_auth(auth_it->second);
        return false;
    };

    if (!committed)
    {
        /// we want to close the session and with that we will remove all the auth related to the session
        if (closed_sessions_to_zxids.contains(session_id))
            return false;

        // check if there are uncommitted
        if (check_session(session_and_auth))
            return true;
    }

    std::shared_lock lock(storage.auth_mutex);
    return check_session(storage.committed_session_and_auth);
}

KeeperStorage::UncommittedState::UncommittedState(KeeperStorage & storage_) : storage(storage_) { }

KeeperStorage::UncommittedState::~UncommittedState() = default;

void KeeperStorage::UncommittedState::addDeltas(std::list<Delta> new_deltas)
{
    std::lock_guard lock(deltas_mutex);
    deltas.splice(deltas.end(), std::move(new_deltas));
}

void KeeperStorage::UncommittedState::cleanup(int64_t commit_zxid)
{
    storage.nodes_storage->cleanupUncommittedState(commit_zxid);

    for (auto it = session_and_auth.begin(); it != session_and_auth.end();)
    {
        auto & auths = it->second;
        std::erase_if(auths, [commit_zxid](auto auth_pair) { return auth_pair.first <= commit_zxid; });
        if (auths.empty())
            it = session_and_auth.erase(it);
        else
            ++it;
    }

    for (auto it = closed_sessions_to_zxids.begin(); it != closed_sessions_to_zxids.end();)
    {
        auto & zxids = it->second;
        std::erase_if(zxids, [commit_zxid](auto close_zxid) { return close_zxid <= commit_zxid; });
        if (zxids.empty())
            it = closed_sessions_to_zxids.erase(it);
        else
            ++it;
    }
}

void KeeperStorage::UncommittedState::rollback(int64_t rollback_zxid)
{
    // we can only rollback the last zxid (if there is any)
    std::list<Delta> rollback_deltas;
    {
        std::lock_guard lock(deltas_mutex);
        if (!deltas.empty() && deltas.back().zxid > rollback_zxid)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Invalid state of deltas found while trying to rollback request. Last ZXID ({}) is larger than the requested ZXID ({})",
                deltas.back().zxid,
                rollback_zxid);

        auto delta_it = deltas.rbegin();
        for (; delta_it != deltas.rend(); ++delta_it)
        {
            if (delta_it->zxid != rollback_zxid)
                break;
        }

        if (delta_it == deltas.rend())
            rollback_deltas = std::move(deltas);
        else
            rollback_deltas.splice(rollback_deltas.end(), deltas, delta_it.base(), deltas.end());
    }

    rollback(std::move(rollback_deltas));
}

void KeeperStorage::UncommittedState::rollback(std::list<Delta> rollback_deltas)
{
    // we need to undo ephemeral mapping modifications
    // CreateNodeDelta added ephemeral for session id -> we need to remove it
    // RemoveNodeDelta removed ephemeral for session id -> we need to add it back
    std::vector<uint64_t> rollbacked_zxids;
    for (auto delta_it = rollback_deltas.rbegin(); delta_it != rollback_deltas.rend(); ++delta_it)
    {
        const auto & delta = *delta_it;
        if (rollbacked_zxids.empty() || rollbacked_zxids.back() != static_cast<uint64_t>(delta.zxid))
            rollbacked_zxids.push_back(delta.zxid);
        if (!delta.path.empty())
        {
            std::visit(
                [&]<typename DeltaType>(const DeltaType & operation)
                {
                    if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
                    {
                        if (operation.stat.isEphemeral())
                            unregisterEphemeralPath(storage.uncommitted_state.ephemerals, operation.stat.getEphemeralOwner(), delta.path, /*throw_if_missing=*/false);
                        storage.acl_map.removeUsage(operation.stat.acl_id);
                    }
                    else if constexpr (std::same_as<DeltaType, UpdateNodeStatDelta>)
                    {
                        if (operation.old_stats.acl_id != operation.new_stats.acl_id)
                            storage.acl_map.removeUsage(operation.new_stats.acl_id);
                    }
                    else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
                    {
                        if (operation.stat.isEphemeral())
                            storage.uncommitted_state.ephemerals[operation.stat.getEphemeralOwner()].emplace(delta.path);
                    }
                },
                delta.operation);

            storage.nodes_storage->rollbackUncommittedDelta(delta);
        }
        else if (const auto * add_auth = std::get_if<AddAuthDelta>(&delta.operation))
        {
            auto & uncommitted_auth = session_and_auth[add_auth->session_id];
            if (uncommitted_auth.back().second == add_auth->auth_id)
            {
                uncommitted_auth.pop_back();
                if (uncommitted_auth.empty())
                    session_and_auth.erase(add_auth->session_id);
            }
        }
        else if (const auto * close_session = std::get_if<CloseSessionDelta>(&delta.operation))
        {
            auto & close_zxids = closed_sessions_to_zxids[close_session->session_id];
            [[maybe_unused]] auto erased = close_zxids.erase(delta.zxid);
            chassert(erased == 1);
            if (close_zxids.empty())
                closed_sessions_to_zxids.erase(close_session->session_id);
        }
    }

    storage.nodes_storage->cleanupAfterRollback(std::move(rollbacked_zxids));
}

void KeeperStorage::UncommittedState::forEachAuthInSession(int64_t session_id, std::function<void(const AuthID &)> func) const
{
    /// we can have some duplicate auths between uncommitted and committed
    std::vector<const AuthID *> processed_auths;
    const auto call_for_each_auth = [&](const auto & auth_ids)
    {
        for (const auto & auth : auth_ids)
        {
            using TAuth = std::remove_cvref_t<decltype(auth)>;

            const AuthID * auth_ptr = nullptr;
            if constexpr (std::same_as<TAuth, AuthID>)
                auth_ptr = &auth;
            else
                auth_ptr = auth.second.get();

            if (std::ranges::find_if(processed_auths, [&](const auto * processed_auth) { return *processed_auth == *auth_ptr; })
                != processed_auths.end())
                continue;

            processed_auths.push_back(auth_ptr);
            if (!auth_ptr->scheme.empty())
                func(*auth_ptr);
        }
    };

    // for uncommitted
    if (auto auth_it = session_and_auth.find(session_id); auth_it != session_and_auth.end())
        call_for_each_auth(auth_it->second);

    std::shared_lock lock(storage.auth_mutex);
    // for committed
    if (auto auth_it = storage.committed_session_and_auth.find(session_id); auth_it != storage.committed_session_and_auth.end())
        call_for_each_auth(auth_it->second);
}

[[noreturn]] void onStorageInconsistency(std::string_view message)
{
    LOG_ERROR(
        getLogger("KeeperStorage"),
        "Inconsistency found between uncommitted and committed data ({}). Keeper will terminate to avoid undefined behaviour.", message);
    std::terminate();
}

/// Get current committed zxid
int64_t KeeperStorage::getZXID() const
{
    std::lock_guard lock(transaction_mutex);
    return zxid;
}

int64_t KeeperStorage::getNextZXIDLocked() const
{
    if (uncommitted_transactions.empty())
        return zxid + 1;

    return uncommitted_transactions.back().zxid + 1;
}

int64_t KeeperStorage::getNextZXID() const
{
    std::lock_guard lock(transaction_mutex);
    return getNextZXIDLocked();
}

uint64_t KeeperStorage::getLastUncommittedLogIdx() const
{
    std::lock_guard lock(transaction_mutex);
    return uncommitted_transactions.empty() ? 0 : uncommitted_transactions.back().log_idx;
}

Coordination::Error KeeperStorage::commit(KeeperStorage::DeltaRange deltas)
{
    auto digest_on_commit = keeper_context->digestEnabled() && keeper_context->digestEnabledOnCommit();
    uint64_t digest_change = 0;
    uint64_t * digest = digest_on_commit ? &digest_change : nullptr;
    for (auto & delta : deltas)
    {
        auto result = std::visit(
            [&, &path = delta.path]<typename DeltaType>(const DeltaType & operation) -> Coordination::Error
            {
                if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
                {
                    if (operation.stat.isEphemeral())
                    {
                        ++committed_ephemeral_nodes;
                        std::lock_guard lock(ephemeral_mutex);
                        committed_ephemerals[operation.stat.getEphemeralOwner()].emplace(path);
                    }
                    if (operation.stat.isTTL())
                        ttl_paths.insert(path);
                    if (operation.stat.isContainer())
                        container_paths.insert(path);
                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, UpdateNodeStatDelta>)
                {
                    if (operation.old_stats.acl_id != operation.new_stats.acl_id)
                        acl_map.removeUsage(operation.old_stats.acl_id);
                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
                {
                    acl_map.removeUsage(operation.stat.acl_id);
                    if (operation.stat.isEphemeral())
                    {
                        chassert(committed_ephemeral_nodes != 0);
                        --committed_ephemeral_nodes;
                        std::lock_guard lock(ephemeral_mutex);
                        unregisterEphemeralPath(committed_ephemerals, operation.stat.getEphemeralOwner(), path, /*throw_if_missing=*/true);
                    }
                    if (operation.stat.isTTL())
                        ttl_paths.erase(path);
                    if (operation.stat.isContainer())
                        container_paths.erase(path);
                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, ErrorDelta>)
                {
                    return operation.error;
                }
                else if constexpr (std::same_as<DeltaType, AddAuthDelta>)
                {
                    std::lock_guard auth_lock{auth_mutex};
                    /// Copy instead of move because the uncommitted state may still hold
                    /// a shared_ptr to the same AuthID object, and a concurrent preprocess
                    /// call can read it without holding auth_mutex.
                    committed_session_and_auth[operation.session_id].emplace_back(*operation.auth_id);
                    return Coordination::Error::ZOK;
                }
                else
                {
                    return Coordination::Error::ZOK;
                }
            },
            delta.operation);

        if (!delta.path.empty())
            nodes_storage->commitDelta(delta, digest);

        if (result != Coordination::Error::ZOK)
        {
            /// Only ErrorDelta can produce error on commit.
            /// ErrorDelta should always be emitted alone (by `preprocess` on rejected request).
            /// If there could be any Deltas after an ErrorDelta, we'd have to do either commit
            /// or rollback on them here.
            chassert(std::distance(deltas.begin(), deltas.end()) == 1);

            return result;
        }
    }

    nodes_digest += digest_change;

    return Coordination::Error::ZOK;
}

bool KeeperStorage::checkACL(ACLId acl_id, int32_t permission, int64_t session_id, bool committed) const
{
    if (acl_id == 0)
        return true;
    const auto node_acls = acl_map.convertNumber(acl_id);

    if (uncommitted_state.hasACL(session_id, committed, [](const auto & auth_id) { return auth_id.scheme == "super"; }))
        return true;

    for (const auto & node_acl : node_acls)
    {
        if (node_acl.permissions & permission)
        {
            if (node_acl.scheme == "world" && node_acl.id == "anyone")
                return true;

            if (uncommitted_state.hasACL(
                    session_id,
                    committed,
                    [&](const auto & auth_id) { return auth_id.scheme == node_acl.scheme && auth_id.id == node_acl.id; }))
                return true;
        }
    }

    return false;
}

bool KeeperStorage::checkCommittedACL(std::string_view path, int32_t permission, int64_t session_id)
{
    ACLId acl_id = 0;
    {
        std::shared_lock lock(storage_mutex);
        KeeperNodeStats stats;
        if (nodes_storage->getCommittedNodeSimple(path, &stats, /*out_data=*/nullptr))
            acl_id = stats.acl_id;
    }

    return checkACL(acl_id, permission, session_id, /*committed=*/ true);
}

void KeeperStorage::finalize()
{
    if (finalized)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage already finalized");

    finalized = true;

    committed_ephemerals.clear();

    watches.clear();
    list_watches.clear();
    sessions_and_watchers.clear();

    session_expiry_queue.clear();

    read_thread_pool.shutdown();

    nodes_storage->shutdown();
}

bool KeeperStorage::isFinalized() const
{
    return finalized;
}

void KeeperStorage::rollbackRequest(int64_t rollback_zxid, bool allow_missing) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    if (allow_missing && (uncommitted_transactions.empty() || uncommitted_transactions.back().zxid < rollback_zxid))
        return;

    if (uncommitted_transactions.empty() || uncommitted_transactions.back().zxid != rollback_zxid)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Trying to rollback invalid ZXID ({}). It should be the last preprocessed.", rollback_zxid);
    }

    // if an exception occurs during rollback, the best option is to terminate because we can end up in an inconsistent state
    // we block memory tracking so we can avoid terminating if we're rolling back because of memory limit
    LockMemoryExceptionInThread blocker{VariableContext::Global};
    try
    {
        uncommitted_transactions.pop_back();
        uncommitted_state.rollback(rollback_zxid);
    }
    catch (...)
    {
        LOG_FATAL(getLogger("KeeperStorage"), "Failed to rollback log. Terminating to avoid inconsistencies");
        std::terminate();
    }
}

KeeperDigest KeeperStorage::getNodesDigest(bool committed, bool lock_transaction_mutex) const TSA_NO_THREAD_SAFETY_ANALYSIS
{
    if (!keeper_context->digestEnabled())
        return {.version = KeeperDigestVersion::NO_DIGEST};

    if (committed)
    {
        std::shared_lock storage_lock(storage_mutex);
        return {KEEPER_CURRENT_DIGEST_VERSION, nodes_digest};
    }

    std::unique_lock transaction_lock(transaction_mutex, std::defer_lock);
    if (lock_transaction_mutex)
        transaction_lock.lock();

    if (uncommitted_transactions.empty())
    {
        if (lock_transaction_mutex)
            transaction_lock.unlock();
        std::shared_lock storage_lock(storage_mutex);
        return {KEEPER_CURRENT_DIGEST_VERSION, nodes_digest};
    }

    return uncommitted_transactions.back().nodes_digest;
}

/// Allocate new session id with the specified timeouts
int64_t KeeperStorage::getSessionID(int64_t session_timeout_ms)
{
    auto result = session_id_counter++;
    session_and_timeout.emplace(result, session_timeout_ms);
    session_expiry_queue.addNewSessionOrUpdate(result, session_timeout_ms);
    return result;
}

/// Add session id. Used when restoring KeeperStorage from snapshot.
void KeeperStorage::addSessionID(int64_t session_id, int64_t session_timeout_ms)
{
    session_and_timeout.emplace(session_id, session_timeout_ms);
    session_expiry_queue.addNewSessionOrUpdate(session_id, session_timeout_ms);
}

std::vector<int64_t> KeeperStorage::getDeadSessions() const
{
    return session_expiry_queue.getExpiredSessions();
}

SessionAndTimeout KeeperStorage::getActiveSessions() const
{
    return session_and_timeout;
}

std::vector<std::pair<std::string, Int32>> KeeperStorage::collectExpiredTTLPaths(int64_t now_ms, size_t batch_size) const
{
    std::vector<std::pair<std::string, Int32>> result;

    SharedLockGuard storage_lock(storage_mutex);

    for (const auto & ttl_path : ttl_paths)
    {
        KeeperNodeStats stats;
        bool exists = nodes_storage->getCommittedNodeSimple(ttl_path, &stats, /*out_data=*/nullptr);
        if (!exists || !stats.isTTL())
        {
            LOG_ERROR(
                getLogger("KeeperStorage"),
                "Inconsistency: node {} is in ttl_paths but {}", ttl_path, exists ? "has no TTL in nodes storage" : "not in nodes storage");
            chassert(false, "inconsistent ttl_paths");
            continue;
        }
        if (now_ms >= stats.destroyTime())
        {
            result.emplace_back(ttl_path, stats.version);
            if (result.size() >= batch_size)
                break;
        }
    }

    return result;
}

std::vector<std::pair<std::string, Int32>> KeeperStorage::collectContainerCandidates(size_t batch_size, UInt64 max_never_used_interval_ms) const
{
    std::vector<std::pair<std::string, Int32>> result;

    const int64_t now_ms = max_never_used_interval_ms > 0
        ? std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()
        : 0;

    {
        /// `container_paths` and `container` are mutated by commit under exclusive
        /// `storage_mutex`. Take it shared to read them consistently.
        SharedLockGuard storage_lock(storage_mutex);

        for (const auto & container_path : container_paths)
        {
            KeeperNodeStats stats;
            bool exists = nodes_storage->getCommittedNodeSimple(container_path, &stats, /*out_data=*/nullptr);
            if (!exists || !stats.isContainer() || stats.getNumChildren() != 0)
                continue;

            /// Standard case: had children at some point but now empty.
            if (stats.cversion > 0)
                result.emplace_back(container_path, stats.version);
            /// Never-used case: created empty and still empty after the configured grace period.
            else if (max_never_used_interval_ms > 0 && now_ms - stats.getCTime() > static_cast<int64_t>(max_never_used_interval_ms))
                result.emplace_back(container_path, stats.version);

            if (result.size() >= batch_size)
                break;
        }
    }

    return result;
}

bool KeeperStorage::containsTTLPath(const std::string & path) const
{
    SharedLockGuard storage_lock(storage_mutex);
    return ttl_paths.contains(path);
}

void KeeperStorage::nodeLoadedFromSnapshot(std::string_view path, const KeeperNodeStats & stats)
{
    if (stats.isEphemeral())
    {
        committed_ephemerals[stats.getEphemeralOwner()].insert(std::string{path});
        ++committed_ephemeral_nodes;
    }
    if (stats.isTTL())
        ttl_paths.insert(std::string{path});
    if (stats.isContainer())
        container_paths.insert(std::string{path});
}

void KeeperStorage::clearDeadWatches(int64_t session_id)
{
    /// Clear all watches for this session
    auto watches_it = sessions_and_watchers.find(session_id);
    if (watches_it == sessions_and_watchers.end())
        return;

    size_t erased_watches = 0;
    for (auto watch_it = watches_it->second.begin(); watch_it != watches_it->second.end();)
    {
        auto erase_session_from_map = [&](auto & watch_map, const auto watch_path)
        {
            auto it = watch_map.find(watch_path);
            chassert(it != watch_map.end());
            auto & watches_for_path = it->second;
            watches_for_path.erase(session_id);
            if (watches_for_path.empty())
                watch_map.erase(it);
            watch_it = watches_it->second.erase(watch_it);
            ++erased_watches;
        };

        const auto [watch_path, watch_type] = *watch_it;
        switch (watch_type)
        {
            case WatchType::LIST_WATCH:
                erase_session_from_map(list_watches, watch_path);
                break;
            case WatchType::WATCH:
                erase_session_from_map(watches, watch_path);
                break;
            case WatchType::PERSISTENT_WATCH:
                erase_session_from_map(persistent_watches, watch_path);
                break;
            case WatchType::PERSISTENT_LIST_WATCH:
                erase_session_from_map(persistent_list_watches, watch_path);
                break;
            case WatchType::PERSISTENT_RECURSIVE_WATCH:
                erase_session_from_map(persistent_recursive_watches, watch_path);
                break;
        }
    }

    total_watches_count -= erased_watches;
    if (watches_it->second.empty())
        sessions_and_watchers.erase(watches_it);
}

void KeeperStorage::dumpWatches(WriteBufferFromOwnString & buf) const
{
    for (const auto & [session_id, watches_paths] : sessions_and_watchers)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
        for (const auto [path, watch_type] : watches_paths)
            buf << "\t" << path << "\n";
    }
}

void KeeperStorage::dumpWatchesByPath(WriteBufferFromOwnString & buf) const
{
    auto write_watches = [&](const auto & watch_map)
    {
        for (const auto & [watch_path, sessions] : watch_map)
        {
            buf << watch_path << "\n";
            for (int64_t session_id : sessions)
                buf << "\t0x" << getHexUIntLowercase(session_id) << "\n";
        }
    };

    write_watches(watches);
    write_watches(list_watches);
    write_watches(persistent_watches);
    write_watches(persistent_list_watches);
    write_watches(persistent_recursive_watches);
}

void KeeperStorage::dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const
{
    auto write_str_set = [&buf](const std::unordered_set<String> & ephemeral_paths)
    {
        for (const String & path : ephemeral_paths)
        {
            buf << "\t" << path << "\n";
        }
    };

    buf << "Sessions dump (" << session_and_timeout.size() << "):\n";

    for (const auto & [session_id, _] : session_and_timeout)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
    }

    buf << "Sessions with Ephemerals (" << committed_ephemerals.size() << "):\n";
    for (const auto & [session_id, ephemeral_paths] : committed_ephemerals)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
        write_str_set(ephemeral_paths);
    }
}

void KeeperStorage::updateWatches(
    const Coordination::ZooKeeperRequestPtr & zk_request,
    const Coordination::Response * response,
    int64_t session_id)
{
    const auto register_watch = [&](const Coordination::ZooKeeperRequestPtr & req, const Coordination::Response * resp)
    {
        if (resp->error == Coordination::Error::ZOK)
        {
            static constexpr std::array list_requests{
                Coordination::OpNum::List, Coordination::OpNum::SimpleList, Coordination::OpNum::FilteredList, Coordination::OpNum::FilteredListWithStatsAndData};

            auto watch_type = std::ranges::contains(list_requests, req->getOpNum()) ? WatchType::LIST_WATCH : WatchType::WATCH;

            auto & watches_type = watch_type == WatchType::LIST_WATCH ? list_watches : watches;

            auto [watch_it, path_inserted] = watches_type.try_emplace(req->getPath());
            auto [path_it, session_inserted] = watch_it->second.emplace(session_id);
            if (session_inserted)
            {
                ++total_watches_count;
                sessions_and_watchers[session_id].emplace(WatchInfo{.path = watch_it->first, .type = watch_type});
            }
        }
        else if (resp->error == Coordination::Error::ZNONODE && req->getOpNum() == Coordination::OpNum::Exists)
        {
            auto [watch_it, path_inserted] = watches.try_emplace(req->getPath());
            auto session_insert_info = watch_it->second.emplace(session_id);
            if (session_insert_info.second)
            {
                ++total_watches_count;
                sessions_and_watchers[session_id].emplace(WatchInfo{.path = watch_it->first, .type = WatchType::WATCH});
            }
        }
    };

    if (zk_request->getOpNum() == Coordination::OpNum::MultiRead)
    {
        const auto * multi_read_request = dynamic_cast<const Coordination::ZooKeeperMultiRequest *>(zk_request.get());
        const auto * multi_read_response = dynamic_cast<const Coordination::ZooKeeperMultiReadResponse *>(response);
        chassert(multi_read_request != nullptr);
        chassert(multi_read_response != nullptr);

        for (const auto [subrequest, subresponse] : std::views::zip(multi_read_request->requests, multi_read_response->responses))
        {
            if (subrequest->has_watch)
                register_watch(subrequest, subresponse.get());
        }
    }
    else if (zk_request->has_watch)
    {
        register_watch(zk_request, response);
    }
}

uint64_t KeeperStorage::getTotalWatchesCount() const
{
#ifndef NDEBUG
    size_t actual_watches_count = 0;
    for (const auto & [_, watches_for_session] : sessions_and_watchers)
        actual_watches_count += watches_for_session.size();

    chassert(actual_watches_count == total_watches_count, fmt::format("Actual watches count {} does not match total watches count {}", actual_watches_count, total_watches_count));
#endif

    return total_watches_count;
}

bool KeeperStorage::checkDigest(const KeeperDigest & first, const KeeperDigest & second)
{
    if (first.version != second.version)
        return true;

    if (first.version == KeeperDigestVersion::NO_DIGEST)
        return true;

    return first.value == second.value;
}

UInt64 KeeperStorage::WatchInfoHash::operator()(WatchInfo info) const
{
    SipHash hash;
    hash.update(info.path);
    hash.update(static_cast<uint8_t>(info.type));
    return hash.get64();
}

String KeeperStorage::generateDigest(const String & userdata)
{
    std::vector<String> user_password;
    boost::split(user_password, userdata, [](char character) { return character == ':'; });
    return user_password[0] + ":" + base64Encode(getSHA1(userdata));
}

bool KeeperStorage::containsWatch(const String & path, Coordination::CheckWatchRequest::CheckWatchType check_type) const
{
    using enum Coordination::CheckWatchRequest::CheckWatchType;
    switch (check_type)
    {
        case ANY:
            return containsWatch(path, DATA) || containsWatch(path, CHILDREN) || containsWatch(path, PERSISTENT)
                || containsWatch(path, PERSISTENT_RECURSIVE);
        case DATA:
            return watches.contains(path);
        case CHILDREN:
            return list_watches.contains(path);
        case PERSISTENT:
            return persistent_watches.contains(path) || persistent_list_watches.contains(path);
        case PERSISTENT_RECURSIVE:
            return persistent_recursive_watches.contains(path);
    }
}

void KeeperStorage::addPersistentWatch(const String & path, Coordination::AddWatchRequest::AddWatchMode mode, int64_t session_id)
{
    const auto add_persistent_watch = [&](auto & watch_map, WatchType watch_type)
    {
        auto [watch_it, path_inserted] = watch_map.try_emplace(path);
        auto [path_it, session_inserted] = watch_it->second.emplace(session_id);
        if (!session_inserted)
            return;

        ++total_watches_count;
        sessions_and_watchers[session_id].emplace(WatchInfo{.path = watch_it->first, .type = watch_type});
    };

    switch (mode)
    {
        case Coordination::AddWatchRequest::AddWatchMode::PERSISTENT:
        {
            add_persistent_watch(persistent_watches, WatchType::PERSISTENT_WATCH);
            add_persistent_watch(persistent_list_watches, WatchType::PERSISTENT_LIST_WATCH);
            break;
        }
        case Coordination::AddWatchRequest::AddWatchMode::PERSISTENT_RECURSIVE:
        {
            add_persistent_watch(persistent_recursive_watches, WatchType::PERSISTENT_RECURSIVE_WATCH);
            break;
        }
    }
}

KeeperResponsesForSessions KeeperStorage::setWatches(
    int64_t last_zxid,
    const std::vector<String> & watches_paths,
    const std::vector<String> & list_watches_paths,
    const std::vector<String> & exist_watches_paths,
    const std::vector<String> & persistent_watches_paths,
    const std::vector<String> & persistent_recursive_watches_paths,
    int64_t session_id)
{
    const auto add_watch = [&](const auto & path,auto & watch_map, WatchType watch_type)
    {
        auto [watch_it, path_inserted] = watch_map.try_emplace(path);
        auto [path_it, session_inserted] = watch_it->second.emplace(session_id);
        if (!session_inserted)
            return;
        ++total_watches_count;
        sessions_and_watchers[session_id].emplace(WatchInfo{.path = watch_it->first, .type = watch_type});
    };

    KeeperResponsesForSessions responses;
    const auto add_watch_response = [&](const auto & path, Coordination::Event event_type)
    {
        std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_response = std::make_shared<Coordination::ZooKeeperWatchResponse>();
        watch_response->path = path;
        watch_response->xid = Coordination::WATCH_XID;
        watch_response->zxid = -1;
        watch_response->type = event_type;
        watch_response->state = Coordination::State::CONNECTED;
        responses.push_back(KeeperResponseForSession{.session_id = session_id, .response = std::move(watch_response)});
    };

    for (const auto & path : watches_paths)
    {
        KeeperNodeStats stats;
        if (!nodes_storage->getCommittedNodeSimple(path, &stats, /*out_data=*/nullptr))
            add_watch_response(path, Coordination::Event::DELETED);
        else if (stats.mzxid <= last_zxid)
            add_watch(path, watches, WatchType::WATCH);
        else
            add_watch_response(path, Coordination::Event::CHANGED);
    }

    for (const auto & path : list_watches_paths)
    {
        KeeperNodeStats stats;
        if (!nodes_storage->getCommittedNodeSimple(path, &stats, /*out_data=*/nullptr))
            add_watch_response(path, Coordination::Event::DELETED);
        else if (stats.pzxid <= last_zxid)
            add_watch(path, list_watches, WatchType::LIST_WATCH);
        else
            add_watch_response(path, Coordination::Event::CHANGED);
    }

    for (const auto & path : exist_watches_paths)
    {
        if (nodes_storage->getCommittedNodeSimple(path, /*out_stats=*/nullptr, /*out_data=*/nullptr))
            add_watch_response(path, Coordination::Event::CREATED);
        else
            add_watch(path, watches, WatchType::WATCH);
    }

    for (const auto & path : persistent_watches_paths)
    {
        add_watch(path, persistent_watches, WatchType::PERSISTENT_WATCH);
        add_watch(path, persistent_list_watches, WatchType::PERSISTENT_LIST_WATCH);
    }

    for (const auto & path : persistent_recursive_watches_paths)
        add_watch(path, persistent_recursive_watches, WatchType::PERSISTENT_RECURSIVE_WATCH);

    return responses;
}

bool KeeperStorage::removePersistentWatch(const String & path, Coordination::RemoveWatchRequest::WatchType type, int64_t session_id)
{
    auto erase_watch_for_session = [&](auto & watch_map, WatchType watch_type)
    {
        auto watches_it = sessions_and_watchers.find(session_id);
        if (watches_it == sessions_and_watchers.end())
            return false;

        auto erased = watches_it->second.erase(WatchInfo{.path = path, .type = watch_type});
        if (!erased)
            return false;

        if (watches_it->second.empty())
            sessions_and_watchers.erase(watches_it);

        auto watch_it = watch_map.find(path);
        chassert(watch_it != watch_map.end());
        auto & watches_for_path = watch_it->second;
        erased = watches_for_path.erase(session_id);
        chassert(erased);
        if (watches_for_path.empty())
            watch_map.erase(watch_it);

        --total_watches_count;
        return true;
    };

    bool removed = false;
    switch (type)
    {
        case Coordination::RemoveWatchRequest::WatchType::ANY:
        {
            removed |= removePersistentWatch(path, Coordination::RemoveWatchRequest::WatchType::DATA, session_id);
            removed |= removePersistentWatch(path, Coordination::RemoveWatchRequest::WatchType::CHILDREN, session_id);
            removed |= removePersistentWatch(path, Coordination::RemoveWatchRequest::WatchType::PERSISTENT, session_id);
            removed |= removePersistentWatch(path, Coordination::RemoveWatchRequest::WatchType::PERSISTENTRECURSIVE, session_id);
            break;
        }
        case Coordination::RemoveWatchRequest::WatchType::DATA:
        {
            removed = erase_watch_for_session(watches, WatchType::WATCH);
            break;
        }
        case Coordination::RemoveWatchRequest::WatchType::CHILDREN:
        {
            removed = erase_watch_for_session(list_watches, WatchType::LIST_WATCH);
            break;
        }
        case Coordination::RemoveWatchRequest::WatchType::PERSISTENT:
        {
            removed |= erase_watch_for_session(persistent_watches, WatchType::PERSISTENT_WATCH);
            removed |= erase_watch_for_session(persistent_list_watches, WatchType::PERSISTENT_LIST_WATCH);
            break;
        }
        case Coordination::RemoveWatchRequest::WatchType::PERSISTENTRECURSIVE:
        {
            removed = erase_watch_for_session(persistent_recursive_watches, WatchType::PERSISTENT_RECURSIVE_WATCH);
            break;
        }
    }

    return removed;
}

void KeeperStorage::prepareAddAuth(std::shared_ptr<KeeperStorage::AuthID> new_auth, int64_t session_id)
{
    auto & uncommitted_auth = uncommitted_state.session_and_auth[session_id];
    uncommitted_auth.push_back(std::pair{staging.zxid, new_auth});
    staging.deltas.emplace_back(staging.zxid, AddAuthDelta{session_id, std::move(new_auth)});
}

KeeperStorageStats KeeperStorage::getStorageStats() const
{
    std::shared_lock lock(storage_mutex);
    KeeperStorageStats res
    {
        .total_watches_count = getTotalWatchesCount(),
        .watched_paths_count = watches.size() + list_watches.size() + persistent_watches.size() + persistent_list_watches.size()
        + persistent_recursive_watches.size(),
        .sessions_with_watches_count = sessions_and_watchers.size(),
        .session_with_ephemeral_nodes_count = committed_ephemerals.size(),
        .total_emphemeral_nodes_count = committed_ephemeral_nodes,
        .last_committed_zxid = getZXID(),
    };
    nodes_storage->getNodeStorageStats(res);
    CurrentMetrics::set(CurrentMetrics::KeeperTTLNodes, ttl_paths.size());
    CurrentMetrics::set(CurrentMetrics::KeeperContainerNodes, container_paths.size());
    return res;
}

}

// NOLINTEND(clang-analyzer-optin.core.EnumCastOutOfRange)
