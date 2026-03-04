
#include <Interpreters/DDLReplicator.h>
#include <Interpreters/DDLReplicateWorker.h>
#include <Interpreters/DDLReplicatorSettings.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLGuard.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DDLReplicatorQueryStatusSource.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/SyncReplicaMode.h>
#include <Processors/Sinks/EmptySink.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/SipHash.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Common/quoteString.h>
#include <base/scope_guard.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace Setting
{
    extern const SettingsDistributedDDLOutputMode distributed_ddl_output_mode;
    extern const SettingsInt64 distributed_ddl_task_timeout;
}

namespace DDLReplicatorSetting
{
    extern const DDLReplicatorSettingsUInt64 max_replication_lag_to_enqueue;
    extern const DDLReplicatorSettingsNonZeroUInt64 logs_to_keep;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_GET_REPLICATED_DATABASE_SNAPSHOT;
    extern const int REPLICA_ALREADY_EXISTS;
}

DDLReplicator::DDLReplicator(
    const String & zookeeper_name_,
    const String & zookeeper_path_,
    const String & shard_name_,
    const String & replica_name_)
    : zookeeper_name(zookeeper_name_)
    , zookeeper_path(zkutil::normalizeZooKeeperPath(zookeeper_path_, false))
    , shard_name(shard_name_)
    , replica_name(replica_name_)
    , replica_path(fs::path(zookeeper_path) / "replicas" / getFullReplicaName(shard_name, replica_name))
    , metadata_digest(0)
{
    if (zookeeper_path.empty() || shard_name.empty() || replica_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path, shard and replica names must be non-empty");
    if (shard_name.contains('/') || replica_name.contains('/'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard and replica names should not contain '/'");
    if (shard_name.contains('|') || replica_name.contains('|'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard and replica names should not contain '|'");
}

String DDLReplicator::getFullReplicaName(const String & shard, const String & replica)
{
    return shard + '|' + replica;
}

String DDLReplicator::getFullReplicaName() const
{
    return getFullReplicaName(shard_name, replica_name);
}

std::pair<String, String> DDLReplicator::parseFullReplicaName(const String & name)
{
    String shard;
    String replica;
    auto pos = name.find('|');
    if (pos == std::string::npos || name.find('|', pos + 1) != std::string::npos)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect replica identifier: {}", name);
    shard = name.substr(0, pos);
    replica = name.substr(pos + 1);
    return {shard, replica};
}

String DDLReplicator::getHostID(ContextPtr global_context, bool secure) const
{
    auto host_port = global_context->getInterserverIOAddress();
    UInt16 port = secure ? global_context->getTCPPortSecure().value_or(DBMS_DEFAULT_SECURE_PORT) : global_context->getTCPPort();
    return Cluster::Address::toString(host_port.first, port);
}

Coordination::Requests DDLReplicator::buildReplicatorNodesInZooKeeper()
{
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, getName(), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/log", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/counter", "", zkutil::CreateMode::Persistent));
    /// We create and remove counter/cnt- node to increment sequential number of counter/ node and make log entry numbers start from 1.
    /// New replicas are created with log pointer equal to 0 and log pointer is a number of the last executed entry.
    /// It means that we cannot have log entry with number 0.
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/counter/cnt-", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/counter/cnt-", -1));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/max_log_ptr", "1", zkutil::CreateMode::Persistent));
    ops.emplace_back(
        zkutil::makeCreateRequest(zookeeper_path + "/logs_to_keep", std::to_string(getSettings()[DDLReplicatorSetting::logs_to_keep].value), zkutil::CreateMode::Persistent));

    return ops;
}

bool DDLReplicator::createDDLReplicatorNodesInZooKeeper(const ZooKeeperPtr & current_zookeeper)
{
    current_zookeeper->createAncestors(zookeeper_path);

    Coordination::Requests ops = buildReplicatorNodesInZooKeeper();

    Coordination::Responses responses;
    auto res = current_zookeeper->tryMulti(ops, responses);
    if (res == Coordination::Error::ZOK)
        return true;    /// Created new replicator (it's the first replica)
    if (res == Coordination::Error::ZNODEEXISTS)
        return false;   /// Replicator exists, we will add new replica

    /// Other codes are unexpected, will throw
    zkutil::KeeperMultiException::check(res, ops, responses);
    UNREACHABLE();
}

bool DDLReplicator::looksLikeDDLReplicatorPath(const ZooKeeperPtr & current_zookeeper, const String & path, const String & mark)
{
    Coordination::Stat stat;
    String maybe_mark;
    if (!current_zookeeper->tryGet(path, maybe_mark, &stat))
        return false;
    if (maybe_mark.starts_with(mark))
        return true;
    if (!maybe_mark.empty())
        return false;

    /// Old versions did not have a mark. Check specific nodes exist and add mark.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCheckRequest(path + "/log", -1));
    ops.emplace_back(zkutil::makeCheckRequest(path + "/replicas", -1));
    ops.emplace_back(zkutil::makeCheckRequest(path + "/counter", -1));
    ops.emplace_back(zkutil::makeCheckRequest(path + "/metadata", -1));
    ops.emplace_back(zkutil::makeCheckRequest(path + "/max_log_ptr", -1));
    ops.emplace_back(zkutil::makeCheckRequest(path + "/logs_to_keep", -1));
    ops.emplace_back(zkutil::makeSetRequest(path, mark, stat.version));
    Coordination::Responses responses;
    auto res = current_zookeeper->tryMulti(ops, responses);
    if (res == Coordination::Error::ZOK)
        return true;

    /// Recheck mark (just in case of concurrent update).
    if (!current_zookeeper->tryGet(path, maybe_mark, &stat))
        return false;

    return maybe_mark.starts_with(mark);
}

void DDLReplicator::createReplicaNodesInZooKeeper(ContextPtr context, const ZooKeeperPtr & current_zookeeper)
{
    if (!looksLikeDDLReplicatorPath(current_zookeeper, zookeeper_path, getName()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot add new {} replica: provided path {} already contains some data and it does not look like DDLReplicator path.",
            getName(), zookeeper_path);

    /// Write host name to replica_path, it will protect from multiple replicas with the same name
    const auto host_id = getHostID(context, false);

    const std::vector<String> check_paths = {
        replica_path,
        replica_path + "/replica_group",
        replica_path + "/digest",
    };
    bool nodes_exist = true;
    auto check_responses = current_zookeeper->tryGet(check_paths);
    for (size_t i = 0; i < check_responses.size(); ++i)
    {
        const auto response = check_responses[i];

        if (response.error == Coordination::Error::ZNONODE)
        {
            nodes_exist = false;
            break;
        }
        if (response.error != Coordination::Error::ZOK)
        {
            throw zkutil::KeeperException::fromPath(response.error, check_paths[i]);
        }
    }

    if (nodes_exist)
    {
        const std::vector<String> expected_data = {
            host_id,
            replica_group_name,
            "0",
        };
        for (size_t i = 0; i != expected_data.size(); ++i)
        {
            if (check_responses[i].data != expected_data[i])
            {
                throw Exception(
                    ErrorCodes::REPLICA_ALREADY_EXISTS,
                    "Replica node {} in ZooKeeper already exists and contains unexpected value: {}",
                    quoteString(check_paths[i]), quoteString(check_responses[i].data));
            }
        }

        LOG_DEBUG(getLogger(), "Newly initialized replica nodes found in ZooKeeper, reusing them");
        createEmptyLogEntry(current_zookeeper);
        return;
    }

    for (int attempts = 10; attempts > 0; --attempts)
    {
        Coordination::Stat stat;
        const String max_log_ptr_str = current_zookeeper->get(zookeeper_path + "/max_log_ptr", &stat);

        const Coordination::Requests ops = {
            zkutil::makeCreateRequest(replica_path, host_id, zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest(replica_path + "/log_ptr", "0", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest(replica_path + "/digest", "0", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest(replica_path + "/replica_group", replica_group_name, zkutil::CreateMode::Persistent),

            /// Previously, this method was not idempotent and max_log_ptr_at_creation could be stored in memory.
            /// we need to store max_log_ptr_at_creation in ZooKeeper to make this method idempotent during replica creation.
            zkutil::makeCreateRequest(replica_path + "/max_log_ptr_at_creation", max_log_ptr_str, zkutil::CreateMode::Persistent),
            zkutil::makeCheckRequest(zookeeper_path + "/max_log_ptr", stat.version),
        };

        Coordination::Responses ops_responses;
        const auto code = current_zookeeper->tryMulti(ops, ops_responses);

        if (code == Coordination::Error::ZOK)
        {
            max_log_ptr_at_creation = parse<UInt32>(max_log_ptr_str);
            createEmptyLogEntry(current_zookeeper);
            return;
        }

        if (attempts == 1)
        {
            zkutil::KeeperMultiException::check(code, ops, ops_responses);
        }
    }
}

void DDLReplicator::createEmptyLogEntry(const ZooKeeperPtr & current_zookeeper)
{
    /// On replica creation add empty entry to log. Can be used to trigger some actions on other replicas (e.g. update cluster info).
    DDLLogEntry entry{};
    DDLReplicateWorker::enqueueQueryImpl(current_zookeeper, entry, this, true);
}

std::map<String, String> DDLReplicator::tryGetConsistentMetadataSnapshot(const ZooKeeperPtr & zookeeper, UInt32 & max_log_ptr) const
{
    return getConsistentMetadataSnapshotImpl(zookeeper, {}, /* max_retries= */ 10, max_log_ptr);
}

std::map<String, String> DDLReplicator::getConsistentMetadataSnapshotImpl(
    const ZooKeeperPtr & zookeeper,
    const std::function<bool(const String &)> & filter_by_name,
    size_t max_retries,
    UInt32 & max_log_ptr) const
{
    std::map<String, String> name_to_metadata;

    if (zookeeper->isFeatureEnabled(KeeperFeatureFlag::FILTERED_LIST) &&
        zookeeper->isFeatureEnabled(KeeperFeatureFlag::MULTI_READ) &&
        zookeeper->isFeatureEnabled(KeeperFeatureFlag::LIST_WITH_STAT_AND_DATA) &&
        !filter_by_name)
    {
        auto paths = {
            zookeeper_path + "/metadata",
            zookeeper_path
        };

        auto responses = zookeeper->getChildren(paths, Coordination::ListRequestType::ALL, /* with_stat = */ false, /* with_data = */ true);

        for (size_t i = 0; i < responses[0].names.size(); ++i)
            name_to_metadata.emplace(unescapeForFileName(responses[0].names[i]), std::move(responses[0].data[i]));

        auto it = std::find(responses[1].names.begin(), responses[1].names.end(), "max_log_ptr");
        if (it == responses[1].names.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "max_log_ptr node not found in ZooKeeper path {}", zookeeper_path);

        max_log_ptr = parse<UInt32>(responses[1].data[it - responses[1].names.begin()]);
        LOG_DEBUG(getLogger(), "Got consistent metadata snapshot for log pointer {}", max_log_ptr);
        return name_to_metadata;
    }

    size_t iteration = 0;
    auto metadata_path = zookeeper_path + "/metadata";
    while (++iteration <= max_retries)
    {
        name_to_metadata.clear();

        Coordination::Stat prev_metadata_path_stat;
        zookeeper->get(metadata_path, &prev_metadata_path_stat);

        LOG_DEBUG(getLogger(), "Trying to get consistent metadata snapshot for log pointer {}", max_log_ptr);

        Strings escaped_names;
        escaped_names = zookeeper->getChildren(metadata_path);
        if (filter_by_name)
            std::erase_if(escaped_names, [&](const String & name) { return !filter_by_name(unescapeForFileName(name)); });

        std::vector<String> paths_to_fetch;
        paths_to_fetch.reserve(escaped_names.size() + 1);

        for (const auto & name : escaped_names)
            paths_to_fetch.push_back(metadata_path + "/" + name);

        paths_to_fetch.push_back(zookeeper_path + "/max_log_ptr");

        auto metadata_and_version = zookeeper->tryGet(paths_to_fetch);

        Coordination::Stat current_metadata_path_stat;
        zookeeper->get(metadata_path, &current_metadata_path_stat);

        if (current_metadata_path_stat.czxid != prev_metadata_path_stat.czxid)
        {
            LOG_DEBUG(
                getLogger(),
                "Metadata zookeeper path was recreated. Created time before {}, current {}",
                prev_metadata_path_stat.czxid,
                current_metadata_path_stat.czxid);
            continue;
        }

        for (size_t i = 0; i < escaped_names.size(); ++i)
        {
            auto & res = metadata_and_version[i];
            if (res.error != Coordination::Error::ZOK)
                break;

            name_to_metadata.emplace(unescapeForFileName(escaped_names[i]), std::move(res.data));
        }

        auto current_max_log_ptr_idx = paths_to_fetch.size() - 1;
        auto current_max_log_ptr = metadata_and_version[current_max_log_ptr_idx];

        if (current_max_log_ptr.error != Coordination::Error::ZOK)
            Coordination::Exception::fromPath(current_max_log_ptr.error, zookeeper_path + "/max_log_ptr");

        UInt32 new_max_log_ptr = parse<UInt32>(current_max_log_ptr.data);
        if (new_max_log_ptr == max_log_ptr && escaped_names.size() == name_to_metadata.size())
            break;

        if (max_log_ptr < new_max_log_ptr)
        {
            LOG_DEBUG(getLogger(), "Log pointer moved from {} to {}, will retry", max_log_ptr, new_max_log_ptr);
            max_log_ptr = new_max_log_ptr;
        }
        else
        {
            chassert(max_log_ptr == new_max_log_ptr);
            chassert(escaped_names.size() != name_to_metadata.size());
            LOG_DEBUG(getLogger(), "Cannot get metadata of some entries due to ZooKeeper error, will retry");
        }
    }

    if (max_retries < iteration)
        throw Exception(ErrorCodes::CANNOT_GET_REPLICATED_DATABASE_SNAPSHOT, "Cannot get consistent metadata snapshot");

    LOG_DEBUG(getLogger(), "Got consistent metadata snapshot for log pointer {}", max_log_ptr);

    return name_to_metadata;
}

bool DDLReplicator::checkDigestValid(const ContextPtr & local_context) const
{
    LOG_TEST(getLogger(), "Current in-memory metadata digest: {}", metadata_digest);

    /// Database is probably being dropped
    if (!local_context->getZooKeeperMetadataTransaction() && (!ddl_worker || !ddl_worker->isCurrentlyActive()))
        return true;

    auto local_digest = getLocalDigest();

    if (local_digest != metadata_digest)
    {
        LOG_ERROR(getLogger(), "Digest of local metadata ({}) is not equal to in-memory digest ({})", local_digest, metadata_digest);

#ifndef NDEBUG
        tryCompareLocalAndZooKeeperTablesAndDumpDiffForDebugOnly(local_context);
#endif

        return false;
    }

    /// Do not check digest in Keeper after internal subquery, it's probably not committed yet
    if (local_context->isInternalSubquery())
        return true;

    /// Check does not make sense to check digest in Keeper during recovering
    if (is_recovering)
        return true;

    String zk_digest = getZooKeeper(local_context)->get(replica_path + "/digest");
    String local_digest_str = toString(local_digest);
    if (zk_digest != local_digest_str)
    {
        LOG_ERROR(getLogger(), "Digest of local metadata ({}) is not equal to digest in Keeper ({})", local_digest_str, zk_digest);
#ifndef NDEBUG
        tryCompareLocalAndZooKeeperTablesAndDumpDiffForDebugOnly(local_context);
#endif
        return false;
    }

    return true;
}

void DDLReplicator::assertDigestWithProbability(const ContextPtr & local_context) const
{
#if defined(DEBUG_OR_SANITIZER_BUILD)
    /// Reduce number of debug checks
    if (thread_local_rng() % 16)
        return;

    if (!checkDigestValid(local_context))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Digest does not match");
#else
    UNUSED(local_context);
#endif
}

void DDLReplicator::assertDigest(const ContextPtr & local_context)
{
#if defined(DEBUG_OR_SANITIZER_BUILD)
    if (local_context->isInternalQuery())
    {
        if (auto txn = local_context->getZooKeeperMetadataTransaction())
        {
            txn->addFinalizer([this, local_context]()
            {
                std::lock_guard lock{metadata_mutex};
                assertDigestWithProbability(local_context);
            });
        }
    }
    else
        assertDigestWithProbability(local_context);
#else
    UNUSED(local_context);
#endif
}

void DDLReplicator::assertDigestInTransactionOrInline(const ContextPtr & local_context, const ZooKeeperMetadataTransactionPtr & txn)
{
#if defined(DEBUG_OR_SANITIZER_BUILD)
    if (txn)
    {
        txn->addFinalizer([this, local_context]()
        {
            std::lock_guard lock{metadata_mutex};
            assertDigestWithProbability(local_context);
        });
    }
    else
        assertDigestWithProbability(local_context);
#else
    UNUSED(local_context);
    UNUSED(txn);
#endif
}

BlockIO DDLReplicator::getQueryStatus(
    const String & zookeeper_name_,
    const String & node_path,
    const String & replicas_path,
    ContextPtr context_,
    const Strings & hosts_to_wait,
    DDLGuardPtr && database_guard)
{
    BlockIO io;
    if (context_->getSettingsRef()[Setting::distributed_ddl_task_timeout] == 0)
        return io;

    auto source = std::make_shared<DDLReplicatorQueryStatusSource>(zookeeper_name_, node_path, replicas_path, context_, hosts_to_wait, std::move(database_guard));
    io.pipeline = QueryPipeline(std::move(source));

    if (context_->getSettingsRef()[Setting::distributed_ddl_output_mode] == DistributedDDLOutputMode::NONE
        || context_->getSettingsRef()[Setting::distributed_ddl_output_mode] == DistributedDDLOutputMode::NONE_ONLY_ACTIVE)
        io.pipeline.complete(std::make_shared<EmptySink>(io.pipeline.getSharedHeader()));

    return io;
}


ZooKeeperPtr DDLReplicator::getZooKeeper(ContextPtr context_) const
{
    return context_->getDefaultOrAuxiliaryZooKeeper(zookeeper_name);
}

void DDLReplicator::shutdownDDLWorker()
{
    std::lock_guard lock{ddl_worker_mutex};
    if (ddl_worker)
        ddl_worker->shutdown();
}

void DDLReplicator::resetDDLWorker()
{
    std::lock_guard lock{ddl_worker_mutex};
    ddl_worker_initialized = false;
    ddl_worker = nullptr;
}

bool DDLReplicator::waitForReplicaToProcessAllEntries(ContextPtr context_, UInt64 timeout_ms, SyncReplicaMode mode) /// NOLINT
{
    chassert(mode == SyncReplicaMode::DEFAULT || mode == SyncReplicaMode::STRICT);
    {
        std::lock_guard lock{ddl_worker_mutex};
        if (!ddl_worker)
            return false;
    }

    if (mode == SyncReplicaMode::DEFAULT)
        return ddl_worker->waitForReplicaToProcessAllEntries(timeout_ms);

    Stopwatch elapsed;
    while (true)
    {
        UInt64 elapsed_ms = elapsed.elapsedMilliseconds();
        if (elapsed_ms > timeout_ms)
            return false;

        if (ddl_worker->waitForReplicaToProcessAllEntries(timeout_ms - elapsed_ms))
            return true;

        UInt32 our_log_ptr = ddl_worker->getLogPointer();
        UInt32 max_log_ptr = parse<UInt32>(getZooKeeper(context_)->get(fs::path(zookeeper_path) / "max_log_ptr"));
        bool became_synced = our_log_ptr + getSettings()[DDLReplicatorSetting::max_replication_lag_to_enqueue] >= max_log_ptr;
        if (became_synced)
            return true;

        /// max_log_ptr might be increased while we were waiting - retry until replication lag is below the threshold
    }
}

}
