#include <Core/Defines.h>

#include <cstddef>
#include <ranges>
#include <chrono>

#include <base/hex.h>
#include <base/interpolate.h>
#include <Common/Macros.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEventsScope.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>
#include <Common/noexcept_scope.h>
#include <Common/thread_local_rng.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadFuzzer.h>

#include <Disks/ObjectStorages/IMetadataStorage.h>

#include <base/sort.h>

#include <Storages/buildQueryTreeForShard.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Freeze.h>
#include <Storages/MergeTree/AsyncBlockIDsCache.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/extractZkPathFromCreateQuery.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/LeaderElection.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeFromLogEntryTask.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MutateFromLogEntryTask.h>
#include <Storages/MergeTree/PinnedPartUUIDs.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAttachThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumAddedParts.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/ZeroCopyLock.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>

#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseReplicated.h>

#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ExpressionListParsers.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sinks/EmptySink.h>

#include <Planner/Utils.h>

#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>

#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/SelectQueryOptions.h>


#include <Backups/BackupEntriesCollector.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/IBackupEntry.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>

#include <Poco/DirectoryIterator.h>

#include <base/scope_guard.h>
#include <Common/scope_guard_safe.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <ctime>
#include <filesystem>
#include <iterator>
#include <numeric>
#include <thread>
#include <future>


namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event ReplicatedPartFailedFetches;
    extern const Event ReplicatedPartFetchesOfMerged;
    extern const Event ObsoleteReplicatedParts;
    extern const Event ReplicatedPartFetches;
    extern const Event CreatedLogEntryForMerge;
    extern const Event NotCreatedLogEntryForMerge;
    extern const Event CreatedLogEntryForMutation;
    extern const Event NotCreatedLogEntryForMutation;
    extern const Event ReplicaPartialShutdown;
}

namespace CurrentMetrics
{
    extern const Metric BackgroundFetchesPoolTask;
    extern const Metric ReadonlyReplica;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int NO_ZOOKEEPER;
    extern const int INCORRECT_DATA;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int REPLICA_ALREADY_EXISTS;
    extern const int NO_REPLICA_HAS_PART;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_UNEXPECTED_DATA_PARTS;
    extern const int ABORTED;
    extern const int REPLICA_IS_NOT_IN_QUORUM;
    extern const int TABLE_IS_READ_ONLY;
    extern const int NOT_FOUND_NODE;
    extern const int BAD_DATA_PART_NAME;
    extern const int NO_ACTIVE_REPLICAS;
    extern const int NOT_A_LEADER;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int PARTITION_ALREADY_EXISTS;
    extern const int TOO_MANY_RETRIES_TO_FETCH_PARTS;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int PARTITION_DOESNT_EXIST;
    extern const int UNFINISHED;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int ALL_REPLICAS_LOST;
    extern const int REPLICA_STATUS_CHANGED;
    extern const int CANNOT_ASSIGN_ALTER;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_POLICY;
    extern const int NO_SUCH_DATA_PART;
    extern const int INTERSERVER_SCHEME_DOESNT_MATCH;
    extern const int DUPLICATE_DATA_PART;
    extern const int BAD_ARGUMENTS;
    extern const int CONCURRENT_ACCESS_NOT_SUPPORTED;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int NOT_INITIALIZED;
    extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
    extern const int TABLE_IS_DROPPED;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsFetch;
    extern const StorageActionBlockType PartsSend;
    extern const StorageActionBlockType ReplicationQueue;
    extern const StorageActionBlockType PartsTTLMerge;
    extern const StorageActionBlockType PartsMove;
    extern const StorageActionBlockType PullReplicationLog;
}


static const auto QUEUE_UPDATE_ERROR_SLEEP_MS        = 1 * 1000;
static const auto MUTATIONS_FINALIZING_SLEEP_MS      = 1 * 1000;
static const auto MUTATIONS_FINALIZING_IDLE_SLEEP_MS = 5 * 1000;
const String StorageReplicatedMergeTree::default_zookeeper_name = "default";

void StorageReplicatedMergeTree::setZooKeeper()
{
    /// Every ReplicatedMergeTree table is using only one ZooKeeper session.
    /// But if several ReplicatedMergeTree tables are using different
    /// ZooKeeper sessions, some queries like ATTACH PARTITION FROM may have
    /// strange effects. So we always use only one session for all tables.
    /// (excluding auxiliary zookeepers)

    std::lock_guard lock(current_zookeeper_mutex);
    if (zookeeper_name == default_zookeeper_name)
    {
        current_zookeeper = getContext()->getZooKeeper();
    }
    else
    {
        current_zookeeper = getContext()->getAuxiliaryZooKeeper(zookeeper_name);
    }
}

zkutil::ZooKeeperPtr StorageReplicatedMergeTree::tryGetZooKeeper() const
{
    std::lock_guard lock(current_zookeeper_mutex);
    return current_zookeeper;
}

zkutil::ZooKeeperPtr StorageReplicatedMergeTree::getZooKeeper() const
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "Cannot get ZooKeeper");
    return res;
}

zkutil::ZooKeeperPtr StorageReplicatedMergeTree::getZooKeeperAndAssertNotReadonly() const
{
    /// There's a short period of time after connection loss when new session is created,
    /// but replication queue is not reinitialized. We must ensure that table is not readonly anymore
    /// before using new ZooKeeper session to write something (except maybe GET_PART) into replication log.
    auto res = getZooKeeper();
    assertNotReadonly();
    return res;
}

String StorageReplicatedMergeTree::getEndpointName() const
{
    const MergeTreeSettings & settings = getContext()->getReplicatedMergeTreeSettings();
    if (settings.enable_the_endpoint_id_with_zookeeper_name_prefix)
        return zookeeper_name + ":" + replica_path;

    return replica_path;
}

static ConnectionTimeouts getHTTPTimeouts(ContextPtr context)
{
    return ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), {context->getConfigRef().getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT), 0});
}

static MergeTreePartInfo makeDummyDropRangeForMovePartitionOrAttachPartitionFrom(const String & partition_id)
{
    /// NOTE We don't have special log entry type for MOVE PARTITION/ATTACH PARTITION FROM,
    /// so we use REPLACE_RANGE with dummy range of one block, which means "attach, not replace".
    /// It's safe to fill drop range for MOVE PARTITION/ATTACH PARTITION FROM with zeros,
    /// because drop range for REPLACE PARTITION must contain at least 2 blocks,
    /// so we can distinguish dummy drop range from any real or virtual part.
    /// But we should never construct such part name, even for virtual part,
    /// because it can be confused with real part <partition>_0_0_0.
    /// TODO get rid of this.

    MergeTreePartInfo drop_range;
    drop_range.partition_id = partition_id;
    drop_range.min_block = 0;
    drop_range.max_block = 0;
    drop_range.level = 0;
    drop_range.mutation = 0;
    return drop_range;
}

StorageReplicatedMergeTree::StorageReplicatedMergeTree(
    const String & zookeeper_path_,
    const String & replica_name_,
    bool attach,
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_,
    bool has_force_restore_data_flag,
    RenamingRestrictions renaming_restrictions_,
    bool need_check_structure)
    : MergeTreeData(table_id_,
                    metadata_,
                    context_,
                    date_column_name,
                    merging_params_,
                    std::move(settings_),
                    true,                   /// require_part_metadata
                    attach,
                    [this] (const std::string & name) { enqueuePartForCheck(name); })
    , zookeeper_name(zkutil::extractZooKeeperName(zookeeper_path_))
    , zookeeper_path(zkutil::extractZooKeeperPath(zookeeper_path_, /* check_starts_with_slash */ !attach, log))
    , replica_name(replica_name_)
    , replica_path(fs::path(zookeeper_path) / "replicas" / replica_name_)
    , reader(*this)
    , writer(*this)
    , merger_mutator(*this)
    , merge_strategy_picker(*this)
    , queue(*this, merge_strategy_picker)
    , fetcher(*this)
    , cleanup_thread(*this)
    , async_block_ids_cache(*this)
    , part_check_thread(*this)
    , restarting_thread(*this)
    , part_moves_between_shards_orchestrator(*this)
    , renaming_restrictions(renaming_restrictions_)
    , replicated_fetches_throttler(std::make_shared<Throttler>(getSettings()->max_replicated_fetches_network_bandwidth, getContext()->getReplicatedFetchesThrottler()))
    , replicated_sends_throttler(std::make_shared<Throttler>(getSettings()->max_replicated_sends_network_bandwidth, getContext()->getReplicatedSendsThrottler()))
{
    initializeDirectoriesAndFormatVersion(relative_data_path_, attach, date_column_name);
    /// We create and deactivate all tasks for consistency.
    /// They all will be scheduled and activated by the restarting thread.
    queue_updating_task = getContext()->getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageReplicatedMergeTree::queueUpdatingTask)", [this]{ queueUpdatingTask(); });

    queue_updating_task->deactivate();

    mutations_updating_task = getContext()->getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageReplicatedMergeTree::mutationsUpdatingTask)", [this]{ mutationsUpdatingTask(); });

    mutations_updating_task->deactivate();

    mutations_watch_callback = std::make_shared<Coordination::WatchCallback>(mutations_updating_task->getWatchCallback());

    merge_selecting_task = getContext()->getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageReplicatedMergeTree::mergeSelectingTask)", [this] { mergeSelectingTask(); });

    /// Will be activated if we will achieve leader state.
    merge_selecting_task->deactivate();
    merge_selecting_sleep_ms = getSettings()->merge_selecting_sleep_ms;

    mutations_finalizing_task = getContext()->getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageReplicatedMergeTree::mutationsFinalizingTask)", [this] { mutationsFinalizingTask(); });

    /// This task can be scheduled by different parts of code even when storage is readonly.
    /// This can lead to redundant exceptions during startup.
    /// Will be activated by restarting thread.
    mutations_finalizing_task->deactivate();

    bool has_zookeeper = getContext()->hasZooKeeper() || getContext()->hasAuxiliaryZooKeeper(zookeeper_name);
    if (has_zookeeper)
    {
        /// It's possible for getZooKeeper() to timeout if  zookeeper host(s) can't
        /// be reached. In such cases Poco::Exception is thrown after a connection
        /// timeout - refer to src/Common/ZooKeeper/ZooKeeperImpl.cpp:866 for more info.
        ///
        /// Side effect of this is that the CreateQuery gets interrupted and it exits.
        /// But the data Directories for the tables being created aren't cleaned up.
        /// This unclean state will hinder table creation on any retries and will
        /// complain that the Directory for table already exists.
        ///
        /// To achieve a clean state on failed table creations, catch this error and
        /// call dropIfEmpty() method only if the operation isn't ATTACH then proceed
        /// throwing the exception. Without this, the Directory for the tables need
        /// to be manually deleted before retrying the CreateQuery.
        try
        {
            if (zookeeper_name == default_zookeeper_name)
            {
                current_zookeeper = getContext()->getZooKeeper();
            }
            else
            {
                current_zookeeper = getContext()->getAuxiliaryZooKeeper(zookeeper_name);
            }
        }
        catch (...)
        {
            if (!attach)
            {
                dropIfEmpty();
                throw;
            }
            else
            {
                current_zookeeper = nullptr;
            }
        }
    }

    bool skip_sanity_checks = false;
    /// It does not make sense for CREATE query
    if (attach)
    {
        try
        {
            if (current_zookeeper && current_zookeeper->exists(replica_path + "/host"))
            {
                /// Check it earlier if we can (we don't want incompatible version to start).
                /// If "/host" doesn't exist, then replica is probably dropped and there's nothing to check.
                ReplicatedMergeTreeAttachThread::checkHasReplicaMetadataInZooKeeper(current_zookeeper, replica_path);
            }

            if (current_zookeeper && current_zookeeper->exists(replica_path + "/flags/force_restore_data"))
            {
                skip_sanity_checks = true;
                current_zookeeper->remove(replica_path + "/flags/force_restore_data");

                LOG_WARNING(
                    log,
                    "Skipping the limits on severity of changes to data parts and columns (flag {}/flags/force_restore_data).",
                    replica_path);
            }
            else if (has_force_restore_data_flag)
            {
                skip_sanity_checks = true;

                LOG_WARNING(log, "Skipping the limits on severity of changes to data parts and columns (flag force_restore_data).");
            }
        }
        catch (const Coordination::Exception & e)
        {
            if (!Coordination::isHardwareError(e.code))
                throw;
            LOG_ERROR(log, "Caught exception while checking table metadata in ZooKeeper, will recheck later: {}", e.displayText());
        }
    }

    loadDataParts(skip_sanity_checks);

    if (attach)
    {
        /// Provide better initial value of merge_selecting_sleep_ms on server startup
        auto settings = getSettings();
        size_t max_parts_in_partition = getMaxPartsCountAndSizeForPartition().first;
        if (settings->parts_to_delay_insert && max_parts_in_partition < settings->parts_to_delay_insert)
        {
            Float64 ratio = 1.0 - static_cast<Float64>(max_parts_in_partition) / settings->parts_to_delay_insert;
            merge_selecting_sleep_ms = static_cast<UInt64>(interpolateLinear(settings->merge_selecting_sleep_ms,
                                                                             settings->max_merge_selecting_sleep_ms, ratio));
        }
    }

    if (!current_zookeeper)
    {
        if (!attach)
        {
            dropIfEmpty();
            throw Exception(ErrorCodes::NO_ZOOKEEPER, "Can't create replicated table without ZooKeeper");
        }

        has_metadata_in_zookeeper = std::nullopt;

        if (!has_zookeeper)
        {
            /// Do not activate the replica. It will be readonly.
            LOG_ERROR(log, "No ZooKeeper defined: table will stay in readonly mode.");
            return;
        }
    }

    if (attach)
    {
        LOG_INFO(log, "Table will be in readonly mode until initialization is finished");
        attach_thread.emplace(*this);
        attach_thread->setSkipSanityChecks(skip_sanity_checks);
        return;
    }

    auto metadata_snapshot = getInMemoryMetadataPtr();

    has_metadata_in_zookeeper = true;

    if (!getDataPartsForInternalUsage().empty())
        throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Data directory for table already contains data parts - probably it was unclean DROP table "
                        "or manual intervention. You must either clear directory by hand "
                        "or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.");

    try
    {
        bool is_first_replica = createTableIfNotExists(metadata_snapshot);

        try
        {
            /// NOTE If it's the first replica, these requests to ZooKeeper look redundant, we already know everything.

            /// We have to check granularity on other replicas. If it's fixed we
            /// must create our new replica with fixed granularity and store this
            /// information in /replica/metadata.
            other_replicas_fixed_granularity = checkFixedGranularityInZookeeper();

            /// Allow structure mismatch for secondary queries from Replicated database.
            /// It may happen if the table was altered just after creation.
            /// Metadata will be updated in cloneMetadataIfNeeded(...), metadata_version will be 0 for a while.
            bool same_structure = checkTableStructure(zookeeper_path, metadata_snapshot, need_check_structure);

            if (same_structure)
            {
                Coordination::Stat metadata_stat;
                current_zookeeper->get(zookeeper_path + "/metadata", &metadata_stat);
                setInMemoryMetadata(metadata_snapshot->withMetadataVersion(metadata_stat.version));
            }
        }
        catch (Coordination::Exception & e)
        {
            if (!is_first_replica && e.code == Coordination::Error::ZNONODE)
                throw Exception(ErrorCodes::ALL_REPLICAS_LOST, "Table {} was suddenly removed.", zookeeper_path);
            else
                throw;
        }

        if (!is_first_replica)
            createReplica(metadata_snapshot);
    }
    catch (...)
    {
        /// If replica was not created, rollback creation of data directory.
        dropIfEmpty();
        throw;
    }

    createNewZooKeeperNodes();
    syncPinnedPartUUIDs();

    if (!has_metadata_in_zookeeper.has_value() || *has_metadata_in_zookeeper)
        createTableSharedID();

    initialization_done = true;
}


String StorageReplicatedMergeTree::getDefaultZooKeeperPath(const Poco::Util::AbstractConfiguration & config)
{
    return config.getString("default_replica_path", "/clickhouse/tables/{uuid}/{shard}");
}


String StorageReplicatedMergeTree::getDefaultReplicaName(const Poco::Util::AbstractConfiguration & config)
{
    return config.getString("default_replica_name", "{replica}");
}


bool StorageReplicatedMergeTree::checkFixedGranularityInZookeeper()
{
    auto zookeeper = getZooKeeper();
    String metadata_str = zookeeper->get(zookeeper_path + "/metadata");
    auto metadata_from_zk = ReplicatedMergeTreeTableMetadata::parse(metadata_str);
    return metadata_from_zk.index_granularity_bytes == 0;
}


void StorageReplicatedMergeTree::waitMutationToFinishOnReplicas(
    const Strings & replicas, const String & mutation_id) const
{
    if (replicas.empty())
        return;

    /// Current replica must always be present in the list as the first element because we use local mutation status
    /// to check for mutation errors. So if it is not there, just add it.
    const Strings * all_required_replicas = &replicas;
    Strings extended_list_of_replicas;
    if (replicas.front() != replica_name)
    {
        extended_list_of_replicas.push_back(replica_name);
        extended_list_of_replicas.insert(extended_list_of_replicas.end(), replicas.begin(), replicas.end());
        all_required_replicas = &extended_list_of_replicas;
    }

    std::set<String> inactive_replicas;
    for (const String & replica : *all_required_replicas)
    {
        LOG_DEBUG(log, "Waiting for {} to apply mutation {}", replica, mutation_id);
        zkutil::EventPtr wait_event = std::make_shared<Poco::Event>();

        while (!partial_shutdown_called)
        {
            /// Mutation maybe killed or whole replica was deleted.
            /// Wait event will unblock at this moment.
            Coordination::Stat exists_stat;
            if (!getZooKeeper()->exists(fs::path(zookeeper_path) / "mutations" / mutation_id, &exists_stat, wait_event))
            {
                throw Exception(ErrorCodes::UNFINISHED, "Mutation {} was killed, manually removed or table was dropped", mutation_id);
            }

            auto zookeeper = getZooKeeper();
            /// Replica could be inactive.
            if (!zookeeper->exists(fs::path(zookeeper_path) / "replicas" / replica / "is_active"))
            {
                LOG_WARNING(log, "Replica {} is not active during mutation. Mutation will be done asynchronously when replica becomes active.", replica);

                inactive_replicas.emplace(replica);
                break;
            }

            String mutation_pointer = fs::path(zookeeper_path) / "replicas" / replica / "mutation_pointer";
            std::string mutation_pointer_value;
            /// Replica could be removed
            if (!zookeeper->tryGet(mutation_pointer, mutation_pointer_value, nullptr, wait_event))
            {
                LOG_WARNING(log, "Replica {} was removed", replica);
                break;
            }
            else if (mutation_pointer_value >= mutation_id) /// Maybe we already processed more fresh mutation
                break;                                      /// (numbers like 0000000000 and 0000000001)

            /// Replica can become inactive, so wait with timeout and recheck it
            if (wait_event->tryWait(1000))
                continue;

            /// Here we check mutation for errors on local replica. If they happen on this replica
            /// they will happen on each replica, so we can check only in-memory info.
            auto mutation_status = queue.getIncompleteMutationsStatus(mutation_id);
            /// If mutation status is empty, than local replica may just not loaded it into memory.
            if (mutation_status && !mutation_status->latest_fail_reason.empty())
                break;
        }

        /// This replica inactive, don't check anything
        if (!inactive_replicas.empty() && inactive_replicas.contains(replica))
            break;

        /// It maybe already removed from zk, but local in-memory mutations
        /// state was not updated.
        if (!getZooKeeper()->exists(fs::path(zookeeper_path) / "mutations" / mutation_id))
        {
            throw Exception(ErrorCodes::UNFINISHED, "Mutation {} was killed, manually removed or table was dropped", mutation_id);
        }

        if (partial_shutdown_called)
            throw Exception(ErrorCodes::UNFINISHED,
                            "Mutation is not finished because table shutdown was called. "
                            "It will be done after table restart.");

        /// Replica inactive, don't check mutation status
        if (!inactive_replicas.empty() && inactive_replicas.contains(replica))
            continue;

        /// At least we have our current mutation
        std::set<String> mutation_ids;
        mutation_ids.insert(mutation_id);

        /// Here we check mutation for errors or kill on local replica. If they happen on this replica
        /// they will happen on each replica, so we can check only in-memory info.
        auto mutation_status = queue.getIncompleteMutationsStatus(mutation_id, &mutation_ids);
        checkMutationStatus(mutation_status, mutation_ids);
    }

    if (!inactive_replicas.empty())
    {
        throw Exception(ErrorCodes::UNFINISHED,
                        "Mutation is not finished because some replicas are inactive right now: {}. Mutation will be done asynchronously",
                        boost::algorithm::join(inactive_replicas, ", "));
    }
}

void StorageReplicatedMergeTree::createNewZooKeeperNodes()
{
    auto zookeeper = getZooKeeper();

    std::vector<zkutil::ZooKeeper::FutureCreate> futures;

    /// These 4 nodes used to be created in createNewZookeeperNodes() and they were moved to createTable()
    /// This means that if the first replica creating the table metadata has an older version of CH (22.3 or previous)
    /// there will be a time between its calls to `createTable` and `createNewZookeeperNodes` where the nodes won't exists
    /// and that will cause issues in newer replicas
    /// See https://github.com/ClickHouse/ClickHouse/issues/38600 for example
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/quorum", String(), zkutil::CreateMode::Persistent));
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/quorum/last_part", String(), zkutil::CreateMode::Persistent));
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/quorum/failed_parts", String(), zkutil::CreateMode::Persistent));
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/mutations", String(), zkutil::CreateMode::Persistent));


    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/quorum/parallel", String(), zkutil::CreateMode::Persistent));
    /// Nodes for remote fs zero-copy replication
    const auto settings = getSettings();
    if (settings->allow_remote_fs_zero_copy_replication)
    {
        futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/zero_copy_s3", String(), zkutil::CreateMode::Persistent));
        futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/zero_copy_s3/shared", String(), zkutil::CreateMode::Persistent));
        futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/zero_copy_hdfs", String(), zkutil::CreateMode::Persistent));
        futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/zero_copy_hdfs/shared", String(), zkutil::CreateMode::Persistent));
    }

    /// Part movement.
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/part_moves_shard", String(), zkutil::CreateMode::Persistent));
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/pinned_part_uuids", getPinnedPartUUIDs()->toString(), zkutil::CreateMode::Persistent));
    /// For ALTER PARTITION with multi-leaders
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/alter_partition_version", String(), zkutil::CreateMode::Persistent));
    /// For deduplication of async inserts
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/async_blocks", String(), zkutil::CreateMode::Persistent));
    /// To track "lost forever" parts count, just for `system.replicas` table
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/lost_part_count", String(), zkutil::CreateMode::Persistent));

    /// As for now, "/temp" node must exist, but we want to be able to remove it in future
    if (zookeeper->exists(zookeeper_path + "/temp"))
    {
        /// For block numbers allocation (since 22.11)
        futures.push_back(zookeeper->asyncTryCreateNoThrow(
            zookeeper_path + "/temp/" + EphemeralLockInZooKeeper::LEGACY_LOCK_INSERT, String(), zkutil::CreateMode::Persistent));
        futures.push_back(zookeeper->asyncTryCreateNoThrow(
            zookeeper_path + "/temp/" + EphemeralLockInZooKeeper::LEGACY_LOCK_OTHER, String(), zkutil::CreateMode::Persistent));
    }

    for (auto & future : futures)
    {
        auto res = future.get();
        if (res.error != Coordination::Error::ZOK && res.error != Coordination::Error::ZNODEEXISTS)
            throw Coordination::Exception(res.error, "Failed to create new nodes at {}", zookeeper_path);
    }
}


bool StorageReplicatedMergeTree::createTableIfNotExists(const StorageMetadataPtr & metadata_snapshot)
{
    auto zookeeper = getZooKeeper();
    zookeeper->createAncestors(zookeeper_path);

    for (size_t i = 0; i < 1000; ++i)
    {
        /// Invariant: "replicas" does not exist if there is no table or if there are leftovers from incompletely dropped table.
        if (zookeeper->exists(zookeeper_path + "/replicas"))
        {
            LOG_DEBUG(log, "This table {} is already created, will add new replica", zookeeper_path);
            return false;
        }

        /// There are leftovers from incompletely dropped table.
        if (zookeeper->exists(zookeeper_path + "/dropped"))
        {
            /// This condition may happen when the previous drop attempt was not completed
            ///  or when table is dropped by another replica right now.
            /// This is Ok because another replica is definitely going to drop the table.

            LOG_WARNING(log, "Removing leftovers from table {} (this might take several minutes)", zookeeper_path);
            String drop_lock_path = zookeeper_path + "/dropped/lock";
            Coordination::Error code = zookeeper->tryCreate(drop_lock_path, "", zkutil::CreateMode::Ephemeral);

            if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_WARNING(log, "The leftovers from table {} were removed by another replica", zookeeper_path);
            }
            else if (code != Coordination::Error::ZOK)
            {
                throw Coordination::Exception::fromPath(code, drop_lock_path);
            }
            else
            {
                auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *zookeeper);
                if (!removeTableNodesFromZooKeeper(zookeeper, zookeeper_path, metadata_drop_lock, log))
                {
                    /// Someone is recursively removing table right now, we cannot create new table until old one is removed
                    continue;
                }
            }
        }

        LOG_DEBUG(log, "Creating table {}", zookeeper_path);

        /// We write metadata of table so that the replicas can check table parameters with them.
        String metadata_str = ReplicatedMergeTreeTableMetadata(*this, metadata_snapshot).toString();

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", metadata_str,
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/columns", metadata_snapshot->getColumns().toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/log", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/blocks", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/async_blocks", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/block_numbers", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/nonincrement_block_numbers", "",
            zkutil::CreateMode::Persistent)); /// /nonincrement_block_numbers dir is unused, but is created nonetheless for backwards compatibility.
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/leader_election", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/temp", "",
            zkutil::CreateMode::Persistent));

        /// The following 2 nodes were added in 22.11
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/temp/" + EphemeralLockInZooKeeper::LEGACY_LOCK_INSERT, "",
                                                   zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/temp/" + EphemeralLockInZooKeeper::LEGACY_LOCK_OTHER, "",
                                                   zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name,
            zkutil::CreateMode::Persistent));

        /// The following 4 nodes were added in version 1.1.xxx, so we create them here, not in createNewZooKeeperNodes()
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/quorum", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/quorum/last_part", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/quorum/failed_parts", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/mutations", "",
            zkutil::CreateMode::Persistent));

        /// And create first replica atomically. See also "createReplica" method that is used to create not the first replicas.

        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/log_pointer", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/queue", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/parts", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/flags", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/is_lost", "0",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata", metadata_str,
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/columns", metadata_snapshot->getColumns().toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata_version", toString(metadata_snapshot->getMetadataVersion()),
            zkutil::CreateMode::Persistent));

        /// The following 3 nodes were added in version 1.1.xxx, so we create them here, not in createNewZooKeeperNodes()
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/min_unprocessed_insert_time", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/max_processed_insert_time", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/mutation_pointer", "",
            zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_INFO(log, "It looks like the table {} was created by another server at the same moment, will retry", zookeeper_path);
            continue;
        }
        else if (code != Coordination::Error::ZOK)
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }

        return true;
    }

    /// Do not use LOGICAL_ERROR code, because it may happen if user has specified wrong zookeeper_path
    throw Exception(ErrorCodes::REPLICA_ALREADY_EXISTS,
                    "Cannot create table, because it is created concurrently every time or because "
                    "of wrong zookeeper_path or because of logical error");
}

void StorageReplicatedMergeTree::createReplica(const StorageMetadataPtr & metadata_snapshot)
{
    auto zookeeper = getZooKeeper();

    LOG_DEBUG(log, "Creating replica {}", replica_path);

    Coordination::Error code;

    do
    {
        Coordination::Stat replicas_stat;
        String replicas_value;

        if (!zookeeper->tryGet(zookeeper_path + "/replicas", replicas_value, &replicas_stat))
            throw Exception(ErrorCodes::ALL_REPLICAS_LOST,
                "Cannot create a replica of the table {}, because the last replica of the table was dropped right now",
                zookeeper_path);

        /// It is not the first replica, we will mark it as "lost", to immediately repair (clone) from existing replica.
        /// By the way, it's possible that the replica will be first, if all previous replicas were removed concurrently.
        const String is_lost_value = replicas_stat.numChildren ? "1" : "0";

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/log_pointer", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/queue", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/parts", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/flags", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/is_lost", is_lost_value,
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata", ReplicatedMergeTreeTableMetadata(*this, metadata_snapshot).toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/columns", metadata_snapshot->getColumns().toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata_version", toString(metadata_snapshot->getMetadataVersion()),
            zkutil::CreateMode::Persistent));

        /// The following 3 nodes were added in version 1.1.xxx, so we create them here, not in createNewZooKeeperNodes()
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/min_unprocessed_insert_time", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/max_processed_insert_time", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/mutation_pointer", "",
            zkutil::CreateMode::Persistent));

        /// Check version of /replicas to see if there are any replicas created at the same moment of time.
        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name, replicas_stat.version));

        Coordination::Responses responses;
        code = zookeeper->tryMulti(ops, responses);

        switch (code)
        {
            case Coordination::Error::ZNODEEXISTS:
                throw Exception(ErrorCodes::REPLICA_ALREADY_EXISTS, "Replica {} already exists", replica_path);
            case Coordination::Error::ZBADVERSION:
                LOG_INFO(log, "Retrying createReplica(), because some other replicas were created at the same time");
                break;
            case Coordination::Error::ZNONODE:
                throw Exception(ErrorCodes::ALL_REPLICAS_LOST, "Table {} was suddenly removed", zookeeper_path);
            default:
                zkutil::KeeperMultiException::check(code, ops, responses);
        }
    } while (code == Coordination::Error::ZBADVERSION);
}


zkutil::ZooKeeperPtr StorageReplicatedMergeTree::getZooKeeperIfTableShutDown() const
{
    zkutil::ZooKeeperPtr maybe_new_zookeeper;
    if (zookeeper_name == default_zookeeper_name)
        maybe_new_zookeeper = getContext()->getZooKeeper();
    else
        maybe_new_zookeeper = getContext()->getAuxiliaryZooKeeper(zookeeper_name);
    maybe_new_zookeeper->sync(zookeeper_path);
    return maybe_new_zookeeper;
}

void StorageReplicatedMergeTree::drop()
{
    /// There is also the case when user has configured ClickHouse to wrong ZooKeeper cluster
    /// or metadata of staled replica were removed manually,
    /// in this case, has_metadata_in_zookeeper = false, and we also permit to drop the table.

    bool maybe_has_metadata_in_zookeeper = !has_metadata_in_zookeeper.has_value() || *has_metadata_in_zookeeper;
    zkutil::ZooKeeperPtr zookeeper;
    if (maybe_has_metadata_in_zookeeper)
    {
        /// Table can be shut down, restarting thread is not active
        /// and calling StorageReplicatedMergeTree::getZooKeeper()/getAuxiliaryZooKeeper() won't suffice.
        zookeeper = getZooKeeperIfTableShutDown();

        /// If probably there is metadata in ZooKeeper, we don't allow to drop the table.
        if (!zookeeper)
            throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Can't drop readonly replicated table (need to drop data in ZooKeeper as well)");
    }

    /// Wait for loading of all outdated parts because
    /// in case of zero copy recursive removal of directory
    /// is not supported and table cannot be dropped.
    if (canUseZeroCopyReplication())
    {
        /// Load remaining parts synchronously because task
        /// for loading is already cancelled in shutdown().
        loadOutdatedDataParts(/*is_async=*/ false);
    }

    dropAllData();

    if (maybe_has_metadata_in_zookeeper)
    {
        /// Session could expire, get it again
        zookeeper = getZooKeeperIfTableShutDown();
        dropReplica(zookeeper, zookeeper_path, replica_name, log, getSettings(), &has_metadata_in_zookeeper);
    }
}

void StorageReplicatedMergeTree::dropReplica(zkutil::ZooKeeperPtr zookeeper, const String & zookeeper_path, const String & replica,
                                             Poco::Logger * logger, MergeTreeSettingsPtr table_settings, std::optional<bool> * has_metadata_out)
{
    if (zookeeper->expired())
        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "Table was not dropped because ZooKeeper session has expired.");

    auto remote_replica_path = zookeeper_path + "/replicas/" + replica;

    LOG_INFO(logger, "Removing replica {}, marking it as lost", remote_replica_path);
    /// Mark itself lost before removing, because the following recursive removal may fail
    /// and partially dropped replica may be considered as alive one (until someone will mark it lost)
    zookeeper->trySet(remote_replica_path + "/is_lost", "1");

    /// NOTE: we should check for remote_replica_path existence,
    /// since otherwise DROP REPLICA will fail if the replica had been already removed.
    if (!zookeeper->exists(remote_replica_path))
    {
        LOG_INFO(logger, "Removing replica {} does not exist", remote_replica_path);
        return;
    }

    {
        /// Remove "host" node first to mark replica as dropped (the choice is arbitrary,
        /// it could be any node without children that exists since ancient server versions and not re-created on startup)
        [[maybe_unused]] auto code = zookeeper->tryRemove(fs::path(remote_replica_path) / "host");
        assert(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE);

        /// Then try to remove paths that are known to be flat (all children are leafs)
        Strings flat_nodes = {"flags", "queue"};
        if (table_settings && table_settings->use_minimalistic_part_header_in_zookeeper)
            flat_nodes.emplace_back("parts");
        for (const auto & node : flat_nodes)
        {
            bool removed_quickly = zookeeper->tryRemoveChildrenRecursive(fs::path(remote_replica_path) / node, /* probably flat */ true);
            if (!removed_quickly)
                LOG_WARNING(logger, "Failed to quickly remove node '{}' and its children, fell back to recursive removal (replica: {})",
                            node, remote_replica_path);
        }

        /// Then try to remove nodes that are known to have no children (and should always exist)
        Coordination::Requests ops;
        for (const auto & node : flat_nodes)
            ops.emplace_back(zkutil::makeRemoveRequest(remote_replica_path + "/" + node, -1));

        ops.emplace_back(zkutil::makeRemoveRequest(remote_replica_path + "/columns", -1));
        ops.emplace_back(zkutil::makeRemoveRequest(remote_replica_path + "/is_lost", -1));
        ops.emplace_back(zkutil::makeRemoveRequest(remote_replica_path + "/log_pointer", -1));
        ops.emplace_back(zkutil::makeRemoveRequest(remote_replica_path + "/max_processed_insert_time", -1));
        ops.emplace_back(zkutil::makeRemoveRequest(remote_replica_path + "/min_unprocessed_insert_time", -1));
        ops.emplace_back(zkutil::makeRemoveRequest(remote_replica_path + "/metadata", -1));
        ops.emplace_back(zkutil::makeRemoveRequest(remote_replica_path + "/metadata_version", -1));
        ops.emplace_back(zkutil::makeRemoveRequest(remote_replica_path + "/mutation_pointer", -1));
        Coordination::Responses res;
        code = zookeeper->tryMulti(ops, res);
        if (code != Coordination::Error::ZOK)
            LOG_WARNING(logger, "Cannot quickly remove nodes without children: {} (replica: {}). Will remove recursively.",
                        code, remote_replica_path);

        /// And finally remove everything else recursively
        /// It may left some garbage if replica_path subtree is concurrently modified
        zookeeper->tryRemoveChildrenRecursive(remote_replica_path);

        /// Update has_metadata_in_zookeeper to avoid retries. Otherwise we can accidentally remove metadata of a new table on retries
        if (has_metadata_out)
            *has_metadata_out = false;

        if (zookeeper->tryRemove(remote_replica_path) != Coordination::Error::ZOK)
            LOG_ERROR(logger, "Replica was not completely removed from ZooKeeper, {} still exists and may contain some garbage.", remote_replica_path);
    }

    /// Check that `zookeeper_path` exists: it could have been deleted by another replica after execution of previous line.
    Strings replicas;
    if (Coordination::Error::ZOK != zookeeper->tryGetChildren(zookeeper_path + "/replicas", replicas) || !replicas.empty())
        return;

    LOG_INFO(logger, "{} is the last replica, will remove table", remote_replica_path);

    /** At this moment, another replica can be created and we cannot remove the table.
      * Try to remove /replicas node first. If we successfully removed it,
      * it guarantees that we are the only replica that proceed to remove the table
      * and no new replicas can be created after that moment (it requires the existence of /replicas node).
      * and table cannot be recreated with new /replicas node on another servers while we are removing data,
      * because table creation is executed in single transaction that will conflict with remaining nodes.
      */

    /// Node /dropped works like a lock that protects from concurrent removal of old table and creation of new table.
    /// But recursive removal may fail in the middle of operation leaving some garbage in zookeeper_path, so
    /// we remove it on table creation if there is /dropped node. Creating thread may remove /dropped node created by
    /// removing thread, and it causes race condition if removing thread is not finished yet.
    /// To avoid this we also create ephemeral child before starting recursive removal.
    /// (The existence of child node does not allow to remove parent node).
    Coordination::Requests ops;
    Coordination::Responses responses;
    String drop_lock_path = zookeeper_path + "/dropped/lock";
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/replicas", -1));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/dropped", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(drop_lock_path, "", zkutil::CreateMode::Ephemeral));
    Coordination::Error code = zookeeper->tryMulti(ops, responses);

    if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
    {
        LOG_WARNING(logger, "Table {} is already started to be removing by another replica right now", remote_replica_path);
    }
    else if (code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_WARNING(logger, "Another replica was suddenly created, will keep the table {}", remote_replica_path);
    }
    else if (code != Coordination::Error::ZOK)
    {
        zkutil::KeeperMultiException::check(code, ops, responses);
    }
    else
    {
        auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *zookeeper);
        LOG_INFO(logger, "Removing table {} (this might take several minutes)", zookeeper_path);
        removeTableNodesFromZooKeeper(zookeeper, zookeeper_path, metadata_drop_lock, logger);
    }
}

void StorageReplicatedMergeTree::dropReplica(const String & drop_zookeeper_path, const String & drop_replica, Poco::Logger * logger)
{
    zkutil::ZooKeeperPtr zookeeper = getZooKeeperIfTableShutDown();

    /// NOTE it's not atomic: replica may become active after this check, but before dropReplica(...)
    /// However, the main use case is to drop dead replica, which cannot become active.
    /// This check prevents only from accidental drop of some other replica.
    if (zookeeper->exists(drop_zookeeper_path + "/replicas/" + drop_replica + "/is_active"))
        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "Can't drop replica: {}, because it's active", drop_replica);

    dropReplica(zookeeper, drop_zookeeper_path, drop_replica, logger);
}


bool StorageReplicatedMergeTree::removeTableNodesFromZooKeeper(zkutil::ZooKeeperPtr zookeeper,
        const String & zookeeper_path, const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock, Poco::Logger * logger)
{
    bool completely_removed = false;

    /// NOTE /block_numbers/ actually is not flat, because /block_numbers/<partition_id>/ may have ephemeral children,
    /// but we assume that all ephemeral block locks are already removed when table is being dropped.
    static constexpr std::array flat_nodes = {"block_numbers", "blocks", "async_blocks", "leader_election", "log", "mutations", "pinned_part_uuids"};

    /// First try to remove paths that are known to be flat
    for (const auto * node : flat_nodes)
    {
        bool removed_quickly = zookeeper->tryRemoveChildrenRecursive(fs::path(zookeeper_path) / node, /* probably flat */ true);
        if (!removed_quickly)
            LOG_WARNING(logger, "Failed to quickly remove node '{}' and its children, fell back to recursive removal (table: {})",
                        node, zookeeper_path);
    }

    /// Then try to remove nodes that are known to have no children (and should always exist)
    Coordination::Requests ops;
    for (const auto * node : flat_nodes)
        ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/" + node, -1));

    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/alter_partition_version", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/columns", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/metadata", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/table_shared_id", -1));
    Coordination::Responses res;
    auto code = zookeeper->tryMulti(ops, res);
    if (code != Coordination::Error::ZOK)
        LOG_WARNING(logger, "Cannot quickly remove nodes without children: {} (table: {}). Will remove recursively.",
                    code, zookeeper_path);

    Strings children;
    code = zookeeper->tryGetChildren(zookeeper_path, children);
    if (code == Coordination::Error::ZNONODE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal of replicated table. It's a bug");

    for (const auto & child : children)
    {
        if (child != "dropped")
            zookeeper->tryRemoveRecursive(fs::path(zookeeper_path) / child);
    }

    ops.clear();
    Coordination::Responses responses;
    ops.emplace_back(zkutil::makeRemoveRequest(metadata_drop_lock->getPath(), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(fs::path(zookeeper_path) / "dropped", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path, -1));
    code = zookeeper->tryMulti(ops, responses);

    if (code == Coordination::Error::ZNONODE)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal of replicated table. It's a bug");
    }
    else if (code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_ERROR(logger, "Table was not completely removed from ZooKeeper, {} still exists and may contain some garbage,"
                          "but someone is removing it right now.", zookeeper_path);
    }
    else if (code != Coordination::Error::ZOK)
    {
        /// It is still possible that ZooKeeper session is expired or server is killed in the middle of the delete operation.
        zkutil::KeeperMultiException::check(code, ops, responses);
    }
    else
    {
        metadata_drop_lock->setAlreadyRemoved();
        completely_removed = true;
        LOG_INFO(logger, "Table {} was successfully removed from ZooKeeper", zookeeper_path);
    }

    return completely_removed;
}


/** Verify that list of columns and table storage_settings_ptr match those specified in ZK (/metadata).
  * If not, throw an exception.
  */
bool StorageReplicatedMergeTree::checkTableStructure(const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot, bool strict_check)
{
    auto zookeeper = getZooKeeper();

    ReplicatedMergeTreeTableMetadata old_metadata(*this, metadata_snapshot);

    Coordination::Stat metadata_stat;
    String metadata_str = zookeeper->get(fs::path(zookeeper_prefix) / "metadata", &metadata_stat);
    auto metadata_from_zk = ReplicatedMergeTreeTableMetadata::parse(metadata_str);
    old_metadata.checkEquals(metadata_from_zk, metadata_snapshot->getColumns(), getContext());

    Coordination::Stat columns_stat;
    auto columns_from_zk = ColumnsDescription::parse(zookeeper->get(fs::path(zookeeper_prefix) / "columns", &columns_stat));

    const ColumnsDescription & old_columns = metadata_snapshot->getColumns();
    if (columns_from_zk == old_columns)
        return true;

    if (!strict_check && metadata_stat.version != 0)
    {
        LOG_WARNING(log, "Table columns structure in ZooKeeper is different from local table structure. "
                    "Assuming it's because the table was altered concurrently. Metadata version: {}. Local columns:\n"
                    "{}\nZookeeper columns:\n{}", metadata_stat.version, old_columns.toString(), columns_from_zk.toString());
        return false;
    }

    throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS,
        "Table columns structure in ZooKeeper is different from local table structure. Local columns:\n"
        "{}\nZookeeper columns:\n{}", old_columns.toString(), columns_from_zk.toString());
}

void StorageReplicatedMergeTree::setTableStructure(const StorageID & table_id, const ContextPtr & local_context,
    ColumnsDescription new_columns, const ReplicatedMergeTreeTableMetadata::Diff & metadata_diff, int32_t new_metadata_version)
{
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    StorageInMemoryMetadata new_metadata = metadata_diff.getNewMetadata(new_columns, local_context, old_metadata);
    new_metadata.setMetadataVersion(new_metadata_version);

    /// Even if the primary/sorting/partition keys didn't change we must reinitialize it
    /// because primary/partition key column types might have changed.
    checkTTLExpressions(new_metadata, old_metadata);
    setProperties(new_metadata, old_metadata);


    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);
}


/** If necessary, restore a part, replica itself adds a record for its receipt.
  * What time should I put for this entry in the queue? Time is taken into account when calculating lag of replica.
  * For these purposes, it makes sense to use creation time of missing part
  *  (that is, in calculating lag, it will be taken into account how old is the part we need to recover).
  */
static time_t tryGetPartCreateTime(zkutil::ZooKeeperPtr & zookeeper, const String & replica_path, const String & part_name)
{
    time_t res = 0;

    /// We get creation time of part, if it still exists (was not merged, for example).
    Coordination::Stat stat;
    String unused;
    if (zookeeper->tryGet(fs::path(replica_path) / "parts" / part_name, unused, &stat))
        res = stat.ctime / 1000;

    return res;
}

void StorageReplicatedMergeTree::paranoidCheckForCoveredPartsInZooKeeperOnStart(const Strings & parts_in_zk, const Strings & parts_to_fetch) const
{
#ifdef ABORT_ON_LOGICAL_ERROR
    constexpr bool paranoid_check_for_covered_parts_default = true;
#else
    constexpr bool paranoid_check_for_covered_parts_default = false;
#endif

    bool paranoid_check_for_covered_parts = Context::getGlobalContextInstance()->getConfigRef().getBool(
        "replicated_merge_tree_paranoid_check_on_startup", paranoid_check_for_covered_parts_default);
    if (!paranoid_check_for_covered_parts)
        return;

    /// FIXME https://github.com/ClickHouse/ClickHouse/issues/51182
    if (getSettings()->use_metadata_cache)
        return;

    ActiveDataPartSet active_set(format_version);
    for (const auto & part_name : parts_in_zk)
        active_set.add(part_name);

    const auto disks = getStoragePolicy()->getDisks();
    auto path = getRelativeDataPath();

    for (const auto & part_name : parts_in_zk)
    {
        String covering_part = active_set.getContainingPart(part_name);
        if (part_name == covering_part)
            continue;

        bool found = false;
        for (const DiskPtr & disk : disks)
            if (disk->exists(fs::path(path) / part_name))
                found = true;

        if (!found)
            found = std::find(parts_to_fetch.begin(), parts_to_fetch.end(), part_name) != parts_to_fetch.end();

        if (!found)
        {
            LOG_WARNING(log, "Part {} exists in ZooKeeper and covered by another part in ZooKeeper ({}), but doesn't exist on any disk. "
                        "It may cause false-positive 'part is lost forever' messages", part_name, covering_part);
            chassert(false);
        }
    }
}

void StorageReplicatedMergeTree::checkParts(bool skip_sanity_checks)
{
    auto zookeeper = getZooKeeper();

    Strings expected_parts_vec = zookeeper->getChildren(fs::path(replica_path) / "parts");

    /// Parts in ZK.
    NameSet expected_parts(expected_parts_vec.begin(), expected_parts_vec.end());

    /// There are no PreActive parts at startup.
    auto parts = getDataParts({MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});

    /** Local parts that are not in ZK.
      * In very rare cases they may cover missing parts
      * and someone may think that pushing them to zookeeper is good idea.
      * But actually we can't precisely determine that ALL missing parts
      * covered by this unexpected part. So missing parts will be downloaded.
      */
    DataParts unexpected_parts;

    /// Intersection of local parts and expected parts
    ActiveDataPartSet local_expected_parts_set(format_version);

    /// Collect unexpected parts
    for (const auto & part : parts)
    {
        if (expected_parts.contains(part->name))
            local_expected_parts_set.add(part->name);
        else
            unexpected_parts.insert(part); /// this parts we will place to detached with ignored_ prefix
    }

    /// Which parts should be taken from other replicas.
    Strings parts_to_fetch;

    for (const String & missing_name : expected_parts)
        if (!getActiveContainingPart(missing_name))
            parts_to_fetch.push_back(missing_name);

    paranoidCheckForCoveredPartsInZooKeeperOnStart(expected_parts_vec, parts_to_fetch);

    /** To check the adequacy, for the parts that are in the FS, but not in ZK, we will only consider not the most recent parts.
      * Because unexpected new parts usually arise only because they did not have time to enroll in ZK with a rough restart of the server.
      * It also occurs from deduplicated parts that did not have time to retire.
      */
    size_t unexpected_parts_nonnew = 0;
    UInt64 unexpected_parts_nonnew_rows = 0;
    UInt64 unexpected_parts_rows = 0;

    Strings covered_unexpected_parts;
    Strings uncovered_unexpected_parts;
    UInt64 uncovered_unexpected_parts_rows = 0;

    for (const auto & part : unexpected_parts)
    {
        unexpected_parts_rows += part->rows_count;

        /// This part may be covered by some expected part that is active and present locally
        /// Probably we just did not remove this part from disk before restart (but removed from ZooKeeper)
        String covering_local_part = local_expected_parts_set.getContainingPart(part->name);
        if (!covering_local_part.empty())
        {
            covered_unexpected_parts.push_back(part->name);
            continue;
        }

        /// Part is unexpected and we don't have covering part: it's suspicious
        uncovered_unexpected_parts.push_back(part->name);
        uncovered_unexpected_parts_rows += part->rows_count;

        if (part->info.level > 0)
        {
            ++unexpected_parts_nonnew;
            unexpected_parts_nonnew_rows += part->rows_count;
        }
    }

    const UInt64 parts_to_fetch_blocks = std::accumulate(parts_to_fetch.cbegin(), parts_to_fetch.cend(), 0,
        [&](UInt64 acc, const String & part_name)
        {
            if (const auto part_info = MergeTreePartInfo::tryParsePartName(part_name, format_version))
                return acc + part_info->getBlocksCount();

            LOG_ERROR(log, "Unexpected part name: {}", part_name);
            return acc;
        });

    /** We can automatically synchronize data,
      *  if the ratio of the total number of errors to the total number of parts (minimum - on the local filesystem or in ZK)
      *  is no more than some threshold (for example 50%).
      *
      * A large ratio of mismatches in the data on the filesystem and the expected data
      *  may indicate a configuration error (the server accidentally connected as a replica not from right shard).
      * In this case, the protection mechanism does not allow the server to start.
      */

    UInt64 total_rows_on_filesystem = 0;
    for (const auto & part : parts)
        total_rows_on_filesystem += part->rows_count;

    const auto storage_settings_ptr = getSettings();
    bool insane = uncovered_unexpected_parts_rows > total_rows_on_filesystem * storage_settings_ptr->replicated_max_ratio_of_wrong_parts;

    constexpr auto sanity_report_fmt = "The local set of parts of table {} doesn't look like the set of parts in ZooKeeper: "
                                               "{} rows of {} total rows in filesystem are suspicious. "
                                               "There are {} uncovered unexpected parts with {} rows ({} of them is not just-written with {} rows), "
                                               "{} missing parts (with {} blocks), {} covered unexpected parts (with {} rows).";

    constexpr auto sanity_report_debug_fmt = "Uncovered unexpected parts: {}. Missing parts: {}. Covered unexpected parts: {}. Expected parts: {}.";

    if (insane && !skip_sanity_checks)
    {
        LOG_DEBUG(log, sanity_report_debug_fmt, fmt::join(uncovered_unexpected_parts, ", "), fmt::join(parts_to_fetch, ", "),
                  fmt::join(covered_unexpected_parts, ", "), fmt::join(expected_parts, ", "));
        throw Exception(ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS, sanity_report_fmt, getStorageID().getNameForLogs(),
                        formatReadableQuantity(uncovered_unexpected_parts_rows),
                        formatReadableQuantity(total_rows_on_filesystem),
                        uncovered_unexpected_parts.size(), uncovered_unexpected_parts_rows, unexpected_parts_nonnew, unexpected_parts_nonnew_rows,
                        parts_to_fetch.size(), parts_to_fetch_blocks, covered_unexpected_parts.size(),
                        unexpected_parts_rows - uncovered_unexpected_parts_rows);
    }

    if (unexpected_parts_nonnew_rows > 0 || uncovered_unexpected_parts_rows > 0)
    {
        LOG_DEBUG(log, sanity_report_debug_fmt, fmt::join(uncovered_unexpected_parts, ", "), fmt::join(parts_to_fetch, ", "),
                  fmt::join(covered_unexpected_parts, ", "), fmt::join(expected_parts, ", "));
        LOG_WARNING(log, sanity_report_fmt, getStorageID().getNameForLogs(),
                    formatReadableQuantity(uncovered_unexpected_parts_rows), formatReadableQuantity(total_rows_on_filesystem),
                    uncovered_unexpected_parts.size(), uncovered_unexpected_parts_rows, unexpected_parts_nonnew, unexpected_parts_nonnew_rows,
                    parts_to_fetch.size(), parts_to_fetch_blocks, covered_unexpected_parts.size(), unexpected_parts_rows - uncovered_unexpected_parts_rows);
    }
    else
    {
        if (!parts_to_fetch.empty())
            LOG_DEBUG(log, "Found parts to fetch (exist in zookeeper, but not locally): [{}]", fmt::join(parts_to_fetch, ", "));
    }

    /// Add to the queue jobs to pick up the missing parts from other replicas and remove from ZK the information that we have them.
    queue.setBrokenPartsToEnqueueFetchesOnLoading(std::move(parts_to_fetch));

    /// Remove extra local parts.
    for (const DataPartPtr & part : unexpected_parts)
    {
        LOG_ERROR(log, "Renaming unexpected part {} to ignored_{}", part->name, part->name);
        forcefullyMovePartToDetachedAndRemoveFromMemory(part, "ignored", true);
    }
}


void StorageReplicatedMergeTree::syncPinnedPartUUIDs()
{
    auto zookeeper = getZooKeeper();

    Coordination::Stat stat;
    String s = zookeeper->get(zookeeper_path + "/pinned_part_uuids", &stat);

    std::lock_guard lock(pinned_part_uuids_mutex);

    /// Unsure whether or not this can be called concurrently.
    if (pinned_part_uuids->stat.version < stat.version)
    {
        auto new_pinned_part_uuids = std::make_shared<PinnedPartUUIDs>();
        new_pinned_part_uuids->fromString(s);
        new_pinned_part_uuids->stat = stat;

        pinned_part_uuids = new_pinned_part_uuids;
    }
}

bool StorageReplicatedMergeTree::checkPartChecksumsAndAddCommitOps(
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    const DataPartPtr & part,
    Coordination::Requests & ops,
    String part_name,
    NameSet & absent_replicas_paths)
{
    if (part_name.empty())
        part_name = part->name;

    auto local_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(
        part->getColumns(), part->checksums);

    Strings replicas = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas");
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);
    bool part_found = false;
    bool part_exists_on_our_replica = false;

    for (const String & replica : replicas)
    {
        String current_part_path = fs::path(zookeeper_path) / "replicas" / replica / "parts" / part_name;
        String part_zk_str;
        if (!zookeeper->tryGet(current_part_path, part_zk_str))
        {
            absent_replicas_paths.emplace(current_part_path);
            continue;
        }
        else
        {
            part_found = true;
            if (replica == replica_name)
                part_exists_on_our_replica = true;
        }

        ReplicatedMergeTreePartHeader replica_part_header;
        if (part_zk_str.empty())
        {
            String columns_str;
            String checksums_str;

            if (zookeeper->tryGet(fs::path(current_part_path) / "columns", columns_str) &&
                zookeeper->tryGet(fs::path(current_part_path) / "checksums", checksums_str))
            {
                replica_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksumsZNodes(columns_str, checksums_str);
            }
            else
            {
                if (zookeeper->exists(current_part_path))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} has empty header and does not have columns and checksums. "
                                                               "Looks like a bug.", current_part_path);
                LOG_INFO(log, "Not checking checksums of part {} with replica {} because part was removed from ZooKeeper", part_name, replica);
                continue;
            }
        }
        else
        {
            replica_part_header = ReplicatedMergeTreePartHeader::fromString(part_zk_str);
        }

        if (replica_part_header.getColumnsHash() != local_part_header.getColumnsHash())
        {
            /// Currently there are two (known) cases when it may happen:
            ///  - KILL MUTATION query had removed mutation before all replicas have executed assigned MUTATE_PART entries.
            ///    Some replicas may skip this mutation and update part version without actually applying any changes.
            ///    It leads to mismatching checksum if changes were applied on other replicas.
            ///  - ALTER_METADATA and MERGE_PARTS were reordered on some replicas.
            ///    It may lead to different number of columns in merged parts on these replicas.
            throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH, "Part {} from {} has different columns hash "
                            "(it may rarely happen on race condition with KILL MUTATION or ALTER COLUMN).", part_name, replica);
        }

        replica_part_header.getChecksums().checkEqual(local_part_header.getChecksums(), true);
        break;
    }

    if (part_found)
        absent_replicas_paths.clear();

    if (!part_exists_on_our_replica)
    {
        const auto storage_settings_ptr = getSettings();
        String part_path = fs::path(replica_path) / "parts" / part_name;

        if (storage_settings_ptr->use_minimalistic_part_header_in_zookeeper)
        {
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path, local_part_header.toString(), zkutil::CreateMode::Persistent));
        }
        else
        {
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path, "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(
                fs::path(part_path) / "columns", part->getColumns().toString(), zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(
                fs::path(part_path) / "checksums", getChecksumsForZooKeeper(part->checksums), zkutil::CreateMode::Persistent));
        }
    }
    else
    {
        LOG_WARNING(log, "checkPartAndAddToZooKeeper: node {} already exists. Will not commit any nodes.",
                    (fs::path(replica_path) / "parts" / part_name).string());
    }
    return part_found;
}

MergeTreeData::DataPartsVector StorageReplicatedMergeTree::checkPartChecksumsAndCommit(Transaction & transaction,
    const MutableDataPartPtr & part, std::optional<HardlinkedFiles> hardlinked_files, bool replace_zero_copy_lock)
{
    auto zookeeper = getZooKeeper();

    while (true)
    {
        LOG_DEBUG(log, "Committing part {} to zookeeper", part->name);

        Coordination::Requests ops;
        NameSet absent_part_paths_on_replicas;

        getLockSharedDataOps(*part, std::make_shared<ZooKeeperWithFaultInjection>(zookeeper), replace_zero_copy_lock, hardlinked_files, ops);
        size_t zero_copy_lock_ops_size = ops.size();

        /// Checksums are checked here and `ops` is filled. In fact, the part is added to ZK just below, when executing `multi`.
        bool part_found = checkPartChecksumsAndAddCommitOps(std::make_shared<ZooKeeperWithFaultInjection>(zookeeper), part, ops, part->name, absent_part_paths_on_replicas);

        /// Do not commit if the part is obsolete, we have just briefly checked its checksums
        if (transaction.isEmpty())
            return {};

        /// Will check that the part did not suddenly appear on skipped replicas
        if (!part_found)
        {
            Coordination::Requests new_ops;
            for (const String & part_path : absent_part_paths_on_replicas)
            {
                /// NOTE Create request may fail with ZNONODE if replica is being dropped, we will throw an exception
                new_ops.emplace_back(zkutil::makeCreateRequest(part_path, "", zkutil::CreateMode::Persistent));
                new_ops.emplace_back(zkutil::makeRemoveRequest(part_path, -1));
            }

            /// Add check ops at the beginning
            new_ops.insert(new_ops.end(), ops.begin(), ops.end());
            ops = std::move(new_ops);
        }
        else
        {
            chassert(absent_part_paths_on_replicas.empty());
        }

        Coordination::Responses responses;
        Coordination::Error e = zookeeper->tryMulti(ops, responses);
        if (e == Coordination::Error::ZOK)
        {
            LOG_DEBUG(log, "Part {} committed to zookeeper", part->name);
            return transaction.commit();
        }

        if (e == Coordination::Error::ZNODEEXISTS)
        {
            size_t num_check_ops = 2 * absent_part_paths_on_replicas.size() + zero_copy_lock_ops_size;
            size_t failed_op_index = zkutil::getFailedOpIndex(e, responses);
            if (failed_op_index < num_check_ops)
            {
                LOG_INFO(log, "The part {} on a replica suddenly appeared, will recheck checksums", ops[failed_op_index]->getPath());
                continue;
            }
        }

        throw zkutil::KeeperException(e);
    }
}

String StorageReplicatedMergeTree::getChecksumsForZooKeeper(const MergeTreeDataPartChecksums & checksums) const
{
    return MinimalisticDataPartChecksums::getSerializedString(checksums,
        getSettings()->use_minimalistic_checksums_in_zookeeper);
}

MergeTreeData::MutableDataPartPtr StorageReplicatedMergeTree::attachPartHelperFoundValidPart(const LogEntry & entry) const
{
    if (format_version != MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        return {};

    const MergeTreePartInfo actual_part_info = MergeTreePartInfo::fromPartName(entry.new_part_name, format_version);
    const String part_new_name = actual_part_info.getPartNameV1();

    for (const DiskPtr & disk : getStoragePolicy()->getDisks())
    {
        for (const auto it = disk->iterateDirectory(fs::path(relative_data_path) / "detached/"); it->isValid(); it->next())
        {
            const auto part_info = MergeTreePartInfo::tryParsePartName(it->name(), format_version);

            if (!part_info || part_info->partition_id != actual_part_info.partition_id)
                continue;

            const auto part_old_name = part_info->getPartNameV1();
            const auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_old_name, disk);

            auto part = getDataPartBuilder(entry.new_part_name, volume, fs::path("detached") / part_old_name)
                .withPartFormatFromDisk()
                .build();

            try
            {
                part->loadColumnsChecksumsIndexes(true, true);
            }
            catch (const Exception&)
            {
                /// This method throws if the part data is corrupted or partly missing. In this case, we simply don't
                /// process the part.
                continue;
            }

            if (entry.part_checksum == part->checksums.getTotalChecksumHex())
            {
                part->modification_time = part->getDataPartStorage().getLastModified().epochTime();
                return part;
            }
        }
    }

    return {};
}

bool StorageReplicatedMergeTree::executeLogEntry(LogEntry & entry)
{
    if (entry.type == LogEntry::DROP_RANGE || entry.type == LogEntry::DROP_PART)
    {
        executeDropRange(entry);
        return true;
    }

    if (entry.type == LogEntry::REPLACE_RANGE)
    {
        executeReplaceRange(entry);
        return true;
    }

    const bool is_get_or_attach = entry.type == LogEntry::GET_PART || entry.type == LogEntry::ATTACH_PART;

    if (is_get_or_attach || entry.type == LogEntry::MERGE_PARTS || entry.type == LogEntry::MUTATE_PART)
    {
        /// If we already have this part or a part covering it, we do not need to do anything.
        /// The part may be still in the PreActive -> Active transition so we first search
        /// among PreActive parts to definitely find the desired part if it exists.
        DataPartPtr existing_part = getPartIfExists(entry.new_part_name, {MergeTreeDataPartState::PreActive});

        if (!existing_part)
            existing_part = getActiveContainingPart(entry.new_part_name);

        /// Even if the part is local, it (in exceptional cases) may not be in ZooKeeper. Let's check that it is there.
        if (existing_part && getZooKeeper()->exists(fs::path(replica_path) / "parts" / existing_part->name))
        {
            if (!is_get_or_attach || entry.source_replica != replica_name)
                LOG_DEBUG(log, "Skipping action for part {} because part {} already exists.",
                    entry.new_part_name, existing_part->name);

            return true;
        }
    }

    if (entry.type == LogEntry::ATTACH_PART)
    {
        ProfileEventsScope profile_events_scope;

        if (MutableDataPartPtr part = attachPartHelperFoundValidPart(entry))
        {
            LOG_TRACE(log, "Found valid local part for {}, preparing the transaction", part->name);

            Transaction transaction(*this, NO_TRANSACTION_RAW);

            part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
            renameTempPartAndReplace(part, transaction);
            checkPartChecksumsAndCommit(transaction, part);

            writePartLog(PartLogElement::Type::NEW_PART, {}, 0 /** log entry is fake so we don't measure the time */,
                part->name, part, {} /** log entry is fake so there are no initial parts */, nullptr,
                profile_events_scope.getSnapshot());

            return true;
        }

        LOG_TRACE(log, "Didn't find valid local part for {} ({}), will fetch it from other replica",
            entry.new_part_name,
            entry.actual_new_part_name);
    }

    if (is_get_or_attach && entry.source_replica == replica_name)
        LOG_WARNING(log, "Part {} from own log doesn't exist.", entry.new_part_name);

    /// Perhaps we don't need this part, because during write with quorum, the quorum has failed
    /// (see below about `/quorum/failed_parts`).
    if (entry.quorum && getZooKeeper()->exists(fs::path(zookeeper_path) / "quorum" / "failed_parts" / entry.new_part_name))
    {
        LOG_DEBUG(log, "Skipping action for part {} because quorum for that part was failed.", entry.new_part_name);
        return true;    /// NOTE Deletion from `virtual_parts` is not done, but it is only necessary for merge.
    }

    switch (entry.type)
    {
        case LogEntry::ATTACH_PART:
            /// We surely don't have this part locally as we've checked it before, so download it.
            [[fallthrough]];
        case LogEntry::GET_PART:
            return executeFetch(entry);
        case LogEntry::MERGE_PARTS:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge has to be executed by another function");
        case LogEntry::MUTATE_PART:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mutation has to be executed by another function");
        case LogEntry::ALTER_METADATA:
            return executeMetadataAlter(entry);
        case LogEntry::SYNC_PINNED_PART_UUIDS:
            syncPinnedPartUUIDs();
            return true;
        case LogEntry::CLONE_PART_FROM_SHARD:
            executeClonePartFromShard(entry);
            return true;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected log entry type: {}", static_cast<int>(entry.type));
    }
}


bool StorageReplicatedMergeTree::executeFetch(LogEntry & entry, bool need_to_check_missing_part)
{
    /// Looking for covering part. After that entry.actual_new_part_name may be filled.
    String replica = findReplicaHavingCoveringPart(entry, true);
    const auto storage_settings_ptr = getSettings();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    try
    {
        if (replica.empty())
        {
            /** If a part is to be written with a quorum and the quorum is not reached yet,
              *  then (due to the fact that a part is impossible to download right now),
              *  the quorum entry should be considered unsuccessful.
              * TODO Complex code, extract separately.
              */
            if (entry.quorum)
            {
                if (entry.type != LogEntry::GET_PART)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: log entry with quorum but type is not GET_PART");

                LOG_DEBUG(log, "No active replica has part {} which needs to be written with quorum. Will try to mark that quorum as failed.", entry.new_part_name);

                /** Atomically:
                  * - if replicas do not become active;
                  * - if there is a `quorum` node with this part;
                  * - delete `quorum` node;
                  * - add a part to the list `quorum/failed_parts`;
                  * - if the part is not already removed from the list for deduplication `blocks/block_num`, then delete it;
                  *
                  * If something changes, then we will nothing - we'll get here again next time.
                  */

                /** We collect the `host` node versions from the replicas.
                  * When the replica becomes active, it changes the value of host in the same transaction (with the creation of `is_active`).
                  * This will ensure that the replicas do not become active.
                  */

                auto zookeeper = getZooKeeper();

                Strings replicas = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas");

                Coordination::Requests ops;

                for (const auto & path_part : replicas)
                {
                    Coordination::Stat stat;
                    String path = fs::path(zookeeper_path) / "replicas" / path_part / "host";
                    zookeeper->get(path, &stat);
                    ops.emplace_back(zkutil::makeCheckRequest(path, stat.version));
                }

                /// We verify that while we were collecting versions, the replica with the necessary part did not come alive.
                replica = findReplicaHavingPart(entry.new_part_name, true);

                /// Also during this time a completely new replica could be created.
                /// But if a part does not appear on the old, then it can not be on the new one either.

                if (replica.empty())
                {
                    Coordination::Stat quorum_stat;
                    const String quorum_unparallel_path = fs::path(zookeeper_path) / "quorum" / "status";
                    const String quorum_parallel_path = fs::path(zookeeper_path) / "quorum" / "parallel" / entry.new_part_name;
                    String quorum_str, quorum_path;
                    ReplicatedMergeTreeQuorumEntry quorum_entry;

                    if (zookeeper->tryGet(quorum_unparallel_path, quorum_str, &quorum_stat))
                        quorum_path = quorum_unparallel_path;
                    else
                    {
                        quorum_str = zookeeper->get(quorum_parallel_path, &quorum_stat);
                        quorum_path = quorum_parallel_path;
                    }

                    quorum_entry.fromString(quorum_str);

                    if (quorum_entry.part_name == entry.new_part_name)
                    {
                        ops.emplace_back(zkutil::makeRemoveRequest(quorum_path, quorum_stat.version));
                        auto part_info = MergeTreePartInfo::fromPartName(entry.new_part_name, format_version);

                        if (part_info.min_block != part_info.max_block)
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: log entry with quorum for part covering more than one block number");

                        ops.emplace_back(zkutil::makeCreateRequest(
                            fs::path(zookeeper_path) / "quorum" / "failed_parts" / entry.new_part_name,
                            "",
                            zkutil::CreateMode::Persistent));

                        /// Deleting from `blocks`.
                        if (!entry.block_id.empty() && zookeeper->exists(fs::path(zookeeper_path) / "blocks" / entry.block_id))
                            ops.emplace_back(zkutil::makeRemoveRequest(fs::path(zookeeper_path) / "blocks" / entry.block_id, -1));

                        Coordination::Responses responses;
                        auto code = zookeeper->tryMulti(ops, responses);

                        if (code == Coordination::Error::ZOK)
                        {
                            LOG_DEBUG(log, "Marked quorum for part {} as failed.", entry.new_part_name);
                            queue.removeFailedQuorumPart(part_info);
                            return true;
                        }
                        else if (code == Coordination::Error::ZBADVERSION || code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
                        {
                            LOG_DEBUG(log, "State was changed or isn't expected when trying to mark quorum for part {} as failed. Code: {}",
                                      entry.new_part_name, code);
                        }
                        else
                            throw Coordination::Exception(code);
                    }
                    else
                    {
                        LOG_WARNING(log, "No active replica has part {}, "
                                         "but that part needs quorum and /quorum/status contains entry about another part {}. "
                                         "It means that part was successfully written to {} replicas, but then all of them goes offline. "
                                         "Or it is a bug.", entry.new_part_name, quorum_entry.part_name, entry.quorum);
                    }
                }
            }

            if (replica.empty())
            {
                ProfileEvents::increment(ProfileEvents::ReplicatedPartFailedFetches);

                if (!need_to_check_missing_part)
                    return false;

                throw Exception(ErrorCodes::NO_REPLICA_HAS_PART, "No active replica has part {} or covering part (cannot execute {}: {})",
                                entry.new_part_name, entry.znode_name, entry.getDescriptionForLogs(format_version));
            }
        }

        try
        {
            String part_name = entry.actual_new_part_name.empty() ? entry.new_part_name : entry.actual_new_part_name;

            if (!entry.actual_new_part_name.empty())
                LOG_DEBUG(log, "Will fetch part {} instead of {}", entry.actual_new_part_name, entry.new_part_name);

            String source_replica_path = fs::path(zookeeper_path) / "replicas" / replica;
            if (!fetchPart(part_name,
                metadata_snapshot,
                zookeeper_name,
                source_replica_path,
                /* to_detached= */ false,
                entry.quorum,
                /* zookeeper_ */ nullptr,
                /* try_fetch_shared= */ true))
            {
                return false;
            }
        }
        catch (Exception & e)
        {
            /// No stacktrace, just log message
            if (e.code() == ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS)
                e.addMessage("Too busy replica. Will try later.");
            throw;
        }

        if (entry.type == LogEntry::MERGE_PARTS)
            ProfileEvents::increment(ProfileEvents::ReplicatedPartFetchesOfMerged);
    }
    catch (...)
    {
        /** If we can not download the part we need for some merge, it's better not to try to get other parts for this merge,
          * but try to get already merged part. To do this, move the action to get the remaining parts
          * for this merge at the end of the queue.
          */
        try
        {
            auto parts_for_merge = queue.moveSiblingPartsForMergeToEndOfQueue(entry.new_part_name);

            if (!parts_for_merge.empty() && replica.empty())
            {
                LOG_INFO(log, "No active replica has part {}. Will fetch merged part instead.", entry.new_part_name);
                /// We should enqueue it for check, because merged part may never appear if source part is lost
                enqueuePartForCheck(entry.new_part_name);
                return false;
            }

            /** If no active replica has a part, and there is no merge in the queue with its participation,
              * check to see if any (active or inactive) replica has such a part or covering it.
              */
            if (replica.empty())
                enqueuePartForCheck(entry.new_part_name);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }

        throw;
    }

    return true;
}


MergeTreeData::MutableDataPartPtr StorageReplicatedMergeTree::executeFetchShared(
    const String & source_replica,
    const String & new_part_name,
    const DiskPtr & disk,
    const String & path)
{
    if (source_replica.empty())
    {
        LOG_INFO(log, "No active replica has part {} on shared storage.", new_part_name);
        return nullptr;
    }

    const auto storage_settings_ptr = getSettings();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    try
    {
        return fetchExistsPart(new_part_name, metadata_snapshot, fs::path(zookeeper_path) / "replicas" / source_replica, disk, path);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS)
            e.addMessage("Too busy replica. Will try later.");
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        throw;
    }
}

static void paranoidCheckForCoveredPartsInZooKeeper(const StorageReplicatedMergeTree * storage, const ZooKeeperPtr & zookeeper, const String & replica_path,
                                                    MergeTreeDataFormatVersion format_version, const String & covering_part_name)
{
#ifdef ABORT_ON_LOGICAL_ERROR
    constexpr bool paranoid_check_for_covered_parts_default = true;
#else
    constexpr bool paranoid_check_for_covered_parts_default = false;
#endif

    bool paranoid_check_for_covered_parts = Context::getGlobalContextInstance()->getConfigRef().getBool(
        "replicated_merge_tree_paranoid_check_on_drop_range", paranoid_check_for_covered_parts_default);
    if (!paranoid_check_for_covered_parts)
        return;

    /// FIXME https://github.com/ClickHouse/ClickHouse/issues/51182
    if (storage->getSettings()->use_metadata_cache)
        return;

    auto drop_range_info = MergeTreePartInfo::fromPartName(covering_part_name, format_version);
    Strings parts_remain = zookeeper->getChildren(replica_path + "/parts");
    for (const auto & part_name : parts_remain)
    {
        auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
        if (drop_range_info.contains(part_info))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Part {} remains in ZooKeeper after DROP_RANGE {}", part_name, covering_part_name);
    }
}

void StorageReplicatedMergeTree::executeDropRange(const LogEntry & entry)
{
    LOG_TRACE(log, "Executing DROP_RANGE {}", entry.new_part_name);

    /// Wait for loading of outdated parts because DROP_RANGE
    /// command must be applied to all parts on disk.
    waitForOutdatedPartsToBeLoaded();

    auto drop_range_info = MergeTreePartInfo::fromPartName(entry.new_part_name, format_version);
    getContext()->getMergeList().cancelInPartition(getStorageID(), drop_range_info.partition_id, drop_range_info.max_block);
    {
        auto pause_checking_parts = part_check_thread.pausePartsCheck();
        queue.removePartProducingOpsInRange(getZooKeeper(), drop_range_info, entry);
        part_check_thread.cancelRemovedPartsCheck(drop_range_info);
    }

    /// Delete the parts contained in the range to be deleted.
    /// It's important that no old parts remain (after the merge), because otherwise,
    ///  after adding a new replica, this new replica downloads them, but does not delete them.
    /// And, if you do not, the parts will come to life after the server is restarted.
    /// Therefore, we use all data parts.

    auto metadata_snapshot = getInMemoryMetadataPtr();
    PartsToRemoveFromZooKeeper parts_to_remove;
    {
        auto data_parts_lock = lockParts();
        parts_to_remove = removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper(NO_TRANSACTION_RAW, drop_range_info, data_parts_lock);
        if (parts_to_remove.empty())
        {
            if (!drop_range_info.isFakeDropRangePart())
                LOG_INFO(log, "Log entry {} tried to drop single part {}, but part does not exist", entry.znode_name, entry.new_part_name);
            return;
        }
    }

    if (entry.detach)
        LOG_DEBUG(log, "Detaching parts.");
    else
        LOG_DEBUG(log, "Removing parts.");

    if (entry.detach)
    {
        /// If DETACH clone parts to detached/ directory
        for (const auto & part : parts_to_remove)
        {
            if (auto part_to_detach = part.getPartIfItWasActive())
            {
                String part_dir = part_to_detach->getDataPartStorage().getPartDirectory();
                LOG_INFO(log, "Detaching {}", part_dir);
                auto holder = getTemporaryPartDirectoryHolder(String(DETACHED_DIR_NAME) + "/" + part_dir);
                part_to_detach->makeCloneInDetached("", metadata_snapshot, /*disk_transaction*/ {});
            }
        }
    }

    /// Forcibly remove parts from ZooKeeper
    removePartsFromZooKeeperWithRetries(parts_to_remove);
    paranoidCheckForCoveredPartsInZooKeeper(this, getZooKeeper(), replica_path, format_version, entry.new_part_name);

    if (entry.detach)
        LOG_DEBUG(log, "Detached {} parts inside {}.", parts_to_remove.size(), entry.new_part_name);
    else
        LOG_DEBUG(log, "Removed {} parts inside {}.", parts_to_remove.size(), entry.new_part_name);

    /// We want to remove dropped parts from disk as soon as possible
    /// To be removed a partition should have zero refcount, therefore call the cleanup thread at exit
    parts_to_remove.clear();
    cleanup_thread.wakeup();
}


bool StorageReplicatedMergeTree::executeReplaceRange(const LogEntry & entry)
{
    Stopwatch watch;
    ProfileEventsScope profile_events_scope;

    auto & entry_replace = *entry.replace_range_entry;
    LOG_DEBUG(log, "Executing log entry {} to replace parts range {} with {} parts from {}.{}",
              entry.znode_name, entry_replace.drop_range_part_name, entry_replace.new_part_names.size(),
              entry_replace.from_database, entry_replace.from_table);

    /// Wait for loading of outdated parts because REPLACE_RANGE
    /// command must be applied to all parts on disk.
    waitForOutdatedPartsToBeLoaded();

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage_settings_ptr = getSettings();

    MergeTreePartInfo drop_range = MergeTreePartInfo::fromPartName(entry_replace.drop_range_part_name, format_version);
    /// Range with only one block has special meaning: it's ATTACH PARTITION or MOVE PARTITION, so there is no drop range
    bool replace = !LogEntry::ReplaceRangeEntry::isMovePartitionOrAttachFrom(drop_range);

    if (replace)
    {
        getContext()->getMergeList().cancelInPartition(getStorageID(), drop_range.partition_id, drop_range.max_block);
        auto pause_checking_parts = part_check_thread.pausePartsCheck();
        queue.removePartProducingOpsInRange(getZooKeeper(), drop_range, entry);
        part_check_thread.cancelRemovedPartsCheck(drop_range);
    }
    else
    {
        drop_range = {};
    }

    struct PartDescription
    {
        PartDescription(
            size_t index_,
            const String & src_part_name_,
            const String & new_part_name_,
            const String & checksum_hex_,
            MergeTreeDataFormatVersion format_version)
            : index(index_)
            , src_part_name(src_part_name_)
            , src_part_info(MergeTreePartInfo::fromPartName(src_part_name_, format_version))
            , new_part_name(new_part_name_)
            , new_part_info(MergeTreePartInfo::fromPartName(new_part_name_, format_version))
            , checksum_hex(checksum_hex_)
        {
        }

        size_t index; // in log entry arrays
        String src_part_name;
        MergeTreePartInfo src_part_info;
        String new_part_name;
        MergeTreePartInfo new_part_info;
        String checksum_hex;

        /// Part which will be committed
        MutableDataPartPtr res_part;

        /// We could find a covering part
        MergeTreePartInfo found_new_part_info;
        String found_new_part_name;

        /// Hold pointer to part in source table if will clone it from local table
        DataPartPtr src_table_part;

        /// A replica that will be used to fetch part
        String replica;

        HardlinkedFiles hardlinked_files;

        scope_guard temporary_part_lock;
    };

    using PartDescriptionPtr = std::shared_ptr<PartDescription>;
    using PartDescriptions = std::vector<PartDescriptionPtr>;

    PartDescriptions all_parts;
    PartDescriptions parts_to_add;
    PartsToRemoveFromZooKeeper parts_to_remove;

    auto table_lock_holder_dst_table = lockForShare(
            RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);
    auto dst_metadata_snapshot = getInMemoryMetadataPtr();

    for (size_t i = 0; i < entry_replace.new_part_names.size(); ++i)
    {
        all_parts.emplace_back(std::make_shared<PartDescription>(i,
            entry_replace.src_part_names.at(i),
            entry_replace.new_part_names.at(i),
            entry_replace.part_names_checksums.at(i),
            format_version));
    }

    /// What parts we should add? Or we have already added all required parts (we an replica-initializer)
    {
        auto data_parts_lock = lockParts();

        for (const PartDescriptionPtr & part_desc : all_parts)
        {
            if (!getActiveContainingPart(part_desc->new_part_info, MergeTreeDataPartState::Active, data_parts_lock))
                parts_to_add.emplace_back(part_desc);
        }

        if (parts_to_add.empty() && replace)
        {
            parts_to_remove = removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper(NO_TRANSACTION_RAW, drop_range, data_parts_lock);
            String parts_to_remove_str;
            for (const auto & part : parts_to_remove)
            {
                parts_to_remove_str += part.getPartName();
                parts_to_remove_str += " ";
            }
            LOG_TRACE(log, "Replacing {} parts {}with empty set", parts_to_remove.size(), parts_to_remove_str);
        }
    }

    if (parts_to_add.empty())
    {
        LOG_INFO(log, "All parts from REPLACE PARTITION command have been already attached");
        removePartsFromZooKeeperWithRetries(parts_to_remove);
        if (replace)
            paranoidCheckForCoveredPartsInZooKeeper(this, getZooKeeper(), replica_path, format_version, entry_replace.drop_range_part_name);
        return true;
    }

    if (parts_to_add.size() < all_parts.size())
    {
        LOG_WARNING(log, "Some (but not all) parts from REPLACE PARTITION command already exist. REPLACE PARTITION will not be atomic.");
    }

    StoragePtr source_table;
    TableLockHolder table_lock_holder_src_table;
    StorageID source_table_id{entry_replace.from_database, entry_replace.from_table};

    auto clone_data_parts_from_source_table = [&] () -> size_t
    {
        source_table = DatabaseCatalog::instance().tryGetTable(source_table_id, getContext());
        if (!source_table)
        {
            LOG_DEBUG(log, "Can't use {} as source table for REPLACE PARTITION command. It does not exist.", source_table_id.getNameForLogs());
            return 0;
        }

        auto src_metadata_snapshot = source_table->getInMemoryMetadataPtr();
        MergeTreeData * src_data = nullptr;
        try
        {
            src_data = &checkStructureAndGetMergeTreeData(source_table, src_metadata_snapshot, dst_metadata_snapshot);
        }
        catch (Exception &)
        {
            LOG_INFO(log, "Can't use {} as source table for REPLACE PARTITION command. Will fetch all parts. Reason: {}", source_table_id.getNameForLogs(), getCurrentExceptionMessage(false));
            return 0;
        }

        table_lock_holder_src_table = source_table->lockForShare(
                RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

        DataPartStates valid_states{
            MergeTreeDataPartState::PreActive, MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated};

        size_t num_clonable_parts = 0;
        for (PartDescriptionPtr & part_desc : parts_to_add)
        {
            auto src_part = src_data->getPartIfExists(part_desc->src_part_info, valid_states);
            if (!src_part)
            {
                LOG_DEBUG(log, "There is no part {} in {}", part_desc->src_part_name, source_table_id.getNameForLogs());
                continue;
            }

            bool avoid_copy_local_part = storage_settings_ptr->allow_remote_fs_zero_copy_replication && src_part->isStoredOnRemoteDiskWithZeroCopySupport();

            if (avoid_copy_local_part)
            {
                LOG_DEBUG(log, "Avoid copy local part {} from table {} because of zero-copy replication", part_desc->src_part_name, source_table_id.getNameForLogs());
                continue;
            }

            String checksum_hex  = src_part->checksums.getTotalChecksumHex();

            if (checksum_hex != part_desc->checksum_hex)
            {
                LOG_DEBUG(log, "Part {} of {} has inappropriate checksum", part_desc->src_part_name, source_table_id.getNameForLogs());
                /// TODO: check version
                continue;
            }

            part_desc->found_new_part_name = part_desc->new_part_name;
            part_desc->found_new_part_info = part_desc->new_part_info;
            part_desc->src_table_part = src_part;

            ++num_clonable_parts;
        }

        return num_clonable_parts;
    };

    size_t num_clonable_parts = clone_data_parts_from_source_table();
    LOG_DEBUG(log, "Found {} parts that could be cloned (of {} required parts)", num_clonable_parts, parts_to_add.size());

    ActiveDataPartSet adding_parts_active_set(format_version);
    std::unordered_map<String, PartDescriptionPtr> part_name_to_desc;

    for (PartDescriptionPtr & part_desc : parts_to_add)
    {
        if (part_desc->src_table_part)
        {
            /// It is clonable part
            adding_parts_active_set.add(part_desc->new_part_name);
            part_name_to_desc.emplace(part_desc->new_part_name, part_desc);
            continue;
        }

        /// Firstly, try find exact part to produce more accurate part set
        String replica = findReplicaHavingPart(part_desc->new_part_name, true);
        String found_part_name;
        /// TODO: check version

        if (replica.empty())
        {
            LOG_DEBUG(log, "Part {} is not found on remote replicas", part_desc->new_part_name);

            /// Fallback to covering part
            replica = findReplicaHavingCoveringPart(part_desc->new_part_name, true, found_part_name);

            if (replica.empty())
            {
                /// It is not fail, since adjacent parts could cover current part
                LOG_DEBUG(log, "Parts covering {} are not found on remote replicas", part_desc->new_part_name);
                continue;
            }
        }
        else
        {
            found_part_name = part_desc->new_part_name;
        }

        part_desc->found_new_part_name = found_part_name;
        part_desc->found_new_part_info = MergeTreePartInfo::fromPartName(found_part_name, format_version);
        part_desc->replica = replica;

        adding_parts_active_set.add(part_desc->found_new_part_name);
        part_name_to_desc.emplace(part_desc->found_new_part_name, part_desc);
    }

    /// Check that we could cover whole range
    for (PartDescriptionPtr & part_desc : parts_to_add)
    {
        if (!adding_parts_active_set.getContainingPart(part_desc->new_part_info).empty())
            continue;

        MergeTreePartInfo covering_drop_range;
        if (queue.isGoingToBeDropped(part_desc->new_part_info, &covering_drop_range))
        {
            LOG_WARNING(log, "Will not add part {} (while replacing {}) because it's going to be dropped (DROP_RANGE: {})",
                        part_desc->new_part_name, entry_replace.drop_range_part_name, covering_drop_range.getPartNameForLogs());
            continue;
        }

        /// We should enqueue missing part for check, so it will be replaced with empty one (if needed)
        /// and we will be able to execute this REPLACE_RANGE.
        /// However, it's quite dangerous, because part may appear in source table.
        /// So we enqueue it for check only if no replicas of source table have part either.
        bool need_check = true;
        if (auto * replicated_src_table = typeid_cast<StorageReplicatedMergeTree *>(source_table.get()))
        {
            String src_replica = replicated_src_table->findReplicaHavingPart(part_desc->src_part_name, false);
            if (!src_replica.empty())
            {
                LOG_DEBUG(log, "Found part {} on replica {} of source table, will not check part {} required for {}",
                          part_desc->src_part_name, src_replica, part_desc->new_part_name, entry.znode_name);
                need_check = false;
            }
        }

        if (need_check)
        {
            LOG_DEBUG(log, "Will check part {} required for {}, because no replicas have it (including replicas of source table)",
                      part_desc->new_part_name, entry.znode_name);
            enqueuePartForCheck(part_desc->new_part_name);
        }

        throw Exception(ErrorCodes::NO_REPLICA_HAS_PART,
                        "Not found part {} (or part covering it) neither source table neither remote replicas",
                        part_desc->new_part_name);
    }

    /// Filter covered parts
    PartDescriptions final_parts;
    Strings final_part_names;
    {
        final_part_names = adding_parts_active_set.getParts();

        for (const String & final_part_name : final_part_names)
        {
            auto part_desc = part_name_to_desc[final_part_name];
            if (!part_desc)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no final part {}. This is a bug", final_part_name);

            final_parts.emplace_back(part_desc);

            if (final_parts.size() > 1)
            {
                auto & prev = *final_parts[final_parts.size() - 2];
                auto & curr = *final_parts[final_parts.size() - 1];

                if (!prev.found_new_part_info.isDisjoint(curr.found_new_part_info))
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Intersected final parts detected: {} and {}. It should be investigated.",
                        prev.found_new_part_name, curr.found_new_part_name);
                }
            }
        }
    }

    static const String TMP_PREFIX = "tmp_replace_from_";

    auto obtain_part = [&] (PartDescriptionPtr & part_desc)
    {
        /// Fetches with zero-copy-replication are cheap, but cloneAndLoadDataPartOnSameDisk will do full copy.
        /// It's okay to check the setting for current table and disk for the source table, because src and dst part are on the same disk.
        bool prefer_fetch_from_other_replica = !part_desc->replica.empty() && storage_settings_ptr->allow_remote_fs_zero_copy_replication
            && part_desc->src_table_part && part_desc->src_table_part->isStoredOnRemoteDiskWithZeroCopySupport();

        if (part_desc->src_table_part && !prefer_fetch_from_other_replica)
        {
            if (part_desc->checksum_hex != part_desc->src_table_part->checksums.getTotalChecksumHex())
                throw Exception(ErrorCodes::UNFINISHED, "Checksums of {} is suddenly changed", part_desc->src_table_part->name);

            /// Don't do hardlinks in case of zero-copy at any side (defensive programming)
            bool source_zero_copy_enabled = dynamic_cast<const MergeTreeData *>(source_table.get())->getSettings()->allow_remote_fs_zero_copy_replication;
            bool our_zero_copy_enabled = storage_settings_ptr->allow_remote_fs_zero_copy_replication;

            IDataPartStorage::ClonePartParams clone_params
            {
                .copy_instead_of_hardlink = (our_zero_copy_enabled || source_zero_copy_enabled) && part_desc->src_table_part->isStoredOnRemoteDiskWithZeroCopySupport(),
                .metadata_version_to_write = metadata_snapshot->getMetadataVersion()
            };
            auto [res_part, temporary_part_lock] = cloneAndLoadDataPartOnSameDisk(
                part_desc->src_table_part, TMP_PREFIX + "clone_", part_desc->new_part_info, metadata_snapshot, clone_params);
            part_desc->res_part = std::move(res_part);
            part_desc->temporary_part_lock = std::move(temporary_part_lock);
        }
        else if (!part_desc->replica.empty())
        {
            String source_replica_path = fs::path(zookeeper_path) / "replicas" / part_desc->replica;
            ReplicatedMergeTreeAddress address(getZooKeeper()->get(fs::path(source_replica_path) / "host"));
            auto timeouts = getFetchPartHTTPTimeouts(getContext());

            auto credentials = getContext()->getInterserverCredentials();
            String interserver_scheme = getContext()->getInterserverScheme();
            scope_guard part_temp_directory_lock;

            if (interserver_scheme != address.scheme)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Interserver schemas are different '{}' != '{}', can't fetch part from {}",
                                interserver_scheme, address.scheme, address.host);

            auto [fetched_part, lock] = fetcher.fetchSelectedPart(
                metadata_snapshot, getContext(), part_desc->found_new_part_name, zookeeper_name, source_replica_path,
                address.host, address.replication_port, timeouts, credentials->getUser(), credentials->getPassword(),
                interserver_scheme, replicated_fetches_throttler, false, TMP_PREFIX + "fetch_");
            part_desc->res_part = fetched_part;
            part_temp_directory_lock = std::move(lock);

            /// TODO: check columns_version of fetched part

            ProfileEvents::increment(ProfileEvents::ReplicatedPartFetches);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no receipt to produce part {}. This is bug", part_desc->new_part_name);
    };

    /// Download or clone parts
    /// TODO: make it in parallel
    for (PartDescriptionPtr & part_desc : final_parts)
        obtain_part(part_desc);

    MutableDataPartsVector res_parts;
    for (PartDescriptionPtr & part_desc : final_parts)
        res_parts.emplace_back(part_desc->res_part);

    try
    {
        /// Commit parts
        auto zookeeper = getZooKeeper();
        Transaction transaction(*this, NO_TRANSACTION_RAW);

        Coordination::Requests ops;
        for (PartDescriptionPtr & part_desc : final_parts)
        {
            renameTempPartAndReplace(part_desc->res_part, transaction);
            getCommitPartOps(ops, part_desc->res_part);

            lockSharedData(*part_desc->res_part, false, part_desc->hardlinked_files);
        }


        if (!ops.empty())
            zookeeper->multi(ops);

        {
            auto data_parts_lock = lockParts();

            transaction.commit(&data_parts_lock);
            if (replace)
            {
                parts_to_remove = removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper(NO_TRANSACTION_RAW, drop_range, data_parts_lock);
                String parts_to_remove_str;
                for (const auto & part : parts_to_remove)
                {
                    parts_to_remove_str += part.getPartName();
                    parts_to_remove_str += " ";
                }
                LOG_TRACE(log, "Replacing {} parts {}with {} parts {}", parts_to_remove.size(), parts_to_remove_str,
                          final_parts.size(), boost::algorithm::join(final_part_names, ", "));
            }
        }

        PartLog::addNewParts(getContext(), PartLog::createPartLogEntries(res_parts, watch.elapsed(), profile_events_scope.getSnapshot()));
    }
    catch (...)
    {
        PartLog::addNewParts(getContext(), PartLog::createPartLogEntries(res_parts, watch.elapsed()), ExecutionStatus::fromCurrentException("", true));

        for (const auto & res_part : res_parts)
            unlockSharedData(*res_part);

        throw;
    }

    removePartsFromZooKeeperWithRetries(parts_to_remove);
    if (replace)
        paranoidCheckForCoveredPartsInZooKeeper(this, getZooKeeper(), replica_path, format_version, entry_replace.drop_range_part_name);
    res_parts.clear();
    parts_to_remove.clear();
    cleanup_thread.wakeup();

    return true;
}

void StorageReplicatedMergeTree::executeClonePartFromShard(const LogEntry & entry)
{
    auto zookeeper = getZooKeeper();

    Strings replicas = zookeeper->getChildren(entry.source_shard + "/replicas");
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);
    String replica;
    for (const String & candidate : replicas)
    {
        if (zookeeper->exists(entry.source_shard + "/replicas/" + candidate + "/is_active"))
        {
            replica = candidate;
            break;
        }
    }

    if (replica.empty())
        throw Exception(ErrorCodes::NO_REPLICA_HAS_PART, "Not found active replica on shard {} to clone part {}",
                        entry.source_shard, entry.new_part_name);

    LOG_INFO(log, "Will clone part from shard {} and replica {}", entry.source_shard, replica);

    MutableDataPartPtr part;

    {
        auto metadata_snapshot = getInMemoryMetadataPtr();
        String source_replica_path = entry.source_shard + "/replicas/" + replica;
        ReplicatedMergeTreeAddress address(getZooKeeper()->get(source_replica_path + "/host"));
        auto timeouts = getHTTPTimeouts(getContext());
        auto credentials = getContext()->getInterserverCredentials();
        String interserver_scheme = getContext()->getInterserverScheme();
        scope_guard part_temp_directory_lock;

        auto get_part = [&, address, timeouts, credentials, interserver_scheme]()
        {
            if (interserver_scheme != address.scheme)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Interserver schemes are different: '{}' != '{}', can't fetch part from {}",
                                interserver_scheme, address.scheme, address.host);

            auto [fetched_part, lock]  = fetcher.fetchSelectedPart(
                metadata_snapshot, getContext(), entry.new_part_name, zookeeper_name, source_replica_path,
                address.host, address.replication_port,
                timeouts, credentials->getUser(), credentials->getPassword(), interserver_scheme,
                replicated_fetches_throttler, true);
            part_temp_directory_lock = std::move(lock);
            return fetched_part;
        };

        part = get_part();
        // The fetched part is valuable and should not be cleaned like a temp part.
        part->is_temp = false;
        part->renameTo("detached/" + entry.new_part_name, true);

        LOG_INFO(log, "Cloned part {} to detached directory", part->name);
    }
}


void StorageReplicatedMergeTree::cloneReplica(const String & source_replica, Coordination::Stat source_is_lost_stat, zkutil::ZooKeeperPtr & zookeeper)
{
    String source_path = fs::path(zookeeper_path) / "replicas" / source_replica;

    /// The order of the following three actions is important.

    Strings source_queue_names;
    /// We are trying to get consistent /log_pointer and /queue state. Otherwise
    /// we can possibly duplicate entries in queue of cloned replica.
    while (true)
    {
        Coordination::Stat log_pointer_stat;
        String raw_log_pointer = zookeeper->get(fs::path(source_path) / "log_pointer", &log_pointer_stat);

        Coordination::Requests ops;
        ops.push_back(zkutil::makeSetRequest(fs::path(replica_path) / "log_pointer", raw_log_pointer, -1));

        /// For support old versions CH.
        if (source_is_lost_stat.version == -1)
        {
            /// We check that it was not suddenly upgraded to new version.
            /// Otherwise it can be upgraded and instantly become lost, but we cannot notice that.
            zkutil::addCheckNotExistsRequest(ops, *zookeeper, fs::path(source_path) / "is_lost");
        }
        else /// The replica we clone should not suddenly become lost.
            ops.push_back(zkutil::makeCheckRequest(fs::path(source_path) / "is_lost", source_is_lost_stat.version));

        Coordination::Responses responses;

        /// Let's remember the queue of the reference/master replica.
        source_queue_names = zookeeper->getChildren(fs::path(source_path) / "queue");

        /// Check that log pointer of source replica didn't changed while we read queue entries
        ops.push_back(zkutil::makeCheckRequest(fs::path(source_path) / "log_pointer", log_pointer_stat.version));

        auto rc = zookeeper->tryMulti(ops, responses);

        if (rc == Coordination::Error::ZOK)
        {
            break;
        }
        else if (rc == Coordination::Error::ZNODEEXISTS)
        {
            throw Exception(ErrorCodes::REPLICA_STATUS_CHANGED,
                            "Can not clone replica, because the {} updated to new ClickHouse version", source_replica);
        }
        else if (responses[1]->error == Coordination::Error::ZBADVERSION)
        {
            /// If is_lost node version changed than source replica also lost,
            /// so we cannot clone from it.
            throw Exception(ErrorCodes::REPLICA_STATUS_CHANGED, "Can not clone replica, because the {} became lost",
                source_replica);
        }
        else if (responses.back()->error == Coordination::Error::ZBADVERSION)
        {
            /// If source replica's log_pointer changed than we probably read
            /// stale state of /queue and have to try one more time.
            LOG_WARNING(log, "Log pointer of source replica {} changed while we loading queue nodes. Will retry.", source_replica);
            continue;
        }
        else
        {
            zkutil::KeeperMultiException::check(rc, ops, responses);
        }
    }

    ::sort(source_queue_names.begin(), source_queue_names.end());

    struct QueueEntryInfo
    {
        String data = {};
        Coordination::Stat stat = {};
        LogEntryPtr parsed_entry = {};
    };

    /// We got log pointer and list of queue entries of source replica.
    /// At first we will get queue entries and then we will get list of active parts of source replica
    /// to enqueue fetches for missing parts. If source replica executes and removes some entry concurrently
    /// we will see produced part (or covering part) in replicas/source/parts and will enqueue fetch.
    /// We will try to parse queue entries before copying them
    /// to avoid creation of excessive and duplicating entries in our queue.
    /// See also removePartAndEnqueueFetch(...)
    std::vector<QueueEntryInfo> source_queue;
    ActiveDataPartSet get_part_set{format_version};
    ActiveDataPartSet drop_range_set{format_version};
    std::unordered_set<String> exact_part_names;

    {
        std::vector<zkutil::ZooKeeper::FutureGet> queue_get_futures;
        queue_get_futures.reserve(source_queue_names.size());

        for (const String & entry_name : source_queue_names)
            queue_get_futures.push_back(zookeeper->asyncTryGet(fs::path(source_path) / "queue" / entry_name));

        source_queue.reserve(source_queue_names.size());
        for (size_t i = 0; i < source_queue_names.size(); ++i)
        {
            auto res = queue_get_futures[i].get();
            /// It's ok if entry is already executed and removed: we also will get source parts set.
            if (res.error == Coordination::Error::ZNONODE)
                continue;

            assert(res.error == Coordination::Error::ZOK);
            source_queue.emplace_back();
            auto & info = source_queue.back();
            info.data = std::move(res.data);
            info.stat = std::move(res.stat);
            try
            {
                info.parsed_entry = LogEntry::parse(info.data, info.stat, format_version);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Cannot parse source queue entry " + source_queue_names[i]);
            }

            /// It may be ok if source replica has newer version. We will copy entry as is.
            if (!info.parsed_entry)
                continue;

            info.parsed_entry->znode_name = source_queue_names[i];

            if (info.parsed_entry->type == LogEntry::DROP_RANGE || info.parsed_entry->type == LogEntry::DROP_PART)
            {
                drop_range_set.add(info.parsed_entry->new_part_name);
            }
            else if (info.parsed_entry->type == LogEntry::GET_PART)
            {
                String maybe_covering_drop_range = drop_range_set.getContainingPart(info.parsed_entry->new_part_name);
                if (maybe_covering_drop_range.empty())
                    get_part_set.add(info.parsed_entry->new_part_name);
            }
            else
            {
                /// We should keep local parts if they present in the queue of source replica.
                /// There's a chance that we are the only replica that has these parts.
                Strings entry_virtual_parts = info.parsed_entry->getVirtualPartNames(format_version);
                std::move(entry_virtual_parts.begin(), entry_virtual_parts.end(), std::inserter(exact_part_names, exact_part_names.end()));
            }
        }
    }

    /// We should do it after copying queue, because some ALTER_METADATA entries can be lost otherwise.
    cloneMetadataIfNeeded(source_replica, source_path, zookeeper);

    /// Add to the queue jobs to receive all the active parts that the reference/master replica has.
    Strings source_replica_parts = zookeeper->getChildren(fs::path(source_path) / "parts");
    for (const auto & active_part : source_replica_parts)
        get_part_set.add(active_part);

    Strings active_parts = get_part_set.getParts();

    /// Remove local parts if source replica does not have them, because such parts will never be fetched by other replicas.
    Strings local_parts_in_zk = zookeeper->getChildren(fs::path(replica_path) / "parts");
    Strings parts_to_remove_from_zk;

    for (const auto & part : local_parts_in_zk)
    {
        /// We look for exact match (and not for any covering part)
        /// because our part might be dropped and covering part might be merged though gap.
        /// (avoid resurrection of data that was removed a long time ago)
        if (get_part_set.getContainingPart(part) == part)
            continue;

        if (exact_part_names.contains(part))
            continue;

        parts_to_remove_from_zk.emplace_back(part);
        LOG_WARNING(log, "Source replica does not have part {}. Removing it from ZooKeeper.", part);
    }

    {
        /// Check "is_lost" version after retrieving queue and parts.
        /// If version has changed, then replica most likely has been dropped and parts set is inconsistent,
        /// so throw exception and retry cloning.
        Coordination::Stat is_lost_stat_new;
        zookeeper->get(fs::path(source_path) / "is_lost", &is_lost_stat_new);
        if (is_lost_stat_new.version != source_is_lost_stat.version)
            throw Exception(ErrorCodes::REPLICA_STATUS_CHANGED, "Cannot clone {}, because it suddenly become lost "
                                                                "or removed broken part from ZooKeeper", source_replica);
    }

    removePartsFromZooKeeperWithRetries(parts_to_remove_from_zk);

    auto local_active_parts = getDataPartsForInternalUsage();

    DataPartsVector parts_to_remove_from_working_set;

    for (const auto & part : local_active_parts)
    {
        if (get_part_set.getContainingPart(part->name) == part->name)
            continue;

        if (exact_part_names.contains(part->name))
            continue;

        parts_to_remove_from_working_set.emplace_back(part);
        LOG_WARNING(log, "Source replica does not have part {}. Removing it from working set.", part->name);
    }

    if (getSettings()->detach_old_local_parts_when_cloning_replica)
    {
        auto metadata_snapshot = getInMemoryMetadataPtr();

        for (const auto & part : parts_to_remove_from_working_set)
        {
            LOG_INFO(log, "Detaching {}", part->getDataPartStorage().getPartDirectory());
            part->makeCloneInDetached("clone", metadata_snapshot, /*disk_transaction*/ {});
        }
    }

    removePartsFromWorkingSet(NO_TRANSACTION_RAW, parts_to_remove_from_working_set, true);

    std::unordered_set<String> created_get_parts;

    /// Avoid creation of GET_PART entries which covered by another GET_PART or DROP_RANGE
    /// and creation of multiple entries with the same new_part_name.
    auto should_ignore_log_entry = [&drop_range_set, &get_part_set, this] (std::unordered_set<String> & created_gets,
                                                                    const String & part_name, const String & log_msg_context) -> bool
    {
        /// We should not create entries covered by DROP_RANGE, because we will remove them anyway (kind of optimization).
        String covering_drop_range = drop_range_set.getContainingPart(part_name);
        if (!covering_drop_range.empty())
        {
            LOG_TRACE(log, "{} {}: it's covered by drop range {}", log_msg_context, part_name, covering_drop_range);
            return true;
        }

        /// We should not create entries covered by GET_PART,
        /// because GET_PART entry has no source parts and we can execute it only by fetching.
        /// Parts covered by GET_PART are useless and may cause replication to stuck if covered part is lost.
        String covering_get_part_entry = get_part_set.getContainingPart(part_name);

        if (covering_get_part_entry.empty())
            return false;

        if (covering_get_part_entry != part_name)
        {
            LOG_TRACE(log, "{} {}: it's covered by GET_PART {}", log_msg_context, part_name, covering_get_part_entry);
            return true;
        }

        /// NOTE: It does not completely avoids duplication of GET_PART entries,
        /// because it's possible that source replica has executed some GET_PART after we copied it's queue,
        /// but before we copied its active parts set. In this case we will GET_PART entry in our queue
        /// and later will pull the original GET_PART from replication log.
        /// It should not cause any issues, but it does not allow to get rid of duplicated entries and add an assertion.
        if (created_gets.contains(part_name))
        {
            /// NOTE It would be better to copy log entry instead of creating GET_PART
            /// if there are GET_PART and log entry of other type with the same new_part_name.
            /// But it's a bit harder to implement, because it requires full-fledged virtual_parts set.
            LOG_TRACE(log, "{} {}: GET_PART for it is already created", log_msg_context, part_name);
            return true;
        }

        return false;
    };

    for (const String & name : active_parts)
    {
        if (should_ignore_log_entry(created_get_parts, name, "Not fetching"))
            continue;

        LogEntry log_entry;

        if (are_restoring_replica)
        {
            LOG_DEBUG(log, "Obtaining checksum for path {}", name);

            // The part we want to fetch is probably present in detached/ folder.
            // However, we need to get part's checksum to check if it's not corrupt.
            log_entry.type = LogEntry::ATTACH_PART;

            MinimalisticDataPartChecksums desired_checksums;

            const fs::path part_path = fs::path(source_path) / "parts" / name;

            const String part_znode = zookeeper->get(part_path);

            if (!part_znode.empty())
                desired_checksums = ReplicatedMergeTreePartHeader::fromString(part_znode).getChecksums();
            else
            {
                String desired_checksums_str = zookeeper->get(part_path / "checksums");
                desired_checksums = MinimalisticDataPartChecksums::deserializeFrom(desired_checksums_str);
            }

            log_entry.part_checksum = getHexUIntUppercase(desired_checksums.hash_of_all_files);
        }
        else
        {
            log_entry.type = LogEntry::GET_PART;
        }

        log_entry.source_replica = "";
        log_entry.new_part_name = name;
        log_entry.create_time = tryGetPartCreateTime(zookeeper, source_path, name);

        LOG_TEST(log, "Enqueueing {} for fetch", name);
        zookeeper->create(fs::path(replica_path) / "queue/queue-", log_entry.toString(), zkutil::CreateMode::PersistentSequential);
        created_get_parts.insert(name);
    }

    size_t total_parts_to_fetch = created_get_parts.size();
    LOG_DEBUG(log, "Queued {} parts to be fetched, {} parts ignored", total_parts_to_fetch, active_parts.size() - total_parts_to_fetch);

    /// Add content of the reference/master replica queue to the queue.
    size_t total_entries_to_copy = 0;
    for (const auto & entry_info : source_queue)
    {
        assert(!entry_info.data.empty());
        if (entry_info.parsed_entry && !entry_info.parsed_entry->new_part_name.empty())
        {
            const String & part_name = entry_info.parsed_entry->new_part_name;
            const String & entry_name = entry_info.parsed_entry->znode_name;
            const auto & entry_type = entry_info.parsed_entry->type;

            if (should_ignore_log_entry(created_get_parts, part_name, fmt::format("Not copying {} {}", entry_name, entry_type)))
                continue;

            if (entry_info.parsed_entry->type == LogEntry::GET_PART)
                created_get_parts.insert(part_name);
        }

        LOG_TEST(log, "Copying entry {}", entry_info.data);
        zookeeper->create(fs::path(replica_path) / "queue/queue-", entry_info.data, zkutil::CreateMode::PersistentSequential);
        ++total_entries_to_copy;
    }

    LOG_DEBUG(log, "Copied {} queue entries, {} entries ignored", total_entries_to_copy, source_queue.size() - total_entries_to_copy);
    LOG_TRACE(log, "Parts in ZooKeeper after mimic: {}", fmt::join(zookeeper->getChildren(replica_path + "/parts"), ", "));
    LOG_TRACE(log, "Enqueued fetches after mimic: {}", fmt::join(created_get_parts, ", "));
}


void StorageReplicatedMergeTree::cloneMetadataIfNeeded(const String & source_replica, const String & source_path, zkutil::ZooKeeperPtr & zookeeper)
{
    String source_metadata_version_str;
    bool metadata_version_exists = zookeeper->tryGet(source_path + "/metadata_version", source_metadata_version_str);
    if (!metadata_version_exists)
    {
        /// For compatibility with version older than 20.3
        /// TODO fix tests and delete it
        LOG_WARNING(log, "Node {} does not exist. "
                         "Most likely it's because too old version of ClickHouse is running on replica {}. "
                         "Will not check metadata consistency",
                         source_path + "/metadata_version", source_replica);
        return;
    }

    auto metadata_snapshot = getInMemoryMetadataPtr();
    Int32 source_metadata_version = parse<Int32>(source_metadata_version_str);
    if (metadata_snapshot->getMetadataVersion() == source_metadata_version)
        return;

    /// Our metadata it not up to date with source replica metadata.
    /// Metadata is updated by ALTER_METADATA entries, but some entries are probably cleaned up from the log.
    /// It's also possible that some newer ALTER_METADATA entries are present in source_queue list,
    /// and source replica are executing such entry right now (or had executed recently).
    /// More than that, /metadata_version update is not atomic with /columns and /metadata update...

    /// Fortunately, ALTER_METADATA seems to be idempotent,
    /// and older entries of such type can be replaced with newer entries.
    /// Let's try to get consistent values of source replica's /columns and /metadata
    /// and prepend dummy ALTER_METADATA to our replication queue.
    /// It should not break anything if source_queue already contains ALTER_METADATA entry
    /// with greater or equal metadata_version, but it will update our metadata
    /// if all such entries were cleaned up from the log and source_queue.

    LOG_WARNING(log, "Metadata version ({}) on replica is not up to date with metadata ({}) on source replica {}",
                metadata_snapshot->getMetadataVersion(), source_metadata_version, source_replica);

    String source_metadata;
    String source_columns;
    while (true)
    {
        Coordination::Stat metadata_stat;
        Coordination::Stat columns_stat;
        source_metadata = zookeeper->get(source_path + "/metadata", &metadata_stat);
        source_columns = zookeeper->get(source_path + "/columns", &columns_stat);

        Coordination::Requests ops;
        Coordination::Responses responses;
        ops.emplace_back(zkutil::makeCheckRequest(source_path + "/metadata", metadata_stat.version));
        ops.emplace_back(zkutil::makeCheckRequest(source_path + "/columns", columns_stat.version));

        Coordination::Error code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZOK)
            break;
        else if (code == Coordination::Error::ZBADVERSION)
            LOG_WARNING(log, "Metadata of replica {} was changed", source_path);
        else
            zkutil::KeeperMultiException::check(code, ops, responses);
    }

    ReplicatedMergeTreeLogEntryData dummy_alter;
    dummy_alter.type = LogEntry::ALTER_METADATA;
    dummy_alter.source_replica = source_replica;
    dummy_alter.metadata_str = source_metadata;
    dummy_alter.columns_str = source_columns;
    dummy_alter.alter_version = source_metadata_version;
    dummy_alter.create_time = time(nullptr);

    String path_created = zookeeper->create(replica_path + "/queue/queue-", dummy_alter.toString(), zkutil::CreateMode::PersistentSequential);
    LOG_INFO(log, "Created an ALTER_METADATA entry {} to force metadata update after cloning replica from {}. Entry: {}",
             path_created, source_replica, dummy_alter.toString());

    /// We don't need to do anything with mutation_pointer, because mutation log cleanup process is different from
    /// replication log cleanup. A mutation is removed from ZooKeeper only if all replicas had executed the mutation,
    /// so all mutations which are greater or equal to our mutation pointer are still present in ZooKeeper.
}


void StorageReplicatedMergeTree::cloneReplicaIfNeeded(zkutil::ZooKeeperPtr zookeeper)
{
    Coordination::Stat is_lost_stat;
    bool is_new_replica = true;
    String res;

    if (zookeeper->tryGet(fs::path(replica_path) / "is_lost", res, &is_lost_stat))
    {
        if (res == "0")
            return;
        if (is_lost_stat.version)
            is_new_replica = false;
    }
    else
    {
        /// Replica was created by old version of CH, so me must create "/is_lost".
        /// Note that in old version of CH there was no "lost" replicas possible.
        /// TODO is_lost node should always exist since v18.12, maybe we can replace `tryGet` with `get` and remove old code?
        zookeeper->create(fs::path(replica_path) / "is_lost", "0", zkutil::CreateMode::Persistent);
        return;
    }

    /// is_lost is "1": it means that we are in repair mode.
    /// Try choose source replica to clone.
    /// Source replica must not be lost and should have minimal queue size and maximal log pointer.
    Strings replicas = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas");
    std::vector<zkutil::ZooKeeper::FutureGet> futures;
    for (const String & source_replica_name : replicas)
    {
        /// Do not clone from myself.
        if (source_replica_name == replica_name)
            continue;

        String source_replica_path = fs::path(zookeeper_path) / "replicas" / source_replica_name;

        /// Obviously the following get operations are not atomic, but it's ok to choose good enough replica, not the best one.
        /// NOTE: We may count some entries twice if log_pointer is moved.
        futures.emplace_back(zookeeper->asyncTryGet(fs::path(source_replica_path) / "is_lost"));
        futures.emplace_back(zookeeper->asyncTryGet(fs::path(source_replica_path) / "log_pointer"));
        futures.emplace_back(zookeeper->asyncTryGet(fs::path(source_replica_path) / "queue"));
    }

    /// Wait for results before getting log entries
    for (auto & future : futures)
        future.wait();

    Strings log_entries = zookeeper->getChildren(fs::path(zookeeper_path) / "log");
    size_t max_log_entry = 0;
    if (!log_entries.empty())
    {
        String last_entry = *std::max_element(log_entries.begin(), log_entries.end());
        max_log_entry = parse<UInt64>(last_entry.substr(strlen("log-")));
    }
    /// log_pointer can point to future entry, which was not created yet
    ++max_log_entry;

    size_t min_replication_lag = std::numeric_limits<size_t>::max();
    String source_replica;
    Coordination::Stat source_is_lost_stat;
    size_t future_num = 0;

    for (const String & source_replica_name : replicas)
    {
        if (source_replica_name == replica_name)
            continue;

        auto get_is_lost     = futures[future_num++].get();
        auto get_log_pointer = futures[future_num++].get();
        auto get_queue       = futures[future_num++].get();

        if (get_is_lost.error != Coordination::Error::ZOK)
        {
            LOG_INFO(log, "Not cloning {}, cannot get '/is_lost': {}", source_replica_name, get_is_lost.error);
            continue;
        }
        else if (get_is_lost.data != "0")
        {
            LOG_INFO(log, "Not cloning {}, it's lost", source_replica_name);
            continue;
        }

        if (get_log_pointer.error != Coordination::Error::ZOK)
        {
            LOG_INFO(log, "Not cloning {}, cannot get '/log_pointer': {}", source_replica_name, get_log_pointer.error);
            continue;
        }
        if (get_queue.error != Coordination::Error::ZOK)
        {
            LOG_INFO(log, "Not cloning {}, cannot get '/queue': {}", source_replica_name, get_queue.error);
            continue;
        }

        /// Replica is not lost and we can clone it. Let's calculate approx replication lag.
        size_t source_log_pointer = get_log_pointer.data.empty() ? 0 : parse<UInt64>(get_log_pointer.data);
        assert(source_log_pointer <= max_log_entry);
        size_t replica_queue_lag = max_log_entry - source_log_pointer;
        size_t replica_queue_size = get_queue.stat.numChildren;
        size_t replication_lag = replica_queue_lag + replica_queue_size;
        LOG_INFO(log, "Replica {} has log pointer '{}', approximate {} queue lag and {} queue size",
                 source_replica_name, get_log_pointer.data, replica_queue_lag, replica_queue_size);
        if (replication_lag < min_replication_lag)
        {
            source_replica = source_replica_name;
            source_is_lost_stat = get_is_lost.stat;
            min_replication_lag = replication_lag;
        }
    }

    if (source_replica.empty())
        throw Exception(ErrorCodes::ALL_REPLICAS_LOST, "All replicas are lost. "
                        "See SYSTEM DROP REPLICA and SYSTEM RESTORE REPLICA queries, they may help");

    if (is_new_replica)
        LOG_INFO(log, "Will mimic {}", source_replica);
    else
        LOG_WARNING(log, "Will mimic {}", source_replica);

    /// Clear obsolete queue that we no longer need.
    zookeeper->removeChildren(fs::path(replica_path) / "queue");
    queue.clear();

    /// Will do repair from the selected replica.
    cloneReplica(source_replica, source_is_lost_stat, zookeeper);
    /// If repair fails to whatever reason, the exception is thrown, is_lost will remain "1" and the replica will be repaired later.

    /// If replica is repaired successfully, we remove is_lost flag.
    zookeeper->set(fs::path(replica_path) / "is_lost", "0");
}

String StorageReplicatedMergeTree::getLastQueueUpdateException() const
{
    std::lock_guard lock(last_queue_update_exception_lock);
    return last_queue_update_exception;
}


void StorageReplicatedMergeTree::queueUpdatingTask()
{
    if (!queue_update_in_progress)
    {
        last_queue_update_start_time.store(time(nullptr));
        queue_update_in_progress = true;
    }
    try
    {
        queue.pullLogsToQueue(getZooKeeperAndAssertNotReadonly(), queue_updating_task->getWatchCallback(), ReplicatedMergeTreeQueue::UPDATE);
        last_queue_update_finish_time.store(time(nullptr));
        queue_update_in_progress = false;
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        std::lock_guard lock(last_queue_update_exception_lock);
        last_queue_update_exception = getCurrentExceptionMessage(false);

        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
        {
            restarting_thread.wakeup();
            return;
        }

        queue_updating_task->scheduleAfter(QUEUE_UPDATE_ERROR_SLEEP_MS);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        std::lock_guard lock(last_queue_update_exception_lock);
        last_queue_update_exception = getCurrentExceptionMessage(false);

        queue_updating_task->scheduleAfter(QUEUE_UPDATE_ERROR_SLEEP_MS);
    }
}


void StorageReplicatedMergeTree::mutationsUpdatingTask()
{
    try
    {
        queue.updateMutations(getZooKeeper(), mutations_watch_callback);
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
            return;

        mutations_updating_task->scheduleAfter(QUEUE_UPDATE_ERROR_SLEEP_MS);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        mutations_updating_task->scheduleAfter(QUEUE_UPDATE_ERROR_SLEEP_MS);
    }
}

ReplicatedMergeTreeQueue::SelectedEntryPtr StorageReplicatedMergeTree::selectQueueEntry()
{
    /// This object will mark the element of the queue as running.
    ReplicatedMergeTreeQueue::SelectedEntryPtr selected;

    try
    {
        selected = queue.selectEntryToProcess(merger_mutator, *this);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    return selected;
}


bool StorageReplicatedMergeTree::processQueueEntry(ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry)
{
    LogEntryPtr & entry = selected_entry->log_entry;
    return queue.processEntry([this]{ return getZooKeeper(); }, entry, [&](LogEntryPtr & entry_to_process)
    {
        try
        {
            return executeLogEntry(*entry_to_process);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::NO_REPLICA_HAS_PART)
            {
                /// If no one has the right part, probably not all replicas work; We will not write to log with Error level.
                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
            }
            else if (e.code() == ErrorCodes::ABORTED)
            {
                /// Interrupted merge or downloading a part is not an error.
                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
            }
            else if (e.code() == ErrorCodes::PART_IS_TEMPORARILY_LOCKED)
            {
                /// Part cannot be added temporarily
                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
                cleanup_thread.wakeup();
            }
            else
                tryLogCurrentException(log, __PRETTY_FUNCTION__);

            /** This exception will be written to the queue element, and it can be looked up using `system.replication_queue` table.
              * The thread that performs this action will sleep a few seconds after the exception.
              * See `queue.processEntry` function.
              */
            throw;
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            throw;
        }
    });
}

bool StorageReplicatedMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    cleanup_thread.wakeupEarlierIfNeeded();

    /// If replication queue is stopped exit immediately as we successfully executed the task
    if (queue.actions_blocker.isCancelled())
        return false;

    /// This object will mark the element of the queue as running.
    ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry = selectQueueEntry();

    if (!selected_entry)
        return false;

    auto job_type = selected_entry->log_entry->type;

    /// Depending on entry type execute in fetches (small) pool or big merge_mutate pool
    if (job_type == LogEntry::GET_PART || job_type == LogEntry::ATTACH_PART)
    {
        assignee.scheduleFetchTask(std::make_shared<ExecutableLambdaAdapter>(
            [this, selected_entry] () mutable
            {
                return processQueueEntry(selected_entry);
            }, common_assignee_trigger, getStorageID()));
        return true;
    }
    else if (job_type == LogEntry::MERGE_PARTS)
    {
        auto task = std::make_shared<MergeFromLogEntryTask>(selected_entry, *this, common_assignee_trigger);
        assignee.scheduleMergeMutateTask(task);
        return true;
    }
    else if (job_type == LogEntry::MUTATE_PART)
    {
        auto task = std::make_shared<MutateFromLogEntryTask>(selected_entry, *this, common_assignee_trigger);
        assignee.scheduleMergeMutateTask(task);
        return true;
    }
    else
    {
        assignee.scheduleCommonTask(std::make_shared<ExecutableLambdaAdapter>(
            [this, selected_entry] () mutable
            {
                return processQueueEntry(selected_entry);
            }, common_assignee_trigger, getStorageID()), /* need_trigger */ true);
        return true;
    }
}


bool StorageReplicatedMergeTree::canExecuteFetch(const ReplicatedMergeTreeLogEntry & entry, String & disable_reason) const
{
    if (fetcher.blocker.isCancelled())
    {
        disable_reason = fmt::format("Not executing fetch of part {} because replicated fetches are cancelled now.", entry.new_part_name);
        return false;
    }

    auto replicated_fetches_pool_size = getContext()->getFetchesExecutor()->getMaxTasksCount();
    size_t busy_threads_in_pool = CurrentMetrics::values[CurrentMetrics::BackgroundFetchesPoolTask].load(std::memory_order_relaxed);
    if (busy_threads_in_pool >= replicated_fetches_pool_size)
    {
        disable_reason = fmt::format("Not executing fetch of part {} because {} fetches already executing, max {}.", entry.new_part_name, busy_threads_in_pool, replicated_fetches_pool_size);
        return false;
    }

    if (replicated_fetches_throttler->isThrottling())
    {
        disable_reason = fmt::format("Not executing fetch of part {} because fetches have already throttled by network settings "
                                     "<max_replicated_fetches_network_bandwidth> or <max_replicated_fetches_network_bandwidth_for_server>.", entry.new_part_name);
        return false;
    }

    if (entry.source_replica.empty())
    {
        auto part = getPartIfExists(entry.new_part_name, {MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated, MergeTreeDataPartState::Deleting});
        if (part && part->was_removed_as_broken)
        {
            disable_reason = fmt::format("Not executing fetch of part {} because we still have broken part with that name. "
                                         "Waiting for the broken part to be removed first.", entry.new_part_name);

            constexpr time_t min_interval_to_wakeup_cleanup_s = 30;
            if (entry.last_postpone_time + min_interval_to_wakeup_cleanup_s < time(nullptr))
                const_cast<StorageReplicatedMergeTree *>(this)->cleanup_thread.wakeup();
            return false;
        }
    }

    return true;
}

bool StorageReplicatedMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    return queue.isVirtualPart(part);
}

void StorageReplicatedMergeTree::mergeSelectingTask()
{
    if (!is_leader)
        return;

    const auto storage_settings_ptr = getSettings();
    const bool deduplicate = false; /// TODO: read deduplicate option from table config
    const Names deduplicate_by_columns = {};
    const bool cleanup = (storage_settings_ptr->clean_deleted_rows != CleanDeletedRows::Never);
    CreateMergeEntryResult create_result = CreateMergeEntryResult::Other;

    enum class AttemptStatus
    {
        EntryCreated,
        NeedRetry,
        Limited,
        CannotSelect,
    };

    auto try_assign_merge = [&]() -> AttemptStatus
    {
        /// We must select parts for merge under merge_selecting_mutex because other threads
        /// (OPTIMIZE queries) can assign new merges.
        std::lock_guard merge_selecting_lock(merge_selecting_mutex);

        auto zookeeper = getZooKeeperAndAssertNotReadonly();

        std::optional<ReplicatedMergeTreeMergePredicate> merge_pred;

        /// If many merges is already queued, then will queue only small enough merges.
        /// Otherwise merge queue could be filled with only large merges,
        /// and in the same time, many small parts could be created and won't be merged.

        auto merges_and_mutations_queued = queue.countMergesAndPartMutations();
        size_t merges_and_mutations_sum = merges_and_mutations_queued.merges + merges_and_mutations_queued.mutations;
        if (!canEnqueueBackgroundTask())
        {
            LOG_TRACE(log, "Reached memory limit for the background tasks ({}), so won't select new parts to merge or mutate."
                "Current background tasks memory usage: {}.",
                formatReadableSizeWithBinarySuffix(background_memory_tracker.getSoftLimit()),
                formatReadableSizeWithBinarySuffix(background_memory_tracker.get()));
            return AttemptStatus::Limited;
        }

        if (merges_and_mutations_sum >= storage_settings_ptr->max_replicated_merges_in_queue)
        {
            LOG_TRACE(log, "Number of queued merges ({}) and part mutations ({})"
                " is greater than max_replicated_merges_in_queue ({}), so won't select new parts to merge or mutate.",
                merges_and_mutations_queued.merges,
                merges_and_mutations_queued.mutations,
                storage_settings_ptr->max_replicated_merges_in_queue);
            return AttemptStatus::Limited;
        }

        UInt64 max_source_parts_size_for_merge = merger_mutator.getMaxSourcePartsSizeForMerge(
            storage_settings_ptr->max_replicated_merges_in_queue, merges_and_mutations_sum);

        UInt64 max_source_part_size_for_mutation = merger_mutator.getMaxSourcePartSizeForMutation();

        bool merge_with_ttl_allowed = merges_and_mutations_queued.merges_with_ttl < storage_settings_ptr->max_replicated_merges_with_ttl_in_queue &&
            getTotalMergesWithTTLInMergeList() < storage_settings_ptr->max_number_of_merges_with_ttl_in_pool;

        auto future_merged_part = std::make_shared<FutureMergedMutatedPart>();
        if (storage_settings.get()->assign_part_uuids)
            future_merged_part->uuid = UUIDHelpers::generateV4();

        bool can_assign_merge = max_source_parts_size_for_merge > 0;
        PartitionIdsHint partitions_to_merge_in;
        if (can_assign_merge)
        {
            auto lightweight_merge_pred = LocalMergePredicate(queue);
            partitions_to_merge_in = merger_mutator.getPartitionsThatMayBeMerged(
                max_source_parts_size_for_merge, lightweight_merge_pred, merge_with_ttl_allowed, NO_TRANSACTION_PTR);
            if (partitions_to_merge_in.empty())
                can_assign_merge = false;
            else
                merge_pred.emplace(queue.getMergePredicate(zookeeper, partitions_to_merge_in));
        }

        String out_reason;
        if (can_assign_merge &&
            merger_mutator.selectPartsToMerge(future_merged_part, false, max_source_parts_size_for_merge, *merge_pred,
                merge_with_ttl_allowed, NO_TRANSACTION_PTR, out_reason, &partitions_to_merge_in) == SelectPartsDecision::SELECTED)
        {
            create_result = createLogEntryToMergeParts(
                zookeeper,
                future_merged_part->parts,
                future_merged_part->name,
                future_merged_part->uuid,
                future_merged_part->part_format,
                deduplicate,
                deduplicate_by_columns,
                cleanup,
                nullptr,
                merge_pred->getVersion(),
                future_merged_part->merge_type);


            if (create_result == CreateMergeEntryResult::Ok)
                return AttemptStatus::EntryCreated;
            if (create_result == CreateMergeEntryResult::LogUpdated)
                return AttemptStatus::NeedRetry;
        }

        /// If there are many mutations in queue, it may happen, that we cannot enqueue enough merges to merge all new parts
        if (max_source_part_size_for_mutation == 0 || merges_and_mutations_queued.mutations >= storage_settings_ptr->max_replicated_mutations_in_queue)
            return AttemptStatus::Limited;

        if (queue.countMutations() > 0)
        {
            /// We don't need the list of committing blocks to choose a part to mutate
            if (!merge_pred)
                merge_pred.emplace(queue.getMergePredicate(zookeeper, PartitionIdsHint{}));

            /// Choose a part to mutate.
            DataPartsVector data_parts = getDataPartsVectorForInternalUsage();
            for (const auto & part : data_parts)
            {
                if (part->getBytesOnDisk() > max_source_part_size_for_mutation)
                    continue;

                std::optional<std::pair<Int64, int>> desired_mutation_version = merge_pred->getDesiredMutationVersion(part);
                if (!desired_mutation_version)
                    continue;

                create_result = createLogEntryToMutatePart(
                    *part,
                    future_merged_part->uuid,
                    desired_mutation_version->first,
                    desired_mutation_version->second,
                    merge_pred->getVersion());

                if (create_result == CreateMergeEntryResult::Ok)
                    return AttemptStatus::EntryCreated;
                if (create_result == CreateMergeEntryResult::LogUpdated)
                    return AttemptStatus::NeedRetry;
            }
        }

        return AttemptStatus::CannotSelect;
    };

    AttemptStatus result = AttemptStatus::CannotSelect;
    try
    {
        result = try_assign_merge();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }


    Float32 new_sleep_ms = merge_selecting_sleep_ms;
    if (result == AttemptStatus::EntryCreated || result == AttemptStatus::NeedRetry)
        new_sleep_ms /= storage_settings_ptr->merge_selecting_sleep_slowdown_factor;
    else if (result == AttemptStatus::CannotSelect)
        new_sleep_ms *= storage_settings_ptr->merge_selecting_sleep_slowdown_factor;
    new_sleep_ms *= std::uniform_real_distribution<Float32>(1.f, 1.1f)(thread_local_rng);
    merge_selecting_sleep_ms = static_cast<UInt64>(new_sleep_ms);

    if (merge_selecting_sleep_ms < storage_settings_ptr->merge_selecting_sleep_ms)
        merge_selecting_sleep_ms = storage_settings_ptr->merge_selecting_sleep_ms;
    if (merge_selecting_sleep_ms > storage_settings_ptr->max_merge_selecting_sleep_ms)
        merge_selecting_sleep_ms = storage_settings_ptr->max_merge_selecting_sleep_ms;

    if (result == AttemptStatus::EntryCreated)
        merge_selecting_task->schedule();
    else
    {
        LOG_TRACE(log, "Scheduling next merge selecting task after {}ms", merge_selecting_sleep_ms);
        merge_selecting_task->scheduleAfter(merge_selecting_sleep_ms);
    }
}


void StorageReplicatedMergeTree::mutationsFinalizingTask()
{
    bool needs_reschedule = false;

    try
    {
        needs_reschedule = queue.tryFinalizeMutations(getZooKeeperAndAssertNotReadonly());
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        needs_reschedule = true;
    }

    if (needs_reschedule)
    {
        mutations_finalizing_task->scheduleAfter(MUTATIONS_FINALIZING_SLEEP_MS);
    }
    else
    {
        /// Even if no mutations seems to be done or appeared we are trying to
        /// finalize them in background because manual control the launch of
        /// this function is error prone. This can lead to mutations that
        /// processed all the parts but have is_done=0 state for a long time. Or
        /// killed mutations, which are also considered as undone.
        mutations_finalizing_task->scheduleAfter(MUTATIONS_FINALIZING_IDLE_SLEEP_MS);
    }
}


StorageReplicatedMergeTree::CreateMergeEntryResult StorageReplicatedMergeTree::createLogEntryToMergeParts(
    zkutil::ZooKeeperPtr & zookeeper,
    const DataPartsVector & parts,
    const String & merged_name,
    const UUID & merged_part_uuid,
    const MergeTreeDataPartFormat & merged_part_format,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ReplicatedMergeTreeLogEntryData * out_log_entry,
    int32_t log_version,
    MergeType merge_type)
{
    Strings exists_paths;
    exists_paths.reserve(parts.size());
    for (const auto & part : parts)
        exists_paths.emplace_back(fs::path(replica_path) / "parts" / part->name);

    auto exists_results = zookeeper->exists(exists_paths);
    bool all_in_zk = true;
    for (size_t i = 0; i < parts.size(); ++i)
    {
        /// If there is no information about part in ZK, we will not merge it.
        if (exists_results[i].error == Coordination::Error::ZNONODE)
        {
            all_in_zk = false;

            const auto & part = parts[i];
            if (part->modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < time(nullptr))
            {
                LOG_WARNING(log, "Part {} (that was selected for merge) with age {} seconds exists locally but not in ZooKeeper. Won't do merge with that part and will check it.", part->name, (time(nullptr) - part->modification_time));
                enqueuePartForCheck(part->name);
            }
        }
    }

    if (!all_in_zk)
        return CreateMergeEntryResult::MissingPart;

    ReplicatedMergeTreeLogEntryData entry;
    entry.type = LogEntry::MERGE_PARTS;
    entry.source_replica = replica_name;
    entry.new_part_name = merged_name;
    entry.new_part_uuid = merged_part_uuid;
    entry.new_part_format = merged_part_format;
    entry.merge_type = merge_type;
    entry.deduplicate = deduplicate;
    entry.deduplicate_by_columns = deduplicate_by_columns;
    entry.cleanup = cleanup;
    entry.create_time = time(nullptr);

    for (const auto & part : parts)
        entry.source_parts.push_back(part->name);

    Coordination::Requests ops;
    Coordination::Responses responses;

    ops.emplace_back(zkutil::makeCreateRequest(
        fs::path(zookeeper_path) / "log/log-", entry.toString(),
        zkutil::CreateMode::PersistentSequential));

    ops.emplace_back(zkutil::makeSetRequest(
        fs::path(zookeeper_path) / "log", "", log_version)); /// Check and update version.

    Coordination::Error code = zookeeper->tryMulti(ops, responses);

    if (code == Coordination::Error::ZOK)
    {
        String path_created = dynamic_cast<const Coordination::CreateResponse &>(*responses.front()).path_created;
        entry.znode_name = path_created.substr(path_created.find_last_of('/') + 1);

        ProfileEvents::increment(ProfileEvents::CreatedLogEntryForMerge);
        LOG_TRACE(log, "Created log entry {} for merge {}", path_created, merged_name);
    }
    else if (code == Coordination::Error::ZBADVERSION)
    {
        ProfileEvents::increment(ProfileEvents::NotCreatedLogEntryForMerge);
        LOG_TRACE(log, "Log entry is not created for merge {} because log was updated", merged_name);
        return CreateMergeEntryResult::LogUpdated;
    }
    else
    {
        zkutil::KeeperMultiException::check(code, ops, responses);
    }

    if (out_log_entry)
        *out_log_entry = entry;

    return CreateMergeEntryResult::Ok;
}


StorageReplicatedMergeTree::CreateMergeEntryResult StorageReplicatedMergeTree::createLogEntryToMutatePart(
    const IMergeTreeDataPart & part, const UUID & new_part_uuid, Int64 mutation_version, int32_t alter_version, int32_t log_version)
{
    auto zookeeper = getZooKeeper();

    /// If there is no information about part in ZK, we will not mutate it.
    if (!zookeeper->exists(fs::path(replica_path) / "parts" / part.name))
    {
        if (part.modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < time(nullptr))
        {
            LOG_WARNING(log, "Part {} (that was selected for mutation) with age {} seconds exists locally but not in ZooKeeper."
                " Won't mutate that part and will check it.", part.name, (time(nullptr) - part.modification_time));
            enqueuePartForCheck(part.name);
        }

        return CreateMergeEntryResult::MissingPart;
    }

    MergeTreePartInfo new_part_info = part.info;
    new_part_info.mutation = mutation_version;

    String new_part_name = part.getNewName(new_part_info);

    ReplicatedMergeTreeLogEntryData entry;
    entry.type = LogEntry::MUTATE_PART;
    entry.source_replica = replica_name;
    entry.source_parts.push_back(part.name);
    entry.new_part_name = new_part_name;
    entry.new_part_uuid = new_part_uuid;
    entry.create_time = time(nullptr);
    entry.alter_version = alter_version;

    Coordination::Requests ops;
    Coordination::Responses responses;

    ops.emplace_back(zkutil::makeCreateRequest(
        fs::path(zookeeper_path) / "log/log-", entry.toString(),
        zkutil::CreateMode::PersistentSequential));

    ops.emplace_back(zkutil::makeSetRequest(
        fs::path(zookeeper_path) / "log", "", log_version)); /// Check and update version.

    Coordination::Error code = zookeeper->tryMulti(ops, responses);

    if (code == Coordination::Error::ZBADVERSION)
    {
        ProfileEvents::increment(ProfileEvents::NotCreatedLogEntryForMutation);
        LOG_TRACE(log, "Log entry is not created for mutation {} because log was updated", new_part_name);
        return CreateMergeEntryResult::LogUpdated;
    }

    zkutil::KeeperMultiException::check(code, ops, responses);

    ProfileEvents::increment(ProfileEvents::CreatedLogEntryForMutation);
    LOG_TRACE(log, "Created log entry for mutation {}", new_part_name);
    return CreateMergeEntryResult::Ok;
}


void StorageReplicatedMergeTree::getRemovePartFromZooKeeperOps(const String & part_name, Coordination::Requests & ops, bool has_children)
{
    String part_path = fs::path(replica_path) / "parts" / part_name;

    if (has_children)
    {
        ops.emplace_back(zkutil::makeRemoveRequest(fs::path(part_path) / "checksums", -1));
        ops.emplace_back(zkutil::makeRemoveRequest(fs::path(part_path) / "columns", -1));
    }
    ops.emplace_back(zkutil::makeRemoveRequest(part_path, -1));
}

void StorageReplicatedMergeTree::removePartAndEnqueueFetch(const String & part_name, bool storage_init)
{
    auto zookeeper = getZooKeeper();

    DataPartPtr broken_part;
    auto outdate_broken_part = [this, &broken_part]()
    {
        if (!broken_part)
            return;
        DataPartsLock lock = lockParts();
        if (broken_part->getState() == DataPartState::Active)
            removePartsFromWorkingSet(NO_TRANSACTION_RAW, {broken_part}, true, &lock);
        broken_part.reset();
        cleanup_thread.wakeup();
    };

    /// We don't know exactly what happened to broken part
    /// and we are going to remove all covered log entries.
    /// It's quite dangerous, so clone covered parts to detached.
    auto broken_part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

    auto partition_range = getDataPartsVectorInPartitionForInternalUsage({MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated},
                                                                         broken_part_info.partition_id);
    Strings detached_parts;
    for (const auto & part : partition_range)
    {
        if (!broken_part_info.contains(part->info))
            continue;

        if (broken_part_info == part->info)
        {
            chassert(!broken_part);
            chassert(!storage_init);
            part->was_removed_as_broken = true;
            part->makeCloneInDetached("broken", getInMemoryMetadataPtr(), /*disk_transaction*/ {});
            broken_part = part;
        }
        else
        {
            part->makeCloneInDetached("covered-by-broken", getInMemoryMetadataPtr(), /*disk_transaction*/ {});
        }
        detached_parts.push_back(part->name);
    }
    LOG_WARNING(log, "Detached {} parts covered by broken part {}: {}", detached_parts.size(), part_name, fmt::join(detached_parts, ", "));

    ThreadFuzzer::maybeInjectSleep();
    ThreadFuzzer::maybeInjectMemoryLimitException();

    /// It's possible that queue contains entries covered by part_name.
    /// For example, we had GET_PART all_1_42_5 and MUTATE_PART all_1_42_5_63,
    /// then all_1_42_5_63 was executed by fetching, but part was written to disk incorrectly.
    /// In this case we have to remove it as broken and create GET_PART all_1_42_5_63 to fetch it again,
    /// but GET_PART all_1_42_5 may be still in the queue.
    /// We should remove all covered entries before creating GET_PART entry, because:
    ///    1. In the situation described above, we do not know how to merge/mutate all_1_42_5_63 from all_1_42_5,
    ///       so GET_PART all_1_42_5 (and all source parts) is useless. The only thing we can do is to fetch all_1_42_5_63.
    ///    2. If all_1_42_5_63 is lost, then replication may stuck waiting for all_1_42_5_63 to appear,
    ///       because we may have some covered parts (more precisely, parts with the same min and max blocks)
    queue.removePartProducingOpsInRange(zookeeper, broken_part_info, /* covering_entry= */ {});

    ThreadFuzzer::maybeInjectSleep();
    ThreadFuzzer::maybeInjectMemoryLimitException();

    String part_path = fs::path(replica_path) / "parts" / part_name;

    while (true)
    {

        Coordination::Requests ops;

        time_t part_create_time = 0;
        Coordination::Stat stat;
        bool exists_in_zookeeper = zookeeper->exists(part_path, &stat);
        if (exists_in_zookeeper)
        {
            /// Update version of /is_lost node to avoid race condition with cloneReplica(...).
            /// cloneReplica(...) expects that if some entry was executed, then its new_part_name is added to /parts,
            /// but we are going to remove it from /parts and add to queue again.
            Coordination::Stat is_lost_stat;
            String is_lost_value = zookeeper->get(replica_path + "/is_lost", &is_lost_stat);
            chassert(is_lost_value == "0");
            ops.emplace_back(zkutil::makeSetRequest(replica_path + "/is_lost", is_lost_value, is_lost_stat.version));

            part_create_time = stat.ctime / 1000;
            getRemovePartFromZooKeeperOps(part_name, ops, stat.numChildren > 0);
        }

        if (!storage_init)
        {
            /// We use merge predicate + version check here, because DROP RANGE update log version and we are trying to avoid race with it. We must be sure, that our part
            /// was not dropped, otherwise we will have fetch entry, but no virtual part for it (DROP RANGE will remove it). So bad sequence is the following:
            /// 1) Create DROP PART in log for broken_part (for example because it's not only broken, but also empty)
            /// 1.5) Pull to log and start execution (removePartProducingOpsInRange)
            /// 2) Execute this method (create GET PART for broken part)
            /// 3) Finish DROP PART execution (remove part from virtual parts)
            /// 4) Now we get GET PART for broken part which is absent in virtual parts == bug
            ///
            /// Because of version check this method will never create FETCH if drop part exists

            ReplicatedMergeTreeMergePredicate merge_pred = queue.getMergePredicate(zookeeper, PartitionIdsHint{broken_part_info.partition_id});
            if (merge_pred.isGoingToBeDropped(broken_part_info))
            {
                LOG_INFO(log, "Broken part {} is covered by drop range, don't need to fetch it", part_name);
                outdate_broken_part();
                return;
            }

            /// Check that our version of log (and queue) is the most fresh. Otherwise don't create new entry fetch entry.
            ops.emplace_back(zkutil::makeCheckRequest(fs::path(zookeeper_path) / "log", merge_pred.getVersion()));
        }

        LogEntryPtr log_entry = std::make_shared<LogEntry>();
        log_entry->type = LogEntry::GET_PART;
        log_entry->create_time = part_create_time;
        log_entry->source_replica = "";
        log_entry->new_part_name = part_name;

        ops.emplace_back(zkutil::makeCreateRequest(
            fs::path(replica_path) / "queue/queue-", log_entry->toString(),
            zkutil::CreateMode::PersistentSequential));

        Coordination::Responses results;
        auto rc = zookeeper->tryMulti(ops, results);

        if (rc == Coordination::Error::ZBADVERSION)
        {
            LOG_TRACE(log, "Log updated, cannot create fetch entry for part {}, will retry.", part_name);
            continue;
        }

        zkutil::KeeperMultiException::check(rc, ops, results);

        String path_created = dynamic_cast<const Coordination::CreateResponse &>(*results.back()).path_created;
        log_entry->znode_name = path_created.substr(path_created.find_last_of('/') + 1);
        LOG_DEBUG(log, "Created entry {} to fetch missing part {}", log_entry->znode_name, part_name);
        queue.insert(zookeeper, log_entry);

        /// Make the part outdated after creating the log entry.
        /// Otherwise, if we failed to create the entry, cleanup thread could remove the part from ZooKeeper (leading to diverged replicas)
        outdate_broken_part();
        return;
    }
}


void StorageReplicatedMergeTree::startBeingLeader()
{
    auto zookeeper = getZooKeeper();

    if (!getSettings()->replicated_can_become_leader)
    {
        LOG_INFO(log, "Will not enter leader election because replicated_can_become_leader=0");
        return;
    }

    zkutil::checkNoOldLeaders(log, *zookeeper, fs::path(zookeeper_path) / "leader_election");

    LOG_INFO(log, "Became leader");
    is_leader = true;
}

void StorageReplicatedMergeTree::stopBeingLeader()
{
    if (!is_leader)
    {
        LOG_TRACE(log, "stopBeingLeader called but we are not a leader already");
        return;
    }

    LOG_INFO(log, "Stopped being leader");
    is_leader = false;
}

ConnectionTimeouts StorageReplicatedMergeTree::getFetchPartHTTPTimeouts(ContextPtr local_context)
{
    auto timeouts = getHTTPTimeouts(local_context);
    auto settings = getSettings();

    if (settings->replicated_fetches_http_connection_timeout.changed)
        timeouts.connection_timeout = settings->replicated_fetches_http_connection_timeout;

    if (settings->replicated_fetches_http_send_timeout.changed)
        timeouts.send_timeout = settings->replicated_fetches_http_send_timeout;

    if (settings->replicated_fetches_http_receive_timeout.changed)
        timeouts.receive_timeout = settings->replicated_fetches_http_receive_timeout;

    return timeouts;
}

bool StorageReplicatedMergeTree::checkReplicaHavePart(const String & replica, const String & part_name)
{
    auto zookeeper = getZooKeeper();
    return zookeeper->exists(fs::path(zookeeper_path) / "replicas" / replica / "parts" / part_name);
}

String StorageReplicatedMergeTree::findReplicaHavingPart(const String & part_name, bool active)
{
    auto zookeeper = getZooKeeper();
    Strings replicas = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas");

    /// Select replicas in uniformly random order.
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);

    LOG_TRACE(log, "Candidate replicas: {}", replicas.size());

    for (const String & replica : replicas)
    {
        /// We aren't interested in ourself.
        if (replica == replica_name)
            continue;

        LOG_TRACE(log, "Candidate replica: {}", replica);

        if (checkReplicaHavePart(replica, part_name) &&
            (!active || zookeeper->exists(fs::path(zookeeper_path) / "replicas" / replica / "is_active")))
            return replica;

        /// Obviously, replica could become inactive or even vanish after return from this method.
    }

    return {};
}

void StorageReplicatedMergeTree::addLastSentPart(const MergeTreePartInfo & info)
{
    {
        std::lock_guard lock(last_sent_parts_mutex);
        last_sent_parts.emplace_back(info);
        static constexpr size_t LAST_SENT_PARS_WINDOW_SIZE = 1000;
        while (last_sent_parts.size() > LAST_SENT_PARS_WINDOW_SIZE)
            last_sent_parts.pop_front();
    }

    last_sent_parts_cv.notify_all();
}

void StorageReplicatedMergeTree::waitForUniquePartsToBeFetchedByOtherReplicas(StorageReplicatedMergeTree::ShutdownDeadline shutdown_deadline_)
{
    /// Will be true in case in case of query
    if (CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() != nullptr)
    {
        LOG_TRACE(log, "Will not wait for unique parts to be fetched by other replicas because shutdown called from DROP/DETACH query");
        return;
    }

    if (!shutdown_called.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Called waitForUniquePartsToBeFetchedByOtherReplicas before shutdown, it's a bug");

    auto settings_ptr = getSettings();

    auto wait_ms = settings_ptr->wait_for_unique_parts_send_before_shutdown_ms.totalMilliseconds();
    if (wait_ms == 0)
    {
        LOG_INFO(log, "Will not wait for unique parts to be fetched by other replicas because wait time is zero");
        return;
    }

    if (shutdown_deadline_ <= std::chrono::system_clock::now())
    {
        LOG_INFO(log, "Will not wait for unique parts to be fetched by other replicas because shutdown_deadline already passed");
        return;
    }

    auto zookeeper = getZooKeeperIfTableShutDown();

    auto unique_parts_set = findReplicaUniqueParts(replica_name, zookeeper_path, format_version, zookeeper, log);
    if (unique_parts_set.empty())
    {
        LOG_INFO(log, "Will not wait for unique parts to be fetched because we don't have any unique parts");
        return;
    }
    else
    {
        LOG_INFO(log, "Will wait for {} unique parts to be fetched", unique_parts_set.size());
    }

    auto wait_predicate = [&] () -> bool
    {
        for (auto it = unique_parts_set.begin(); it != unique_parts_set.end();)
        {
            const auto & part = *it;

            bool found = false;
            for (const auto & sent_part : last_sent_parts | std::views::reverse)
            {
                if (sent_part.contains(part))
                {
                    LOG_TRACE(log, "Part {} was fetched by some replica", part.getPartNameForLogs());
                    found = true;
                    it = unique_parts_set.erase(it);
                    break;
                }
            }
            if (!found)
                break;
        }
        return unique_parts_set.empty();
    };

    std::unique_lock lock(last_sent_parts_mutex);
    if (!last_sent_parts_cv.wait_until(lock, shutdown_deadline_, wait_predicate))
        LOG_INFO(log, "Failed to wait for unique parts to be fetched in {} ms, {} parts can be left on this replica", wait_ms, unique_parts_set.size());
    else
        LOG_INFO(log, "Successfully waited all the parts");
}

std::set<MergeTreePartInfo> StorageReplicatedMergeTree::findReplicaUniqueParts(const String & replica_name_, const String & zookeeper_path_, MergeTreeDataFormatVersion format_version_, zkutil::ZooKeeper::Ptr zookeeper_, Poco::Logger * log_)
{
    if (!zookeeper_->exists(fs::path(zookeeper_path_) / "replicas" / replica_name_ / "is_active"))
    {
        LOG_INFO(log_, "Our replica is not active, nobody will try to fetch anything");
        return {};
    }

    Strings replicas = zookeeper_->getChildren(fs::path(zookeeper_path_) / "replicas");
    Strings our_parts;
    std::vector<ActiveDataPartSet> data_parts_on_replicas;
    for (const String & replica : replicas)
    {
        if (!zookeeper_->exists(fs::path(zookeeper_path_) / "replicas" / replica / "is_active"))
        {
            LOG_TRACE(log_, "Replica {} is not active, skipping", replica);
            continue;
        }

        Strings parts = zookeeper_->getChildren(fs::path(zookeeper_path_) / "replicas" / replica / "parts");
        if (replica == replica_name_)
        {
            LOG_TRACE(log_, "Our replica parts collected {}", replica);
            our_parts = parts;
        }
        else
        {
            LOG_TRACE(log_, "Fetching parts for replica {}: [{}]", replica, fmt::join(parts, ", "));
            data_parts_on_replicas.emplace_back(format_version_, parts);
        }
    }

    if (data_parts_on_replicas.empty())
    {
        LOG_TRACE(log_, "Has no active replicas, will no try to wait for fetch");
        return {};
    }

    std::set<MergeTreePartInfo> our_unique_parts;
    for (const auto & part : our_parts)
    {
        bool found = false;
        for (const auto & active_parts_set : data_parts_on_replicas)
        {
            if (!active_parts_set.getContainingPart(part).empty())
            {
                found = true;
                break;
            }
        }

        if (!found)
        {
            LOG_TRACE(log_, "Part not {} found on other replicas", part);
            our_unique_parts.emplace(MergeTreePartInfo::fromPartName(part, format_version_));
        }
    }

    if (!our_parts.empty() && our_unique_parts.empty())
        LOG_TRACE(log_, "All parts found on replicas");

    return our_unique_parts;
}

String StorageReplicatedMergeTree::findReplicaHavingCoveringPart(LogEntry & entry, bool active)
{
    auto zookeeper = getZooKeeper();
    Strings replicas = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas");

    /// Select replicas in uniformly random order.
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);

    for (const String & replica : replicas)
    {
        if (replica == replica_name)
            continue;

        if (active && !zookeeper->exists(fs::path(zookeeper_path) / "replicas" / replica / "is_active"))
            continue;

        String largest_part_found;
        Strings parts = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas" / replica / "parts");
        for (const String & part_on_replica : parts)
        {
            if (part_on_replica == entry.new_part_name
                || MergeTreePartInfo::contains(part_on_replica, entry.new_part_name, format_version))
            {
                if (largest_part_found.empty()
                    || MergeTreePartInfo::contains(part_on_replica, largest_part_found, format_version))
                {
                    largest_part_found = part_on_replica;
                }
            }
        }

        if (!largest_part_found.empty())
        {
            bool the_same_part = largest_part_found == entry.new_part_name;

            /// Make a check in case if selected part differs from source part
            if (!the_same_part)
            {
                String reject_reason;
                if (!queue.addFuturePartIfNotCoveredByThem(largest_part_found, entry, reject_reason))
                {
                    LOG_INFO(log, "Will not fetch part {} covering {}. {}", largest_part_found, entry.new_part_name, reject_reason);
                    return {};
                }
            }

            return replica;
        }
    }

    return {};
}


String StorageReplicatedMergeTree::findReplicaHavingCoveringPart(
    const String & part_name, bool active, String & found_part_name)
{
    auto zookeeper = getZooKeeper();
    Strings replicas = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas");

    /// Select replicas in uniformly random order.
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);

    String largest_part_found;
    String largest_replica_found;

    for (const String & replica : replicas)
    {
        if (replica == replica_name)
            continue;

        if (active && !zookeeper->exists(fs::path(zookeeper_path) / "replicas" / replica / "is_active"))
            continue;

        Strings parts = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas" / replica / "parts");
        for (const String & part_on_replica : parts)
        {
            if (part_on_replica == part_name
                || MergeTreePartInfo::contains(part_on_replica, part_name, format_version))
            {
                if (largest_part_found.empty()
                    || MergeTreePartInfo::contains(part_on_replica, largest_part_found, format_version))
                {
                    largest_part_found = part_on_replica;
                    largest_replica_found = replica;
                }
            }
        }
    }

    found_part_name = largest_part_found;
    return largest_replica_found;
}


/** If a quorum is tracked for a part, update information about it in ZK.
  */
void StorageReplicatedMergeTree::updateQuorum(const String & part_name, bool is_parallel)
{
    if (is_parallel && format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Parallel quorum inserts are not compatible with the deprecated syntax of *MergeTree engines");

    auto zookeeper = getZooKeeper();

    /// Information on which replicas a part has been added, if the quorum has not yet been reached.
    String quorum_status_path = fs::path(zookeeper_path) / "quorum" / "status";
    if (is_parallel)
        quorum_status_path = fs::path(zookeeper_path) / "quorum" / "parallel" / part_name;
    /// The name of the previous part for which the quorum was reached.
    const String quorum_last_part_path = fs::path(zookeeper_path) / "quorum" / "last_part";

    String value;
    Coordination::Stat stat;

    /// If there is no node, then all quorum INSERTs have already reached the quorum, and nothing is needed.
    while (zookeeper->tryGet(quorum_status_path, value, &stat))
    {
        ReplicatedMergeTreeQuorumEntry quorum_entry(value);
        if (quorum_entry.part_name != part_name)
        {
            LOG_TRACE(log, "Quorum {}, already achieved for part {} current part {}",
                      quorum_status_path, part_name, quorum_entry.part_name);
            /// The quorum has already been achieved. Moreover, another INSERT with a quorum has already started.
            break;
        }

        quorum_entry.replicas.insert(replica_name);

        if (quorum_entry.replicas.size() >= quorum_entry.required_number_of_replicas)
        {
            /// The quorum is reached. Delete the node, and update information about the last part that was successfully written with quorum.
            LOG_TRACE(log, "Got {} (of {} required) replicas confirmed quorum {}, going to remove node",
                      quorum_entry.replicas.size(), quorum_entry.required_number_of_replicas, quorum_status_path);

            Coordination::Requests ops;
            Coordination::Responses responses;

            if (!is_parallel)
            {
                Coordination::Stat added_parts_stat;
                String old_added_parts = zookeeper->get(quorum_last_part_path, &added_parts_stat);

                ReplicatedMergeTreeQuorumAddedParts parts_with_quorum(format_version);

                if (!old_added_parts.empty())
                    parts_with_quorum.fromString(old_added_parts);

                auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
                /// We store one last part which reached quorum for each partition.
                parts_with_quorum.added_parts[part_info.partition_id] = part_name;

                String new_added_parts = parts_with_quorum.toString();

                ops.emplace_back(zkutil::makeRemoveRequest(quorum_status_path, stat.version));
                ops.emplace_back(zkutil::makeSetRequest(quorum_last_part_path, new_added_parts, added_parts_stat.version));
            }
            else
                ops.emplace_back(zkutil::makeRemoveRequest(quorum_status_path, stat.version));

            auto code = zookeeper->tryMulti(ops, responses);

            if (code == Coordination::Error::ZOK)
            {
                break;
            }
            else if (code == Coordination::Error::ZNONODE)
            {
                /// The quorum has already been achieved.
                break;
            }
            else if (code == Coordination::Error::ZBADVERSION)
            {
                /// Node was updated meanwhile. We must re-read it and repeat all the actions.
                continue;
            }
            else
                throw Coordination::Exception::fromPath(code, quorum_status_path);
        }
        else
        {
            LOG_TRACE(log, "Quorum {} still not satisfied (have only {} of {} replicas), updating node",
                      quorum_status_path, quorum_entry.replicas.size(), quorum_entry.required_number_of_replicas);
            /// We update the node, registering there one more replica.
            auto code = zookeeper->trySet(quorum_status_path, quorum_entry.toString(), stat.version);

            if (code == Coordination::Error::ZOK)
            {
                break;
            }
            else if (code == Coordination::Error::ZNONODE)
            {
                /// The quorum has already been achieved.
                break;
            }
            else if (code == Coordination::Error::ZBADVERSION)
            {
                /// Node was updated meanwhile. We must re-read it and repeat all the actions.
                continue;
            }
            else
                throw Coordination::Exception::fromPath(code, quorum_status_path);
        }
    }
}


void StorageReplicatedMergeTree::cleanLastPartNode(const String & partition_id)
{
    auto zookeeper = getZooKeeper();

    LOG_DEBUG(log, "Cleaning up last part node for partition {}", partition_id);

    /// The name of the previous part for which the quorum was reached.
    const String quorum_last_part_path = fs::path(zookeeper_path) / "quorum" / "last_part";

    /// Delete information from "last_part" node.

    while (true)
    {
        Coordination::Stat added_parts_stat;
        String old_added_parts = zookeeper->get(quorum_last_part_path, &added_parts_stat);

        ReplicatedMergeTreeQuorumAddedParts parts_with_quorum(format_version);

        if (!old_added_parts.empty())
            parts_with_quorum.fromString(old_added_parts);

        /// Delete information about particular partition.
        if (!parts_with_quorum.added_parts.contains(partition_id))
        {
            /// There is no information about interested part.
            LOG_TEST(log, "There is no information about the partition");
            break;
        }

        parts_with_quorum.added_parts.erase(partition_id);

        String new_added_parts = parts_with_quorum.toString();

        auto code = zookeeper->trySet(quorum_last_part_path, new_added_parts, added_parts_stat.version);

        if (code == Coordination::Error::ZOK)
        {
            LOG_DEBUG(log, "Last parent node for partition {} is cleaned up", partition_id);
            break;
        }
        else if (code == Coordination::Error::ZNONODE)
        {
            /// Node is deleted. It is impossible, but it is Ok.
            LOG_WARNING(log, "The last part node {} was deleted", quorum_last_part_path);
            break;
        }
        else if (code == Coordination::Error::ZBADVERSION)
        {
            /// Node was updated meanwhile. We must re-read it and repeat all the actions.
            continue;
        }
        else
            throw Coordination::Exception::fromPath(code, quorum_last_part_path);
    }
}


bool StorageReplicatedMergeTree::partIsInsertingWithParallelQuorum(const MergeTreePartInfo & part_info) const
{
    auto zookeeper = getZooKeeper();
    return zookeeper->exists(fs::path(zookeeper_path) / "quorum" / "parallel" / part_info.getPartNameV1());
}


bool StorageReplicatedMergeTree::partIsLastQuorumPart(const MergeTreePartInfo & part_info) const
{
    auto zookeeper = getZooKeeper();

    const String parts_with_quorum_path = fs::path(zookeeper_path) / "quorum" / "last_part";

    String parts_with_quorum_str = zookeeper->get(parts_with_quorum_path);

    if (parts_with_quorum_str.empty())
        return false;

    ReplicatedMergeTreeQuorumAddedParts parts_with_quorum(format_version);
    parts_with_quorum.fromString(parts_with_quorum_str);

    auto partition_it = parts_with_quorum.added_parts.find(part_info.partition_id);
    if (partition_it == parts_with_quorum.added_parts.end())
        return false;

    return partition_it->second == part_info.getPartNameAndCheckFormat(format_version);
}


bool StorageReplicatedMergeTree::fetchPart(
    const String & part_name,
    const StorageMetadataPtr & metadata_snapshot,
    const String & source_zookeeper_name,
    const String & source_replica_path,
    bool to_detached,
    size_t quorum,
    zkutil::ZooKeeper::Ptr zookeeper_,
    bool try_fetch_shared)
{
    auto zookeeper = zookeeper_ ? zookeeper_ : getZooKeeper();
    const auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

    if (!to_detached)
    {
        if (auto part = getPartIfExists(part_info, {MergeTreeDataPartState::Outdated, MergeTreeDataPartState::Deleting}))
        {
            LOG_DEBUG(log, "Part {} should be deleted after previous attempt before fetch", part->name);
            /// Force immediate parts cleanup to delete the part that was left from the previous fetch attempt.
            cleanup_thread.wakeup();
            return false;
        }
    }

    {
        std::lock_guard lock(currently_fetching_parts_mutex);
        if (!currently_fetching_parts.insert(part_name).second)
        {
            LOG_DEBUG(log, "Part {} is already fetching right now", part_name);
            return false;
        }
    }

    SCOPE_EXIT_MEMORY
    ({
        std::lock_guard lock(currently_fetching_parts_mutex);
        currently_fetching_parts.erase(part_name);
    });

    LOG_DEBUG(log, "Fetching part {} from {}:{}", part_name, source_zookeeper_name, source_replica_path);

    auto settings_ptr = getSettings();
    TableLockHolder table_lock_holder;
    if (!to_detached)
        table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, settings_ptr->lock_acquire_timeout_for_background_operations);

    /// Logging
    Stopwatch stopwatch;
    MutableDataPartPtr part;
    DataPartsVector replaced_parts;
    ProfileEventsScope profile_events_scope;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        writePartLog(
            PartLogElement::DOWNLOAD_PART, execution_status, stopwatch.elapsed(),
            part_name, part, replaced_parts, nullptr,
            profile_events_scope.getSnapshot());
    };

    auto is_zero_copy_part = [&settings_ptr](const auto & data_part)
    {
        return settings_ptr->allow_remote_fs_zero_copy_replication && data_part->isStoredOnRemoteDiskWithZeroCopySupport();
    };

    DataPartPtr part_to_clone;
    {
        /// If the desired part is a result of a part mutation, try to find the source part and compare
        /// its checksums to the checksums of the desired part. If they match, we can just clone the local part.

        /// If we have the source part, its part_info will contain covered_part_info.
        auto covered_part_info = part_info;
        covered_part_info.mutation = 0;
        auto source_part = getActiveContainingPart(covered_part_info);

        /// Fetch for zero-copy replication is cheap and straightforward, so we don't use local clone here
        if (source_part && !is_zero_copy_part(source_part))
        {
            auto source_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(
                source_part->getColumns(), source_part->checksums);

            String part_path = fs::path(source_replica_path) / "parts" / part_name;
            String part_znode = zookeeper->get(part_path);

            std::optional<ReplicatedMergeTreePartHeader> desired_part_header;
            if (!part_znode.empty())
            {
                desired_part_header = ReplicatedMergeTreePartHeader::fromString(part_znode);
            }
            else
            {
                String columns_str;
                String checksums_str;

                if (zookeeper->tryGet(fs::path(part_path) / "columns", columns_str) &&
                    zookeeper->tryGet(fs::path(part_path) / "checksums", checksums_str))
                {
                    desired_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksumsZNodes(columns_str, checksums_str);
                }
                else
                {
                    LOG_INFO(log, "Not checking checksums of part {} with replica {}:{} because part was removed from ZooKeeper",
                        part_name, source_zookeeper_name, source_replica_path);
                }
            }

            /// Checking both checksums and columns hash. For example we can have empty part
            /// with same checksums but different columns. And we attaching it exception will
            /// be thrown.
            if (desired_part_header
                && source_part_header.getColumnsHash() == desired_part_header->getColumnsHash()
                && source_part_header.getChecksums() == desired_part_header->getChecksums())
            {
                LOG_TRACE(log, "Found local part {} with the same checksums and columns hash as {}", source_part->name, part_name);
                part_to_clone = source_part;
            }
        }
    }

    ReplicatedMergeTreeAddress address;
    ConnectionTimeouts timeouts;
    String interserver_scheme;
    InterserverCredentialsPtr credentials;
    std::optional<CurrentlySubmergingEmergingTagger> tagger_ptr;
    std::function<MutableDataPartPtr()> get_part;
    scope_guard part_directory_lock;

    if (part_to_clone)
    {
        get_part = [&, part_to_clone]()
        {
            chassert(!is_zero_copy_part(part_to_clone));
            IDataPartStorage::ClonePartParams clone_params{ .keep_metadata_version = true };
            auto [cloned_part, lock] = cloneAndLoadDataPartOnSameDisk(part_to_clone, "tmp_clone_", part_info, metadata_snapshot, clone_params);
            part_directory_lock = std::move(lock);
            return cloned_part;
        };
    }
    else
    {
        address.fromString(zookeeper->get(fs::path(source_replica_path) / "host"));
        timeouts = getFetchPartHTTPTimeouts(getContext());

        credentials = getContext()->getInterserverCredentials();
        interserver_scheme = getContext()->getInterserverScheme();

        get_part = [&, address, timeouts, credentials, interserver_scheme]()
        {
            if (interserver_scheme != address.scheme)
                throw Exception(ErrorCodes::INTERSERVER_SCHEME_DOESNT_MATCH, "Interserver schemes are different: "
                    "'{}' != '{}', can't fetch part from {}", interserver_scheme, address.scheme, address.host);

            auto [fetched_part, lock] =  fetcher.fetchSelectedPart(
                metadata_snapshot,
                getContext(),
                part_name,
                source_zookeeper_name,
                source_replica_path,
                address.host,
                address.replication_port,
                timeouts,
                credentials->getUser(),
                credentials->getPassword(),
                interserver_scheme,
                replicated_fetches_throttler,
                to_detached,
                "",
                &tagger_ptr,
                try_fetch_shared);
            part_directory_lock = std::move(lock);
            return fetched_part;
        };
    }

    try
    {
        part = get_part();

        if (!to_detached)
        {
            Transaction transaction(*this, NO_TRANSACTION_RAW);
            renameTempPartAndReplace(part, transaction);

            chassert(!part_to_clone || !is_zero_copy_part(part));
            replaced_parts = checkPartChecksumsAndCommit(transaction, part, /*hardlinked_files*/ {}, /*replace_zero_copy_lock*/ true);

            /** If a quorum is tracked for this part, you must update it.
              * If you do not have time, in case of losing the session, when you restart the server - see the `ReplicatedMergeTreeRestartingThread::updateQuorumIfWeHavePart` method.
              */
            if (quorum)
            {
                /// Check if this quorum insert is parallel or not
                if (zookeeper->exists(fs::path(zookeeper_path) / "quorum" / "parallel" / part_name))
                    updateQuorum(part_name, true);
                else if (zookeeper->exists(fs::path(zookeeper_path) / "quorum" / "status"))
                    updateQuorum(part_name, false);
            }

            /// merged parts that are still inserted with quorum. if it only contains one block, it hasn't been merged before
            if (part_info.level != 0 || part_info.mutation != 0)
            {
                Strings quorum_parts = zookeeper->getChildren(fs::path(zookeeper_path) / "quorum" / "parallel");
                for (const String & quorum_part : quorum_parts)
                {
                    auto quorum_part_info = MergeTreePartInfo::fromPartName(quorum_part, format_version);
                    if (part_info.contains(quorum_part_info))
                        updateQuorum(quorum_part, true);
                }
            }

            merge_selecting_task->schedule();

            for (const auto & replaced_part : replaced_parts)
            {
                LOG_DEBUG(log, "Part {} is rendered obsolete by fetching part {}", replaced_part->name, part_name);
                ProfileEvents::increment(ProfileEvents::ObsoleteReplicatedParts);
            }

            write_part_log({});
        }
        else
        {
            // The fetched part is valuable and should not be cleaned like a temp part.
            part->is_temp = false;
            part->renameTo(fs::path("detached") / part_name, true);
        }
    }
    catch (const Exception & e)
    {
        /// The same part is being written right now (but probably it's not committed yet).
        /// We will check the need for fetch later.
        if (e.code() == ErrorCodes::DIRECTORY_ALREADY_EXISTS)
        {
            LOG_TRACE(log, "Not fetching part: {}", e.message());
            return false;
        }

        throw;
    }
    catch (...)
    {
        if (!to_detached)
            write_part_log(ExecutionStatus::fromCurrentException("", true));

        throw;
    }

    ProfileEvents::increment(ProfileEvents::ReplicatedPartFetches);

    if (part_to_clone)
        LOG_DEBUG(log, "Cloned part {} from {}{}", part_name, part_to_clone->name, to_detached ? " (to 'detached' directory)" : "");
    else
        LOG_DEBUG(log, "Fetched part {} from {}:{}{}", part_name, source_zookeeper_name, source_replica_path, to_detached ? " (to 'detached' directory)" : "");

    return true;
}


MergeTreeData::MutableDataPartPtr StorageReplicatedMergeTree::fetchExistsPart(
    const String & part_name,
    const StorageMetadataPtr & metadata_snapshot,
    const String & source_replica_path,
    DiskPtr replaced_disk,
    String replaced_part_path)
{
    auto zookeeper = getZooKeeper();
    const auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

    if (auto part = getPartIfExists(part_info, {MergeTreeDataPartState::Outdated, MergeTreeDataPartState::Deleting}))
    {
        LOG_DEBUG(log, "Part {} should be deleted after previous attempt before fetch", part->name);
        /// Force immediate parts cleanup to delete the part that was left from the previous fetch attempt.
        cleanup_thread.wakeup();
        return nullptr;
    }

    {
        std::lock_guard lock(currently_fetching_parts_mutex);
        if (!currently_fetching_parts.insert(part_name).second)
        {
            LOG_DEBUG(log, "Part {} is already fetching right now", part_name);
            return nullptr;
        }
    }

    SCOPE_EXIT_MEMORY
    ({
        std::lock_guard lock(currently_fetching_parts_mutex);
        currently_fetching_parts.erase(part_name);
    });

    LOG_DEBUG(log, "Fetching already known part {} from {}:{}", part_name, zookeeper_name, source_replica_path);

    TableLockHolder table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

    /// Logging
    Stopwatch stopwatch;
    MutableDataPartPtr part;
    DataPartsVector replaced_parts;
    ProfileEventsScope profile_events_scope;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        writePartLog(
            PartLogElement::DOWNLOAD_PART, execution_status, stopwatch.elapsed(),
            part_name, part, replaced_parts, nullptr,
            profile_events_scope.getSnapshot());
    };

    std::function<MutableDataPartPtr()> get_part;

    ReplicatedMergeTreeAddress address(zookeeper->get(fs::path(source_replica_path) / "host"));
    auto timeouts = getHTTPTimeouts(getContext());
    auto credentials = getContext()->getInterserverCredentials();
    String interserver_scheme = getContext()->getInterserverScheme();
    scope_guard part_temp_directory_lock;

    get_part = [&, address, timeouts, interserver_scheme, credentials]()
    {
        if (interserver_scheme != address.scheme)
            throw Exception(ErrorCodes::INTERSERVER_SCHEME_DOESNT_MATCH, "Interserver schemes are different: "
                "'{}' != '{}', can't fetch part from {}", interserver_scheme, address.scheme, address.host);

        auto [fetched_part, lock] = fetcher.fetchSelectedPart(
            metadata_snapshot, getContext(), part_name, zookeeper_name, source_replica_path,
            address.host, address.replication_port,
            timeouts, credentials->getUser(), credentials->getPassword(),
            interserver_scheme, replicated_fetches_throttler, false, "", nullptr, true,
            replaced_disk);
        part_temp_directory_lock = std::move(lock);
        return fetched_part;
    };

    try
    {
        part = get_part();

        if (part->getDataPartStorage().getDiskName() != replaced_disk->getName())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} fetched on wrong disk {}", part->name, part->getDataPartStorage().getDiskName());

        auto replaced_path = fs::path(replaced_part_path);
        part->getDataPartStorage().rename(replaced_path.parent_path(), replaced_path.filename(), nullptr, true, false);
    }
    catch (const Exception & e)
    {
        /// The same part is being written right now (but probably it's not committed yet).
        /// We will check the need for fetch later.
        if (e.code() == ErrorCodes::DIRECTORY_ALREADY_EXISTS)
        {
            LOG_TRACE(log, "Not fetching part: {}", e.message());
            return nullptr;
        }

        throw;
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException("", true));
        throw;
    }

    ProfileEvents::increment(ProfileEvents::ReplicatedPartFetches);

    LOG_DEBUG(log, "Fetched part {} from {}:{}", part_name, zookeeper_name, source_replica_path);
    return part;
}

void StorageReplicatedMergeTree::startup()
{
    LOG_TRACE(log, "Starting up table");
    startOutdatedDataPartsLoadingTask();
    if (attach_thread)
    {
        attach_thread->start();
        attach_thread->waitFirstTry();
        return;
    }

    startupImpl(/* from_attach_thread */ false);
}

void StorageReplicatedMergeTree::startupImpl(bool from_attach_thread)
{
    /// Do not start replication if ZooKeeper is not configured or there is no metadata in zookeeper
    if (!has_metadata_in_zookeeper.has_value() || !*has_metadata_in_zookeeper)
    {
        if (!since_metadata_err_incr_readonly_metric)
        {
            since_metadata_err_incr_readonly_metric = true;
            CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);
        }

        LOG_TRACE(log, "No connection to ZooKeeper or no metadata in ZooKeeper, will not startup");
        return;
    }

    if (since_metadata_err_incr_readonly_metric)
    {
        since_metadata_err_incr_readonly_metric = false;
        CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);
        assert(CurrentMetrics::get(CurrentMetrics::ReadonlyReplica) >= 0);
    }

    try
    {
        auto zookeeper = getZooKeeper();
        InterserverIOEndpointPtr data_parts_exchange_ptr = std::make_shared<DataPartsExchange::Service>(*this);
        [[maybe_unused]] auto prev_ptr = std::atomic_exchange(&data_parts_exchange_endpoint, data_parts_exchange_ptr);
        assert(prev_ptr == nullptr);

        /// The endpoint id:
        ///     old format: DataPartsExchange:/clickhouse/tables/default/t1/{shard}/{replica}
        ///     new format: DataPartsExchange:{zookeeper_name}:/clickhouse/tables/default/t1/{shard}/{replica}
        /// Notice:
        ///     They are incompatible and the default is the old format.
        ///     If you want to use the new format, please ensure that 'enable_the_endpoint_id_with_zookeeper_name_prefix' of all nodes is true .
        ///
        getContext()->getInterserverIOHandler().addEndpoint(
            data_parts_exchange_ptr->getId(getEndpointName()), data_parts_exchange_ptr);

        startBeingLeader();

        /// Activate replica in a separate thread if we are not calling from attach thread
        restarting_thread.start(/*schedule=*/!from_attach_thread);

        if (from_attach_thread)
        {
            LOG_TRACE(log, "Trying to startup table from right now");
            /// Try activating replica in current thread.
            restarting_thread.run();
        }
        else
        {
            /// Wait while restarting_thread finishing initialization.
            /// NOTE It does not mean that replication is actually started after receiving this event.
            /// It only means that an attempt to startup replication was made.
            /// Table may be still in readonly mode if this attempt failed for any reason.
            while (!startup_event.tryWait(10 * 1000))
                LOG_TRACE(log, "Waiting for RestartingThread to startup table");
        }

        auto lock = std::unique_lock<std::mutex>(flush_and_shutdown_mutex, std::defer_lock);
        do
        {
            if (shutdown_prepared_called.load() || shutdown_called.load())
                throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Cannot startup table because it is dropped");
        }
        while (!lock.try_lock());

        /// And this is just a callback
        session_expired_callback_handler = EventNotifier::instance().subscribe(Coordination::Error::ZSESSIONEXPIRED, [this]()
        {
            LOG_TEST(log, "Received event for expired session. Waking up restarting thread");
            restarting_thread.start();
        });

        startBackgroundMovesIfNeeded();

        part_moves_between_shards_orchestrator.start();
    }
    catch (...)
    {
        /// Exception safety: failed "startup" does not require a call to "shutdown" from the caller.
        /// And it should be able to safely destroy table after exception in "startup" method.
        /// It means that failed "startup" must not create any background tasks that we will have to wait.
        try
        {
            /// it's important to avoid full shutdown here, because it even tries to shutdown attach thread which was
            /// designed exactly for this: try to start table if no zookeeper connection available.
            if (from_attach_thread)
            {
                restarting_thread.shutdown(/* part_of_full_shutdown */false);
            }
            else
            {
                shutdown();
            }
        }
        catch (...)
        {
            std::terminate();
        }

        /// Note: after failed "startup", the table will be in a state that only allows to destroy the object.
        throw;
    }
}


void StorageReplicatedMergeTree::flushAndPrepareForShutdown()
{
    std::lock_guard lock{flush_and_shutdown_mutex};
    if (shutdown_prepared_called.exchange(true))
        return;

    try
    {
        auto settings_ptr = getSettings();
        /// Cancel fetches, merges and mutations to force the queue_task to finish ASAP.
        fetcher.blocker.cancelForever();
        merger_mutator.merges_blocker.cancelForever();
        parts_mover.moves_blocker.cancelForever();
        stopBeingLeader();

        if (attach_thread)
            attach_thread->shutdown();

        restarting_thread.shutdown(/* part_of_full_shutdown */true);
        /// Explicitly set the event, because the restarting thread will not set it again
        startup_event.set();
        shutdown_deadline.emplace(std::chrono::system_clock::now() + std::chrono::milliseconds(settings_ptr->wait_for_unique_parts_send_before_shutdown_ms.totalMilliseconds()));
    }
    catch (...)
    {
        /// Don't wait anything in case of improper prepare for shutdown
        shutdown_deadline.emplace(std::chrono::system_clock::now());
        throw;
    }
}

void StorageReplicatedMergeTree::partialShutdown()
{
    ProfileEvents::increment(ProfileEvents::ReplicaPartialShutdown);

    partial_shutdown_called = true;
    partial_shutdown_event.set();
    queue.notifySubscribersOnPartialShutdown();
    replica_is_active_node = nullptr;

    LOG_TRACE(log, "Waiting for threads to finish");
    merge_selecting_task->deactivate();
    queue_updating_task->deactivate();
    mutations_updating_task->deactivate();
    mutations_finalizing_task->deactivate();

    cleanup_thread.stop();
    async_block_ids_cache.stop();
    part_check_thread.stop();

    /// Stop queue processing
    {
        auto fetch_lock = fetcher.blocker.cancel();
        auto merge_lock = merger_mutator.merges_blocker.cancel();
        auto move_lock = parts_mover.moves_blocker.cancel();
        background_operations_assignee.finish();
    }

    LOG_TRACE(log, "Threads finished");
}

void StorageReplicatedMergeTree::shutdown()
{
    if (shutdown_called.exchange(true))
        return;

    flushAndPrepareForShutdown();

    if (!shutdown_deadline.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shutdown deadline is not set in shutdown");

    try
    {
        waitForUniquePartsToBeFetchedByOtherReplicas(*shutdown_deadline);
    }
    catch (const Exception & ex)
    {
        if (ex.code() == ErrorCodes::LOGICAL_ERROR)
            throw;

        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    session_expired_callback_handler.reset();
    stopOutdatedDataPartsLoadingTask();

    partialShutdown();

    part_moves_between_shards_orchestrator.shutdown();

    {
        auto lock = queue.lockQueue();
        /// Cancel logs pulling after background task were cancelled. It's still
        /// required because we can trigger pullLogsToQueue during manual OPTIMIZE,
        /// MUTATE, etc. query.
        queue.pull_log_blocker.cancelForever();
    }
    background_moves_assignee.finish();

    auto data_parts_exchange_ptr = std::atomic_exchange(&data_parts_exchange_endpoint, InterserverIOEndpointPtr{});
    if (data_parts_exchange_ptr)
    {
        getContext()->getInterserverIOHandler().removeEndpointIfExists(data_parts_exchange_ptr->getId(getEndpointName()));
        /// Ask all parts exchange handlers to finish asap. New ones will fail to start
        data_parts_exchange_ptr->blocker.cancelForever();
        /// Wait for all of them
        std::lock_guard lock(data_parts_exchange_ptr->rwlock);
    }
}


StorageReplicatedMergeTree::~StorageReplicatedMergeTree()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


ReplicatedMergeTreeQuorumAddedParts::PartitionIdToMaxBlock StorageReplicatedMergeTree::getMaxAddedBlocks() const
{
    ReplicatedMergeTreeQuorumAddedParts::PartitionIdToMaxBlock max_added_blocks;

    for (const auto & data_part : getDataPartsForInternalUsage())
    {
        max_added_blocks[data_part->info.partition_id]
            = std::max(max_added_blocks[data_part->info.partition_id], data_part->info.max_block);
    }

    auto zookeeper = getZooKeeper();

    const String quorum_status_path = fs::path(zookeeper_path) / "quorum" / "status";

    String value;
    Coordination::Stat stat;

    if (zookeeper->tryGet(quorum_status_path, value, &stat))
    {
        ReplicatedMergeTreeQuorumEntry quorum_entry;
        quorum_entry.fromString(value);

        auto part_info = MergeTreePartInfo::fromPartName(quorum_entry.part_name, format_version);

        max_added_blocks[part_info.partition_id] = part_info.max_block - 1;
    }

    String added_parts_str;
    if (zookeeper->tryGet(fs::path(zookeeper_path) / "quorum" / "last_part", added_parts_str))
    {
        if (!added_parts_str.empty())
        {
            ReplicatedMergeTreeQuorumAddedParts part_with_quorum(format_version);
            part_with_quorum.fromString(added_parts_str);

            auto added_parts = part_with_quorum.added_parts;

            for (const auto & added_part : added_parts)
            {
                if (!getActiveContainingPart(added_part.second))
                    throw Exception(ErrorCodes::REPLICA_IS_NOT_IN_QUORUM,
                        "Replica doesn't have part '{}' which was successfully written to quorum of other replicas. "
                        "Send query to another replica or disable 'select_sequential_consistency' setting", added_part.second);
            }

            for (const auto & max_block : part_with_quorum.getMaxInsertedBlocks())
                max_added_blocks[max_block.first] = max_block.second;
        }
    }
    return max_added_blocks;
}


void StorageReplicatedMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const size_t num_streams)
{
    SCOPE_EXIT({
        /// Now, copy of parts that is required for the query, stored in the processors,
        /// while snapshot_data.parts includes all parts, even one that had been filtered out with partition pruning,
        /// reset them to avoid holding them.
        auto & snapshot_data = assert_cast<MergeTreeData::SnapshotData &>(*storage_snapshot->data);
        snapshot_data.parts = {};
        snapshot_data.alter_conversions = {};
    });

    const auto & settings = local_context->getSettingsRef();

    /// The `select_sequential_consistency` setting has two meanings:
    /// 1. To throw an exception if on a replica there are not all parts which have been written down on quorum of remaining replicas.
    /// 2. Do not read parts that have not yet been written to the quorum of the replicas.
    /// For this you have to synchronously go to ZooKeeper.
    if (settings.select_sequential_consistency)
        return readLocalSequentialConsistencyImpl(query_plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);

    if (local_context->canUseParallelReplicasOnInitiator())
        return readParallelReplicasImpl(query_plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);

    readLocalImpl(query_plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
}

void StorageReplicatedMergeTree::readLocalSequentialConsistencyImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto max_added_blocks = std::make_shared<ReplicatedMergeTreeQuorumAddedParts::PartitionIdToMaxBlock>(getMaxAddedBlocks());
    auto plan = reader.read(column_names, storage_snapshot, query_info, local_context,
            max_block_size, num_streams, processed_stage, std::move(max_added_blocks),
            /* enable_parallel_reading= */false);
    if (plan)
        query_plan = std::move(*plan);
}

void StorageReplicatedMergeTree::readParallelReplicasImpl(
    QueryPlan & query_plan,
    const Names & /*column_names*/,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    auto table_id = getStorageID();

    auto scalars = local_context->hasQueryContext() ? local_context->getQueryContext()->getScalars() : Scalars{};
    String cluster_for_parallel_replicas = local_context->getSettingsRef().cluster_for_parallel_replicas;
    auto parallel_replicas_cluster = local_context->getCluster(cluster_for_parallel_replicas);

    ASTPtr modified_query_ast;
    Block header;
    if (local_context->getSettingsRef().allow_experimental_analyzer)
    {
        auto modified_query_tree = buildQueryTreeForShard(query_info, query_info.query_tree);

        header = InterpreterSelectQueryAnalyzer::getSampleBlock(
            modified_query_tree, local_context, SelectQueryOptions(processed_stage).analyze());
        modified_query_ast = queryNodeToSelectQuery(modified_query_tree);
    }
    else
    {
        modified_query_ast = ClusterProxy::rewriteSelectQuery(local_context, query_info.query,
            table_id.database_name, table_id.table_name, /*remote_table_function_ptr*/nullptr);
        header
            = InterpreterSelectQuery(modified_query_ast, local_context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();
    }

    ClusterProxy::SelectStreamFactory select_stream_factory = ClusterProxy::SelectStreamFactory(
        header,
        {},
        storage_snapshot,
        processed_stage);

    ClusterProxy::executeQueryWithParallelReplicas(
        query_plan, getStorageID(),
        /* table_func_ptr= */ nullptr,
        select_stream_factory, modified_query_ast,
        local_context, query_info, parallel_replicas_cluster);
}

void StorageReplicatedMergeTree::readLocalImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const size_t num_streams)
{
    auto plan = reader.read(
        column_names, storage_snapshot, query_info,
        local_context, max_block_size, num_streams,
        processed_stage,
        /* max_block_numbers_to_read= */ nullptr,
        /* enable_parallel_reading= */ local_context->canUseParallelReplicasOnFollower());
    if (plan)
        query_plan = std::move(*plan);
}

template <class Func>
void StorageReplicatedMergeTree::foreachActiveParts(Func && func, bool select_sequential_consistency) const
{
    std::optional<ReplicatedMergeTreeQuorumAddedParts::PartitionIdToMaxBlock> max_added_blocks = {};

    /**
     * Synchronously go to ZooKeeper when select_sequential_consistency enabled
     */
    if (select_sequential_consistency)
        max_added_blocks = getMaxAddedBlocks();

    auto lock = lockParts();
    /// TODO Transactions: should we count visible parts only?
    for (const auto & part : getDataPartsStateRange(DataPartState::Active))
    {
        if (part->isEmpty())
            continue;

        if (max_added_blocks)
        {
            auto blocks_iterator = max_added_blocks->find(part->info.partition_id);
            if (blocks_iterator == max_added_blocks->end() || part->info.max_block > blocks_iterator->second)
                continue;
        }

        func(part);
    }
}

std::optional<UInt64> StorageReplicatedMergeTree::totalRows(const Settings & settings) const
{
    UInt64 res = 0;
    foreachActiveParts([&res](auto & part) { res += part->rows_count; }, settings.select_sequential_consistency);
    return res;
}

std::optional<UInt64> StorageReplicatedMergeTree::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr local_context) const
{
    DataPartsVector parts;
    foreachActiveParts([&](auto & part) { parts.push_back(part); }, local_context->getSettingsRef().select_sequential_consistency);
    return totalRowsByPartitionPredicateImpl(query_info, local_context, parts);
}

std::optional<UInt64> StorageReplicatedMergeTree::totalBytes(const Settings & settings) const
{
    UInt64 res = 0;
    foreachActiveParts([&res](auto & part) { res += part->getBytesOnDisk(); }, settings.select_sequential_consistency);
    return res;
}


void StorageReplicatedMergeTree::assertNotReadonly() const
{
    if (is_readonly)
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode (replica path: {})", replica_path);
}


SinkToStoragePtr StorageReplicatedMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool async_insert)
{
    if (!initialization_done)
        throw Exception(ErrorCodes::NOT_INITIALIZED, "Table is not initialized yet");

    /// If table is read-only because it doesn't have metadata in zk yet, then it's not possible to insert into it
    /// Without this check, we'll write data parts on disk, and afterwards will remove them since we'll fail to commit them into zk
    /// In case of remote storage like s3, it'll generate unnecessary PUT requests
    if (is_readonly && (!has_metadata_in_zookeeper.has_value() || false == has_metadata_in_zookeeper.value()))
        throw Exception(
            ErrorCodes::TABLE_IS_READ_ONLY,
            "Table is in readonly mode since table metadata was not found in zookeeper: replica_path={}",
            replica_path);

    const auto storage_settings_ptr = getSettings();
    const Settings & query_settings = local_context->getSettingsRef();
    bool deduplicate = storage_settings_ptr->replicated_deduplication_window != 0 && query_settings.insert_deduplicate;
    bool async_deduplicate = async_insert && query_settings.async_insert_deduplicate && storage_settings_ptr->replicated_deduplication_window_for_async_inserts != 0 && query_settings.insert_deduplicate;
    if (async_deduplicate)
        return std::make_shared<ReplicatedMergeTreeSinkWithAsyncDeduplicate>(
            *this, metadata_snapshot, query_settings.insert_quorum.valueOr(0),
            query_settings.insert_quorum_timeout.totalMilliseconds(),
            query_settings.max_partitions_per_insert_block,
            query_settings.insert_quorum_parallel,
            deduplicate,
            query_settings.insert_quorum.is_auto,
            local_context);

    // TODO: should we also somehow pass list of columns to deduplicate on to the ReplicatedMergeTreeSink?
    return std::make_shared<ReplicatedMergeTreeSink>(
        *this, metadata_snapshot, query_settings.insert_quorum.valueOr(0),
        query_settings.insert_quorum_timeout.totalMilliseconds(),
        query_settings.max_partitions_per_insert_block,
        query_settings.insert_quorum_parallel,
        deduplicate,
        query_settings.insert_quorum.is_auto,
        local_context);
}


std::optional<QueryPipeline> StorageReplicatedMergeTree::distributedWriteFromClusterStorage(const std::shared_ptr<IStorageCluster> & src_storage_cluster, const ASTInsertQuery & query, ContextPtr local_context)
{
    const auto & settings = local_context->getSettingsRef();
    auto extension = src_storage_cluster->getTaskIteratorExtension(nullptr, local_context);

    /// Here we won't check that the cluster formed from table replicas is a subset of a cluster specified in s3Cluster/hdfsCluster table function
    auto src_cluster = src_storage_cluster->getCluster(local_context);

    /// Actually the query doesn't change, we just serialize it to string
    String query_str;
    {
        WriteBufferFromOwnString buf;
        IAST::FormatSettings ast_format_settings(buf, /*one_line*/ true, /*hilite*/ false, /*always_quote_identifiers*/ true);
        query.IAST::format(ast_format_settings);
        query_str = buf.str();
    }

    QueryPipeline pipeline;
    ContextMutablePtr query_context = Context::createCopy(local_context);
    query_context->increaseDistributedDepth();

    for (const auto & replicas : src_cluster->getShardsAddresses())
    {
        /// There will be only one replica, because we consider each replica as a shard
        for (const auto & node : replicas)
        {
            auto connection = std::make_shared<Connection>(
                node.host_name, node.port, query_context->getGlobalContext()->getCurrentDatabase(),
                node.user, node.password, node.quota_key, node.cluster, node.cluster_secret,
                "ParallelInsertSelectInititiator",
                node.compression,
                node.secure
            );

            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                connection,
                query_str,
                Block{},
                query_context,
                /*throttler=*/nullptr,
                Scalars{},
                Tables{},
                QueryProcessingStage::Complete,
                extension);

            QueryPipeline remote_pipeline(std::make_shared<RemoteSource>(remote_query_executor, false, settings.async_socket_for_remote, settings.async_query_sending_for_remote));
            remote_pipeline.complete(std::make_shared<EmptySink>(remote_query_executor->getHeader()));

            pipeline.addCompletedPipeline(std::move(remote_pipeline));
        }
    }

    return pipeline;
}

std::optional<QueryPipeline> StorageReplicatedMergeTree::distributedWrite(const ASTInsertQuery & query, ContextPtr local_context)
{
    /// Do not enable parallel distributed INSERT SELECT in case when query probably comes from another server
    if (local_context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        return {};

    const Settings & settings = local_context->getSettingsRef();
    if (settings.max_distributed_depth && local_context->getClientInfo().distributed_depth >= settings.max_distributed_depth)
        throw Exception(ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH, "Maximum distributed depth exceeded");

    auto & select = query.select->as<ASTSelectWithUnionQuery &>();

    StoragePtr src_storage;

    if (select.list_of_selects->children.size() == 1)
    {
        if (auto * select_query = select.list_of_selects->children.at(0)->as<ASTSelectQuery>())
        {
            JoinedTables joined_tables(Context::createCopy(local_context), *select_query);

            if (joined_tables.tablesCount() == 1)
            {
                src_storage = joined_tables.getLeftTableStorage();
            }
        }
    }

    if (!src_storage)
        return {};

    if (auto src_distributed = std::dynamic_pointer_cast<IStorageCluster>(src_storage))
    {
        return distributedWriteFromClusterStorage(src_distributed, query, local_context);
    }
    else if (local_context->getClientInfo().distributed_depth == 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parallel distributed INSERT SELECT is not possible. Reason: distributed "
            "reading into Replicated table is supported only from *Cluster table functions, but got {} storage", src_storage->getName());
    }

    return {};
}


bool StorageReplicatedMergeTree::optimize(
    const ASTPtr &,
    const StorageMetadataPtr &,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr query_context)
{
    /// NOTE: exclusive lock cannot be used here, since this may lead to deadlock (see comments below),
    /// but it should be safe to use non-exclusive to avoid dropping parts that may be required for processing queue.
    auto table_lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);

    assertNotReadonly();

    if (!is_leader)
        throw Exception(ErrorCodes::NOT_A_LEADER, "OPTIMIZE cannot be done on this replica because it is not a leader");

    if (cleanup)
        LOG_DEBUG(log, "Cleanup the ReplicatedMergeTree.");

    auto handle_noop = [&]<typename... Args>(FormatStringHelper<Args...> fmt_string, Args && ...args)
    {
        PreformattedMessage message = fmt_string.format(std::forward<Args>(args)...);
        LOG_DEBUG(log, message);
        if (query_context->getSettingsRef().optimize_throw_if_noop)
            throw Exception(std::move(message), ErrorCodes::CANNOT_ASSIGN_OPTIMIZE);
        return false;
    };

    auto zookeeper = getZooKeeperAndAssertNotReadonly();
    const auto storage_settings_ptr = getSettings();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    std::vector<ReplicatedMergeTreeLogEntryData> merge_entries;

    auto try_assign_merge = [&](const String & partition_id) -> bool
    {
        constexpr size_t max_retries = 10;
        size_t try_no = 0;
        for (; try_no < max_retries; ++try_no)
        {
            /// We must select parts for merge under merge_selecting_mutex because other threads
            /// (merge_selecting_thread or OPTIMIZE queries) could assign new merges.
            std::lock_guard merge_selecting_lock(merge_selecting_mutex);
            PartitionIdsHint partition_ids_hint;
            if (partition_id.empty())
            {
                partition_ids_hint = getAllPartitionIds();
            }
            else
            {
                auto parts_lock = lockParts();
                if (!getAnyPartInPartition(partition_id, parts_lock))
                    handle_noop("Cannot select parts for optimization: there are no parts in partition {}", partition_id);
                partition_ids_hint.insert(partition_id);
            }
            ReplicatedMergeTreeMergePredicate can_merge = queue.getMergePredicate(zookeeper, std::move(partition_ids_hint));

            auto future_merged_part = std::make_shared<FutureMergedMutatedPart>();
            if (storage_settings.get()->assign_part_uuids)
                future_merged_part->uuid = UUIDHelpers::generateV4();

            constexpr const char * unknown_disable_reason = "unknown reason";
            String disable_reason = unknown_disable_reason;
            SelectPartsDecision select_decision = SelectPartsDecision::CANNOT_SELECT;

            if (partition_id.empty())
            {
                select_decision = merger_mutator.selectPartsToMerge(
                    future_merged_part, /* aggressive */ true, storage_settings_ptr->max_bytes_to_merge_at_max_space_in_pool,
                    can_merge, /* merge_with_ttl_allowed */ false, NO_TRANSACTION_PTR, disable_reason);
            }
            else
            {
                select_decision = merger_mutator.selectAllPartsToMergeWithinPartition(
                    future_merged_part, can_merge, partition_id, final, metadata_snapshot, NO_TRANSACTION_PTR,
                    disable_reason, query_context->getSettingsRef().optimize_skip_merged_partitions);
            }

            /// If there is nothing to merge then we treat this merge as successful (needed for optimize final optimization)
            if (select_decision == SelectPartsDecision::NOTHING_TO_MERGE)
                return false;

            if (select_decision != SelectPartsDecision::SELECTED)
            {
                constexpr const char * message_fmt = "Cannot select parts for optimization: {}";
                assert(disable_reason != unknown_disable_reason);
                if (!partition_id.empty())
                    disable_reason += fmt::format(" (in partition {})", partition_id);
                return handle_noop(message_fmt, disable_reason);
            }

            ReplicatedMergeTreeLogEntryData merge_entry;
            CreateMergeEntryResult create_result = createLogEntryToMergeParts(
                zookeeper, future_merged_part->parts,
                future_merged_part->name,
                future_merged_part->uuid,
                future_merged_part->part_format,
                deduplicate, deduplicate_by_columns,
                cleanup,
                &merge_entry, can_merge.getVersion(),
                future_merged_part->merge_type);

            if (create_result == CreateMergeEntryResult::MissingPart)
            {
                static constexpr const char * message_fmt = "Can't create merge queue node in ZooKeeper, because some parts are missing";
                return handle_noop(message_fmt);
            }

            if (create_result == CreateMergeEntryResult::LogUpdated)
                continue;

            merge_entries.push_back(std::move(merge_entry));
            return true;
        }

        assert(try_no == max_retries);
        static constexpr const char * message_fmt = "Can't create merge queue node in ZooKeeper, because log was updated in every of {} tries";
        return handle_noop(message_fmt, try_no);
    };

    bool assigned = false;
    if (!partition && final)
    {
        if (cleanup && this->merging_params.mode != MergingParams::Mode::Replacing)
        {
            constexpr const char * message = "Cannot OPTIMIZE with CLEANUP table: {}";
            String disable_reason = "only ReplacingMergeTree can be CLEANUP";
            throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, message, disable_reason);
        }

        DataPartsVector data_parts = getVisibleDataPartsVector(query_context);
        std::unordered_set<String> partition_ids;

        for (const DataPartPtr & part : data_parts)
            partition_ids.emplace(part->info.partition_id);

        for (const String & partition_id : partition_ids)
        {
            assigned = try_assign_merge(partition_id);
            if (!assigned)
                break;
        }
    }
    else
    {
        String partition_id;
        if (partition)
            partition_id = getPartitionIDFromQuery(partition, query_context);
        assigned = try_assign_merge(partition_id);
    }

    table_lock.reset();

    for (auto & merge_entry : merge_entries)
        waitForLogEntryToBeProcessedIfNecessary(merge_entry, query_context);

    return assigned;
}

bool StorageReplicatedMergeTree::executeMetadataAlter(const StorageReplicatedMergeTree::LogEntry & entry)
{
    auto current_metadata = getInMemoryMetadataPtr();
    if (entry.alter_version < current_metadata->getMetadataVersion())
    {
        /// TODO Can we replace it with LOGICAL_ERROR?
        /// As for now, it may rarely happen due to reordering of ALTER_METADATA entries in the queue of
        /// non-initial replica and also may happen after stale replica recovery.
        LOG_WARNING(log, "Attempt to update metadata of version {} "
                         "to older version {} when processing log entry {}: {}",
                         current_metadata->getMetadataVersion(), entry.alter_version, entry.znode_name, entry.toString());
        return true;
    }

    auto zookeeper = getZooKeeper();

    auto columns_from_entry = ColumnsDescription::parse(entry.columns_str);
    auto metadata_from_entry = ReplicatedMergeTreeTableMetadata::parse(entry.metadata_str);

    MergeTreeData::DataParts parts;

    /// If metadata nodes have changed, we will update table structure locally.
    Coordination::Requests requests;
    requests.emplace_back(zkutil::makeSetRequest(fs::path(replica_path) / "columns", entry.columns_str, -1));
    requests.emplace_back(zkutil::makeSetRequest(fs::path(replica_path) / "metadata", entry.metadata_str, -1));

    auto table_id = getStorageID();
    auto alter_context = getContext();

    auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    bool is_in_replicated_database = database->getEngineName() == "Replicated";

    if (is_in_replicated_database)
    {
        auto mutable_alter_context = Context::createCopy(getContext());
        const auto * replicated = dynamic_cast<const DatabaseReplicated *>(database.get());
        mutable_alter_context->makeQueryContext();
        auto alter_txn = std::make_shared<ZooKeeperMetadataTransaction>(zookeeper, replicated->getZooKeeperPath(),
                                                                       /* is_initial_query */ false, /* task_zk_path */ "");
        mutable_alter_context->initZooKeeperMetadataTransaction(alter_txn);
        alter_context = mutable_alter_context;

        for (auto & op : requests)
            alter_txn->addOp(std::move(op));
        requests.clear();
        /// Requests will be executed by database in setTableStructure
    }
    else
    {
        zookeeper->multi(requests);
    }

    {
        auto table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);
        auto alter_lock_holder = lockForAlter(getSettings()->lock_acquire_timeout_for_background_operations);
        LOG_INFO(log, "Metadata changed in ZooKeeper. Applying changes locally.");

        auto metadata_diff = ReplicatedMergeTreeTableMetadata(*this, getInMemoryMetadataPtr()).checkAndFindDiff(metadata_from_entry, getInMemoryMetadataPtr()->getColumns(), getContext());
        setTableStructure(table_id, alter_context, std::move(columns_from_entry), metadata_diff, entry.alter_version);

        current_metadata = getInMemoryMetadataPtr();
        LOG_INFO(log, "Applied changes to the metadata of the table. Current metadata version: {}", current_metadata->getMetadataVersion());
    }

    {
        /// Reset Object columns, because column of type
        /// Object may be added or dropped by alter.
        auto parts_lock = lockParts();
        resetObjectColumnsFromActiveParts(parts_lock);
    }

    /// This transaction may not happen, but it's OK, because on the next retry we will eventually create/update this node
    /// TODO Maybe do in in one transaction for Replicated database?
    zookeeper->createOrUpdate(fs::path(replica_path) / "metadata_version", std::to_string(current_metadata->getMetadataVersion()), zkutil::CreateMode::Persistent);

    return true;
}


PartitionBlockNumbersHolder StorageReplicatedMergeTree::allocateBlockNumbersInAffectedPartitions(
    const MutationCommands & commands, ContextPtr query_context, const zkutil::ZooKeeperPtr & zookeeper) const
{
    const std::set<String> mutation_affected_partition_ids = getPartitionIdsAffectedByCommands(commands, query_context);

    if (mutation_affected_partition_ids.size() == 1)
    {
        const auto & affected_partition_id = *mutation_affected_partition_ids.cbegin();
        auto block_number_holder = allocateBlockNumber(affected_partition_id, zookeeper);
        if (!block_number_holder.has_value())
            return {};
        auto block_number = block_number_holder->getNumber();  /// Avoid possible UB due to std::move
        return {{{affected_partition_id, block_number}}, std::move(block_number_holder)};
    }
    else
    {
        /// TODO: Implement optimal block number acquisition algorithm in multiple (but not all) partitions
        EphemeralLocksInAllPartitions lock_holder(
            fs::path(zookeeper_path) / "block_numbers", "block-", fs::path(zookeeper_path) / "temp", *zookeeper);

        PartitionBlockNumbersHolder::BlockNumbersType block_numbers;
        for (const auto & lock : lock_holder.getLocks())
        {
            if (mutation_affected_partition_ids.empty() || mutation_affected_partition_ids.contains(lock.partition_id))
                block_numbers[lock.partition_id] = lock.number;
        }

        return {std::move(block_numbers), std::move(lock_holder)};
    }
}


void StorageReplicatedMergeTree::alter(
    const AlterCommands & commands, ContextPtr query_context, AlterLockHolder & table_lock_holder)
{
    assertNotReadonly();

    auto table_id = getStorageID();

    if (commands.isSettingsAlter())
    {
        /// We don't replicate storage_settings_ptr ALTER. It's local operation.
        /// Also we don't upgrade alter lock to table structure lock.
        StorageInMemoryMetadata future_metadata = getInMemoryMetadata();
        commands.apply(future_metadata, query_context);

        merge_strategy_picker.refreshState();

        changeSettings(future_metadata.settings_changes, table_lock_holder);

        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(query_context, table_id, future_metadata);
        return;
    }

    auto ast_to_str = [](ASTPtr query) -> String
    {
        if (!query)
            return "";
        return queryToString(query);
    };

    const auto zookeeper = getZooKeeperAndAssertNotReadonly();

    std::optional<ReplicatedMergeTreeLogEntryData> alter_entry;
    std::optional<String> mutation_znode;

    while (true)
    {
        /// Clear nodes from previous iteration
        alter_entry.emplace();
        mutation_znode.reset();

        auto current_metadata = getInMemoryMetadataPtr();

        StorageInMemoryMetadata future_metadata = *current_metadata;
        commands.apply(future_metadata, query_context);

        ReplicatedMergeTreeTableMetadata future_metadata_in_zk(*this, current_metadata);
        if (ast_to_str(future_metadata.sorting_key.definition_ast) != ast_to_str(current_metadata->sorting_key.definition_ast))
        {
            /// We serialize definition_ast as list, because code which apply ALTER (setTableStructure) expect serialized non empty expression
            /// list here and we cannot change this representation for compatibility. Also we have preparsed AST `sorting_key.expression_list_ast`
            /// in KeyDescription, but it contain version column for VersionedCollapsingMergeTree, which shouldn't be defined as a part of key definition AST.
            /// So the best compatible way is just to convert definition_ast to list and serialize it. In all other places key.expression_list_ast should be used.
            future_metadata_in_zk.sorting_key = serializeAST(*extractKeyExpressionList(future_metadata.sorting_key.definition_ast));
        }

        if (ast_to_str(future_metadata.sampling_key.definition_ast) != ast_to_str(current_metadata->sampling_key.definition_ast))
            future_metadata_in_zk.sampling_expression = serializeAST(*extractKeyExpressionList(future_metadata.sampling_key.definition_ast));

        if (ast_to_str(future_metadata.partition_key.definition_ast) != ast_to_str(current_metadata->partition_key.definition_ast))
            future_metadata_in_zk.partition_key = serializeAST(*extractKeyExpressionList(future_metadata.partition_key.definition_ast));

        if (ast_to_str(future_metadata.table_ttl.definition_ast) != ast_to_str(current_metadata->table_ttl.definition_ast))
        {
            if (future_metadata.table_ttl.definition_ast)
                future_metadata_in_zk.ttl_table = serializeAST(*future_metadata.table_ttl.definition_ast);
            else /// TTL was removed
                future_metadata_in_zk.ttl_table = "";
        }

        String new_indices_str = future_metadata.secondary_indices.toString();
        if (new_indices_str != current_metadata->secondary_indices.toString())
            future_metadata_in_zk.skip_indices = new_indices_str;

        String new_projections_str = future_metadata.projections.toString();
        if (new_projections_str != current_metadata->projections.toString())
            future_metadata_in_zk.projections = new_projections_str;

        String new_constraints_str = future_metadata.constraints.toString();
        if (new_constraints_str != current_metadata->constraints.toString())
            future_metadata_in_zk.constraints = new_constraints_str;

        Coordination::Requests ops;
        size_t alter_path_idx = std::numeric_limits<size_t>::max();
        size_t mutation_path_idx = std::numeric_limits<size_t>::max();

        String new_metadata_str = future_metadata_in_zk.toString();
        ops.emplace_back(zkutil::makeSetRequest(fs::path(zookeeper_path) / "metadata", new_metadata_str, current_metadata->getMetadataVersion()));

        String new_columns_str = future_metadata.columns.toString();
        ops.emplace_back(zkutil::makeSetRequest(fs::path(zookeeper_path) / "columns", new_columns_str, -1));

        if (ast_to_str(current_metadata->settings_changes) != ast_to_str(future_metadata.settings_changes))
        {
            /// Just change settings
            StorageInMemoryMetadata metadata_copy = *current_metadata;
            metadata_copy.settings_changes = future_metadata.settings_changes;
            changeSettings(metadata_copy.settings_changes, table_lock_holder);
            DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(query_context, table_id, metadata_copy);
        }

        /// We can be sure, that in case of successful commit in zookeeper our
        /// version will increments by 1. Because we update with version check.
        int new_metadata_version = current_metadata->getMetadataVersion() + 1;

        alter_entry->type = LogEntry::ALTER_METADATA;
        alter_entry->source_replica = replica_name;
        alter_entry->metadata_str = new_metadata_str;
        alter_entry->columns_str = new_columns_str;
        alter_entry->alter_version = new_metadata_version;
        alter_entry->create_time = time(nullptr);

        auto maybe_mutation_commands = commands.getMutationCommands(
            *current_metadata,
            query_context->getSettingsRef().materialize_ttl_after_modify,
            query_context);

        bool have_mutation = !maybe_mutation_commands.empty();
        alter_entry->have_mutation = have_mutation;

        alter_path_idx = ops.size();
        ops.emplace_back(zkutil::makeCreateRequest(
            fs::path(zookeeper_path) / "log/log-", alter_entry->toString(), zkutil::CreateMode::PersistentSequential));

        PartitionBlockNumbersHolder partition_block_numbers_holder;
        ReplicatedMergeTreeMutationEntry mutation_entry;
        if (have_mutation)
        {
            delayMutationOrThrowIfNeeded(&partial_shutdown_event, query_context);
            const String mutations_path(fs::path(zookeeper_path) / "mutations");

            mutation_entry.alter_version = new_metadata_version;
            mutation_entry.source_replica = replica_name;
            mutation_entry.commands = std::move(maybe_mutation_commands);

            Coordination::Stat mutations_stat;
            zookeeper->get(mutations_path, &mutations_stat);

            partition_block_numbers_holder =
                allocateBlockNumbersInAffectedPartitions(mutation_entry.commands, query_context, zookeeper);

            mutation_entry.block_numbers = partition_block_numbers_holder.getBlockNumbers();
            mutation_entry.create_time = time(nullptr);

            ops.emplace_back(zkutil::makeSetRequest(mutations_path, String(), mutations_stat.version));
            mutation_path_idx = ops.size();
            ops.emplace_back(
                zkutil::makeCreateRequest(fs::path(mutations_path) / "", mutation_entry.toString(), zkutil::CreateMode::PersistentSequential));
        }

        if (auto txn = query_context->getZooKeeperMetadataTransaction())
        {
            /// It would be better to clone ops instead of moving, so we could retry on ZBADVERSION,
            /// but clone() is not implemented for Coordination::Request.
            txn->moveOpsTo(ops);
            /// NOTE: IDatabase::alterTable(...) is called when executing ALTER_METADATA queue entry without query context,
            /// so we have to update metadata of DatabaseReplicated here.
            String metadata_zk_path = fs::path(txn->getDatabaseZooKeeperPath()) / "metadata" / escapeForFileName(table_id.table_name);
            auto ast = DatabaseCatalog::instance().getDatabase(table_id.database_name)->getCreateTableQuery(table_id.table_name, query_context);
            applyMetadataChangesToCreateQuery(ast, future_metadata);
            ops.emplace_back(zkutil::makeSetRequest(metadata_zk_path, getObjectDefinitionFromCreateQuery(ast), -1));
        }

        Coordination::Responses results;
        Coordination::Error rc = zookeeper->tryMulti(ops, results);

        /// For the sake of consistency with mechanics of concurrent background process of assigning parts merge tasks
        /// this placeholder must be held up until the moment of committing into ZK of the mutation entry
        /// See ReplicatedMergeTreeMergePredicate::canMergeTwoParts() method
        partition_block_numbers_holder.reset();

        if (rc == Coordination::Error::ZOK)
        {
            if (have_mutation)
            {
                /// ALTER_METADATA record in replication /log
                String alter_path = dynamic_cast<const Coordination::CreateResponse &>(*results[alter_path_idx]).path_created;
                alter_entry->znode_name = alter_path.substr(alter_path.find_last_of('/') + 1);

                /// ReplicatedMergeTreeMutationEntry record in /mutations
                String mutation_path = dynamic_cast<const Coordination::CreateResponse &>(*results[mutation_path_idx]).path_created;
                mutation_znode = mutation_path.substr(mutation_path.find_last_of('/') + 1);
                LOG_DEBUG(log, "Created log entry {} to update table metadata to version {}, created a mutation {} (data versions: {})",
                          alter_entry->znode_name, alter_entry->alter_version, *mutation_znode, mutation_entry.getBlockNumbersForLogs());
            }
            else
            {
                /// ALTER_METADATA record in replication /log
                String alter_path = dynamic_cast<const Coordination::CreateResponse &>(*results[alter_path_idx]).path_created;
                alter_entry->znode_name = alter_path.substr(alter_path.find_last_of('/') + 1);
                LOG_DEBUG(log, "Created log entry {} to update table metadata to version {}",
                          alter_entry->znode_name, alter_entry->alter_version);
            }
            break;
        }
        else if (rc == Coordination::Error::ZBADVERSION)
        {
            if (results[0]->error != Coordination::Error::ZOK)
                throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER,
                                "Metadata on replica is not up to date with common metadata in Zookeeper. "
                                "It means that this replica still not applied some of previous alters."
                                " Probably too many alters executing concurrently (highly not recommended). "
                                "You can retry this error");

            /// Cannot retry automatically, because some zookeeper ops were lost on the first attempt. Will retry on DDLWorker-level.
            if (query_context->getZooKeeperMetadataTransaction())
                throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER,
                                "Cannot execute alter, because mutations version was suddenly changed due "
                                "to concurrent alter");

            continue;
        }
        else
        {
            throw Coordination::Exception::fromMessage(rc, "Alter cannot be assigned because of Zookeeper error");
        }
    }

    table_lock_holder.unlock();

    LOG_DEBUG(log, "Updated shared metadata nodes in ZooKeeper. Waiting for replicas to apply changes.");
    waitForLogEntryToBeProcessedIfNecessary(*alter_entry, query_context, "Some replicas doesn't finish metadata alter: ");

    if (mutation_znode)
    {
        LOG_DEBUG(log, "Metadata changes applied. Will wait for data changes.");
        merge_selecting_task->schedule();
        waitMutation(*mutation_znode, query_context->getSettingsRef().alter_sync);
        LOG_DEBUG(log, "Data changes applied.");
    }
}

/// If new version returns ordinary name, else returns part name containing the first and last month of the month
/// NOTE: use it in pair with getFakePartCoveringAllPartsInPartition(...)
String getPartNamePossiblyFake(MergeTreeDataFormatVersion format_version, const MergeTreePartInfo & part_info)
{
    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// The date range is all month long.
        const auto & lut = DateLUT::serverTimezoneInstance();
        time_t start_time = lut.YYYYMMDDToDate(parse<UInt32>(part_info.partition_id + "01"));
        DayNum left_date = DayNum(lut.toDayNum(start_time).toUnderType());
        DayNum right_date = DayNum(static_cast<size_t>(left_date) + lut.daysInMonth(start_time) - 1);
        return part_info.getPartNameV0(left_date, right_date);
    }

    return part_info.getPartNameV1();
}

bool StorageReplicatedMergeTree::getFakePartCoveringAllPartsInPartition(
    const String & partition_id, MergeTreePartInfo & part_info,
    std::optional<EphemeralLockInZooKeeper> & delimiting_block_lock, bool for_replace_range)
{
    /// Even if there is no data in the partition, you still need to mark the range for deletion.
    /// - Because before executing DETACH, tasks for downloading parts to this partition can be executed.
    Int64 left = 0;

    /** Let's skip one number in `block_numbers` for the partition being deleted, and we will only delete parts until this number.
      * This prohibits merges of deleted parts with the new inserted
      * Invariant: merges of deleted parts with other parts do not appear in the log.
      * NOTE: If you need to similarly support a `DROP PART` request, you will have to think of some new mechanism for it,
      *     to guarantee this invariant.
      */
    Int64 right;
    Int64 mutation_version;

    {
        delimiting_block_lock = allocateBlockNumber(partition_id, getZooKeeper());
        right = delimiting_block_lock->getNumber();
        /// Make sure we cover all parts in drop range.
        /// There might be parts with mutation version greater than current block number
        /// if some part mutation has been assigned after block number allocation, but before creation of DROP_RANGE entry.
        mutation_version = MergeTreePartInfo::MAX_BLOCK_NUMBER;
    }

    if (for_replace_range)
    {
        /// NOTE Do not decrement max block number for REPLACE_RANGE, because there are invariants:
        /// - drop range for REPLACE PARTITION must contain at least 2 blocks (1 skipped block and at least 1 real block)
        /// - drop range for MOVE PARTITION/ATTACH PARTITION FROM always contains 1 block

        /// NOTE UINT_MAX was previously used as max level for REPLACE/MOVE PARTITION (it was incorrect)
        part_info = MergeTreePartInfo(partition_id, left, right, MergeTreePartInfo::MAX_LEVEL, mutation_version);
        return right != 0;
    }

    /// Empty partition.
    if (right == 0)
        return false;

    --right;

    /// Artificial high level is chosen, to make this part "covering" all parts inside.
    part_info = MergeTreePartInfo(partition_id, left, right, MergeTreePartInfo::MAX_LEVEL, mutation_version);
    return true;
}

void StorageReplicatedMergeTree::restoreMetadataInZooKeeper()
{
    LOG_INFO(log, "Restoring replica metadata");

    if (!initialization_done)
        throw Exception(ErrorCodes::NOT_INITIALIZED, "Table is not initialized yet");

    if (!is_readonly)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica must be readonly");

    if (getZooKeeper()->exists(replica_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Replica path is present at {} - nothing to restore. "
                        "If you are sure that metadata is lost and that replica path contains some garbage, "
                        "then use SYSTEM DROP REPLICA query first.", replica_path);

    if (has_metadata_in_zookeeper.has_value() && *has_metadata_in_zookeeper)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Replica has metadata in ZooKeeper: "
                                                   "it's either a bug or it's a result of manual intervention to ZooKeeper");

    if (are_restoring_replica.exchange(true))
        throw Exception(ErrorCodes::CONCURRENT_ACCESS_NOT_SUPPORTED, "Replica restoration in progress");
    SCOPE_EXIT({ are_restoring_replica.store(false); });

    auto metadata_snapshot = getInMemoryMetadataPtr();

    waitForOutdatedPartsToBeLoaded();
    const DataPartsVector all_parts = getAllDataPartsVector();
    Strings active_parts_names;

    /// Why all parts (not only Active) are moved to detached/:
    /// After ZK metadata restoration ZK resets sequential counters (including block number counters), so one may
    /// potentially encounter a situation that a part we want to attach already exists.
    for (const auto & part : all_parts)
    {
        if (part->getState() == DataPartState::Active)
            active_parts_names.push_back(part->name);

        forcefullyMovePartToDetachedAndRemoveFromMemory(part);
    }

    LOG_INFO(log, "Moved all parts to detached/");

    const bool is_first_replica = createTableIfNotExists(metadata_snapshot);

    LOG_INFO(log, "Created initial ZK nodes, replica is first: {}", is_first_replica);

    if (!is_first_replica)
        createReplica(metadata_snapshot);

    createNewZooKeeperNodes();

    LOG_INFO(log, "Created ZK nodes for table");

    has_metadata_in_zookeeper = true;

    if (is_first_replica)
        for (const String& part_name : active_parts_names)
            attachPartition(std::make_shared<ASTLiteral>(part_name), metadata_snapshot, true, getContext());

    LOG_INFO(log, "Attached all partitions, starting table");

    startupImpl(/* from_attach_thread */ false);
}

void StorageReplicatedMergeTree::dropPartNoWaitNoThrow(const String & part_name)
{
    assertNotReadonly();
    if (!is_leader)
        throw Exception(ErrorCodes::NOT_A_LEADER, "DROP PART cannot be done on this replica because it is not a leader");

    zkutil::ZooKeeperPtr zookeeper = getZooKeeperAndAssertNotReadonly();
    LogEntry entry;

    dropPartImpl(zookeeper, part_name, entry, /*detach=*/ false, /*throw_if_noop=*/ false);
}

void StorageReplicatedMergeTree::dropPart(const String & part_name, bool detach, ContextPtr query_context)
{
    assertNotReadonly();
    if (!is_leader)
        throw Exception(ErrorCodes::NOT_A_LEADER, "DROP PART cannot be done on this replica because it is not a leader");

    zkutil::ZooKeeperPtr zookeeper = getZooKeeperAndAssertNotReadonly();
    LogEntry entry;

    dropPartImpl(zookeeper, part_name, entry, detach, /*throw_if_noop=*/ true);

    waitForLogEntryToBeProcessedIfNecessary(entry, query_context);
}

void StorageReplicatedMergeTree::dropAllPartitionsImpl(const zkutil::ZooKeeperPtr & zookeeper, bool detach, ContextPtr query_context)
{
    Strings partitions = zookeeper->getChildren(fs::path(zookeeper_path) / "block_numbers");

    std::vector<LogEntryPtr> entries;
    dropAllPartsInPartitions(*zookeeper, partitions, entries, query_context, detach);

    for (const auto & entry : entries)
    {
        waitForLogEntryToBeProcessedIfNecessary(*entry, query_context);
        auto drop_range_info = MergeTreePartInfo::fromPartName(entry->new_part_name, format_version);
        cleanLastPartNode(drop_range_info.partition_id);
    }
}

void StorageReplicatedMergeTree::dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context)
{
    assertNotReadonly();
    if (!is_leader)
        throw Exception(ErrorCodes::NOT_A_LEADER, "DROP PARTITION cannot be done on this replica because it is not a leader");

    zkutil::ZooKeeperPtr zookeeper = getZooKeeperAndAssertNotReadonly();

    const auto * partition_ast = partition->as<ASTPartition>();
    if (partition_ast && partition_ast->all)
    {
        dropAllPartitionsImpl(zookeeper, detach, query_context);
    }
    else
    {
        String partition_id = getPartitionIDFromQuery(partition, query_context);
        auto entry = dropAllPartsInPartition(*zookeeper, partition_id, query_context, detach);
        if (entry)
        {
            waitForLogEntryToBeProcessedIfNecessary(*entry, query_context);
            cleanLastPartNode(partition_id);
        }
    }
}


void StorageReplicatedMergeTree::truncate(
    const ASTPtr &, const StorageMetadataPtr &, ContextPtr query_context, TableExclusiveLockHolder & table_lock)
{
    table_lock.release();   /// Truncate is done asynchronously.

    assertNotReadonly();
    if (!is_leader)
        throw Exception(ErrorCodes::NOT_A_LEADER, "TRUNCATE cannot be done on this replica because it is not a leader");

    waitForOutdatedPartsToBeLoaded();
    zkutil::ZooKeeperPtr zookeeper = getZooKeeperAndAssertNotReadonly();
    dropAllPartitionsImpl(zookeeper, /* detach */ false, query_context);
}


PartitionCommandsResultInfo StorageReplicatedMergeTree::attachPartition(
    const ASTPtr & partition,
    const StorageMetadataPtr & metadata_snapshot,
    bool attach_part,
    ContextPtr query_context)
{
    /// Allow ATTACH PARTITION on readonly replica when restoring it.
    if (!are_restoring_replica)
        assertNotReadonly();

    PartitionCommandsResultInfo results;
    PartsTemporaryRename renamed_parts(*this, "detached/");
    MutableDataPartsVector loaded_parts = tryLoadPartsToAttach(partition, attach_part, query_context, renamed_parts);

    /// TODO Allow to use quorum here.
    ReplicatedMergeTreeSink output(*this, metadata_snapshot, /* quorum */ 0, /* quorum_timeout_ms */ 0, /* max_parts_per_block */ 0,
                                   /* quorum_parallel */ false, query_context->getSettingsRef().insert_deduplicate,
                                   /* majority_quorum */ false, query_context, /*is_attach*/true);

    for (size_t i = 0; i < loaded_parts.size(); ++i)
    {
        const String old_name = loaded_parts[i]->name;

        output.writeExistingPart(loaded_parts[i]);

        renamed_parts.old_and_new_names[i].old_name.clear();

        LOG_DEBUG(log, "Attached part {} as {}", old_name, loaded_parts[i]->name);

        results.push_back(PartitionCommandResultInfo{
            .command_type = "ATTACH PART",
            .partition_id = loaded_parts[i]->info.partition_id,
            .part_name = loaded_parts[i]->name,
            .old_part_name = old_name,
        });
    }
    return results;
}


void StorageReplicatedMergeTree::checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const
{
    auto table_id = getStorageID();
    getContext()->checkTableCanBeDropped(table_id.database_name, table_id.table_name, getTotalActiveSizeInBytes());
}

void StorageReplicatedMergeTree::checkTableCanBeRenamed(const StorageID & new_name) const
{
    if (renaming_restrictions == RenamingRestrictions::ALLOW_ANY)
        return;

    if (renaming_restrictions == RenamingRestrictions::DO_NOT_ALLOW)
    {
        auto old_name = getStorageID();
        bool is_server_startup = Context::getGlobalContextInstance()->getApplicationType() == Context::ApplicationType::SERVER
            && !Context::getGlobalContextInstance()->isServerCompletelyStarted();
        bool move_to_atomic = old_name.uuid == UUIDHelpers::Nil && new_name.uuid != UUIDHelpers::Nil;

        bool likely_converting_ordinary_to_atomic = is_server_startup && move_to_atomic;
        if (likely_converting_ordinary_to_atomic)
        {
            LOG_INFO(log, "Table {} should not be renamed, because zookeeper_path contains implicit 'database' or 'table' macro. "
                          "We cannot rename path in ZooKeeper, so path may become inconsistent with table name. "
                          "However, we allow renaming while converting Ordinary database to Atomic, because all tables will be renamed back",
                          old_name.getNameForLogs());
            return;
        }

        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot rename Replicated table, because zookeeper_path contains implicit 'database' "
                        "or 'table' macro. We cannot rename path "
                        "in ZooKeeper, so path may become inconsistent with table name. "
                        "If you really want to rename table, you should edit metadata file first and restart server or reattach the table.");
    }

    assert(renaming_restrictions == RenamingRestrictions::ALLOW_PRESERVING_UUID);
    if (!new_name.hasUUID() && getStorageID().hasUUID())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot move Replicated table to Ordinary database, because zookeeper_path contains implicit "
                        "'uuid' macro. If you really want to rename table, you should edit metadata file first "
                        "and restart server or reattach the table.");
}

void StorageReplicatedMergeTree::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    checkTableCanBeRenamed(new_table_id);
    MergeTreeData::rename(new_path_to_table_data, new_table_id);

    /// Update table name in zookeeper
    if (!is_readonly)
    {
        /// We don't do it for readonly tables, because it will be updated on next table startup.
        /// It is also Ok to skip ZK error for the same reason.
        try
        {
            auto zookeeper = getZooKeeper();
            zookeeper->set(fs::path(replica_path) / "host", getReplicatedMergeTreeAddress().toString());
        }
        catch (Coordination::Exception & e)
        {
            LOG_WARNING(log, "Cannot update the value of 'host' node (replica address) in ZooKeeper: {}", e.displayText());
        }
    }

    /// TODO: You can update names of loggers.
}


bool StorageReplicatedMergeTree::existsNodeCached(const ZooKeeperWithFaultInjectionPtr & zookeeper, const std::string & path) const
{
    {
        std::lock_guard lock(existing_nodes_cache_mutex);
        if (existing_nodes_cache.contains(path))
            return true;
    }

    bool res = zookeeper->exists(path);

    if (res)
    {
        std::lock_guard lock(existing_nodes_cache_mutex);
        existing_nodes_cache.insert(path);
    }

    return res;
}

std::optional<EphemeralLockInZooKeeper> StorageReplicatedMergeTree::allocateBlockNumber(
    const String & partition_id,
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & zookeeper_block_id_path,
    const String & zookeeper_path_prefix) const
{
    return allocateBlockNumber(
        partition_id, std::make_shared<ZooKeeperWithFaultInjection>(zookeeper), zookeeper_block_id_path, zookeeper_path_prefix);
}

template<typename T>
std::optional<EphemeralLockInZooKeeper> StorageReplicatedMergeTree::allocateBlockNumber(
    const String & partition_id,
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    const T & zookeeper_block_id_path,
    const String & zookeeper_path_prefix) const
{
    String zookeeper_table_path;
    if (zookeeper_path_prefix.empty())
        zookeeper_table_path = zookeeper_path;
    else
        zookeeper_table_path = zookeeper_path_prefix;

    String block_numbers_path = fs::path(zookeeper_table_path) / "block_numbers";
    String partition_path = fs::path(block_numbers_path) / partition_id;

    if (!existsNodeCached(zookeeper, partition_path))
    {
        Coordination::Requests ops;
        /// Check that table is not being dropped ("host" is the first node that is removed on replica drop)
        ops.push_back(zkutil::makeCheckRequest(fs::path(replica_path) / "host", -1));
        ops.push_back(zkutil::makeCreateRequest(partition_path, "", zkutil::CreateMode::Persistent));
        /// We increment data version of the block_numbers node so that it becomes possible
        /// to check in a ZK transaction that the set of partitions didn't change
        /// (unfortunately there is no CheckChildren op).
        ops.push_back(zkutil::makeSetRequest(block_numbers_path, "", -1));

        Coordination::Responses responses;
        Coordination::Error code = zookeeper->tryMulti(ops, responses);
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
            zkutil::KeeperMultiException::check(code, ops, responses);
    }

    return createEphemeralLockInZooKeeper(
        fs::path(partition_path) / "block-", fs::path(zookeeper_table_path) / "temp", zookeeper, zookeeper_block_id_path);
}

Strings StorageReplicatedMergeTree::tryWaitForAllReplicasToProcessLogEntry(
    const String & table_zookeeper_path, const ReplicatedMergeTreeLogEntryData & entry, Int64 wait_for_inactive_timeout)
{
    LOG_DEBUG(log, "Waiting for all replicas to process {}", entry.znode_name);

    auto zookeeper = getZooKeeper();
    Strings replicas = zookeeper->getChildren(fs::path(table_zookeeper_path) / "replicas");
    Strings unwaited;
    bool wait_for_inactive = wait_for_inactive_timeout != 0;
    for (const String & replica : replicas)
    {
        if (wait_for_inactive || zookeeper->exists(fs::path(table_zookeeper_path) / "replicas" / replica / "is_active"))
        {
            if (!tryWaitForReplicaToProcessLogEntry(table_zookeeper_path, replica, entry, wait_for_inactive_timeout))
                unwaited.push_back(replica);
        }
        else
        {
            unwaited.push_back(replica);
        }
    }

    LOG_DEBUG(log, "Finished waiting for all replicas to process {}", entry.znode_name);
    return unwaited;
}

void StorageReplicatedMergeTree::waitForAllReplicasToProcessLogEntry(
    const String & table_zookeeper_path, const ReplicatedMergeTreeLogEntryData & entry, Int64 wait_for_inactive_timeout, const String & error_context)
{
    Strings unfinished_replicas = tryWaitForAllReplicasToProcessLogEntry(table_zookeeper_path, entry, wait_for_inactive_timeout);
    if (unfinished_replicas.empty())
        return;

    throw Exception(ErrorCodes::UNFINISHED, "{}Timeout exceeded while waiting for replicas {} to process entry {}. "
                    "Probably some replicas are inactive", error_context, fmt::join(unfinished_replicas, ", "), entry.znode_name);
}

void StorageReplicatedMergeTree::waitForLogEntryToBeProcessedIfNecessary(const ReplicatedMergeTreeLogEntryData & entry, ContextPtr query_context, const String & error_context)
{
    /// If necessary, wait until the operation is performed on itself or on all replicas.
    Int64 wait_for_inactive_timeout = query_context->getSettingsRef().replication_wait_for_inactive_replica_timeout;
    if (query_context->getSettingsRef().alter_sync == 1)
    {
        bool finished = tryWaitForReplicaToProcessLogEntry(zookeeper_path, replica_name, entry, wait_for_inactive_timeout);
        if (!finished)
        {
            throw Exception(ErrorCodes::UNFINISHED, "{}Log entry {} is not precessed on local replica, "
                            "most likely because the replica was shut down.", error_context, entry.znode_name);
        }
    }
    else if (query_context->getSettingsRef().alter_sync == 2)
    {
        waitForAllReplicasToProcessLogEntry(zookeeper_path, entry, wait_for_inactive_timeout, error_context);
    }
}

bool StorageReplicatedMergeTree::tryWaitForReplicaToProcessLogEntry(
    const String & table_zookeeper_path, const String & replica, const ReplicatedMergeTreeLogEntryData & entry, Int64 wait_for_inactive_timeout)
{
    String entry_str = entry.toString();
    String log_node_name;

    /** Wait for entries from `log` directory (a common log, from where replicas copy entries to their queue) to be processed.
      *
      * The problem is that the numbers (`sequential` node) of the queue elements in `log` and in `queue` do not match.
      * (And the numbers of the same log element for different replicas do not match in the `queue`.)
      */

    /** First, you need to wait until replica takes `queue` element from the `log` to its queue,
      *  if it has not been done already (see the `pullLogsToQueue` function).
      *
      * To do this, check its node `log_pointer` - the maximum number of the element taken from `log` + 1.
      */

    bool waiting_itself = replica == replica_name;
    /// Do not wait if timeout is zero
    bool wait_for_inactive = wait_for_inactive_timeout != 0;
    /// Wait for unlimited time if timeout is negative
    bool check_timeout = wait_for_inactive_timeout > 0;
    Stopwatch time_waiting;

    const auto & stop_waiting = [&]()
    {
        bool stop_waiting_itself = waiting_itself && (partial_shutdown_called || shutdown_prepared_called || shutdown_called);
        bool timeout_exceeded = check_timeout && wait_for_inactive_timeout < time_waiting.elapsedSeconds();
        bool stop_waiting_inactive = (!wait_for_inactive || timeout_exceeded)
            && !getZooKeeper()->exists(fs::path(table_zookeeper_path) / "replicas" / replica / "is_active");
        return is_dropped || stop_waiting_itself || stop_waiting_inactive;
    };

    /// Don't recheck ZooKeeper too often
    constexpr auto event_wait_timeout_ms = 3000;

    LOG_DEBUG(log, "Waiting for {} to process log entry", replica);

    if (startsWith(entry.znode_name, "log-"))
    {
        /// Take the number from the node name `log-xxxxxxxxxx`.
        UInt64 log_index = parse<UInt64>(entry.znode_name.substr(entry.znode_name.size() - 10));
        log_node_name = entry.znode_name;

        LOG_DEBUG(log, "Waiting for {} to pull {} to queue", replica, log_node_name);

        /// Let's wait until entry gets into the replica queue.
        bool pulled_to_queue = false;
        do
        {
            zkutil::EventPtr event = std::make_shared<Poco::Event>();

            String log_pointer = getZooKeeper()->get(fs::path(table_zookeeper_path) / "replicas" / replica / "log_pointer", nullptr, event);
            if (!log_pointer.empty() && parse<UInt64>(log_pointer) > log_index)
            {
                pulled_to_queue = true;
                break;
            }

            /// Wait with timeout because we can be already shut down, but not dropped.
            /// So log_pointer node will exist, but we will never update it because all background threads already stopped.
            /// It can lead to query hung because table drop query can wait for some query (alter, optimize, etc) which called this method,
            /// but the query will never finish because the drop already shut down the table.
            if (!stop_waiting())
                event->tryWait(event_wait_timeout_ms);
        } while (!stop_waiting());

        if (!pulled_to_queue)
            return false;

        LOG_DEBUG(log, "Looking for node corresponding to {} in {} queue", log_node_name, replica);
    }
    else if (!entry.log_entry_id.empty())
    {
        /// First pass, check the table log.
        /// If found in the log, wait for replica to fetch it to the queue.
        /// If not found in the log, it is already in the queue.
        LOG_DEBUG(log, "Looking for log entry with id `{}` in the log", entry.log_entry_id);

        String log_pointer = getZooKeeper()->get(fs::path(table_zookeeper_path) / "replicas" / replica / "log_pointer");

        Strings log_entries = getZooKeeper()->getChildren(fs::path(table_zookeeper_path) / "log");
        UInt64 log_index = 0;
        bool found = false;

        for (const String & log_entry_name : log_entries)
        {
            log_index = parse<UInt64>(log_entry_name.substr(log_entry_name.size() - 10));

            if (!log_pointer.empty() && log_index < parse<UInt64>(log_pointer))
                continue;

            String log_entry_str;
            Coordination::Stat log_entry_stat;
            bool exists = getZooKeeper()->tryGet(fs::path(table_zookeeper_path) / "log" / log_entry_name, log_entry_str, &log_entry_stat);
            ReplicatedMergeTreeLogEntryData log_entry = *ReplicatedMergeTreeLogEntry::parse(log_entry_str, log_entry_stat, format_version);
            if (exists && entry.log_entry_id == log_entry.log_entry_id)
            {
                LOG_DEBUG(log, "Found log entry with id `{}` in the log", entry.log_entry_id);

                found = true;
                log_node_name = log_entry_name;
                break;
            }
        }

        if (found)
        {
            LOG_DEBUG(log, "Waiting for {} to pull {} to queue", replica, log_node_name);

            /// Let's wait until entry gets into the replica queue.
            bool pulled_to_queue = false;
            do
            {
                zkutil::EventPtr event = std::make_shared<Poco::Event>();

                log_pointer = getZooKeeper()->get(fs::path(table_zookeeper_path) / "replicas" / replica / "log_pointer", nullptr, event);
                if (!log_pointer.empty() && parse<UInt64>(log_pointer) > log_index)
                {
                    pulled_to_queue = true;
                    break;
                }

                /// Wait with timeout because we can be already shut down, but not dropped.
                /// So log_pointer node will exist, but we will never update it because all background threads already stopped.
                /// It can lead to query hung because table drop query can wait for some query (alter, optimize, etc) which called this method,
                /// but the query will never finish because the drop already shut down the table.
                if (!stop_waiting())
                    event->tryWait(event_wait_timeout_ms);
            } while (!stop_waiting());

            if (!pulled_to_queue)
                return false;
        }
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: unexpected name of log node: {}", entry.znode_name);
    }

    /** Second - find the corresponding entry in the queue of the specified replica.
      * Its number may not match the `log` node. Therefore, we search by comparing the content.
      */

    Strings queue_entries = getZooKeeper()->getChildren(fs::path(table_zookeeper_path) / "replicas" / replica / "queue");
    String queue_entry_to_wait_for;

    for (const String & entry_name : queue_entries)
    {
        String queue_entry_str;
        Coordination::Stat queue_entry_stat;
        bool exists = getZooKeeper()->tryGet(fs::path(table_zookeeper_path) / "replicas" / replica / "queue" / entry_name, queue_entry_str, &queue_entry_stat);
        if (exists && queue_entry_str == entry_str)
        {
            queue_entry_to_wait_for = entry_name;
            break;
        }
        else if (!entry.log_entry_id.empty())
        {
            /// Check if the id matches rather than just contents. This entry
            /// might have been written by different ClickHouse versions and
            /// it is hard to guarantee same text representation.
            ReplicatedMergeTreeLogEntryData queue_entry = *ReplicatedMergeTreeLogEntry::parse(queue_entry_str, queue_entry_stat, format_version);
            if (entry.log_entry_id == queue_entry.log_entry_id)
            {
                queue_entry_to_wait_for = entry_name;
                break;
            }
        }
    }

    /// While looking for the record, it has already been executed and deleted.
    if (queue_entry_to_wait_for.empty())
    {
        LOG_DEBUG(log, "No corresponding node found. Assuming it has been already processed. Found {} nodes", queue_entries.size());
        return true;
    }

    LOG_DEBUG(log, "Waiting for {} to disappear from {} queue", queue_entry_to_wait_for, replica);

    /// Third - wait until the entry disappears from the replica queue or replica become inactive.
    String path_to_wait_on = fs::path(table_zookeeper_path) / "replicas" / replica / "queue" / queue_entry_to_wait_for;

    return getZooKeeper()->waitForDisappear(path_to_wait_on, stop_waiting);
}


void StorageReplicatedMergeTree::getStatus(ReplicatedTableStatus & res, bool with_zk_fields)
{
    auto zookeeper = tryGetZooKeeper();
    const auto storage_settings_ptr = getSettings();

    res.is_leader = is_leader;
    res.can_become_leader = storage_settings_ptr->replicated_can_become_leader;
    res.is_readonly = is_readonly;
    res.is_session_expired = !zookeeper || zookeeper->expired();

    res.queue = queue.getStatus();
    res.absolute_delay = getAbsoluteDelay(); /// NOTE: may be slightly inconsistent with queue status.

    /// NOTE: consider convert to UInt64
    res.parts_to_check = static_cast<UInt32>(part_check_thread.size());

    res.zookeeper_name = zookeeper_name;
    res.zookeeper_path = zookeeper_path;
    res.replica_name = replica_name;
    res.replica_path = replica_path;
    res.columns_version = -1;

    res.log_max_index = 0;
    res.log_pointer = 0;
    res.total_replicas = 0;
    res.active_replicas = 0;
    res.lost_part_count = 0;
    res.last_queue_update_exception = getLastQueueUpdateException();

    if (with_zk_fields && !res.is_session_expired)
    {
        try
        {
            std::vector<std::string> paths;
            paths.push_back(fs::path(zookeeper_path) / "log");
            paths.push_back(fs::path(zookeeper_path) / "replicas");

            auto children_result = zookeeper->getChildren(paths);
            const auto & log_entries = children_result[0].names;
            const auto & all_replicas = children_result[1].names;

            paths.clear();
            paths.push_back(fs::path(replica_path) / "log_pointer");
            paths.push_back(fs::path(zookeeper_path) / "lost_part_count");
            for (const String & replica : all_replicas)
                paths.push_back(fs::path(zookeeper_path) / "replicas" / replica / "is_active");

            auto get_result = zookeeper->tryGet(paths);
            const auto & log_pointer_str = get_result[0].data;

            if (get_result[0].error == Coordination::Error::ZNONODE)
                throw zkutil::KeeperException(get_result[0].error);

            if (!log_entries.empty())
            {
                const String & last_log_entry = *std::max_element(log_entries.begin(), log_entries.end());
                res.log_max_index = parse<UInt64>(last_log_entry.substr(strlen("log-")));
            }

            res.log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);
            res.total_replicas = all_replicas.size();
            if (get_result[1].error == Coordination::Error::ZNONODE)
                res.lost_part_count = 0;
            else
                res.lost_part_count = get_result[1].data.empty() ? 0 : parse<UInt64>(get_result[1].data);

            for (size_t i = 0, size = all_replicas.size(); i < size; ++i)
            {
                bool is_replica_active = get_result[i + 2].error != Coordination::Error::ZNONODE;
                res.active_replicas += static_cast<UInt8>(is_replica_active);
                res.replica_is_active.emplace(all_replicas[i], is_replica_active);
            }
        }
        catch (const Coordination::Exception &)
        {
            res.zookeeper_exception = getCurrentExceptionMessage(false);
        }
    }
}


void StorageReplicatedMergeTree::getQueue(LogEntriesData & res, String & replica_name_)
{
    replica_name_ = replica_name;
    queue.getEntries(res);
}

std::vector<PartMovesBetweenShardsOrchestrator::Entry> StorageReplicatedMergeTree::getPartMovesBetweenShardsEntries()
{
    return part_moves_between_shards_orchestrator.getEntries();
}

time_t StorageReplicatedMergeTree::getAbsoluteDelay() const
{
    time_t min_unprocessed_insert_time = 0;
    time_t max_processed_insert_time = 0;
    queue.getInsertTimes(min_unprocessed_insert_time, max_processed_insert_time);

    /// Load start time, then finish time to avoid reporting false delay when start time is updated
    /// between loading of two variables.
    time_t queue_update_start_time = last_queue_update_start_time.load();
    time_t queue_update_finish_time = last_queue_update_finish_time.load();

    time_t current_time = time(nullptr);

    if (!queue_update_finish_time)
    {
        /// We have not updated queue even once yet (perhaps replica is readonly).
        /// As we have no info about the current state of replication log, return effectively infinite delay.
        return current_time;
    }
    else if (min_unprocessed_insert_time)
    {
        /// There are some unprocessed insert entries in queue.
        return (current_time > min_unprocessed_insert_time) ? (current_time - min_unprocessed_insert_time) : 0;
    }
    else if (queue_update_start_time > queue_update_finish_time)
    {
        /// Queue is empty, but there are some in-flight or failed queue update attempts
        /// (likely because of problems with connecting to ZooKeeper).
        /// Return the time passed since last attempt.
        return (current_time > queue_update_start_time) ? (current_time - queue_update_start_time) : 0;
    }
    else
    {
        /// Everything is up-to-date.
        return 0;
    }
}

void StorageReplicatedMergeTree::getReplicaDelays(time_t & out_absolute_delay, time_t & out_relative_delay)
{
    assertNotReadonly();

    time_t current_time = time(nullptr);

    out_absolute_delay = getAbsoluteDelay();
    out_relative_delay = 0;
    const auto storage_settings_ptr = getSettings();

    /** Relative delay is the maximum difference of absolute delay from any other replica,
      *  (if this replica lags behind any other live replica, or zero, otherwise).
      * Calculated only if the absolute delay is large enough.
      */

    if (out_absolute_delay < static_cast<time_t>(storage_settings_ptr->min_relative_delay_to_measure))
        return;

    auto zookeeper = getZooKeeper();

    time_t max_replicas_unprocessed_insert_time = 0;
    bool have_replica_with_nothing_unprocessed = false;

    Strings replicas = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas");

    for (const auto & replica : replicas)
    {
        if (replica == replica_name)
            continue;

        /// Skip dead replicas.
        if (!zookeeper->exists(fs::path(zookeeper_path) / "replicas" / replica / "is_active"))
            continue;

        String value;
        if (!zookeeper->tryGet(fs::path(zookeeper_path) / "replicas" / replica / "min_unprocessed_insert_time", value))
            continue;

        time_t replica_time = value.empty() ? 0 : parse<time_t>(value);

        if (replica_time == 0)
        {
            /** Note
              * The conclusion that the replica does not lag may be incorrect,
              *  because the information about `min_unprocessed_insert_time` is taken
              *  only from that part of the log that has been moved to the queue.
              * If the replica for some reason has stalled `queueUpdatingTask`,
              *  then `min_unprocessed_insert_time` will be incorrect.
              */

            have_replica_with_nothing_unprocessed = true;
            break;
        }

        if (replica_time > max_replicas_unprocessed_insert_time)
            max_replicas_unprocessed_insert_time = replica_time;
    }

    if (have_replica_with_nothing_unprocessed)
        out_relative_delay = out_absolute_delay;
    else
    {
        max_replicas_unprocessed_insert_time = std::min(current_time, max_replicas_unprocessed_insert_time);
        time_t min_replicas_delay = current_time - max_replicas_unprocessed_insert_time;
        if (out_absolute_delay > min_replicas_delay)
            out_relative_delay = out_absolute_delay - min_replicas_delay;
    }
}

void StorageReplicatedMergeTree::fetchPartition(
    const ASTPtr & partition,
    const StorageMetadataPtr & metadata_snapshot,
    const String & from_,
    bool fetch_part,
    ContextPtr query_context)
{
    Macros::MacroExpansionInfo info;
    info.expand_special_macros_only = false;
    info.table_id = getStorageID();
    info.table_id.uuid = UUIDHelpers::Nil;
    auto expand_from = query_context->getMacros()->expand(from_, info);
    String from_zookeeper_name = zkutil::extractZooKeeperName(expand_from);
    String from = zkutil::extractZooKeeperPath(expand_from, /* check_starts_with_slash */ true);
    if (from.empty())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "ZooKeeper path should not be empty");

    zkutil::ZooKeeperPtr zookeeper;
    if (from_zookeeper_name != default_zookeeper_name)
        zookeeper = getContext()->getAuxiliaryZooKeeper(from_zookeeper_name);
    else
        zookeeper = getZooKeeper();

    if (from.back() == '/')
        from.resize(from.size() - 1);

    if (fetch_part)
    {
        String part_name = partition->as<ASTLiteral &>().value.safeGet<String>();
        auto part_path = findReplicaHavingPart(part_name, from, zookeeper);

        if (part_path.empty())
            throw Exception(ErrorCodes::NO_REPLICA_HAS_PART, "Part {} does not exist on any replica", part_name);
        /** Let's check that there is no such part in the `detached` directory (where we will write the downloaded parts).
          * Unreliable (there is a race condition) - such a part may appear a little later.
          */
        if (checkIfDetachedPartExists(part_name))
            throw Exception(ErrorCodes::DUPLICATE_DATA_PART, "Detached part {} already exists.", part_name);
        LOG_INFO(log, "Will fetch part {} from shard {}", part_name, from_);

        try
        {
            /// part name, metadata, part_path, true, 0, zookeeper
            if (!fetchPart(part_name, metadata_snapshot, from_zookeeper_name, part_path, true, 0, zookeeper, /* try_fetch_shared = */ false))
                throw Exception(ErrorCodes::UNFINISHED, "Failed to fetch part {} from {}", part_name, from_);
        }
        catch (const DB::Exception & e)
        {
            if (e.code() != ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER && e.code() != ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
                && e.code() != ErrorCodes::CANNOT_READ_ALL_DATA)
                throw;

            LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
        }
        return;
    }

    String partition_id = getPartitionIDFromQuery(partition, query_context);
    LOG_INFO(log, "Will fetch partition {} from shard {}", partition_id, from_);

    /** Let's check that there is no such partition in the `detached` directory (where we will write the downloaded parts).
      * Unreliable (there is a race condition) - such a partition may appear a little later.
      */
    if (checkIfDetachedPartitionExists(partition_id))
        throw Exception(ErrorCodes::PARTITION_ALREADY_EXISTS, "Detached partition {} already exists.", partition_id);

    zkutil::Strings replicas;
    zkutil::Strings active_replicas;
    String best_replica;

    {
        /// List of replicas of source shard.
        replicas = zookeeper->getChildren(fs::path(from) / "replicas");

        /// Leave only active replicas.
        active_replicas.reserve(replicas.size());

        for (const String & replica : replicas)
            if (zookeeper->exists(fs::path(from) / "replicas" / replica / "is_active"))
                active_replicas.push_back(replica);

        if (active_replicas.empty())
            throw Exception(ErrorCodes::NO_ACTIVE_REPLICAS, "No active replicas for shard {}", from_);

        /** You must select the best (most relevant) replica.
        * This is a replica with the maximum `log_pointer`, then with the minimum `queue` size.
        * NOTE This is not exactly the best criteria. It does not make sense to download old partitions,
        *  and it would be nice to be able to choose the replica closest by network.
        * NOTE Of course, there are data races here. You can solve it by retrying.
        */
        Int64 max_log_pointer = -1;
        UInt64 min_queue_size = std::numeric_limits<UInt64>::max();

        for (const String & replica : active_replicas)
        {
            String current_replica_path = fs::path(from) / "replicas" / replica;

            String log_pointer_str = zookeeper->get(fs::path(current_replica_path) / "log_pointer");
            Int64 log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);

            Coordination::Stat stat;
            zookeeper->get(fs::path(current_replica_path) / "queue", &stat);
            size_t queue_size = stat.numChildren;

            if (log_pointer > max_log_pointer
                || (log_pointer == max_log_pointer && queue_size < min_queue_size))
            {
                max_log_pointer = log_pointer;
                min_queue_size = queue_size;
                best_replica = replica;
            }
        }
    }

    if (best_replica.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: cannot choose best replica.");

    LOG_INFO(log, "Found {} replicas, {} of them are active. Selected {} to fetch from.", replicas.size(), active_replicas.size(), best_replica);

    String best_replica_path = fs::path(from) / "replicas" / best_replica;

    /// Let's find out which parts are on the best replica.

    /** Trying to download these parts.
      * Some of them could be deleted due to the merge.
      * In this case, update the information about the available parts and try again.
      */

    unsigned try_no = 0;
    Strings missing_parts;
    do
    {
        if (try_no)
            LOG_INFO(log, "Some of parts ({}) are missing. Will try to fetch covering parts.", missing_parts.size());

        if (try_no >= query_context->getSettings().max_fetch_partition_retries_count)
            throw Exception(ErrorCodes::TOO_MANY_RETRIES_TO_FETCH_PARTS,
                "Too many retries to fetch parts from {}:{}", from_zookeeper_name, best_replica_path);

        Strings parts = zookeeper->getChildren(fs::path(best_replica_path) / "parts");
        ActiveDataPartSet active_parts_set(format_version, parts);
        Strings parts_to_fetch;

        if (missing_parts.empty())
        {
            parts_to_fetch = active_parts_set.getParts();

            /// Leaving only the parts of the desired partition.
            Strings parts_to_fetch_partition;
            for (const String & part : parts_to_fetch)
            {
                if (MergeTreePartInfo::fromPartName(part, format_version).partition_id == partition_id)
                    parts_to_fetch_partition.push_back(part);
            }

            parts_to_fetch = std::move(parts_to_fetch_partition);

            if (parts_to_fetch.empty())
                throw Exception(ErrorCodes::PARTITION_DOESNT_EXIST,
                    "Partition {} on {}:{} doesn't exist", partition_id, from_zookeeper_name, best_replica_path);
        }
        else
        {
            for (const String & missing_part : missing_parts)
            {
                String containing_part = active_parts_set.getContainingPart(missing_part);
                if (!containing_part.empty())
                    parts_to_fetch.push_back(containing_part);
                else
                    LOG_WARNING(log, "Part {} on replica {}:{} has been vanished.", missing_part, from_zookeeper_name, best_replica_path);
            }
        }

        LOG_INFO(log, "Parts to fetch: {}", parts_to_fetch.size());

        missing_parts.clear();
        for (const String & part : parts_to_fetch)
        {
            bool fetched = false;

            try
            {
                fetched = fetchPart(part, metadata_snapshot, from_zookeeper_name, best_replica_path, true, 0, zookeeper, /* try_fetch_shared = */ false);
            }
            catch (const DB::Exception & e)
            {
                if (e.code() != ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER && e.code() != ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
                    && e.code() != ErrorCodes::CANNOT_READ_ALL_DATA)
                    throw;

                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
            }

            if (!fetched)
                missing_parts.push_back(part);
        }

        ++try_no;
    } while (!missing_parts.empty());
}


void StorageReplicatedMergeTree::mutate(const MutationCommands & commands, ContextPtr query_context)
{
    /// Overview of the mutation algorithm.
    ///
    /// When the client executes a mutation, this method is called. It acquires block numbers in all
    /// partitions, saves them in the mutation entry and writes the mutation entry to a new ZK node in
    /// the /mutations folder. This block numbers are needed to determine which parts should be mutated and
    /// which shouldn't (parts inserted after the mutation will have the block number higher than the
    /// block number acquired by the mutation in that partition and so will not be mutated).
    /// This block number is called "mutation version" in that partition.
    ///
    /// Mutation versions are acquired atomically in all partitions, so the case when an insert in some
    /// partition has the block number higher than the mutation version but the following insert into another
    /// partition acquires the block number lower than the mutation version in that partition is impossible.
    /// Another important invariant: mutation entries appear in /mutations in the order of their mutation
    /// versions (in any partition). This means that mutations form a sequence and we can execute them in
    /// the order of their mutation versions and not worry that some mutation with the smaller version
    /// will suddenly appear.
    ///
    /// During mutations individual parts are immutable - when we want to change the contents of a part
    /// we prepare the new part and add it to MergeTreeData (the original part gets replaced). The fact that
    /// we have mutated the part is recorded in the part->info.mutation field of MergeTreePartInfo.
    /// The relation with the original part is preserved because the new part covers the same block range
    /// as the original one.
    ///
    /// We then can for each part determine its "mutation version": the version of the last mutation in
    /// the mutation sequence that we regard as already applied to that part. All mutations with the greater
    /// version number will still need to be applied to that part.
    ///
    /// Execution of mutations is done asynchronously. All replicas watch the /mutations directory and
    /// load new mutation entries as they appear (see mutationsUpdatingTask()). Next we need to determine
    /// how to mutate individual parts consistently with part merges. This is done by the leader replica
    /// (see mergeSelectingTask() and class ReplicatedMergeTreeMergePredicate for details). Important
    /// invariants here are that a) all source parts for a single merge must have the same mutation version
    /// and b) any part can be mutated only once or merged only once (e.g. once we have decided to mutate
    /// a part then we need to execute that mutation and can assign merges only to the new part and not to the
    /// original part). Multiple consecutive mutations can be executed at once (without writing the
    /// intermediate result to a part).
    ///
    /// Leader replica records its decisions to the replication log (/log directory in ZK) in the form of
    /// MUTATE_PART entries and all replicas then execute them in the background pool
    /// (see MutateTask class). When a replica encounters a MUTATE_PART command, it is
    /// guaranteed that the corresponding mutation entry is already loaded (when we pull entries from
    /// replication log into the replica queue, we also load mutation entries). Note that just as with merges
    /// the replica can decide not to do the mutation locally and fetch the mutated part from another replica
    /// instead.
    ///
    /// Mutations of individual parts are in fact pretty similar to merges, e.g. their assignment and execution
    /// is governed by the same storage_settings. TODO: support a single "merge-mutation" operation when the data
    /// read from the the source parts is first mutated on the fly to some uniform mutation version and then
    /// merged to a resulting part.
    ///
    /// After all needed parts are mutated (i.e. all active parts have the mutation version greater than
    /// the version of this mutation), the mutation is considered done and can be deleted.

    delayMutationOrThrowIfNeeded(&partial_shutdown_event, query_context);

    ReplicatedMergeTreeMutationEntry mutation_entry;
    mutation_entry.source_replica = replica_name;
    mutation_entry.commands = commands;

    const String mutations_path = fs::path(zookeeper_path) / "mutations";
    const auto zookeeper = getZooKeeper();

    /// Update the mutations_path node when creating the mutation and check its version to ensure that
    /// nodes for mutations are created in the same order as the corresponding block numbers.
    /// Should work well if the number of concurrent mutation requests is small.
    while (true)
    {
        Coordination::Stat mutations_stat;
        zookeeper->get(mutations_path, &mutations_stat);

        PartitionBlockNumbersHolder partition_block_numbers_holder =
                allocateBlockNumbersInAffectedPartitions(mutation_entry.commands, query_context, zookeeper);

        mutation_entry.block_numbers = partition_block_numbers_holder.getBlockNumbers();
        mutation_entry.create_time = time(nullptr);

        /// The following version check guarantees the linearizability property for any pair of mutations:
        /// mutation with higher sequence number is guaranteed to have higher block numbers in every partition
        /// (and thus will be applied strictly according to sequence numbers of mutations)
        Coordination::Requests requests;
        requests.emplace_back(zkutil::makeSetRequest(mutations_path, String(), mutations_stat.version));
        requests.emplace_back(zkutil::makeCreateRequest(
            fs::path(mutations_path) / "", mutation_entry.toString(), zkutil::CreateMode::PersistentSequential));

        if (auto txn = query_context->getZooKeeperMetadataTransaction())
            txn->moveOpsTo(requests);

        Coordination::Responses responses;
        Coordination::Error rc = zookeeper->tryMulti(requests, responses);

        partition_block_numbers_holder.reset();

        if (rc == Coordination::Error::ZOK)
        {
            const String & path_created =
                dynamic_cast<const Coordination::CreateResponse *>(responses[1].get())->path_created;
            mutation_entry.znode_name = path_created.substr(path_created.find_last_of('/') + 1);
            LOG_TRACE(log, "Created mutation with ID {} (data versions: {})",
                      mutation_entry.znode_name, mutation_entry.getBlockNumbersForLogs());
            break;
        }
        else if (rc == Coordination::Error::ZBADVERSION)
        {
            /// Cannot retry automatically, because some zookeeper ops were lost on the first attempt. Will retry on DDLWorker-level.
            if (query_context->getZooKeeperMetadataTransaction())
                throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER,
                                "Cannot execute alter, because mutations version was suddenly changed due "
                                "to concurrent alter");
            LOG_TRACE(log, "Version conflict when trying to create a mutation node, retrying...");
            continue;
        }
        else
            throw Coordination::Exception::fromMessage(rc, "Unable to create a mutation znode");
    }

    merge_selecting_task->schedule();

    waitMutation(mutation_entry.znode_name, query_context->getSettingsRef().mutations_sync);
}

void StorageReplicatedMergeTree::waitMutation(const String & znode_name, size_t mutations_sync) const
{
    if (!mutations_sync)
        return;

    /// we have to wait
    auto zookeeper = getZooKeeper();
    Strings replicas;
    if (mutations_sync == 2) /// wait for all replicas
    {
        replicas = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas");
        /// This replica should be first, to ensure that the mutation will be loaded into memory
        for (auto it = replicas.begin(); it != replicas.end(); ++it)
        {
            if (*it == replica_name)
            {
                std::iter_swap(it, replicas.begin());
                break;
            }
        }
    }
    else if (mutations_sync == 1) /// just wait for ourself
        replicas.push_back(replica_name);

    waitMutationToFinishOnReplicas(replicas, znode_name);
}

std::vector<MergeTreeMutationStatus> StorageReplicatedMergeTree::getMutationsStatus() const
{
    return queue.getMutationsStatus();
}

CancellationCode StorageReplicatedMergeTree::killMutation(const String & mutation_id)
{
    assertNotReadonly();

    zkutil::ZooKeeperPtr zookeeper = getZooKeeperAndAssertNotReadonly();

    LOG_INFO(log, "Killing mutation {}", mutation_id);

    auto mutation_entry = queue.removeMutation(zookeeper, mutation_id);
    if (!mutation_entry)
        return CancellationCode::NotFound;

    /// After this point no new part mutations will start and part mutations that still exist
    /// in the queue will be skipped.

    /// Cancel already running part mutations.
    for (const auto & pair : mutation_entry->block_numbers)
    {
        const String & partition_id = pair.first;
        Int64 block_number = pair.second;
        getContext()->getMergeList().cancelPartMutations(getStorageID(), partition_id, block_number);
    }
    return CancellationCode::CancelSent;
}

bool StorageReplicatedMergeTree::hasLightweightDeletedMask() const
{
    return has_lightweight_delete_parts.load(std::memory_order_relaxed);
}

size_t StorageReplicatedMergeTree::clearOldPartsAndRemoveFromZK()
{
    auto table_lock = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);
    auto zookeeper = getZooKeeper();

    /// Now these parts are in Deleting state. If we fail to remove some of them we must roll them back to Outdated state.
    /// Otherwise they will not be deleted.
    DataPartsVector parts = grabOldParts();
    size_t total_parts_to_remove = parts.size();
    if (parts.empty())
        return total_parts_to_remove;

    NOEXCEPT_SCOPE({ clearOldPartsAndRemoveFromZKImpl(zookeeper, std::move(parts)); });
    return total_parts_to_remove;
}

void StorageReplicatedMergeTree::clearOldPartsAndRemoveFromZKImpl(zkutil::ZooKeeperPtr zookeeper, DataPartsVector && parts)
{
    DataPartsVector parts_to_delete_only_from_filesystem;    // Only duplicates
    DataPartsVector parts_to_delete_completely;              // All parts except duplicates
    DataPartsVector parts_to_retry_deletion;                 // Parts that should be retried due to network problems
    DataPartsVector parts_to_remove_from_filesystem;         // Parts removed from ZK

    for (const auto & part : parts)
    {
        /// Broken part can be removed from zk by removePartAndEnqueueFetch(...) only.
        /// Removal without enqueueing a fetch leads to intersecting parts.
        if (part->is_duplicate || part->is_unexpected_local_part)
        {
            LOG_WARNING(log, "Will not remove part {} from ZooKeeper (is_duplicate: {}, is_unexpected_local_part: {})",
                        part->name, part->is_duplicate, part->is_unexpected_local_part);
            parts_to_delete_only_from_filesystem.emplace_back(part);
        }
        else
            parts_to_delete_completely.emplace_back(part);
    }
    parts.clear();

    auto delete_parts_from_fs_and_rollback_in_case_of_error = [this] (const DataPartsVector & parts_to_delete, const String & parts_type)
    {
        NameSet parts_failed_to_delete;
        clearPartsFromFilesystem(parts_to_delete, false, &parts_failed_to_delete);

        DataPartsVector finally_remove_parts;
        if (!parts_failed_to_delete.empty())
        {
            DataPartsVector rollback_parts;
            for (const auto & part : parts_to_delete)
            {
                if (!parts_failed_to_delete.contains(part->name))
                    finally_remove_parts.push_back(part);
                else
                    rollback_parts.push_back(part);
            }

            if (!rollback_parts.empty())
                rollbackDeletingParts(rollback_parts);
        }
        else  /// all parts were successfully removed
        {
            finally_remove_parts = parts_to_delete;
        }

        try
        {
            removePartsFinally(finally_remove_parts);
            LOG_DEBUG(log, "Removed {} {} parts", finally_remove_parts.size(), parts_type);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to remove some parts from memory, or write info about them into part log");
        }
    };

    /// Delete duplicate parts from filesystem
    if (!parts_to_delete_only_from_filesystem.empty())
    {
        /// It can happen that some error appear during part removal from FS.
        /// In case of such exception we have to change state of failed parts from Deleting to Outdated.
        /// Otherwise nobody will try to remove them again (see grabOldParts).
        delete_parts_from_fs_and_rollback_in_case_of_error(parts_to_delete_only_from_filesystem, "old duplicate");
    }

    /// Delete normal parts from ZooKeeper
    NameSet part_names_to_retry_deletion;
    try
    {
        Strings part_names_to_delete_completely;
        for (const auto & part : parts_to_delete_completely)
            part_names_to_delete_completely.emplace_back(part->name);

        LOG_DEBUG(log, "Removing {} old parts from ZooKeeper", parts_to_delete_completely.size());
        removePartsFromZooKeeper(zookeeper, part_names_to_delete_completely, &part_names_to_retry_deletion);
    }
    catch (...)
    {
        LOG_ERROR(log, "There is a problem with deleting parts from ZooKeeper: {}", getCurrentExceptionMessage(true));
    }

    /// Part names that were reliably deleted from ZooKeeper should be deleted from filesystem
    auto num_reliably_deleted_parts = parts_to_delete_completely.size() - part_names_to_retry_deletion.size();
    LOG_DEBUG(log, "Removed {} old parts from ZooKeeper. Removing them from filesystem.", num_reliably_deleted_parts);

    /// Delete normal parts on two sets
    for (auto & part : parts_to_delete_completely)
    {
        if (!part_names_to_retry_deletion.contains(part->name))
            parts_to_remove_from_filesystem.emplace_back(part);
        else
            parts_to_retry_deletion.emplace_back(part);
    }

    /// Will retry deletion
    if (!parts_to_retry_deletion.empty())
    {
        rollbackDeletingParts(parts_to_retry_deletion);
        LOG_DEBUG(log, "Will retry deletion of {} parts in the next time", parts_to_retry_deletion.size());
    }


    /// Remove parts from filesystem and finally from data_parts
    if (!parts_to_remove_from_filesystem.empty())
    {
        /// It can happen that some error appear during part removal from FS.
        /// In case of such exception we have to change state of failed parts from Deleting to Outdated.
        /// Otherwise nobody will try to remove them again (see grabOldParts).
        delete_parts_from_fs_and_rollback_in_case_of_error(parts_to_remove_from_filesystem, "old");
    }
}


void StorageReplicatedMergeTree::forcefullyRemoveBrokenOutdatedPartFromZooKeeperBeforeDetaching(const String & part_name)
{
    /// An outdated part is broken and we are going to move it do detached/
    /// But we need to remove it from ZooKeeper as well. Otherwise it will be considered as "lost forever".

    /// Since the part is Outdated, it should be safe to remove it, but it's still dangerous.
    /// It could became Outdated because it was merged/mutated (so we have a covering part) or because it was dropped.
    /// But DROP [PART]ITION waits for all Outdated parts to be loaded, so it's not the case.

    auto zookeeper = getZooKeeper();
    String part_path = replica_path + "/parts/" + part_name;
    if (!zookeeper->exists(part_path))
        return;

    auto part = getActiveContainingPart(part_name);
    if (!part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Outdated part {} is broken and going to be detached, "
                        "but there's no active covering part, so we are not sure that it can be safely removed from ZooKeeper "
                        "(path: {})", part_name, part_path);

    LOG_WARNING(log, "Outdated part {} is broken and going to be detached, removing it from ZooKeeper. The part is covered by {}",
                part_name, part->name);
    removePartsFromZooKeeperWithRetries({part_name}, /* infinite retries */ 0);
}

void StorageReplicatedMergeTree::removePartsFromZooKeeperWithRetries(PartsToRemoveFromZooKeeper & parts, size_t max_retries)
{
    Strings part_names_to_remove;
    for (const auto & part : parts)
        part_names_to_remove.emplace_back(part.getPartName());

    return removePartsFromZooKeeperWithRetries(part_names_to_remove, max_retries);
}

void StorageReplicatedMergeTree::removePartsFromZooKeeperWithRetries(const Strings & part_names, size_t max_retries)
{
    auto zookeeper = getZooKeeper();
    NameSet parts_to_retry_set;
    removePartsFromZooKeeper(zookeeper, part_names, &parts_to_retry_set);

    size_t num_tries = 0;
    while (!parts_to_retry_set.empty() && (max_retries == 0 || num_tries < max_retries))
    {
        zookeeper = getZooKeeper();
        Strings parts_to_retry;
        std::move(parts_to_retry_set.begin(), parts_to_retry_set.end(), std::back_inserter(parts_to_retry));
        parts_to_retry_set.clear();
        removePartsFromZooKeeper(zookeeper, parts_to_retry, &parts_to_retry_set);
        ++num_tries;
    }

    if (!parts_to_retry_set.empty())
        throw Exception(ErrorCodes::UNFINISHED, "Failed to remove {} parts from ZooKeeper after {} retries", parts_to_retry_set.size(), num_tries);
}

void StorageReplicatedMergeTree::removePartsFromZooKeeper(
    zkutil::ZooKeeperPtr & zookeeper, const Strings & part_names, NameSet * parts_should_be_retried)
try
{
    Strings exists_paths;
    std::vector<std::future<Coordination::MultiResponse>> remove_futures;
    exists_paths.reserve(part_names.size());
    remove_futures.reserve(part_names.size());
    /// Exception can be thrown from loop
    /// if zk session will be dropped
    for (const String & part_name : part_names)
    {
        exists_paths.emplace_back(fs::path(replica_path) / "parts" / part_name);
    }

    auto exists_results = zookeeper->exists(exists_paths);

    for (size_t i = 0; i < part_names.size(); ++i)
    {
        auto exists_resp = exists_results[i];
        if (exists_resp.error == Coordination::Error::ZOK)
        {
            Coordination::Requests ops;
            getRemovePartFromZooKeeperOps(part_names[i], ops, exists_resp.stat.numChildren > 0);
            remove_futures.emplace_back(zookeeper->asyncTryMultiNoThrow(ops));
        }
        else
        {
            LOG_DEBUG(log, "There is no part {} in ZooKeeper, it was only in filesystem", part_names[i]);
            // emplace invalid future so that the total number of futures is the same as part_names.size();
            remove_futures.emplace_back();
        }
    }

    for (size_t i = 0; i < remove_futures.size(); ++i)
    {
        auto & future = remove_futures[i];

        if (!future.valid())
            continue;

        auto response = future.get();
        if (response.error == Coordination::Error::ZOK)
            continue;

        if (response.error == Coordination::Error::ZNONODE)
        {
            LOG_DEBUG(log, "There is no part {} in ZooKeeper, it was only in filesystem", part_names[i]);
        }
        else
        {
            if (parts_should_be_retried)
                parts_should_be_retried->insert(part_names[i]);

            if (!Coordination::isHardwareError(response.error))
                LOG_WARNING(log, "Cannot remove part {} from ZooKeeper: {}", part_names[i], Coordination::errorMessage(response.error));
        }
    }
}
catch (...)
{
    if (parts_should_be_retried)
        parts_should_be_retried->insert(part_names.begin(), part_names.end());
    throw;
}

void StorageReplicatedMergeTree::clearLockedBlockNumbersInPartition(
    zkutil::ZooKeeper & zookeeper, const String & partition_id, Int64 min_block_num, Int64 max_block_num)
{
    /// Imagine that some INSERT query has allocated block number 42, but it's still in progress.
    /// Some DROP PARTITION query gets block number 43 and commits DROP_RANGE all_0_42_999_999.
    /// And after that INSERT commits GET_PART all_42_42_0. Oops, intersecting parts.
    /// So we have to either wait for unfinished INSERTs or cancel them.
    /// It's totally fine to cancel since we are going to remove data anyway.
    /// We can safely cancel INSERT query by removing its ephemeral block number.
    /// Usually it's bad idea to remove ephemeral nodes owned by someone else,
    /// but INSERTs remove such nodes atomically with part commit, so INSERT will fail if node does not exist.

    fs::path partition_path = fs::path(zookeeper_path) / "block_numbers" / partition_id;
    Strings queries_in_progress = zookeeper.getChildren(partition_path);
    if (queries_in_progress.empty())
        return;

    Strings paths_to_get;
    for (const auto & block : queries_in_progress)
    {
        if (!startsWith(block, "block-"))
            continue;
        Int64 block_number = parse<Int64>(block.substr(strlen("block-")));
        if (min_block_num <= block_number && block_number <= max_block_num)
            paths_to_get.push_back(partition_path / block);
    }

    auto results = zookeeper.tryGet(paths_to_get);
    for (size_t i = 0; i < paths_to_get.size(); ++i)
    {
        auto & result = results[i];

        /// The query already finished
        if (result.error == Coordination::Error::ZNONODE)
            continue;

        /// The query is not an insert (it does not have block_id)
        if (result.data.ends_with(EphemeralLockInZooKeeper::LEGACY_LOCK_OTHER))
            continue;

        if (result.data.ends_with(EphemeralLockInZooKeeper::LEGACY_LOCK_INSERT))
        {
            /// Remove block number, so insert will fail to commit (it will try to remove this node too)
            LOG_WARNING(log, "Some query is trying to concurrently insert block {}, will cancel it", paths_to_get[i]);
            zookeeper.tryRemove(paths_to_get[i]);
        }
        else
        {
            constexpr const char * old_version_warning = "Ephemeral lock {} (referencing {}) is created by a replica "
                "that running old version of ClickHouse (< 22.11). Cannot remove it, will wait for this lock to disappear. "
                "Upgrade remaining hosts in the cluster to address this warning.";
            constexpr const char * new_version_warning = "Ephemeral lock {} has unexpected content ({}), "
                "probably it is created by a replica that running newer version of ClickHouse. "
                "Cannot remove it, will wait for this lock to disappear. Upgrade remaining hosts in the cluster to address this warning.";

            if (result.data.starts_with(zookeeper_path + EphemeralLockInZooKeeper::LEGACY_LOCK_PREFIX))
                LOG_WARNING(log, old_version_warning, paths_to_get[i], result.data);
            else
                LOG_WARNING(log, new_version_warning, paths_to_get[i], result.data);

            Stopwatch time_waiting;
            const auto & stop_waiting = [this, &time_waiting]()
            {
                auto timeout = getContext()->getSettingsRef().lock_acquire_timeout.value.seconds();
                return partial_shutdown_called || (timeout < time_waiting.elapsedSeconds());
            };
            zookeeper.waitForDisappear(paths_to_get[i], stop_waiting);
        }
    }
}

void StorageReplicatedMergeTree::getClearBlocksInPartitionOps(
    Coordination::Requests & ops, zkutil::ZooKeeper & zookeeper, const String & partition_id, Int64 min_block_num, Int64 max_block_num)
{
    getClearBlocksInPartitionOpsImpl(ops, zookeeper, partition_id, min_block_num, max_block_num, "blocks");
    getClearBlocksInPartitionOpsImpl(ops, zookeeper, partition_id, min_block_num, max_block_num, "async_blocks");
}

void StorageReplicatedMergeTree::getClearBlocksInPartitionOpsImpl(
    Coordination::Requests & ops, zkutil::ZooKeeper & zookeeper, const String & partition_id, Int64 min_block_num, Int64 max_block_num, const String & blocks_dir_name)
{
    Strings blocks;
    if (Coordination::Error::ZOK != zookeeper.tryGetChildren(fs::path(zookeeper_path) / blocks_dir_name, blocks))
        throw Exception(ErrorCodes::NOT_FOUND_NODE, "Node {}/{} doesn't exist", zookeeper_path, blocks_dir_name);

    String partition_prefix = partition_id + "_";
    Strings paths_to_get;

    for (const String & block_id : blocks)
        if (startsWith(block_id, partition_prefix))
            paths_to_get.push_back(fs::path(zookeeper_path) / blocks_dir_name / block_id);

    auto results = zookeeper.tryGet(paths_to_get);

    for (size_t i = 0; i < paths_to_get.size(); ++i)
    {
        const String & path = paths_to_get[i];
        auto & result = results[i];

        if (result.error == Coordination::Error::ZNONODE)
            continue;

        ReadBufferFromString buf(result.data);

        const auto part_info = MergeTreePartInfo::tryParsePartName(result.data, format_version);

        if (!part_info || (min_block_num <= part_info->min_block && part_info->max_block <= max_block_num))
            ops.emplace_back(zkutil::makeRemoveRequest(path, -1));
    }
}

void StorageReplicatedMergeTree::clearBlocksInPartition(
    zkutil::ZooKeeper & zookeeper, const String & partition_id, Int64 min_block_num, Int64 max_block_num)
{
    Coordination::Requests delete_requests;
    getClearBlocksInPartitionOps(delete_requests, zookeeper, partition_id, min_block_num, max_block_num);
    Coordination::Responses delete_responses;
    auto code = zookeeper.tryMulti(delete_requests, delete_responses);
    if (code != Coordination::Error::ZOK)
    {
        for (size_t i = 0; i < delete_requests.size(); ++i)
            if (delete_responses[i]->error != Coordination::Error::ZOK)
                LOG_WARNING(log, "Error while deleting ZooKeeper path `{}`: {}, ignoring.", delete_requests[i]->getPath(), delete_responses[i]->error);
    }

    LOG_TRACE(log, "Deleted {} deduplication block IDs in partition ID {}", delete_requests.size(), partition_id);
}

void StorageReplicatedMergeTree::replacePartitionFrom(
    const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr query_context)
{
    /// First argument is true, because we possibly will add new data to current table.
    auto lock1 = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
    auto lock2 = source_table->lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
    auto storage_settings_ptr = getSettings();

    auto source_metadata_snapshot = source_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    Stopwatch watch;
    ProfileEventsScope profile_events_scope;

    MergeTreeData & src_data = checkStructureAndGetMergeTreeData(source_table, source_metadata_snapshot, metadata_snapshot);
    String partition_id = getPartitionIDFromQuery(partition, query_context);

    /// NOTE: Some covered parts may be missing in src_all_parts if corresponding log entries are not executed yet.
    DataPartsVector src_all_parts = src_data.getVisibleDataPartsVectorInPartition(query_context, partition_id);

    LOG_DEBUG(log, "Cloning {} parts", src_all_parts.size());

    static const String TMP_PREFIX = "tmp_replace_from_";
    auto zookeeper = getZooKeeper();

    /// Retry if alter_partition_version changes
    for (size_t retry = 0; retry < 1000; ++retry)
    {
        DataPartsVector src_parts;
        MutableDataPartsVector dst_parts;
        std::vector<scope_guard> dst_parts_locks;
        Strings block_id_paths;
        Strings part_checksums;
        std::vector<EphemeralLockInZooKeeper> ephemeral_locks;
        String alter_partition_version_path = zookeeper_path + "/alter_partition_version";
        Coordination::Stat alter_partition_version_stat;
        zookeeper->get(alter_partition_version_path, &alter_partition_version_stat);

        /// Firstly, generate last block number and compute drop_range
        /// NOTE: Even if we make ATTACH PARTITION instead of REPLACE PARTITION drop_range will not be empty, it will contain a block.
        /// So, such case has special meaning, if drop_range contains only one block it means that nothing to drop.
        /// TODO why not to add normal DROP_RANGE entry to replication queue if `replace` is true?
        MergeTreePartInfo drop_range;
        std::optional<EphemeralLockInZooKeeper> delimiting_block_lock;
        bool partition_was_empty = !getFakePartCoveringAllPartsInPartition(partition_id, drop_range, delimiting_block_lock, true);
        if (replace && partition_was_empty)
        {
            /// Nothing to drop, will just attach new parts
            LOG_INFO(log, "Partition {} was empty, REPLACE PARTITION will work as ATTACH PARTITION FROM", drop_range.partition_id);
            replace = false;
        }

        if (!replace)
        {
            /// It's ATTACH PARTITION FROM, not REPLACE PARTITION. We have to reset drop range
            drop_range = makeDummyDropRangeForMovePartitionOrAttachPartitionFrom(partition_id);
        }

        assert(replace == !LogEntry::ReplaceRangeEntry::isMovePartitionOrAttachFrom(drop_range));

        String drop_range_fake_part_name = getPartNamePossiblyFake(format_version, drop_range);

        for (const auto & src_part : src_all_parts)
        {
            /// We also make some kind of deduplication to avoid duplicated parts in case of ATTACH PARTITION
            /// Assume that merges in the partition are quite rare
            /// Save deduplication block ids with special prefix replace_partition

            if (!canReplacePartition(src_part))
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Cannot replace partition '{}' because part '{}"
                                "' has inconsistent granularity with table", partition_id, src_part->name);

            String hash_hex = src_part->checksums.getTotalChecksumHex();

            if (replace)
                LOG_INFO(log, "Trying to replace {} with hash_hex {}", src_part->name, hash_hex);
            else
                LOG_INFO(log, "Trying to attach {} with hash_hex {}", src_part->name, hash_hex);

            String block_id_path = replace ? "" : (fs::path(zookeeper_path) / "blocks" / (partition_id + "_replace_from_" + hash_hex));

            auto lock = allocateBlockNumber(partition_id, zookeeper, block_id_path);
            if (!lock)
            {
                LOG_INFO(log, "Part {} (hash {}) has been already attached", src_part->name, hash_hex);
                continue;
            }

            UInt64 index = lock->getNumber();
            MergeTreePartInfo dst_part_info(partition_id, index, index, src_part->info.level);

            bool zero_copy_enabled = storage_settings_ptr->allow_remote_fs_zero_copy_replication
                || dynamic_cast<const MergeTreeData *>(source_table.get())->getSettings()->allow_remote_fs_zero_copy_replication;
            IDataPartStorage::ClonePartParams clone_params
            {
                .copy_instead_of_hardlink = zero_copy_enabled && src_part->isStoredOnRemoteDiskWithZeroCopySupport(),
                .metadata_version_to_write = metadata_snapshot->getMetadataVersion()
            };
            auto [dst_part, part_lock] = cloneAndLoadDataPartOnSameDisk(src_part, TMP_PREFIX, dst_part_info, metadata_snapshot, clone_params);
            src_parts.emplace_back(src_part);
            dst_parts.emplace_back(dst_part);
            dst_parts_locks.emplace_back(std::move(part_lock));
            ephemeral_locks.emplace_back(std::move(*lock));
            block_id_paths.emplace_back(block_id_path);
            part_checksums.emplace_back(hash_hex);
        }

        ReplicatedMergeTreeLogEntryData entry;
        {
            auto src_table_id = src_data.getStorageID();
            entry.type = ReplicatedMergeTreeLogEntryData::REPLACE_RANGE;
            entry.source_replica = replica_name;
            entry.create_time = time(nullptr);
            entry.replace_range_entry = std::make_shared<ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry>();

            auto & entry_replace = *entry.replace_range_entry;
            entry_replace.drop_range_part_name = drop_range_fake_part_name;
            entry_replace.from_database = src_table_id.database_name;
            entry_replace.from_table = src_table_id.table_name;
            for (const auto & part : src_parts)
                entry_replace.src_part_names.emplace_back(part->name);
            for (const auto & part : dst_parts)
                entry_replace.new_part_names.emplace_back(part->name);
            for (const String & checksum : part_checksums)
                entry_replace.part_names_checksums.emplace_back(checksum);
            entry_replace.columns_version = -1;
        }

        if (replace)
        {
            /// Cancel concurrent inserts in range
            clearLockedBlockNumbersInPartition(*zookeeper, drop_range.partition_id, drop_range.max_block, drop_range.max_block);
            /// Remove deduplication block_ids of replacing parts
            clearBlocksInPartition(*zookeeper, drop_range.partition_id, drop_range.max_block, drop_range.max_block);
        }

        Coordination::Responses op_results;
        DataPartsVector parts_holder;

        try
        {
            Coordination::Requests ops;
            for (size_t i = 0; i < dst_parts.size(); ++i)
            {
                getCommitPartOps(ops, dst_parts[i], block_id_paths[i]);
                ephemeral_locks[i].getUnlockOp(ops);
            }

            if (auto txn = query_context->getZooKeeperMetadataTransaction())
                txn->moveOpsTo(ops);

            delimiting_block_lock->getUnlockOp(ops);
            /// Check and update version to avoid race with DROP_RANGE
            ops.emplace_back(zkutil::makeSetRequest(alter_partition_version_path, "", alter_partition_version_stat.version));
            /// Just update version, because merges assignment relies on it
            ops.emplace_back(zkutil::makeSetRequest(fs::path(zookeeper_path) / "log", "", -1));
            ops.emplace_back(zkutil::makeCreateRequest(fs::path(zookeeper_path) / "log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential));

            Transaction transaction(*this, NO_TRANSACTION_RAW);
            {
                auto data_parts_lock = lockParts();
                for (auto & part : dst_parts)
                    renameTempPartAndReplaceUnlocked(part, transaction, data_parts_lock);
            }

            for (const auto & dst_part : dst_parts)
                lockSharedData(*dst_part, false, /*hardlinked_files*/ {});

            Coordination::Error code = zookeeper->tryMulti(ops, op_results);
            if (code == Coordination::Error::ZOK)
                delimiting_block_lock->assumeUnlocked();
            else if (code == Coordination::Error::ZBADVERSION)
            {
                /// Cannot retry automatically, because some zookeeper ops were lost on the first attempt. Will retry on DDLWorker-level.
                if (query_context->getZooKeeperMetadataTransaction())
                    throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER,
                                    "Cannot execute alter, because alter partition version was suddenly changed due "
                                    "to concurrent alter");
                continue;
            }
            else
                zkutil::KeeperMultiException::check(code, ops, op_results);

            {
                auto data_parts_lock = lockParts();
                transaction.commit(&data_parts_lock);
                if (replace)
                {
                    parts_holder = getDataPartsVectorInPartitionForInternalUsage(MergeTreeDataPartState::Active, drop_range.partition_id, &data_parts_lock);
                    removePartsInRangeFromWorkingSet(NO_TRANSACTION_RAW, drop_range, data_parts_lock);
                }
            }

            PartLog::addNewParts(getContext(), PartLog::createPartLogEntries(dst_parts, watch.elapsed(), profile_events_scope.getSnapshot()));
        }
        catch (...)
        {
            PartLog::addNewParts(getContext(), PartLog::createPartLogEntries(dst_parts, watch.elapsed()), ExecutionStatus::fromCurrentException("", true));
            for (const auto & dst_part : dst_parts)
                unlockSharedData(*dst_part);

            throw;
        }

        String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*op_results.back()).path_created;
        entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

        for (auto & lock : ephemeral_locks)
            lock.assumeUnlocked();

        lock2.reset();
        lock1.reset();

        /// We need to pull the DROP_RANGE before cleaning the replaced parts (otherwise CHeckThread may decide that parts are lost)
        queue.pullLogsToQueue(getZooKeeperAndAssertNotReadonly(), {}, ReplicatedMergeTreeQueue::SYNC);
        parts_holder.clear();
        cleanup_thread.wakeup();


        waitForLogEntryToBeProcessedIfNecessary(entry, query_context);

        return;
    }

    throw Exception(
        ErrorCodes::CANNOT_ASSIGN_ALTER, "Cannot assign ALTER PARTITION, because another ALTER PARTITION query was concurrently executed");
}

void StorageReplicatedMergeTree::movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr query_context)
{
    auto lock1 = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
    auto lock2 = dest_table->lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
    auto storage_settings_ptr = getSettings();

    auto dest_table_storage = std::dynamic_pointer_cast<StorageReplicatedMergeTree>(dest_table);
    if (!dest_table_storage)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Table {} supports movePartitionToTable only for ReplicatedMergeTree family of table engines. "
                        "Got {}", getStorageID().getNameForLogs(), dest_table->getName());
    if (dest_table_storage->getStoragePolicy() != this->getStoragePolicy())
        throw Exception(ErrorCodes::UNKNOWN_POLICY,
                        "Destination table {} should have the same storage policy of source table {}. {}: {}, {}: {}",
                        dest_table_storage->getStorageID().getNameForLogs(),
                        getStorageID().getNameForLogs(), getStorageID().getNameForLogs(),
                        this->getStoragePolicy()->getName(), getStorageID().getNameForLogs(),
                        dest_table_storage->getStoragePolicy()->getName());

    auto dest_metadata_snapshot = dest_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    Stopwatch watch;
    ProfileEventsScope profile_events_scope;

    MergeTreeData & src_data = dest_table_storage->checkStructureAndGetMergeTreeData(*this, metadata_snapshot, dest_metadata_snapshot);
    auto src_data_id = src_data.getStorageID();
    String partition_id = getPartitionIDFromQuery(partition, query_context);

    /// A range for log entry to remove parts from the source table (myself).
    auto zookeeper = getZooKeeper();
    /// Retry if alter_partition_version changes
    for (size_t retry = 0; retry < 1000; ++retry)
    {
        String alter_partition_version_path = zookeeper_path + "/alter_partition_version";
        Coordination::Stat alter_partition_version_stat;
        zookeeper->get(alter_partition_version_path, &alter_partition_version_stat);

        MergeTreePartInfo drop_range;
        std::optional<EphemeralLockInZooKeeper> delimiting_block_lock;
        getFakePartCoveringAllPartsInPartition(partition_id, drop_range, delimiting_block_lock, true);
        String drop_range_fake_part_name = getPartNamePossiblyFake(format_version, drop_range);

        DataPartPtr covering_part;
        DataPartsVector src_all_parts;
        {
            /// NOTE: Some covered parts may be missing in src_all_parts if corresponding log entries are not executed yet.
            auto parts_lock = src_data.lockParts();
            src_all_parts = src_data.getActivePartsToReplace(drop_range, drop_range_fake_part_name, covering_part, parts_lock);
        }

        if (covering_part)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got part {} covering drop range {}, it's a bug",
                            covering_part->name, drop_range_fake_part_name);

        /// After allocating block number for drop_range we must ensure that it does not intersect block numbers
        /// allocated by concurrent REPLACE query.
        /// We could check it in multi-request atomically with creation of DROP_RANGE entry in source table log,
        /// but it's better to check it here and fail as early as possible (before we have done something to destination table).
        Coordination::Error version_check_code = zookeeper->trySet(alter_partition_version_path, "", alter_partition_version_stat.version);
        if (version_check_code != Coordination::Error::ZOK)
            throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER, "Cannot DROP PARTITION in {} after copying partition to {}, "
                            "because another ALTER PARTITION query was concurrently executed",
                            getStorageID().getFullTableName(), dest_table_storage->getStorageID().getFullTableName());

        DataPartsVector src_parts;
        MutableDataPartsVector dst_parts;
        Strings block_id_paths;
        Strings part_checksums;
        std::vector<EphemeralLockInZooKeeper> ephemeral_locks;

        LOG_DEBUG(log, "Cloning {} parts", src_all_parts.size());

        static const String TMP_PREFIX = "tmp_move_from_";

        /// Clone parts into destination table.
        String dest_alter_partition_version_path = dest_table_storage->zookeeper_path + "/alter_partition_version";
        Coordination::Stat dest_alter_partition_version_stat;
        zookeeper->get(dest_alter_partition_version_path, &dest_alter_partition_version_stat);
        std::vector<scope_guard> temporary_parts_locks;

        for (const auto & src_part : src_all_parts)
        {
            if (!dest_table_storage->canReplacePartition(src_part))
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Cannot move partition '{}' because part '{}"
                                "' has inconsistent granularity with table", partition_id, src_part->name);

            String hash_hex = src_part->checksums.getTotalChecksumHex();
            String block_id_path;

            auto lock = dest_table_storage->allocateBlockNumber(partition_id, zookeeper, block_id_path);
            if (!lock)
            {
                LOG_INFO(log, "Part {} (hash {}) has been already attached", src_part->name, hash_hex);
                continue;
            }

            UInt64 index = lock->getNumber();
            MergeTreePartInfo dst_part_info(partition_id, index, index, src_part->info.level);

            /// Don't do hardlinks in case of zero-copy at any side (defensive programming)
            bool zero_copy_enabled = storage_settings_ptr->allow_remote_fs_zero_copy_replication
                || dynamic_cast<const MergeTreeData *>(dest_table.get())->getSettings()->allow_remote_fs_zero_copy_replication;

            IDataPartStorage::ClonePartParams clone_params
            {
                .copy_instead_of_hardlink = zero_copy_enabled && src_part->isStoredOnRemoteDiskWithZeroCopySupport(),
                .metadata_version_to_write = dest_metadata_snapshot->getMetadataVersion()
            };
            auto [dst_part, dst_part_lock] = dest_table_storage->cloneAndLoadDataPartOnSameDisk(src_part, TMP_PREFIX, dst_part_info, dest_metadata_snapshot, clone_params);

            src_parts.emplace_back(src_part);
            dst_parts.emplace_back(dst_part);
            temporary_parts_locks.emplace_back(std::move(dst_part_lock));
            ephemeral_locks.emplace_back(std::move(*lock));
            block_id_paths.emplace_back(block_id_path);
            part_checksums.emplace_back(hash_hex);
        }

        ReplicatedMergeTreeLogEntryData entry_delete;
        {
            entry_delete.type = LogEntry::DROP_RANGE;
            entry_delete.source_replica = replica_name;
            entry_delete.new_part_name = drop_range_fake_part_name;
            entry_delete.detach = false;
            entry_delete.create_time = time(nullptr);
        }

        ReplicatedMergeTreeLogEntryData entry;
        {
            MergeTreePartInfo drop_range_dest = makeDummyDropRangeForMovePartitionOrAttachPartitionFrom(partition_id);

            entry.type = ReplicatedMergeTreeLogEntryData::REPLACE_RANGE;
            entry.source_replica = dest_table_storage->replica_name;
            entry.create_time = time(nullptr);
            entry.replace_range_entry = std::make_shared<ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry>();

            auto & entry_replace = *entry.replace_range_entry;
            entry_replace.drop_range_part_name = getPartNamePossiblyFake(format_version, drop_range_dest);
            entry_replace.from_database = src_data_id.database_name;
            entry_replace.from_table = src_data_id.table_name;
            for (const auto & part : src_parts)
                entry_replace.src_part_names.emplace_back(part->name);
            for (const auto & part : dst_parts)
                entry_replace.new_part_names.emplace_back(part->name);
            for (const String & checksum : part_checksums)
                entry_replace.part_names_checksums.emplace_back(checksum);
            entry_replace.columns_version = -1;
        }

        /// Cancel concurrent inserts in range
        clearLockedBlockNumbersInPartition(*zookeeper, drop_range.partition_id, drop_range.max_block, drop_range.max_block);

        clearBlocksInPartition(*zookeeper, drop_range.partition_id, drop_range.max_block, drop_range.max_block);

        Coordination::Responses op_results;

        /// We should hold replaced parts until we actually create DROP_RANGE in ZooKeeper
        DataPartsVector parts_holder;
        try
        {
            Coordination::Requests ops;
            for (size_t i = 0; i < dst_parts.size(); ++i)
            {
                dest_table_storage->getCommitPartOps(ops, dst_parts[i], block_id_paths[i]);
                ephemeral_locks[i].getUnlockOp(ops);
            }

            /// Check and update version to avoid race with DROP_RANGE
            ops.emplace_back(zkutil::makeSetRequest(dest_alter_partition_version_path, "", dest_alter_partition_version_stat.version));
            /// Just update version, because merges assignment relies on it
            ops.emplace_back(zkutil::makeSetRequest(fs::path(dest_table_storage->zookeeper_path) / "log", "", -1));
            ops.emplace_back(zkutil::makeCreateRequest(fs::path(dest_table_storage->zookeeper_path) / "log/log-",
                                                       entry.toString(), zkutil::CreateMode::PersistentSequential));

            {
                Transaction transaction(*dest_table_storage, NO_TRANSACTION_RAW);

                auto src_data_parts_lock = lockParts();
                auto dest_data_parts_lock = dest_table_storage->lockParts();

                for (auto & part : dst_parts)
                    dest_table_storage->renameTempPartAndReplaceUnlocked(part, transaction, dest_data_parts_lock);

                for (const auto & dst_part : dst_parts)
                    dest_table_storage->lockSharedData(*dst_part, false, /*hardlinked_files*/ {});

                Coordination::Error code = zookeeper->tryMulti(ops, op_results);
                if (code == Coordination::Error::ZBADVERSION)
                    continue;
                else
                    zkutil::KeeperMultiException::check(code, ops, op_results);

                parts_holder = getDataPartsVectorInPartitionForInternalUsage(MergeTreeDataPartState::Active, drop_range.partition_id, &src_data_parts_lock);
                removePartsInRangeFromWorkingSet(NO_TRANSACTION_RAW, drop_range, src_data_parts_lock);
                transaction.commit(&src_data_parts_lock);
            }

            PartLog::addNewParts(getContext(), PartLog::createPartLogEntries(dst_parts, watch.elapsed(), profile_events_scope.getSnapshot()));
        }
        catch (...)
        {
            PartLog::addNewParts(getContext(), PartLog::createPartLogEntries(dst_parts, watch.elapsed()), ExecutionStatus::fromCurrentException("", true));

            for (const auto & dst_part : dst_parts)
                dest_table_storage->unlockSharedData(*dst_part);

            throw;
        }

        String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*op_results.back()).path_created;
        entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

        for (auto & lock : ephemeral_locks)
            lock.assumeUnlocked();

        lock2.reset();

        dest_table_storage->waitForLogEntryToBeProcessedIfNecessary(entry, query_context);

        /// Create DROP_RANGE for the source table
        Coordination::Requests ops_src;
        ops_src.emplace_back(zkutil::makeCreateRequest(
            fs::path(zookeeper_path) / "log/log-", entry_delete.toString(), zkutil::CreateMode::PersistentSequential));
        /// Just update version, because merges assignment relies on it
        ops_src.emplace_back(zkutil::makeSetRequest(fs::path(zookeeper_path) / "log", "", -1));
        delimiting_block_lock->getUnlockOp(ops_src);

        op_results = zookeeper->multi(ops_src);

        log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*op_results.front()).path_created;
        entry_delete.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

        lock1.reset();

        /// We need to pull the DROP_RANGE before cleaning the replaced parts (otherwise CHeckThread may decide that parts are lost)
        queue.pullLogsToQueue(getZooKeeperAndAssertNotReadonly(), {}, ReplicatedMergeTreeQueue::SYNC);
        parts_holder.clear();
        cleanup_thread.wakeup();

        waitForLogEntryToBeProcessedIfNecessary(entry_delete, query_context);

        /// Cleaning possibly stored information about parts from /quorum/last_part node in ZooKeeper.
        cleanLastPartNode(partition_id);

        return;
    }

    throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER,
                    "Cannot assign ALTER PARTITION, because another ALTER PARTITION query was concurrently executed");
}

void StorageReplicatedMergeTree::movePartitionToShard(
    const ASTPtr & partition, bool move_part, const String & to, ContextPtr /*query_context*/)
{
    /// This is a lightweight operation that only optimistically checks if it could succeed and queues tasks.

    if (!move_part)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MOVE PARTITION TO SHARD is not supported, use MOVE PART instead");

    if (zkutil::normalizeZooKeeperPath(zookeeper_path, /* check_starts_with_slash */ true) == zkutil::normalizeZooKeeperPath(to, /* check_starts_with_slash */ true))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Source and destination are the same");

    auto zookeeper = getZooKeeperAndAssertNotReadonly();

    String part_name = partition->as<ASTLiteral &>().value.safeGet<String>();
    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

    auto part = getPartIfExists(part_info, {MergeTreeDataPartState::Active});
    if (!part)
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Part {} not found locally", part_name);

    if (part->uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Part {} does not have an uuid assigned and it can't be moved between shards", part_name);


    ReplicatedMergeTreeMergePredicate merge_pred = queue.getMergePredicate(zookeeper, PartitionIdsHint{part_info.partition_id});

    /// The following block is pretty much copy & paste from StorageReplicatedMergeTree::dropPart to avoid conflicts while this is WIP.
    /// Extract it to a common method and re-use it before merging.
    {
        if (partIsLastQuorumPart(part->info))
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "Part {} is last inserted part with quorum in partition. Would not be able to drop",
                            part_name);
        }

        /// canMergeSinglePart is overlapping with dropPart, let's try to use the same code.
        String out_reason;
        if (!merge_pred.canMergeSinglePart(part, out_reason))
            throw Exception(ErrorCodes::PART_IS_TEMPORARILY_LOCKED, "Part is busy, reason: {}", out_reason);
    }

    {
        /// Optimistic check that for compatible destination table structure.
        checkTableStructure(to, getInMemoryMetadataPtr());
    }

    PinnedPartUUIDs src_pins;
    PinnedPartUUIDs dst_pins;

    {
        String s = zookeeper->get(zookeeper_path + "/pinned_part_uuids", &src_pins.stat);
        src_pins.fromString(s);
    }

    {
        String s = zookeeper->get(to + "/pinned_part_uuids", &dst_pins.stat);
        dst_pins.fromString(s);
    }

    if (src_pins.part_uuids.contains(part->uuid) || dst_pins.part_uuids.contains(part->uuid))
        throw Exception(ErrorCodes::PART_IS_TEMPORARILY_LOCKED, "Part {} has it's uuid ({}) already pinned.", part_name, part->uuid);

    src_pins.part_uuids.insert(part->uuid);
    dst_pins.part_uuids.insert(part->uuid);

    PartMovesBetweenShardsOrchestrator::Entry part_move_entry;
    part_move_entry.state = PartMovesBetweenShardsOrchestrator::EntryState::SYNC_SOURCE;
    part_move_entry.create_time = std::time(nullptr);
    part_move_entry.update_time = part_move_entry.create_time;
    part_move_entry.task_uuid = UUIDHelpers::generateV4();
    part_move_entry.part_name = part->name;
    part_move_entry.part_uuid = part->uuid;
    part_move_entry.to_shard = to;

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCheckRequest(zookeeper_path + "/log", merge_pred.getVersion())); /// Make sure no new events were added to the log.
    ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/pinned_part_uuids", src_pins.toString(), src_pins.stat.version));
    ops.emplace_back(zkutil::makeSetRequest(to + "/pinned_part_uuids", dst_pins.toString(), dst_pins.stat.version));
    ops.emplace_back(zkutil::makeCreateRequest(
        part_moves_between_shards_orchestrator.entries_znode_path + "/task-",
        part_move_entry.toString(),
        zkutil::CreateMode::PersistentSequential));

    Coordination::Responses responses;
    Coordination::Error rc = zookeeper->tryMulti(ops, responses);
    zkutil::KeeperMultiException::check(rc, ops, responses);

    String task_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*responses.back()).path_created;
    LOG_DEBUG(log, "Created task for part movement between shards at {}", task_znode_path);

    /// TODO(nv): Nice to have support for `alter_sync`.
    ///     For now use the system.part_moves_between_shards table for status.
}

CancellationCode StorageReplicatedMergeTree::killPartMoveToShard(const UUID & task_uuid)
{
    return part_moves_between_shards_orchestrator.killPartMoveToShard(task_uuid);
}

void StorageReplicatedMergeTree::getCommitPartOps(
    Coordination::Requests & ops,
    const DataPartPtr & part,
    const String & block_id_path) const
{
    if (block_id_path.empty())
        return getCommitPartOps(ops, part, std::vector<String>());
    else
        return getCommitPartOps(ops, part, std::vector<String>({block_id_path}));
}

void StorageReplicatedMergeTree::getCommitPartOps(
    Coordination::Requests & ops,
    const DataPartPtr & part,
    const std::vector<String> & block_id_paths) const
{
    const String & part_name = part->name;
    const auto storage_settings_ptr = getSettings();
    for (const String & block_id_path : block_id_paths)
    {
        /// Make final duplicate check and commit block_id
        ops.emplace_back(
            zkutil::makeCreateRequest(
                block_id_path,
                part_name,  /// We will be able to know original part number for duplicate blocks, if we want.
                zkutil::CreateMode::Persistent));
    }

    /// Information about the part, in the replica
    if (storage_settings_ptr->use_minimalistic_part_header_in_zookeeper)
    {
        ops.emplace_back(zkutil::makeCreateRequest(
            fs::path(replica_path) / "parts" / part->name,
            ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(part->getColumns(), part->checksums).toString(),
            zkutil::CreateMode::Persistent));
    }
    else
    {
        ops.emplace_back(zkutil::makeCreateRequest(
            fs::path(replica_path) / "parts" / part->name,
            "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            fs::path(replica_path) / "parts" / part->name / "columns",
            part->getColumns().toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            fs::path(replica_path) / "parts" / part->name / "checksums",
            getChecksumsForZooKeeper(part->checksums),
            zkutil::CreateMode::Persistent));
    }
}

ReplicatedMergeTreeAddress StorageReplicatedMergeTree::getReplicatedMergeTreeAddress() const
{
    auto host_port = getContext()->getInterserverIOAddress();
    auto table_id = getStorageID();

    ReplicatedMergeTreeAddress res;
    res.host = host_port.first;
    res.replication_port = host_port.second;
    res.queries_port = getContext()->getTCPPort();
    res.database = table_id.database_name;
    res.table = table_id.table_name;
    res.scheme = getContext()->getInterserverScheme();
    return res;
}

ActionLock StorageReplicatedMergeTree::getActionLock(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return merger_mutator.merges_blocker.cancel();

    if (action_type == ActionLocks::PartsTTLMerge)
        return merger_mutator.ttl_merges_blocker.cancel();

    if (action_type == ActionLocks::PartsFetch)
        return fetcher.blocker.cancel();

    if (action_type == ActionLocks::PartsSend)
    {
        auto data_parts_exchange_ptr = std::atomic_load(&data_parts_exchange_endpoint);
        return data_parts_exchange_ptr ? data_parts_exchange_ptr->blocker.cancel() : ActionLock();
    }

    if (action_type == ActionLocks::ReplicationQueue)
        return queue.actions_blocker.cancel();

    if (action_type == ActionLocks::PartsMove)
        return parts_mover.moves_blocker.cancel();

    if (action_type == ActionLocks::PullReplicationLog)
        return queue.pull_log_blocker.cancel();

    return {};
}

void StorageReplicatedMergeTree::onActionLockRemove(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge || action_type == ActionLocks::PartsTTLMerge
        || action_type == ActionLocks::PartsFetch || action_type == ActionLocks::PartsSend
        || action_type == ActionLocks::ReplicationQueue)
        background_operations_assignee.trigger();
    else if (action_type == ActionLocks::PartsMove)
        background_moves_assignee.trigger();
}

bool StorageReplicatedMergeTree::waitForProcessingQueue(UInt64 max_wait_milliseconds, SyncReplicaMode sync_mode)
{
    /// Let's fetch new log entries firstly
    queue.pullLogsToQueue(getZooKeeperAndAssertNotReadonly(), {}, ReplicatedMergeTreeQueue::SYNC);

    if (sync_mode == SyncReplicaMode::PULL)
        return true;

    /// This is significant, because the execution of this task could be delayed at BackgroundPool.
    /// And we force it to be executed.
    background_operations_assignee.trigger();

    std::unordered_set<String> wait_for_ids;
    std::atomic_bool was_interrupted = false;

    Poco::Event target_entry_event;
    auto callback = [this, &target_entry_event, &wait_for_ids, &was_interrupted, sync_mode]
        (size_t new_queue_size, const String * removed_log_entry_id)
    {
        if (partial_shutdown_called)
        {
            was_interrupted = true;
            target_entry_event.set();
            return;
        }

        if (sync_mode == SyncReplicaMode::STRICT)
        {
            /// Wait for queue to become empty
            if (new_queue_size == 0)
                target_entry_event.set();
            return;
        }

        if (removed_log_entry_id)
            wait_for_ids.erase(*removed_log_entry_id);

        if (wait_for_ids.empty())
            target_entry_event.set();
    };
    const auto handler = queue.addSubscriber(std::move(callback), wait_for_ids, sync_mode);

    if (!target_entry_event.tryWait(max_wait_milliseconds))
        return false;

    if (was_interrupted)
        throw Exception(ErrorCodes::ABORTED, "Shutdown is called for table");

    return true;
}

bool StorageReplicatedMergeTree::dropPartImpl(
    zkutil::ZooKeeperPtr & zookeeper, String part_name, LogEntry & entry, bool detach, bool throw_if_noop)
{
    LOG_TRACE(log, "Will try to insert a log entry to DROP_PART for part {}", part_name);

    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

    while (true)
    {
        ReplicatedMergeTreeMergePredicate merge_pred = queue.getMergePredicate(zookeeper, PartitionIdsHint{part_info.partition_id});

        auto part = getPartIfExists(part_info, {MergeTreeDataPartState::Active});

        if (!part)
        {
            if (throw_if_noop)
                throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Part {} not found locally, won't try to drop it.", part_name);
            return false;
        }

        if (merge_pred.isGoingToBeDropped(part->info))
        {
            if (throw_if_noop)
                throw Exception(ErrorCodes::PART_IS_TEMPORARILY_LOCKED, "Already has DROP RANGE for part {} in queue.", part_name);

            return false;
        }

        /// There isn't a lot we can do otherwise. Can't cancel merges because it is possible that a replica already
        /// finished the merge.
        String out_reason;
        if (!merge_pred.canMergeSinglePart(part, out_reason))
        {
            if (throw_if_noop)
                throw Exception::createDeprecated(out_reason, ErrorCodes::PART_IS_TEMPORARILY_LOCKED);
            return false;
        }

        if (merge_pred.partParticipatesInReplaceRange(part, out_reason))
        {
            if (throw_if_noop)
                throw Exception::createDeprecated(out_reason, ErrorCodes::PART_IS_TEMPORARILY_LOCKED);
            return false;
        }

        if (partIsLastQuorumPart(part->info))
        {
            if (throw_if_noop)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Part {} is last inserted part with quorum in partition. Cannot drop", part_name);
            return false;
        }

        if (partIsInsertingWithParallelQuorum(part->info))
        {
            if (throw_if_noop)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Part {} is inserting with parallel quorum. Cannot drop", part_name);
            return false;
        }

        Coordination::Requests ops;
        /// NOTE Don't need to remove block numbers too, because no in-progress inserts in the range are possible
        getClearBlocksInPartitionOps(ops, *zookeeper, part_info.partition_id, part_info.min_block, part_info.max_block);
        size_t clear_block_ops_size = ops.size();

        /// If `part_name` is result of a recent merge and source parts are still available then
        /// DROP_PART with detach will move this part together with source parts to `detached/` dir.
        entry.type = LogEntry::DROP_PART;
        entry.source_replica = replica_name;
        /// We don't set fake drop level (999999999) for the single part drop range.
        /// First of all we don't guarantee anything other than the part will not be
        /// active after DROP_PART, but covering part (without data of dropped part) can exist.
        /// If we add part with 9999999 level than we can break invariant in virtual_parts of
        /// the queue.
        entry.new_part_name = getPartNamePossiblyFake(format_version, part->info);
        entry.detach = detach;
        entry.create_time = time(nullptr);

        ops.emplace_back(zkutil::makeCheckRequest(fs::path(zookeeper_path) / "log", merge_pred.getVersion())); /// Make sure no new events were added to the log.
        ops.emplace_back(zkutil::makeCreateRequest(fs::path(zookeeper_path) / "log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential));
        /// Just update version, because merges assignment relies on it
        ops.emplace_back(zkutil::makeSetRequest(fs::path(zookeeper_path) / "log", "", -1));
        Coordination::Responses responses;
        Coordination::Error rc = zookeeper->tryMulti(ops, responses);

        if (rc == Coordination::Error::ZBADVERSION)
        {
            LOG_TRACE(log, "A new log entry appeared while trying to commit DROP RANGE. Retry.");
            continue;
        }
        else if (rc == Coordination::Error::ZNONODE)
        {
            LOG_TRACE(log, "Other replica already removing same part {} or part deduplication node was removed by background thread. Retry.", part_name);
            continue;
        }
        else
            zkutil::KeeperMultiException::check(rc, ops, responses);

        String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*responses[clear_block_ops_size + 1]).path_created;
        entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

        LOG_TRACE(log, "DROP RANGE for part {} inserted with znode name {}", part_name, entry.znode_name);
        return true;
    }
}

bool StorageReplicatedMergeTree::addOpsToDropAllPartsInPartition(
    zkutil::ZooKeeper & zookeeper, const String & partition_id, bool detach,
    Coordination::Requests & ops, std::vector<LogEntryPtr> & entries,
    std::vector<EphemeralLockInZooKeeper> & delimiting_block_locks,
    std::vector<size_t> & log_entry_ops_idx)
{
    MergeTreePartInfo drop_range_info;

    /// It would prevent other replicas from assigning merges which intersect locked block number.
    std::optional<EphemeralLockInZooKeeper> delimiting_block_lock;

    if (!getFakePartCoveringAllPartsInPartition(partition_id, drop_range_info, delimiting_block_lock))
    {
        LOG_INFO(log, "Will not drop partition {}, it is empty.", partition_id);
        return false;
    }

    /// Cancel concurrent inserts in range
    clearLockedBlockNumbersInPartition(zookeeper, partition_id, drop_range_info.min_block, drop_range_info.max_block);

    clearBlocksInPartition(zookeeper, partition_id, drop_range_info.min_block, drop_range_info.max_block);

    String drop_range_fake_part_name = getPartNamePossiblyFake(format_version, drop_range_info);

    LOG_DEBUG(log, "Disabled merges covered by range {}", drop_range_fake_part_name);

    /// Finally, having achieved the necessary invariants, you can put an entry in the log.
    auto entry = std::make_shared<LogEntry>();
    entry->type = LogEntry::DROP_RANGE;
    entry->source_replica = replica_name;
    entry->new_part_name = drop_range_fake_part_name;
    entry->detach = detach;
    entry->create_time = time(nullptr);

    log_entry_ops_idx.push_back(ops.size());
    ops.emplace_back(zkutil::makeCreateRequest(fs::path(zookeeper_path) / "log/log-", entry->toString(),
                                               zkutil::CreateMode::PersistentSequential));
    delimiting_block_lock->getUnlockOp(ops);
    delimiting_block_locks.push_back(std::move(*delimiting_block_lock));
    entries.push_back(std::move(entry));
    return true;
}

void StorageReplicatedMergeTree::dropAllPartsInPartitions(
    zkutil::ZooKeeper & zookeeper, const Strings & partition_ids, std::vector<LogEntryPtr> & entries, ContextPtr query_context, bool detach)
{
    entries.reserve(partition_ids.size());

    /// Retry if alter_partition_version changes
    for (size_t retry = 0; retry < 1000; ++retry)
    {
        entries.clear();
        String alter_partition_version_path = zookeeper_path + "/alter_partition_version";
        Coordination::Stat alter_partition_version_stat;
        zookeeper.get(alter_partition_version_path, &alter_partition_version_stat);

        Coordination::Requests ops;
        std::vector<EphemeralLockInZooKeeper> delimiting_block_locks;
        std::vector<size_t> log_entry_ops_idx;
        ops.reserve(partition_ids.size() * 2);
        delimiting_block_locks.reserve(partition_ids.size());
        log_entry_ops_idx.reserve(partition_ids.size());
        for (const auto & partition_id : partition_ids)
            addOpsToDropAllPartsInPartition(zookeeper, partition_id, detach, ops, entries, delimiting_block_locks, log_entry_ops_idx);

        /// Check and update version to avoid race with REPLACE_RANGE.
        /// Otherwise new parts covered by drop_range_info may appear after execution of current DROP_RANGE entry
        /// as a result of execution of concurrently created REPLACE_RANGE entry.
        ops.emplace_back(zkutil::makeSetRequest(alter_partition_version_path, "", alter_partition_version_stat.version));

        /// Just update version, because merges assignment relies on it
        ops.emplace_back(zkutil::makeSetRequest(fs::path(zookeeper_path) / "log", "", -1));

        if (auto txn = query_context->getZooKeeperMetadataTransaction())
            txn->moveOpsTo(ops);

        Coordination::Responses responses;
        Coordination::Error code = zookeeper.tryMulti(ops, responses);

        if (code == Coordination::Error::ZOK)
        {
            for (auto & lock : delimiting_block_locks)
                lock.assumeUnlocked();
        }
        else if (code == Coordination::Error::ZBADVERSION)
        {
            /// Cannot retry automatically, because some zookeeper ops were lost on the first attempt. Will retry on DDLWorker-level.
            if (query_context->getZooKeeperMetadataTransaction())
                throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER,
                                    "Cannot execute alter, because alter partition version was suddenly changed due "
                                    "to concurrent alter");
            continue;
        }
        else
            zkutil::KeeperMultiException::check(code, ops, responses);

        assert(entries.size() == log_entry_ops_idx.size());
        for (size_t i = 0; i < entries.size(); ++i)
        {
            String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*responses[log_entry_ops_idx[i]]).path_created;
            entries[i]->znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

            auto drop_range_info = MergeTreePartInfo::fromPartName(entries[i]->new_part_name, format_version);
            getContext()->getMergeList().cancelInPartition(getStorageID(), drop_range_info.partition_id, drop_range_info.max_block);
        }

        return;
    }
    throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER,
                    "Cannot assign ALTER PARTITION because another ALTER PARTITION query was concurrently executed");
}

StorageReplicatedMergeTree::LogEntryPtr StorageReplicatedMergeTree::dropAllPartsInPartition(
    zkutil::ZooKeeper & zookeeper, const String & partition_id, ContextPtr query_context, bool detach)
{
    Strings partition_ids = {partition_id};
    std::vector<LogEntryPtr> entries;
    dropAllPartsInPartitions(zookeeper, partition_ids, entries, query_context, detach);
    if (entries.empty())
        return {};
    return entries[0];
}

void StorageReplicatedMergeTree::enqueuePartForCheck(const String & part_name, time_t delay_to_check_seconds)
{
    MergeTreePartInfo covering_drop_range;
    /// NOTE This check is just an optimization, it's not reliable because drop entry could be removed concurrently.
    /// See also ReplicatedMergeTreePartCheckThread::cancelRemovedPartsCheck
    if (queue.isGoingToBeDropped(MergeTreePartInfo::fromPartName(part_name, format_version), &covering_drop_range))
    {
        LOG_WARNING(log, "Do not enqueue part {} for check because it's covered by drop range {} and going to be removed",
                    part_name, covering_drop_range.getPartNameForLogs());
        return;
    }
    part_check_thread.enqueuePart(part_name, delay_to_check_seconds);
}

CheckResults StorageReplicatedMergeTree::checkData(const ASTPtr & query, ContextPtr local_context)
{
    CheckResults results;
    DataPartsVector data_parts;
    if (const auto & check_query = query->as<ASTCheckQuery &>(); check_query.partition)
    {
        String partition_id = getPartitionIDFromQuery(check_query.partition, local_context);
        data_parts = getVisibleDataPartsVectorInPartition(local_context, partition_id);
    }
    else
        data_parts = getVisibleDataPartsVector(local_context);

    {
        auto part_check_lock = part_check_thread.pausePartsCheck();

        for (auto & part : data_parts)
        {
            try
            {
                results.push_back(part_check_thread.checkPartAndFix(part->name));
            }
            catch (const Exception & ex)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                results.emplace_back(part->name, false, "Check of part finished with error: '" + ex.message() + "'");
            }
        }
    }

    return results;
}


bool StorageReplicatedMergeTree::canUseZeroCopyReplication() const
{
    auto settings_ptr = getSettings();
    if (!settings_ptr->allow_remote_fs_zero_copy_replication)
        return false;

    auto disks = getStoragePolicy()->getDisks();
    for (const auto & disk : disks)
    {
        if (disk->supportZeroCopyReplication())
            return true;
    }
    return false;
}

void StorageReplicatedMergeTree::checkBrokenDisks()
{
    auto disks = getStoragePolicy()->getDisks();
    std::unique_ptr<DataPartsVector> parts;

    for (auto disk_it = disks.rbegin(); disk_it != disks.rend(); ++disk_it)
    {
        auto disk_ptr = *disk_it;
        if (disk_ptr->isBroken())
        {
            {
                std::lock_guard lock(last_broken_disks_mutex);
                if (!last_broken_disks.insert(disk_ptr->getName()).second)
                    continue;
            }

            LOG_INFO(log, "Scanning parts to recover on broken disk {} with path {}", disk_ptr->getName(), disk_ptr->getPath());

            if (!parts)
                parts = std::make_unique<DataPartsVector>(getDataPartsVectorForInternalUsage());

            for (auto & part : *parts)
            {
                if (part->getDataPartStorage().getDiskName() == disk_ptr->getName())
                    broken_part_callback(part->name);
            }
            continue;
        }
        else
        {
            {
                std::lock_guard lock(last_broken_disks_mutex);
                if (last_broken_disks.erase(disk_ptr->getName()) > 0)
                    LOG_INFO(
                        log,
                        "Disk {} with path {} is recovered. Exclude it from last_broken_disks",
                        disk_ptr->getName(),
                        disk_ptr->getPath());
            }
        }
    }
}


bool StorageReplicatedMergeTree::canUseAdaptiveGranularity() const
{
    const auto storage_settings_ptr = getSettings();
    return storage_settings_ptr->index_granularity_bytes != 0 &&
        (storage_settings_ptr->enable_mixed_granularity_parts ||
            (!has_non_adaptive_index_granularity_parts && !other_replicas_fixed_granularity));
}


std::map<int64_t, MutationCommands> StorageReplicatedMergeTree::getAlterMutationCommandsForPart(const DataPartPtr & part) const
{
    return queue.getAlterMutationCommandsForPart(part);
}


void StorageReplicatedMergeTree::startBackgroundMovesIfNeeded()
{
    if (areBackgroundMovesNeeded())
        background_moves_assignee.start();
}


std::unique_ptr<MergeTreeSettings> StorageReplicatedMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getReplicatedMergeTreeSettings());
}

String StorageReplicatedMergeTree::getTableSharedID() const
{
    std::lock_guard lock(table_shared_id_mutex);

    /// If we has metadata or, we don't know about metadata -- try to create shared ID
    /// Otherwise table is already dropped, doesn't make sense to do anything with shared ID
    if (has_metadata_in_zookeeper.value_or(true))
    {
        /// Can happen if table was partially initialized before drop by DatabaseCatalog
        if (table_shared_id == UUIDHelpers::Nil)
            createTableSharedID();
    }
    else
    {
        return toString(UUIDHelpers::Nil);
    }

    return toString(table_shared_id);
}

size_t StorageReplicatedMergeTree::getNumberOfUnfinishedMutations() const
{
    return queue.countUnfinishedMutations();
}

void StorageReplicatedMergeTree::createTableSharedID() const
{
    LOG_DEBUG(log, "Creating shared ID for table {}", getStorageID().getNameForLogs());
    // can be set by the call to getTableSharedID
    if (table_shared_id != UUIDHelpers::Nil)
    {
        LOG_INFO(log, "Shared ID already set to {}", table_shared_id);
        return;
    }

    /// We may call getTableSharedID when table is shut down. If exception happen, restarting thread will be already turned
    /// off and nobody will reconnect our zookeeper connection. In this case we use zookeeper connection from
    /// context.
    ZooKeeperPtr zookeeper;
    if (shutdown_called.load())
        zookeeper = getZooKeeperIfTableShutDown();
    else
        zookeeper = getZooKeeper();

    String zookeeper_table_id_path = fs::path(zookeeper_path) / "table_shared_id";
    String id;
    if (!zookeeper->tryGet(zookeeper_table_id_path, id))
    {
        LOG_DEBUG(log, "Shared ID for table {} doesn't exist in ZooKeeper on path {}", getStorageID().getNameForLogs(), zookeeper_table_id_path);
        UUID table_id_candidate;
        auto local_storage_id = getStorageID();
        if (local_storage_id.uuid != UUIDHelpers::Nil)
            table_id_candidate = local_storage_id.uuid;
        else
            table_id_candidate = UUIDHelpers::generateV4();

        id = toString(table_id_candidate);
        LOG_DEBUG(log, "Got candidate ID {}, will try to create it in ZooKeeper on path {}", id, zookeeper_table_id_path);

        auto code = zookeeper->tryCreate(zookeeper_table_id_path, id, zkutil::CreateMode::Persistent);
        if (code == Coordination::Error::ZNODEEXISTS)
        { /// Other replica create node early
            id = zookeeper->get(zookeeper_table_id_path);
            LOG_DEBUG(log, "Shared ID on path {} concurrently created, will set ID {}", zookeeper_table_id_path, id);
        }
        else if (code == Coordination::Error::ZNONODE) /// table completely dropped, we can choose any id we want
        {
            id = toString(UUIDHelpers::Nil);
            LOG_DEBUG(log, "Table was completely drop, we can use anything as ID (will use {})", id);
        }
        else if (code != Coordination::Error::ZOK)
        {
            throw zkutil::KeeperException::fromPath(code, zookeeper_table_id_path);
        }
    }

    LOG_DEBUG(log, "Initializing table shared ID with {}", id);
    table_shared_id = parseFromString<UUID>(id);
}


std::optional<String> StorageReplicatedMergeTree::tryGetTableSharedIDFromCreateQuery(const IAST & create_query, const ContextPtr & global_context)
{
    auto zk_path = tryExtractZkPathFromCreateQuery(create_query, global_context);
    if (!zk_path)
        return {};

    String zk_name = zkutil::extractZooKeeperName(*zk_path);
    zk_path = zkutil::extractZooKeeperPath(*zk_path, false, nullptr);
    zkutil::ZooKeeperPtr zookeeper = (zk_name == getDefaultZooKeeperName()) ? global_context->getZooKeeper() : global_context->getAuxiliaryZooKeeper(zk_name);

    String id;
    if (!zookeeper->tryGet(fs::path(*zk_path) / "table_shared_id", id))
        return {};

    return id;
}


zkutil::EphemeralNodeHolderPtr StorageReplicatedMergeTree::lockSharedDataTemporary(const String & part_name, const String & part_id, const DiskPtr & disk) const
{
    auto settings = getSettings();

    if (!disk || !disk->supportZeroCopyReplication() || !settings->allow_remote_fs_zero_copy_replication)
        return {};

    zkutil::ZooKeeperPtr zookeeper = tryGetZooKeeper();
    if (!zookeeper)
        return {};

    String id = part_id;
    boost::replace_all(id, "/", "_");

    String zc_zookeeper_path = getZeroCopyPartPath(*getSettings(), toString(disk->getDataSourceDescription().type), getTableSharedID(),
        part_name, zookeeper_path)[0];

    String zookeeper_node = fs::path(zc_zookeeper_path) / id / replica_name;

    LOG_TRACE(log, "Set zookeeper temporary ephemeral lock {}", zookeeper_node);
    createZeroCopyLockNode(
        std::make_shared<ZooKeeperWithFaultInjection>(zookeeper), zookeeper_node, zkutil::CreateMode::Ephemeral, false);

    LOG_TRACE(log, "Zookeeper temporary ephemeral lock {} created", zookeeper_node);
    return zkutil::EphemeralNodeHolder::existing(zookeeper_node, *zookeeper);
}

void StorageReplicatedMergeTree::lockSharedData(
    const IMergeTreeDataPart & part,
    bool replace_existing_lock,
    std::optional<HardlinkedFiles> hardlinked_files) const
{
    LOG_DEBUG(log, "Trying to create zero-copy lock for part {}", part.name);
    auto zookeeper = tryGetZooKeeper();
    if (zookeeper)
        return lockSharedData(part, std::make_shared<ZooKeeperWithFaultInjection>(zookeeper), replace_existing_lock, hardlinked_files);
    else
        return lockSharedData(part, std::make_shared<ZooKeeperWithFaultInjection>(nullptr), replace_existing_lock, hardlinked_files);
}

void StorageReplicatedMergeTree::getLockSharedDataOps(
    const IMergeTreeDataPart & part,
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    bool replace_existing_lock,
    std::optional<HardlinkedFiles> hardlinked_files,
    Coordination::Requests & requests) const
{
    auto settings = getSettings();

    if (!part.isStoredOnDisk() || !settings->allow_remote_fs_zero_copy_replication)
        return;

    if (!part.getDataPartStorage().supportZeroCopyReplication())
        return;

    if (zookeeper->isNull())
        return;

    String id = part.getUniqueId();
    boost::replace_all(id, "/", "_");

    Strings zc_zookeeper_paths = getZeroCopyPartPath(
        *getSettings(), part.getDataPartStorage().getDiskType(), getTableSharedID(),
        part.name, zookeeper_path);

    String path_to_set_hardlinked_files;
    NameSet hardlinks;

    if (hardlinked_files.has_value() && !hardlinked_files->hardlinks_from_source_part.empty())
    {
        path_to_set_hardlinked_files = getZeroCopyPartPath(
            *getSettings(), part.getDataPartStorage().getDiskType(), hardlinked_files->source_table_shared_id,
            hardlinked_files->source_part_name, zookeeper_path)[0];

        hardlinks = hardlinked_files->hardlinks_from_source_part;
    }

    for (const auto & zc_zookeeper_path : zc_zookeeper_paths)
    {
        String zookeeper_node = fs::path(zc_zookeeper_path) / id / replica_name;

        if (!path_to_set_hardlinked_files.empty() && !hardlinks.empty())
        {
            LOG_DEBUG(log, "Locking shared node {} with hardlinks from the other shared node {}, "
                           "hardlinks: [{}]",
                      zookeeper_node, path_to_set_hardlinked_files,
                      boost::algorithm::join(hardlinks, ","));
        }

        getZeroCopyLockNodeCreateOps(
            zookeeper, zookeeper_node, requests, zkutil::CreateMode::Persistent,
            replace_existing_lock, path_to_set_hardlinked_files, hardlinks);
    }
}


void StorageReplicatedMergeTree::lockSharedData(
    const IMergeTreeDataPart & part,
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    bool replace_existing_lock,
    std::optional<HardlinkedFiles> hardlinked_files) const
{
    auto settings = getSettings();

    if (!part.isStoredOnDisk() || !settings->allow_remote_fs_zero_copy_replication)
        return;

    if (!part.getDataPartStorage().supportZeroCopyReplication())
        return;

    if (zookeeper->isNull())
        return;

    String id = part.getUniqueId();
    boost::replace_all(id, "/", "_");

    Strings zc_zookeeper_paths = getZeroCopyPartPath(
        *getSettings(), part.getDataPartStorage().getDiskType(), getTableSharedID(),
        part.name, zookeeper_path);

    String path_to_set_hardlinked_files;
    NameSet hardlinks;

    if (hardlinked_files.has_value() && !hardlinked_files->hardlinks_from_source_part.empty())
    {
        path_to_set_hardlinked_files = getZeroCopyPartPath(
            *getSettings(), part.getDataPartStorage().getDiskType(), hardlinked_files->source_table_shared_id,
            hardlinked_files->source_part_name, zookeeper_path)[0];

        hardlinks = hardlinked_files->hardlinks_from_source_part;
    }

    for (const auto & zc_zookeeper_path : zc_zookeeper_paths)
    {
        String zookeeper_node = fs::path(zc_zookeeper_path) / id / replica_name;

        LOG_TRACE(log, "Trying to create zookeeper persistent lock {} with hardlinks [{}]", zookeeper_node, fmt::join(hardlinks, ", "));

        createZeroCopyLockNode(
            zookeeper, zookeeper_node, zkutil::CreateMode::Persistent,
            replace_existing_lock, path_to_set_hardlinked_files, hardlinks);

        LOG_TRACE(log, "Zookeeper persistent lock {} created", zookeeper_node);
    }
}

std::pair<bool, NameSet>
StorageReplicatedMergeTree::unlockSharedData(const IMergeTreeDataPart & part) const
{
    return unlockSharedData(part, std::make_shared<ZooKeeperWithFaultInjection>(nullptr));
}

std::pair<bool, NameSet>
StorageReplicatedMergeTree::unlockSharedData(const IMergeTreeDataPart & part, const ZooKeeperWithFaultInjectionPtr & zookeeper) const
{
    auto settings = getSettings();
    if (!settings->allow_remote_fs_zero_copy_replication)
        return std::make_pair(true, NameSet{});

    if (!part.isStoredOnDisk())
    {
        LOG_TRACE(log, "Part {} is not stored on disk, blobs can be removed", part.name);
        return std::make_pair(true, NameSet{});
    }

    if (!part.getDataPartStorage().supportZeroCopyReplication())
    {
        LOG_TRACE(log, "Part {} is not stored on zero-copy replicated disk, blobs can be removed", part.name);
        return std::make_pair(true, NameSet{});
    }

    auto shared_id = getTableSharedID();
    if (shared_id == toString(UUIDHelpers::Nil))
    {
        if (zookeeper->exists(zookeeper_path))
        {
            LOG_WARNING(log, "Not removing shared data for part {} because replica does not have metadata in ZooKeeper, "
                             "but table path exist and other replicas may exist. It may leave some garbage on S3", part.name);
            return std::make_pair(false, NameSet{});
        }
        LOG_TRACE(log, "Part {} blobs can be removed, because table {} completely dropped", part.name, getStorageID().getNameForLogs());
        return std::make_pair(true, NameSet{});
    }

    /// If part is temporary refcount file may be absent
    if (part.getDataPartStorage().exists(IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK))
    {
        auto ref_count = part.getDataPartStorage().getRefCount(IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK);
        if (ref_count > 0) /// Keep part shard info for frozen backups
        {
            LOG_TRACE(log, "Part {} has more than zero local references ({}), blobs cannot be removed", part.name, ref_count);
            return std::make_pair(false, NameSet{});
        }
        else
        {
            LOG_TRACE(log, "Part {} local references is zero, will check blobs can be removed in zookeeper", part.name);
        }
    }
    else
    {
        LOG_TRACE(log, "Part {} looks temporary, because {} file doesn't exists, blobs can be removed", part.name, IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK);
        /// Temporary part with some absent file cannot be locked in shared mode
        return std::make_pair(true, NameSet{});
    }

    if (part.getState() == MergeTreeDataPartState::Temporary && part.is_temp)
    {
        /// Part {} is in temporary state and has it_temp flag. it means that it is under construction.
        /// That path hasn't been added to active set, no commit procedure has begun.
        /// The metadata files is about to delete now. Clichouse has to make a decision remove or preserve blobs on remote FS.
        /// In general remote data might be shared and has to be unlocked in the keeper before removing.
        /// However there are some cases when decision is clear without asking keeper:
        /// When the part has been fetched then remote data has to be preserved, part doesn't own it.
        /// When the part has been merged then remote data can be removed, part owns it.
        /// In opposition, when the part has been mutated in generally it hardlinks the files from source part.
        /// Therefore remote data could be shared, it has to be unlocked in the keeper.
        /// In order to track all that cases remove_tmp_policy is used.
        /// Clickhouse set that field as REMOVE_BLOBS or PRESERVE_BLOBS when it sure about the decision without asking keeper.

        if (part.remove_tmp_policy == IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS
            || part.remove_tmp_policy == IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::PRESERVE_BLOBS)
        {
            bool can_remove_blobs = part.remove_tmp_policy == IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;
            LOG_INFO(log, "Looks like CH knows the origin of that part. "
                          "Part {} can be deleted without unlocking shared data in zookeeper. "
                          "Part blobs {}.",
                     part.name,
                     can_remove_blobs ? "will be removed" : "have to be preserved");
            return std::make_pair(can_remove_blobs, NameSet{});
        }
    }

    if (has_metadata_in_zookeeper.has_value() && !has_metadata_in_zookeeper)
    {
        if (zookeeper->exists(zookeeper_path))
        {
            LOG_WARNING(log, "Not removing shared data for part {} because replica does not have metadata in ZooKeeper, "
                             "but table path exist and other replicas may exist. It may leave some garbage on S3", part.name);
            return std::make_pair(false, NameSet{});
        }

        /// If table was completely dropped (no meta in zookeeper) we can safely remove parts
        return std::make_pair(true, NameSet{});
    }

    /// We remove parts during table shutdown. If exception happen, restarting thread will be already turned
    /// off and nobody will reconnect our zookeeper connection. In this case we use zookeeper connection from
    /// context.
    if (shutdown_called.load())
        zookeeper->setKeeper(getZooKeeperIfTableShutDown());
    else
        zookeeper->setKeeper(getZooKeeper());

    /// It can happen that we didn't had the connection to zookeeper during table creation, but actually
    /// table is completely dropped, so we can drop it without any additional checks.
    if (!has_metadata_in_zookeeper.has_value() && !zookeeper->exists(zookeeper_path))
        return std::make_pair(true, NameSet{});

    return unlockSharedDataByID(
        part.getUniqueId(), shared_id, part.info, replica_name,
        part.getDataPartStorage().getDiskType(), zookeeper, *getSettings(), log, zookeeper_path, format_version);
}

namespace
{

/// What is going on here?
/// Actually we need this code because of flaws in hardlinks tracking. When we create child part during mutation we can hardlink some files from parent part, like
/// all_0_0_0:
///                     a.bin a.mrk2 columns.txt ...
/// all_0_0_0_1:          ^     ^
///                     a.bin a.mrk2 columns.txt
/// So when we deleting all_0_0_0 it doesn't remove blobs for a.bin and a.mrk2 because all_0_0_0_1 use them.
/// But sometimes we need an opposite. When we deleting all_0_0_0_1 it can be non replicated to other replicas, so we are the only owner of this part.
/// In this case when we will drop all_0_0_0_1 we will drop blobs for all_0_0_0. But it will lead to dataloss. For such case we need to check that other replicas
/// still need parent part.
std::pair<bool, std::optional<NameSet>> getParentLockedBlobs(const ZooKeeperWithFaultInjectionPtr & zookeeper_ptr, const std::string & zero_copy_part_path_prefix, const MergeTreePartInfo & part_info, MergeTreeDataFormatVersion format_version, Poco::Logger * log)
{
    NameSet files_not_to_remove;

    /// No mutations -- no hardlinks -- no issues
    if (part_info.mutation == 0)
        return {false, files_not_to_remove};

    /// Getting all zero copy parts
    Strings parts_str;
    zookeeper_ptr->tryGetChildren(zero_copy_part_path_prefix, parts_str);

    /// Parsing infos. It's hard to convert info -> string for old-format merge tree
    /// so storing string as is.
    std::vector<std::pair<MergeTreePartInfo, std::string>> parts_infos;
    for (const auto & part_str : parts_str)
    {
        MergeTreePartInfo parent_candidate_info = MergeTreePartInfo::fromPartName(part_str, format_version);
        parts_infos.emplace_back(parent_candidate_info, part_str);
    }

    /// Sort is important. We need to find our closest parent, like:
    /// for part all_0_0_0_64 we can have parents
    /// all_0_0_0_6 < we need the closest parent, not others
    /// all_0_0_0_1
    /// all_0_0_0
    std::sort(parts_infos.begin(), parts_infos.end());
    std::string part_info_str = part_info.getPartNameV1();

    /// In reverse order to process from bigger to smaller
    for (const auto & [parent_candidate_info, part_candidate_info_str] : parts_infos | std::views::reverse)
    {
        if (parent_candidate_info == part_info)
            continue;

        /// We are mutation child of this parent
        if (part_info.isMutationChildOf(parent_candidate_info))
        {
            LOG_TRACE(log, "Found mutation parent {} for part {}", part_candidate_info_str, part_info_str);
            /// Get hardlinked files
            String files_not_to_remove_str;
            Coordination::Error code;
            zookeeper_ptr->tryGet(fs::path(zero_copy_part_path_prefix) / part_candidate_info_str, files_not_to_remove_str, nullptr, nullptr, &code);
            if (code != Coordination::Error::ZOK)
            {
                LOG_TRACE(log, "Cannot get parent files from ZooKeeper on path ({}), error {}", (fs::path(zero_copy_part_path_prefix) / part_candidate_info_str).string(), code);
                return {true, std::nullopt};
            }

            if (!files_not_to_remove_str.empty())
            {
                boost::split(files_not_to_remove, files_not_to_remove_str, boost::is_any_of("\n "));
                LOG_TRACE(log, "Found files not to remove from parent part {}: [{}]", part_candidate_info_str, fmt::join(files_not_to_remove, ", "));
            }
            else
            {
                std::vector<std::string> children;
                code = zookeeper_ptr->tryGetChildren(fs::path(zero_copy_part_path_prefix) / part_candidate_info_str, children);
                if (code != Coordination::Error::ZOK)
                {
                    LOG_TRACE(log, "Cannot get parent locks in ZooKeeper on path ({}), error {}", (fs::path(zero_copy_part_path_prefix) / part_candidate_info_str).string(), errorMessage(code));
                    return {true, std::nullopt};
                }

                if (children.size() > 1 || (children.size() == 1 && children[0] != ZeroCopyLock::ZERO_COPY_LOCK_NAME))
                {
                    LOG_TRACE(log, "No files not to remove found for part {} from parent {}", part_info_str, part_candidate_info_str);
                }
                else
                {
                    /// The case when part is actually removed, but some stale replica trying to execute merge/mutation.
                    /// We shouldn't use the part to check hardlinked blobs, it just doesn't exist.
                    LOG_TRACE(log, "Part {} is not parent (only merge/mutation locks exist), refusing to use as parent", part_candidate_info_str);
                    continue;
                }
            }

            return {true, files_not_to_remove};
        }
    }
    LOG_TRACE(log, "No mutation parent found for part {}", part_info_str);
    return {false, files_not_to_remove};
}

}

std::pair<bool, NameSet> StorageReplicatedMergeTree::unlockSharedDataByID(
        String part_id, const String & table_uuid, const MergeTreePartInfo & part_info,
        const String & replica_name_, const std::string & disk_type, const ZooKeeperWithFaultInjectionPtr & zookeeper_ptr, const MergeTreeSettings & settings,
        Poco::Logger * logger, const String & zookeeper_path_old, MergeTreeDataFormatVersion data_format_version)
{
    boost::replace_all(part_id, "/", "_");

    auto part_name = part_info.getPartNameV1();

    Strings zc_zookeeper_paths = getZeroCopyPartPath(settings, disk_type, table_uuid, part_name, zookeeper_path_old);

    bool part_has_no_more_locks = true;
    NameSet files_not_to_remove;

    for (const auto & zc_zookeeper_path : zc_zookeeper_paths)
    {
        String files_not_to_remove_str;
        zookeeper_ptr->tryGet(zc_zookeeper_path, files_not_to_remove_str);

        files_not_to_remove.clear();
        if (!files_not_to_remove_str.empty())
            boost::split(files_not_to_remove, files_not_to_remove_str, boost::is_any_of("\n "));

        String zookeeper_part_uniq_node = fs::path(zc_zookeeper_path) / part_id;

        /// Delete our replica node for part from zookeeper (we are not interested in it anymore)
        String zookeeper_part_replica_node = fs::path(zookeeper_part_uniq_node) / replica_name_;

        auto [has_parent, parent_not_to_remove] = getParentLockedBlobs(
            zookeeper_ptr, fs::path(zc_zookeeper_path).parent_path(), part_info, data_format_version, logger);

        // parent_not_to_remove == std::nullopt means that we were unable to retrieve parts set
        if (has_parent && parent_not_to_remove == std::nullopt)
        {
            LOG_TRACE(logger, "Failed to get mutation parent on {} for part {}, refusing to remove blobs", zookeeper_part_replica_node, part_name);
            return {false, {}};
        }

        files_not_to_remove.insert(parent_not_to_remove->begin(), parent_not_to_remove->end());


        LOG_TRACE(logger, "Remove zookeeper lock {} for part {}", zookeeper_part_replica_node, part_name);

        if (auto ec = zookeeper_ptr->tryRemove(zookeeper_part_replica_node); ec != Coordination::Error::ZOK)
        {
            /// Very complex case. It means that lock already doesn't exist when we tried to remove it.
            /// So we don't know are we owner of this part or not. Maybe we just mutated it, renamed on disk and failed to lock in ZK.
            /// But during mutation we can have hardlinks to another part. So it's not Ok to remove blobs of this part if it was mutated.
            if (ec == Coordination::Error::ZNONODE)
            {
                if (has_parent)
                {
                    LOG_INFO(logger, "Lock on path {} for part {} doesn't exist, refuse to remove blobs", zookeeper_part_replica_node, part_name);
                    return {false, {}};
                }
                else
                {
                    LOG_INFO(logger, "Lock on path {} for part {} doesn't exist, but we don't have mutation parent, can remove blobs", zookeeper_part_replica_node, part_name);
                }
            }
            else
            {
                throw zkutil::KeeperException::fromPath(ec, zookeeper_part_replica_node);
            }
        }

        /// Check, maybe we were the last replica and can remove part forever
        Strings children;
        zookeeper_ptr->tryGetChildren(zookeeper_part_uniq_node, children);

        if (!children.empty())
        {
            LOG_TRACE(logger, "Found {} ({}) zookeeper locks for {}", children.size(), fmt::join(children, ", "), zookeeper_part_uniq_node);
            part_has_no_more_locks = false;
            continue;
        }
        else
        {
            LOG_TRACE(logger, "No more children left for for {}, will try to remove the whole node", zookeeper_part_uniq_node);
        }

        auto error_code = zookeeper_ptr->tryRemove(zookeeper_part_uniq_node);

        if (error_code == Coordination::Error::ZOK)
        {
            LOG_TRACE(logger, "Removed last parent zookeeper lock {} for part {} with id {}", zookeeper_part_uniq_node, part_name, part_id);
        }
        else if (error_code == Coordination::Error::ZNOTEMPTY)
        {
            LOG_TRACE(logger, "Cannot remove last parent zookeeper lock {} for part {} with id {}, another replica locked part concurrently", zookeeper_part_uniq_node, part_name, part_id);
        }
        else if (error_code == Coordination::Error::ZNONODE)
        {
            LOG_TRACE(logger, "Node with parent zookeeper lock {} for part {} with id {} doesn't exist", zookeeper_part_uniq_node, part_name, part_id);
        }
        else
        {
            throw zkutil::KeeperException::fromPath(error_code, zookeeper_part_uniq_node);
        }


        /// Even when we have lock with same part name, but with different uniq, we can remove files on S3
        children.clear();
        String zookeeper_part_node = fs::path(zookeeper_part_uniq_node).parent_path();
        zookeeper_ptr->tryGetChildren(zookeeper_part_node, children);

        if (children.empty())
        {
            /// Cleanup after last uniq removing
            error_code = zookeeper_ptr->tryRemove(zookeeper_part_node);

            if (error_code == Coordination::Error::ZOK)
            {
                LOG_TRACE(logger, "Removed last parent zookeeper lock {} for part {} (part is finally unlocked)", zookeeper_part_node, part_name);
            }
            else if (error_code == Coordination::Error::ZNOTEMPTY)
            {
                LOG_TRACE(logger, "Cannot remove last parent zookeeper lock {} for part {}, another replica locked part concurrently", zookeeper_part_uniq_node, part_name);
            }
            else if (error_code == Coordination::Error::ZNONODE)
            {
                /// We don't know what to do, because this part can be mutation part
                /// with hardlinked columns. Since we don't have this information (about blobs not to remove)
                /// we refuse to remove blobs.
                LOG_WARNING(logger, "Node with parent zookeeper lock {} for part {} doesn't exist (part was unlocked before), refuse to remove blobs", zookeeper_part_uniq_node, part_name);
                return {false, {}};
            }
            else
            {
                throw zkutil::KeeperException::fromPath(error_code, zookeeper_part_uniq_node);
            }
        }
        else
        {
            LOG_TRACE(logger, "Can't remove parent zookeeper lock {} for part {}, because children {} ({}) exists",
                zookeeper_part_node, part_name, children.size(), fmt::join(children, ", "));
        }
    }

    return std::make_pair(part_has_no_more_locks, files_not_to_remove);
}


MergeTreeData::MutableDataPartPtr StorageReplicatedMergeTree::tryToFetchIfShared(
    const IMergeTreeDataPart & part,
    const DiskPtr & disk,
    const String & path)
{
    const auto settings = getSettings();
    auto data_source_description = disk->getDataSourceDescription();
    if (!(disk->supportZeroCopyReplication() && settings->allow_remote_fs_zero_copy_replication))
        return nullptr;

    String replica = getSharedDataReplica(part, data_source_description.type);

    /// We can't fetch part when none replicas have this part on a same type remote disk
    if (replica.empty())
        return nullptr;

    return executeFetchShared(replica, part.name, disk, path);
}

String StorageReplicatedMergeTree::getSharedDataReplica(
    const IMergeTreeDataPart & part, DataSourceType data_source_type) const
{
    String best_replica;

    zkutil::ZooKeeperPtr zookeeper = tryGetZooKeeper();
    if (!zookeeper)
        return "";

    Strings zc_zookeeper_paths = getZeroCopyPartPath(*getSettings(), toString(data_source_type), getTableSharedID(), part.name,
            zookeeper_path);

    std::set<String> replicas;

    for (const auto & zc_zookeeper_path : zc_zookeeper_paths)
    {
        Strings ids;
        zookeeper->tryGetChildren(zc_zookeeper_path, ids);

        for (const auto & id : ids)
        {
            String zookeeper_part_uniq_node = fs::path(zc_zookeeper_path) / id;
            Strings id_replicas;
            zookeeper->tryGetChildren(zookeeper_part_uniq_node, id_replicas);
            LOG_TRACE(log, "Found zookeeper replicas for {}: {}", zookeeper_part_uniq_node, id_replicas.size());
            replicas.insert(id_replicas.begin(), id_replicas.end());
        }
    }

    LOG_TRACE(log, "Found zookeeper replicas for part {}: {}", part.name, replicas.size());

    Strings active_replicas;

    /// TODO: Move best replica choose in common method (here is the same code as in StorageReplicatedMergeTree::fetchPartition)

    /// Leave only active replicas.
    active_replicas.reserve(replicas.size());

    for (const String & replica : replicas)
        if ((replica != replica_name) && (zookeeper->exists(fs::path(zookeeper_path) / "replicas" / replica / "is_active")))
            active_replicas.push_back(replica);

    LOG_TRACE(log, "Found zookeeper active replicas for part {}: {}", part.name, active_replicas.size());

    if (active_replicas.empty())
        return "";

    /** You must select the best (most relevant) replica.
    * This is a replica with the maximum `log_pointer`, then with the minimum `queue` size.
    * NOTE This is not exactly the best criteria. It does not make sense to download old partitions,
    *  and it would be nice to be able to choose the replica closest by network.
    * NOTE Of course, there are data races here. You can solve it by retrying.
    */
    Int64 max_log_pointer = -1;
    UInt64 min_queue_size = std::numeric_limits<UInt64>::max();

    for (const String & replica : active_replicas)
    {
        String current_replica_path = fs::path(zookeeper_path) / "replicas" / replica;

        String log_pointer_str = zookeeper->get(fs::path(current_replica_path) / "log_pointer");
        Int64 log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);

        Coordination::Stat stat;
        zookeeper->get(fs::path(current_replica_path) / "queue", &stat);
        size_t queue_size = stat.numChildren;

        if (log_pointer > max_log_pointer
            || (log_pointer == max_log_pointer && queue_size < min_queue_size))
        {
            max_log_pointer = log_pointer;
            min_queue_size = queue_size;
            best_replica = replica;
        }
    }

    return best_replica;
}

Strings StorageReplicatedMergeTree::getZeroCopyPartPath(
    const MergeTreeSettings & settings, const std::string & disk_type, const String & table_uuid,
    const String & part_name, const String & zookeeper_path_old)
{
    Strings res;

    String zero_copy = fmt::format("zero_copy_{}", disk_type);

    String new_path = fs::path(settings.remote_fs_zero_copy_zookeeper_path.toString()) / zero_copy / table_uuid / part_name;
    res.push_back(std::move(new_path));
    if (settings.remote_fs_zero_copy_path_compatible_mode && !zookeeper_path_old.empty())
    { /// Compatibility mode for cluster with old and new versions
        String old_path = fs::path(zookeeper_path_old) / zero_copy / "shared" / part_name;
        res.push_back(std::move(old_path));
    }

    return res;
}

void StorageReplicatedMergeTree::watchZeroCopyLock(const String & part_name, const DiskPtr & disk)
{
    auto path = getZeroCopyPartPath(part_name, disk);
    if (path)
    {
        auto zookeeper = getZooKeeper();
        auto lock_path = fs::path(*path) / "part_exclusive_lock";
        LOG_TEST(log, "Adding zero-copy lock on {}", lock_path);
        /// Looks ugly, but we cannot touch any storage fields inside Watch callback
        /// because it could lead to use-after-free (storage dropped and watch triggered)
        std::shared_ptr<std::atomic<bool>> flag = std::make_shared<std::atomic<bool>>(true);
        std::string replica;
        bool exists = zookeeper->tryGetWatch(lock_path, replica, nullptr, [flag] (const Coordination::WatchResponse &)
        {
            *flag = false;
        });

        if (exists)
        {
            std::lock_guard lock(existing_zero_copy_locks_mutex);
            existing_zero_copy_locks[lock_path] = ZeroCopyLockDescription{replica, flag};
        }
    }
}

bool StorageReplicatedMergeTree::checkZeroCopyLockExists(const String & part_name, const DiskPtr & disk, String & lock_replica)
{
    auto path = getZeroCopyPartPath(part_name, disk);

    std::lock_guard lock(existing_zero_copy_locks_mutex);
    /// Cleanup abandoned locks during each check. The set of locks is small and this is quite fast loop.
    /// Also it's hard to properly remove locks because we can execute replication queue
    /// in arbitrary order and some parts can be replaced by covering parts without merges.
    for (auto it = existing_zero_copy_locks.begin(); it != existing_zero_copy_locks.end();)
    {
        if (*it->second.exists)
            ++it;
        else
        {
            LOG_TEST(log, "Removing zero-copy lock on {}", it->first);
            it = existing_zero_copy_locks.erase(it);
        }
    }

    if (path)
    {
        auto lock_path = fs::path(*path) / "part_exclusive_lock";
        if (auto it = existing_zero_copy_locks.find(lock_path); it != existing_zero_copy_locks.end())
        {
            lock_replica = it->second.replica;
            if (*it->second.exists)
            {
                LOG_TEST(log, "Zero-copy lock on path {} exists", it->first);
                return true;
            }
        }

        LOG_TEST(log, "Zero-copy lock on path {} doesn't exist", lock_path);
    }

    return false;
}

std::optional<String> StorageReplicatedMergeTree::getZeroCopyPartPath(const String & part_name, const DiskPtr & disk)
{
    if (!disk || !disk->supportZeroCopyReplication())
        return std::nullopt;

    return getZeroCopyPartPath(*getSettings(), toString(disk->getDataSourceDescription().type), getTableSharedID(), part_name, zookeeper_path)[0];
}

bool StorageReplicatedMergeTree::waitZeroCopyLockToDisappear(const ZeroCopyLock & lock, size_t milliseconds_to_wait)
{
    if (lock.isLocked())
        return true;

    if (partial_shutdown_called.load(std::memory_order_relaxed))
        return true;

    auto lock_path = lock.lock->getLockPath();
    zkutil::ZooKeeperPtr zookeeper = tryGetZooKeeper();
    if (!zookeeper)
        return true;

    Stopwatch time_waiting;
    const auto & stop_waiting = [&]()
    {
        bool timeout_exceeded = milliseconds_to_wait < time_waiting.elapsedMilliseconds();
        return partial_shutdown_called.load(std::memory_order_relaxed) || is_readonly.load(std::memory_order_relaxed) || timeout_exceeded;
    };

    return zookeeper->waitForDisappear(lock_path, stop_waiting);
}

std::optional<ZeroCopyLock> StorageReplicatedMergeTree::tryCreateZeroCopyExclusiveLock(const String & part_name, const DiskPtr & disk)
{
    if (!disk || !disk->supportZeroCopyReplication())
        return std::nullopt;

    if (partial_shutdown_called.load(std::memory_order_relaxed) || is_readonly.load(std::memory_order_relaxed))
        return std::nullopt;

    zkutil::ZooKeeperPtr zookeeper = tryGetZooKeeper();
    if (!zookeeper)
        return std::nullopt;

    String zc_zookeeper_path = *getZeroCopyPartPath(part_name, disk);

    /// Just recursively create ancestors for lock
    zookeeper->createAncestors(zc_zookeeper_path + "/");

    /// Create actual lock
    ZeroCopyLock lock(zookeeper, zc_zookeeper_path, replica_name);
    lock.lock->tryLock();
    return lock;
}

String StorageReplicatedMergeTree::findReplicaHavingPart(
    const String & part_name, const String & zookeeper_path_, zkutil::ZooKeeper::Ptr zookeeper_ptr)
{
    Strings replicas = zookeeper_ptr->getChildren(fs::path(zookeeper_path_) / "replicas");

    /// Select replicas in uniformly random order.
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);

    for (const String & replica : replicas)
    {
        if (zookeeper_ptr->exists(fs::path(zookeeper_path_) / "replicas" / replica / "parts" / part_name)
            && zookeeper_ptr->exists(fs::path(zookeeper_path_) / "replicas" / replica / "is_active"))
            return fs::path(zookeeper_path_) / "replicas" / replica;
    }

    return {};
}


bool StorageReplicatedMergeTree::checkIfDetachedPartExists(const String & part_name)
{
    fs::directory_iterator dir_end;
    for (const std::string & path : getDataPaths())
        for (fs::directory_iterator dir_it{fs::path(path) / "detached/"}; dir_it != dir_end; ++dir_it)
            if (dir_it->path().filename().string() == part_name)
                return true;
    return false;
}


bool StorageReplicatedMergeTree::checkIfDetachedPartitionExists(const String & partition_name)
{
    fs::directory_iterator dir_end;

    for (const std::string & path : getDataPaths())
    {
        for (fs::directory_iterator dir_it{fs::path(path) / "detached/"}; dir_it != dir_end; ++dir_it)
        {
            const String file_name = dir_it->path().filename().string();
            auto part_info = MergeTreePartInfo::tryParsePartName(file_name, format_version);

            if (part_info && part_info->partition_id == partition_name)
                return true;
        }
    }
    return false;
}


bool StorageReplicatedMergeTree::createEmptyPartInsteadOfLost(zkutil::ZooKeeperPtr zookeeper, const String & lost_part_name)
{
    LOG_INFO(log, "Going to replace lost part {} with empty part", lost_part_name);

    auto new_part_info = MergeTreePartInfo::fromPartName(lost_part_name, format_version);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    MergeTreePartition partition;
    {
        DataPartsLock lock = lockParts();

        auto parts_in_partition = getDataPartsPartitionRange(new_part_info.partition_id);
        if (!parts_in_partition.empty())
        {
            partition = (*parts_in_partition.begin())->partition;
        }
        else if (auto parsed_partition = MergeTreePartition::tryParseValueFromID(
                     new_part_info.partition_id,
                     metadata_snapshot->getPartitionKey().sample_block))
        {
            partition = MergeTreePartition(*parsed_partition);
        }
        else
        {
            LOG_WARNING(log, "Empty part {} is not created instead of lost part because there are no parts in partition {} (it's empty), "
                             "resolve this manually using DROP/DETACH PARTITION.", lost_part_name, new_part_info.partition_id);
            return false;
        }
    }

    auto [new_data_part, tmp_dir_holder] = createEmptyPart(new_part_info, partition, lost_part_name, NO_TRANSACTION_PTR);
    new_data_part->setName(lost_part_name);

    try
    {
        MergeTreeData::Transaction transaction(*this, NO_TRANSACTION_RAW);
        auto replaced_parts = renameTempPartAndReplace(new_data_part, transaction);

        if (!replaced_parts.empty())
        {
            Strings part_names;
            for (const auto & part : replaced_parts)
                part_names.emplace_back(part->name);

            /// Why this exception is not a LOGICAL_ERROR? Because it's possible
            /// to have some source parts for the lost part if replica currently
            /// cloning from another replica, but source replica lost covering
            /// part and finished MERGE_PARTS before clone. It's an extremely
            /// rare case and it's unclear how to resolve it better. Eventually
            /// source replica will replace lost part with empty part and we
            /// will fetch this empty part instead of our source parts. This
            /// will make replicas consistent, but some data will be lost.
            throw Exception(ErrorCodes::INCORRECT_DATA,
                            "Tried to create empty part {}, but it replaces existing parts {}.",
                            lost_part_name, fmt::join(part_names, ", "));
        }

        lockSharedData(*new_data_part, false, {});

        while (true)
        {
            /// We should be careful when creating an empty part, because we are not sure that this part is still needed.
            /// For example, it's possible that part (or partition) was dropped (or replaced) concurrently.
            /// We can enqueue part for check from DataPartExchange or SelectProcessor
            /// and it's hard to synchronize it with ReplicatedMergeTreeQueue and PartCheckThread...
            /// But at least we can ignore parts that are definitely not needed according to virtual parts and drop ranges.
            auto pred = queue.getMergePredicate(zookeeper, PartitionIdsHint{new_part_info.partition_id});
            String covering_virtual = pred.getCoveringVirtualPart(lost_part_name);
            if (covering_virtual.empty())
            {
                LOG_WARNING(log, "Will not create empty part instead of lost {}, because there's no covering part in replication queue", lost_part_name);
                return false;
            }
            MergeTreePartInfo drop_info;
            if (pred.isGoingToBeDropped(MergeTreePartInfo::fromPartName(lost_part_name, format_version), &drop_info))
            {
                LOG_WARNING(log, "Will not create empty part instead of lost {}, "
                                 "because it's going to be removed (by range {})",
                            lost_part_name, drop_info.getPartNameForLogs());
                return false;
            }

            Coordination::Requests ops;
            Coordination::Stat replicas_stat;
            auto replicas_path = fs::path(zookeeper_path) / "replicas";
            Strings replicas = zookeeper->getChildren(replicas_path, &replicas_stat);

            ops.emplace_back(zkutil::makeCheckRequest(zookeeper_path + "/log", pred.getVersion()));

            /// In rare cases new replica can appear during check
            ops.emplace_back(zkutil::makeCheckRequest(replicas_path, replicas_stat.version));

            for (const String & replica : replicas)
            {
                String current_part_path = fs::path(zookeeper_path) / "replicas" / replica / "parts" / lost_part_name;

                /// We must be sure that this part doesn't exist on other replicas
                if (!zookeeper->exists(current_part_path))
                {
                    zkutil::addCheckNotExistsRequest(ops, *zookeeper, current_part_path);
                }
                else
                {
                    throw Exception(ErrorCodes::DUPLICATE_DATA_PART,
                                    "Part {} already exists on replica {} on path {}",
                                    lost_part_name, replica, current_part_path);
                }
            }

            getCommitPartOps(ops, new_data_part);

            /// Increment lost_part_count
            auto lost_part_count_path = fs::path(zookeeper_path) / "lost_part_count";
            Coordination::Stat lost_part_count_stat;
            String lost_part_count_str;
            if (zookeeper->tryGet(lost_part_count_path, lost_part_count_str, &lost_part_count_stat))
            {
                UInt64 lost_part_count = lost_part_count_str.empty() ? 0 : parse<UInt64>(lost_part_count_str);
                ops.emplace_back(zkutil::makeSetRequest(lost_part_count_path, toString(lost_part_count + 1), lost_part_count_stat.version));
            }
            else
            {
                ops.emplace_back(zkutil::makeCreateRequest(lost_part_count_path, "1", zkutil::CreateMode::Persistent));
            }

            Coordination::Responses responses;
            if (auto code = zookeeper->tryMulti(ops, responses); code == Coordination::Error::ZOK)
            {
                transaction.commit();
                break;
            }
            else if (code == Coordination::Error::ZBADVERSION)
            {
                LOG_INFO(log, "Looks like log was updated or new replica appeared while creating new empty part, will retry");
            }
            else
            {
                zkutil::KeeperMultiException::check(code, ops, responses);
            }
        }
    }
    catch (const Exception & ex)
    {
        LOG_WARNING(log, "Cannot commit empty part {} with error {}", lost_part_name, ex.displayText());
        return false;
    }

    LOG_INFO(log, "Created empty part {} instead of lost part", lost_part_name);

    return true;
}

void StorageReplicatedMergeTree::getZeroCopyLockNodeCreateOps(
    const ZooKeeperWithFaultInjectionPtr & zookeeper, const String & zookeeper_node, Coordination::Requests & requests,
    int32_t mode, bool replace_existing_lock,
    const String & path_to_set_hardlinked_files, const NameSet & hardlinked_files)
{

    /// Ephemeral locks can be created only when we fetch shared data.
    /// So it never require to create ancestors. If we create them
    /// race condition with source replica drop is possible.
    if (mode == zkutil::CreateMode::Persistent)
        zookeeper->checkExistsAndGetCreateAncestorsOps(zookeeper_node, requests);

    if (replace_existing_lock && zookeeper->exists(zookeeper_node))
    {
        requests.emplace_back(zkutil::makeRemoveRequest(zookeeper_node, -1));
        requests.emplace_back(zkutil::makeCreateRequest(zookeeper_node, "", mode));
        if (!path_to_set_hardlinked_files.empty() && !hardlinked_files.empty())
        {
            std::string data = boost::algorithm::join(hardlinked_files, "\n");
            /// List of files used to detect hardlinks. path_to_set_hardlinked_files --
            /// is a path to source part zero copy node. During part removal hardlinked
            /// files will be left for source part.
            requests.emplace_back(zkutil::makeSetRequest(path_to_set_hardlinked_files, data, -1));
        }
    }
    else
    {
        Coordination::Requests ops;
        if (!path_to_set_hardlinked_files.empty() && !hardlinked_files.empty())
        {
            std::string data = boost::algorithm::join(hardlinked_files, "\n");
            /// List of files used to detect hardlinks. path_to_set_hardlinked_files --
            /// is a path to source part zero copy node. During part removal hardlinked
            /// files will be left for source part.
            requests.emplace_back(zkutil::makeSetRequest(path_to_set_hardlinked_files, data, -1));
        }
        requests.emplace_back(zkutil::makeCreateRequest(zookeeper_node, "", mode));
    }
}


void StorageReplicatedMergeTree::createZeroCopyLockNode(
    const ZooKeeperWithFaultInjectionPtr & zookeeper, const String & zookeeper_node, int32_t mode,
    bool replace_existing_lock, const String & path_to_set_hardlinked_files, const NameSet & hardlinked_files)
{
    /// In rare case other replica can remove path between createAncestors and createIfNotExists
    /// So we make up to 5 attempts

    bool created = false;
    for (int attempts = 5; attempts > 0; --attempts)
    {
        Coordination::Requests ops;
        Coordination::Responses responses;
        getZeroCopyLockNodeCreateOps(zookeeper, zookeeper_node, ops, mode, replace_existing_lock, path_to_set_hardlinked_files, hardlinked_files);
        auto error = zookeeper->tryMulti(ops, responses);
        if (error == Coordination::Error::ZOK)
        {
            created = true;
            break;
        }
        else if (mode == zkutil::CreateMode::Persistent)
        {
            if (error == Coordination::Error::ZNONODE)
                continue;

            if (error == Coordination::Error::ZNODEEXISTS)
            {
                size_t failed_op = zkutil::getFailedOpIndex(error, responses);
                /// Part was locked before, unfortunately it's possible during moves
                if (ops[failed_op]->getPath() == zookeeper_node)
                {
                    created = true;
                    break;
                }
                continue;
            }
        }
        else if (mode == zkutil::CreateMode::Ephemeral)
        {
            /// It is super rare case when we had part, but it was lost and we were unable to unlock it from keeper.
            /// Now we are trying to fetch it from other replica and unlocking.
            if (error == Coordination::Error::ZNODEEXISTS)
            {
                size_t failed_op = zkutil::getFailedOpIndex(error, responses);
                if (ops[failed_op]->getPath() == zookeeper_node)
                {
                    LOG_WARNING(&Poco::Logger::get("ZeroCopyLocks"), "Replacing persistent lock with ephemeral for path {}. It can happen only in case of local part loss", zookeeper_node);
                    replace_existing_lock = true;
                    continue;
                }
            }
        }

        zkutil::KeeperMultiException::check(error, ops, responses);
    }

    if (!created)
    {
        String mode_str = mode == zkutil::CreateMode::Persistent ? "persistent" : "ephemeral";
        throw Exception(ErrorCodes::NOT_FOUND_NODE,
                        "Cannot create {} zero copy lock {} because part was unlocked from zookeeper",
                        mode_str, zookeeper_node);
    }
}

bool StorageReplicatedMergeTree::removeDetachedPart(DiskPtr disk, const String & path, const String & part_name)
{
    auto settings_ptr = getSettings();
    if (disk->supportZeroCopyReplication() && settings_ptr->allow_remote_fs_zero_copy_replication)
    {
        String table_id = getTableSharedID();
        return removeSharedDetachedPart(disk, path, part_name, table_id, replica_name, zookeeper_path, getContext(), current_zookeeper);
    }

    disk->removeRecursive(path);

    return false;
}


bool StorageReplicatedMergeTree::removeSharedDetachedPart(DiskPtr disk, const String & path, const String & part_name, const String & table_uuid,
    const String & detached_replica_name, const String & detached_zookeeper_path, const ContextPtr & local_context, const zkutil::ZooKeeperPtr & zookeeper)
{
    bool keep_shared = false;

    NameSet files_not_to_remove;

    // zero copy replication is only available since format version 1 so we can safely use it here
    auto part_info = DetachedPartInfo::parseDetachedPartName(disk, part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    if (!part_info.valid_name)
        throw Exception(ErrorCodes::BAD_DATA_PART_NAME, "Invalid detached part name {} on disk {}", path, disk->getName());

    fs::path checksums = fs::path(path) / IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK;
    if (disk->exists(checksums))
    {
        if (disk->getRefCount(checksums) == 0)
        {
            String id = disk->getUniqueId(checksums);
            bool can_remove = false;
            std::tie(can_remove, files_not_to_remove) = StorageReplicatedMergeTree::unlockSharedDataByID(
                id, table_uuid, part_info,
                detached_replica_name,
                toString(disk->getDataSourceDescription().type),
                std::make_shared<ZooKeeperWithFaultInjection>(zookeeper), local_context->getReplicatedMergeTreeSettings(),
                &Poco::Logger::get("StorageReplicatedMergeTree"),
                detached_zookeeper_path,
                MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);

            keep_shared = !can_remove;
        }
        else
            keep_shared = true;
    }

    disk->removeSharedRecursive(path, keep_shared, files_not_to_remove);

    return keep_shared;
}


void StorageReplicatedMergeTree::createAndStoreFreezeMetadata(DiskPtr disk, DataPartPtr, String backup_part_path) const
{
    if (disk->supportZeroCopyReplication())
    {
        FreezeMetaData meta;
        meta.fill(*this);
        meta.save(disk, backup_part_path);
    }
}


void StorageReplicatedMergeTree::adjustCreateQueryForBackup(ASTPtr & create_query) const
{
    /// Adjust the create query using values from ZooKeeper.
    auto zookeeper = getZooKeeper();
    auto columns_from_entry = ColumnsDescription::parse(zookeeper->get(fs::path(zookeeper_path) / "columns"));
    auto metadata_from_entry = ReplicatedMergeTreeTableMetadata::parse(zookeeper->get(fs::path(zookeeper_path) / "metadata"));

    auto current_metadata = getInMemoryMetadataPtr();
    auto metadata_diff = ReplicatedMergeTreeTableMetadata(*this, current_metadata).checkAndFindDiff(metadata_from_entry, current_metadata->getColumns(), getContext());
    auto adjusted_metadata = metadata_diff.getNewMetadata(columns_from_entry, getContext(), *current_metadata);
    applyMetadataChangesToCreateQuery(create_query, adjusted_metadata);

    /// Check that tryGetTableSharedIDFromCreateQuery() works for this storage.
    if (tryGetTableSharedIDFromCreateQuery(*create_query, getContext()) != getTableSharedID())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} has its shared ID to be different from one from the create query");
}

void StorageReplicatedMergeTree::backupData(
    BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions)
{
    /// First we generate backup entries in the same way as an ordinary MergeTree does.
    /// But then we don't add them to the BackupEntriesCollector right away,
    /// because we need to coordinate them with other replicas (other replicas can have better parts).

    const auto & backup_settings = backup_entries_collector.getBackupSettings();
    const auto & read_settings = backup_entries_collector.getReadSettings();
    auto local_context = backup_entries_collector.getContext();

    DataPartsVector data_parts;
    if (partitions)
        data_parts = getVisibleDataPartsVectorInPartitions(local_context, getPartitionIDsFromQuery(*partitions, local_context));
    else
        data_parts = getVisibleDataPartsVector(local_context);

    auto parts_backup_entries = backupParts(data_parts, /* data_path_in_backup */ "", backup_settings, read_settings, local_context);

    auto coordination = backup_entries_collector.getBackupCoordination();
    String shared_id = getTableSharedID();
    coordination->addReplicatedDataPath(shared_id, data_path_in_backup);

    using PartNameAndChecksum = IBackupCoordination::PartNameAndChecksum;
    std::vector<PartNameAndChecksum> part_names_with_hashes;
    part_names_with_hashes.reserve(parts_backup_entries.size());
    for (const auto & part_backup_entries : parts_backup_entries)
        part_names_with_hashes.emplace_back(PartNameAndChecksum{part_backup_entries.part_name, part_backup_entries.part_checksum});

    /// Send our list of part names to the coordination (to compare with other replicas).
    coordination->addReplicatedPartNames(shared_id, getStorageID().getFullTableName(), getReplicaName(), part_names_with_hashes);

    /// Send a list of mutations to the coordination too (we need to find the mutations which are not finished for added part names).
    {
        auto zookeeper = getZooKeeper();
        Strings mutation_ids;
        if (zookeeper->tryGetChildren(fs::path(zookeeper_path) / "mutations", mutation_ids) == Coordination::Error::ZOK)
        {
            std::vector<IBackupCoordination::MutationInfo> mutation_infos;
            mutation_infos.reserve(mutation_ids.size());
            for (const auto & mutation_id : mutation_ids)
            {
                String mutation;
                if (zookeeper->tryGet(fs::path(zookeeper_path) / "mutations" / mutation_id, mutation))
                    mutation_infos.emplace_back(IBackupCoordination::MutationInfo{mutation_id, mutation});
            }
            coordination->addReplicatedMutations(shared_id, getStorageID().getFullTableName(), getReplicaName(), mutation_infos);
        }
    }

    /// This task will be executed after all replicas have collected their parts and the coordination is ready to
    /// give us the final list of parts to add to the BackupEntriesCollector.
    auto post_collecting_task = [shared_id,
                                 my_replica_name = getReplicaName(),
                                 coordination,
                                 my_parts_backup_entries = std::move(parts_backup_entries),
                                 &backup_entries_collector]()
    {
        Strings data_paths = coordination->getReplicatedDataPaths(shared_id);
        std::vector<fs::path> data_paths_fs;
        data_paths_fs.reserve(data_paths.size());
        for (const auto & data_path : data_paths)
            data_paths_fs.push_back(data_path);

        Strings part_names = coordination->getReplicatedPartNames(shared_id, my_replica_name);
        std::unordered_set<std::string_view> part_names_set{part_names.begin(), part_names.end()};

        for (const auto & part_backup_entries : my_parts_backup_entries)
        {
            if (part_names_set.contains(part_backup_entries.part_name))
            {
                for (const auto & [relative_path, backup_entry] : part_backup_entries.backup_entries)
                    for (const auto & data_path : data_paths_fs)
                        backup_entries_collector.addBackupEntry(data_path / relative_path, backup_entry);
            }
        }

        auto mutation_infos = coordination->getReplicatedMutations(shared_id, my_replica_name);
        for (const auto & mutation_info : mutation_infos)
        {
            auto backup_entry = ReplicatedMergeTreeMutationEntry::parse(mutation_info.entry, mutation_info.id).backup();
            for (const auto & data_path : data_paths_fs)
                backup_entries_collector.addBackupEntry(data_path / "mutations" / (mutation_info.id + ".txt"), backup_entry);
        }
    };

    backup_entries_collector.addPostTask(post_collecting_task);
}

void StorageReplicatedMergeTree::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions)
{
    String full_zk_path = getZooKeeperName() + getZooKeeperPath();
    if (!restorer.getRestoreCoordination()->acquireInsertingDataIntoReplicatedTable(full_zk_path))
    {
        /// Other replica is already restoring the data of this table.
        /// We'll get them later due to replication, it's not necessary to read it from the backup.
        return;
    }

    if (!restorer.isNonEmptyTableAllowed())
    {
        bool empty = !getTotalActiveSizeInBytes();
        if (empty)
        {
            /// New parts could be in the replication queue but not fetched yet.
            /// In that case we consider the table as not empty.
            ReplicatedTableStatus status;
            getStatus(status, /* with_zk_fields = */ false);
            if (status.queue.inserts_in_queue)
                empty = false;
        }
        auto backup = restorer.getBackup();
        if (!empty && backup->hasFiles(data_path_in_backup))
            restorer.throwTableIsNotEmpty(getStorageID());
    }

    restorePartsFromBackup(restorer, data_path_in_backup, partitions);
}

void StorageReplicatedMergeTree::attachRestoredParts(MutableDataPartsVector && parts)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto sink = std::make_shared<ReplicatedMergeTreeSink>(*this, metadata_snapshot, 0, 0, 0, false, false, false,  getContext(), /*is_attach*/true);
    for (auto part : parts)
        sink->writeExistingPart(part);
}

template std::optional<EphemeralLockInZooKeeper> StorageReplicatedMergeTree::allocateBlockNumber<String>(
    const String & partition_id,
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    const String & zookeeper_block_id_path,
    const String & zookeeper_path_prefix) const;

template std::optional<EphemeralLockInZooKeeper> StorageReplicatedMergeTree::allocateBlockNumber<std::vector<String>>(
    const String & partition_id,
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    const std::vector<String> & zookeeper_block_id_path,
    const String & zookeeper_path_prefix) const;

}
