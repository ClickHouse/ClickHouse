#include <Core/Defines.h>

#include <Common/FieldVisitors.h>
#include <Common/Macros.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>
#include <Common/thread_local_rng.h>
#include <Common/typeid_cast.h>

#include <Storages/AlterCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumAddedParts.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/VirtualColumnUtils.h>

#include <Disks/StoragePolicy.h>

#include <Databases/IDatabase.h>

#include <Parsers/formatAST.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>

#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>

#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/copyData.h>

#include <Poco/DirectoryIterator.h>

#include <ext/range.h>
#include <ext/scope_guard.h>

#include <ctime>
#include <thread>
#include <future>

#include <boost/algorithm/string/join.hpp>

namespace ProfileEvents
{
    extern const Event ReplicatedPartMerges;
    extern const Event ReplicatedPartMutations;
    extern const Event ReplicatedPartFailedFetches;
    extern const Event ReplicatedPartFetchesOfMerged;
    extern const Event ObsoleteReplicatedParts;
    extern const Event ReplicatedPartFetches;
    extern const Event DataAfterMergeDiffersFromReplica;
    extern const Event DataAfterMutationDiffersFromReplica;
    extern const Event CreatedLogEntryForMerge;
    extern const Event NotCreatedLogEntryForMerge;
    extern const Event CreatedLogEntryForMutation;
    extern const Event NotCreatedLogEntryForMutation;
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
    extern const int REPLICA_IS_ALREADY_EXIST;
    extern const int NO_REPLICA_HAS_PART;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_UNEXPECTED_DATA_PARTS;
    extern const int ABORTED;
    extern const int REPLICA_IS_NOT_IN_QUORUM;
    extern const int TABLE_IS_READ_ONLY;
    extern const int NOT_FOUND_NODE;
    extern const int NO_ACTIVE_REPLICAS;
    extern const int NOT_A_LEADER;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int PARTITION_ALREADY_EXISTS;
    extern const int TOO_MANY_RETRIES_TO_FETCH_PARTS;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int PARTITION_DOESNT_EXIST;
    extern const int UNFINISHED;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int TOO_MANY_FETCHES;
    extern const int BAD_DATA_PART_NAME;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int KEEPER_EXCEPTION;
    extern const int ALL_REPLICAS_LOST;
    extern const int REPLICA_STATUS_CHANGED;
    extern const int CANNOT_ASSIGN_ALTER;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsFetch;
    extern const StorageActionBlockType PartsSend;
    extern const StorageActionBlockType ReplicationQueue;
    extern const StorageActionBlockType PartsTTLMerge;
    extern const StorageActionBlockType PartsMove;
}


static const auto QUEUE_UPDATE_ERROR_SLEEP_MS        = 1 * 1000;
static const auto MERGE_SELECTING_SLEEP_MS           = 5 * 1000;
static const auto MUTATIONS_FINALIZING_SLEEP_MS      = 1 * 1000;
static const auto MUTATIONS_FINALIZING_IDLE_SLEEP_MS = 5 * 1000;


void StorageReplicatedMergeTree::setZooKeeper(zkutil::ZooKeeperPtr zookeeper)
{
    std::lock_guard lock(current_zookeeper_mutex);
    current_zookeeper = zookeeper;
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
        throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    return res;
}


static std::string normalizeZooKeeperPath(std::string zookeeper_path)
{
    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;

    return zookeeper_path;
}


StorageReplicatedMergeTree::StorageReplicatedMergeTree(
    const String & zookeeper_path_,
    const String & replica_name_,
    bool attach,
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    Context & context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_,
    bool has_force_restore_data_flag)
    : MergeTreeData(table_id_,
                    relative_data_path_,
                    metadata_,
                    context_,
                    date_column_name,
                    merging_params_,
                    std::move(settings_),
                    true,                   /// require_part_metadata
                    attach,
                    [this] (const std::string & name) { enqueuePartForCheck(name); })
    , zookeeper_path(normalizeZooKeeperPath(global_context.getMacros()->expand(zookeeper_path_, table_id_.database_name, table_id_.table_name)))
    , replica_name(global_context.getMacros()->expand(replica_name_, table_id_.database_name, table_id_.table_name))
    , replica_path(zookeeper_path + "/replicas/" + replica_name)
    , reader(*this)
    , writer(*this)
    , merger_mutator(*this, global_context.getBackgroundPool().getNumberOfThreads())
    , queue(*this)
    , fetcher(*this)
    , cleanup_thread(*this)
    , part_check_thread(*this)
    , restarting_thread(*this)
{
    queue_updating_task = global_context.getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageReplicatedMergeTree::queueUpdatingTask)", [this]{ queueUpdatingTask(); });

    mutations_updating_task = global_context.getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageReplicatedMergeTree::mutationsUpdatingTask)", [this]{ mutationsUpdatingTask(); });

    merge_selecting_task = global_context.getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageReplicatedMergeTree::mergeSelectingTask)", [this] { mergeSelectingTask(); });

    /// Will be activated if we win leader election.
    merge_selecting_task->deactivate();

    mutations_finalizing_task = global_context.getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageReplicatedMergeTree::mutationsFinalizingTask)", [this] { mutationsFinalizingTask(); });

    if (global_context.hasZooKeeper())
        current_zookeeper = global_context.getZooKeeper();

    bool skip_sanity_checks = false;

    if (current_zookeeper && current_zookeeper->exists(replica_path + "/flags/force_restore_data"))
    {
        skip_sanity_checks = true;
        current_zookeeper->remove(replica_path + "/flags/force_restore_data");

        LOG_WARNING(log, "Skipping the limits on severity of changes to data parts and columns (flag {}/flags/force_restore_data).", replica_path);
    }
    else if (has_force_restore_data_flag)
    {
        skip_sanity_checks = true;

        LOG_WARNING(log, "Skipping the limits on severity of changes to data parts and columns (flag force_restore_data).");
    }

    loadDataParts(skip_sanity_checks);

    if (!current_zookeeper)
    {
        if (!attach)
            throw Exception("Can't create replicated table without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

        /// Do not activate the replica. It will be readonly.
        LOG_ERROR(log, "No ZooKeeper: table will be in readonly mode.");
        is_readonly = true;
        return;
    }

    if (attach && !current_zookeeper->exists(zookeeper_path + "/metadata"))
    {
        LOG_WARNING(log, "No metadata in ZooKeeper: table will be in readonly mode.");
        is_readonly = true;
        has_metadata_in_zookeeper = false;
        return;
    }

    auto metadata_snapshot = getInMemoryMetadataPtr();

    if (!attach)
    {
        if (!getDataParts().empty())
            throw Exception("Data directory for table already containing data parts"
                " - probably it was unclean DROP table or manual intervention."
                " You must either clear directory by hand or use ATTACH TABLE"
                " instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);

        try
        {
            bool is_first_replica = createTableIfNotExists(metadata_snapshot);

            /// We have to check granularity on other replicas. If it's fixed we
            /// must create our new replica with fixed granularity and store this
            /// information in /replica/metadata.
            other_replicas_fixed_granularity = checkFixedGranualrityInZookeeper();

            checkTableStructure(zookeeper_path, metadata_snapshot);

            Coordination::Stat metadata_stat;
            current_zookeeper->get(zookeeper_path + "/metadata", &metadata_stat);
            metadata_version = metadata_stat.version;

            if (!is_first_replica)
                createReplica(metadata_snapshot);
        }
        catch (...)
        {
            /// If replica was not created, rollback creation of data directory.
            dropIfEmpty();
            throw;
        }
    }
    else
    {

        /// In old tables this node may missing or be empty
        String replica_metadata;
        bool replica_metadata_exists = current_zookeeper->tryGet(replica_path + "/metadata", replica_metadata);
        if (!replica_metadata_exists || replica_metadata.empty())
        {
            /// We have to check shared node granularity before we create ours.
            other_replicas_fixed_granularity = checkFixedGranualrityInZookeeper();
            ReplicatedMergeTreeTableMetadata current_metadata(*this, metadata_snapshot);
            current_zookeeper->createOrUpdate(replica_path + "/metadata", current_metadata.toString(), zkutil::CreateMode::Persistent);
        }

        checkTableStructure(replica_path, metadata_snapshot);
        checkParts(skip_sanity_checks);

        if (current_zookeeper->exists(replica_path + "/metadata_version"))
        {
            metadata_version = parse<int>(current_zookeeper->get(replica_path + "/metadata_version"));
        }
        else
        {
            /// This replica was created with old clickhouse version, so we have
            /// to take version of global node. If somebody will alter our
            /// table, then we will fill /metadata_version node in zookeeper.
            /// Otherwise on the next restart we can again use version from
            /// shared metadata node because it was not changed.
            Coordination::Stat metadata_stat;
            current_zookeeper->get(zookeeper_path + "/metadata", &metadata_stat);
            metadata_version = metadata_stat.version;
        }
        /// Temporary directories contain untinalized results of Merges or Fetches (after forced restart)
        ///  and don't allow to reinitialize them, so delete each of them immediately
        clearOldTemporaryDirectories(0);
        clearOldWriteAheadLogs();
    }

    createNewZooKeeperNodes();
}


bool StorageReplicatedMergeTree::checkFixedGranualrityInZookeeper()
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

    zkutil::EventPtr wait_event = std::make_shared<Poco::Event>();

    std::set<String> inactive_replicas;
    for (const String & replica : replicas)
    {
        LOG_DEBUG(log, "Waiting for {} to apply mutation {}", replica, mutation_id);

        while (!partial_shutdown_called)
        {
            /// Mutation maybe killed or whole replica was deleted.
            /// Wait event will unblock at this moment.
            Coordination::Stat exists_stat;
            if (!getZooKeeper()->exists(zookeeper_path + "/mutations/" + mutation_id, &exists_stat, wait_event))
            {
                throw Exception(ErrorCodes::UNFINISHED, "Mutation {} was killed, manually removed or table was dropped", mutation_id);
            }

            auto zookeeper = getZooKeeper();
            /// Replica could be inactive.
            if (!zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
            {
                LOG_WARNING(log, "Replica {} is not active during mutation. Mutation will be done asynchronously when replica becomes active.", replica);

                inactive_replicas.emplace(replica);
                break;
            }

            String mutation_pointer = zookeeper_path + "/replicas/" + replica + "/mutation_pointer";
            std::string mutation_pointer_value;
            Coordination::Stat get_stat;
            /// Replica could be removed
            if (!zookeeper->tryGet(mutation_pointer, mutation_pointer_value, &get_stat, wait_event))
            {
                LOG_WARNING(log, "Replica {} was removed", replica);
                break;
            }
            else if (mutation_pointer_value >= mutation_id) /// Maybe we already processed more fresh mutation
                break;                                      /// (numbers like 0000000000 and 0000000001)

            /// Replica can become inactive, so wait with timeout and recheck it
            if (wait_event->tryWait(1000))
                break;

            auto mutation_status = queue.getIncompleteMutationsStatus(mutation_id);
            if (!mutation_status || !mutation_status->latest_fail_reason.empty())
                break;
        }

        /// It maybe already removed from zk, but local in-memory mutations
        /// state was not update.
        if (!getZooKeeper()->exists(zookeeper_path + "/mutations/" + mutation_id))
        {
            throw Exception(ErrorCodes::UNFINISHED, "Mutation {} was killed, manually removed or table was dropped", mutation_id);
        }

        /// At least we have our current mutation
        std::set<String> mutation_ids;
        mutation_ids.insert(mutation_id);

        auto mutation_status = queue.getIncompleteMutationsStatus(mutation_id, &mutation_ids);
        checkMutationStatus(mutation_status, mutation_ids);

        if (partial_shutdown_called)
            throw Exception("Mutation is not finished because table shutdown was called. It will be done after table restart.",
                ErrorCodes::UNFINISHED);
    }

    if (!inactive_replicas.empty())
    {
        std::stringstream exception_message;
        exception_message << "Mutation is not finished because";

        if (!inactive_replicas.empty())
            exception_message << " some replicas are inactive right now: " << boost::algorithm::join(inactive_replicas, ", ");

        exception_message << ". Mutation will be done asynchronously";

        throw Exception(exception_message.str(), ErrorCodes::UNFINISHED);
    }
}

void StorageReplicatedMergeTree::createNewZooKeeperNodes()
{
    auto zookeeper = getZooKeeper();

    /// Working with quorum.
    zookeeper->createIfNotExists(zookeeper_path + "/quorum", String());
    zookeeper->createIfNotExists(zookeeper_path + "/quorum/last_part", String());
    zookeeper->createIfNotExists(zookeeper_path + "/quorum/failed_parts", String());

    /// Tracking lag of replicas.
    zookeeper->createIfNotExists(replica_path + "/min_unprocessed_insert_time", String());
    zookeeper->createIfNotExists(replica_path + "/max_processed_insert_time", String());

    /// Mutations
    zookeeper->createIfNotExists(zookeeper_path + "/mutations", String());
    zookeeper->createIfNotExists(replica_path + "/mutation_pointer", String());
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

            Strings children;
            Coordination::Error code = zookeeper->tryGetChildren(zookeeper_path, children);
            if (code == Coordination::Error::ZNONODE)
            {
                LOG_WARNING(log, "Table {} is already finished removing by another replica right now", replica_path);
            }
            else
            {
                for (const auto & child : children)
                    if (child != "dropped")
                        zookeeper->tryRemoveRecursive(zookeeper_path + "/" + child);

                Coordination::Requests ops;
                Coordination::Responses responses;
                ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/dropped", -1));
                ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path, -1));
                code = zookeeper->tryMulti(ops, responses);

                if (code == Coordination::Error::ZNONODE)
                {
                    LOG_WARNING(log, "Table {} is already finished removing by another replica right now", replica_path);
                }
                else if (code == Coordination::Error::ZNOTEMPTY)
                {
                    throw Exception(fmt::format(
                        "The old table was not completely removed from ZooKeeper, {} still exists and may contain some garbage. But it should never happen according to the logic of operations (it's a bug).", zookeeper_path), ErrorCodes::LOGICAL_ERROR);
                }
                else if (code != Coordination::Error::ZOK)
                {
                    /// It is still possible that ZooKeeper session is expired or server is killed in the middle of the delete operation.
                    zkutil::KeeperMultiException::check(code, ops, responses);
                }
                else
                {
                    LOG_WARNING(log, "The leftovers from table {} was successfully removed from ZooKeeper", zookeeper_path);
                }
            }
        }

        LOG_DEBUG(log, "Creating table {}", zookeeper_path);

        /// We write metadata of table so that the replicas can check table parameters with them.
        String metadata_str = ReplicatedMergeTreeTableMetadata(*this, metadata_snapshot).toString();

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));

        /// Check that the table is not being dropped right now.
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/dropped", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/dropped", -1));

        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", metadata_str,
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/columns", metadata_snapshot->getColumns().toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/log", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/blocks", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/block_numbers", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/nonincrement_block_numbers", "",
            zkutil::CreateMode::Persistent)); /// /nonincrement_block_numbers dir is unused, but is created nonetheless for backwards compatibility.
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/leader_election", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/temp", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name,
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
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata_version", std::to_string(metadata_version),
            zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_WARNING(log, "It looks like the table {} was created by another server at the same moment, will retry", zookeeper_path);
            continue;
        }
        else if (code != Coordination::Error::ZOK)
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }

        return true;
    }

    throw Exception("Cannot create table, because it is created concurrently every time or because of logical error", ErrorCodes::LOGICAL_ERROR);
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
            throw Exception(fmt::format("Cannot create a replica of the table {}, because the last replica of the table was dropped right now",
                zookeeper_path), ErrorCodes::ALL_REPLICAS_LOST);

        /// It is not the first replica, we will mark it as "lost", to immediately repair (clone) from existing replica.
        /// By the way, it's possible that the replica will be first, if all previous replicas were removed concurrently.
        String is_lost_value = replicas_stat.numChildren ? "1" : "0";

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
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata_version", std::to_string(metadata_version),
            zkutil::CreateMode::Persistent));

        /// Check version of /replicas to see if there are any replicas created at the same moment of time.
        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name, replicas_stat.version));

        Coordination::Responses responses;
        code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            throw Exception("Replica " + replica_path + " already exists.", ErrorCodes::REPLICA_IS_ALREADY_EXIST);
        }
        else if (code == Coordination::Error::ZBADVERSION)
        {
            LOG_ERROR(log, "Retrying createReplica(), because some other replicas were created at the same time");
        }
        else if (code == Coordination::Error::ZNONODE)
        {
            throw Exception("Table " + zookeeper_path + " was suddenly removed.", ErrorCodes::ALL_REPLICAS_LOST);
        }
        else
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }
    } while (code == Coordination::Error::ZBADVERSION);
}

void StorageReplicatedMergeTree::drop()
{
    /// There is also the case when user has configured ClickHouse to wrong ZooKeeper cluster
    /// or metadata of staled replica were removed manually,
    /// in this case, has_metadata_in_zookeeper = false, and we also permit to drop the table.

    if (has_metadata_in_zookeeper)
    {
        auto zookeeper = tryGetZooKeeper();

        /// If probably there is metadata in ZooKeeper, we don't allow to drop the table.
        if (is_readonly || !zookeeper)
            throw Exception("Can't drop readonly replicated table (need to drop data in ZooKeeper as well)", ErrorCodes::TABLE_IS_READ_ONLY);

        shutdown();
        dropReplica(zookeeper, zookeeper_path, replica_name, log);
    }

    dropAllData();
}

void StorageReplicatedMergeTree::dropReplica(zkutil::ZooKeeperPtr zookeeper, const String & zookeeper_path, const String & replica, Poco::Logger * logger)
{
    if (zookeeper->expired())
        throw Exception("Table was not dropped because ZooKeeper session has expired.", ErrorCodes::TABLE_WAS_NOT_DROPPED);

    auto remote_replica_path = zookeeper_path + "/replicas/" + replica;
    LOG_INFO(logger, "Removing replica {}", remote_replica_path);
    /// It may left some garbage if replica_path subtree are concurrently modified
    zookeeper->tryRemoveRecursive(remote_replica_path);
    if (zookeeper->exists(remote_replica_path))
        LOG_ERROR(logger, "Replica was not completely removed from ZooKeeper, {} still exists and may contain some garbage.", remote_replica_path);

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

    Coordination::Requests ops;
    Coordination::Responses responses;
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/replicas", -1));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/dropped", "", zkutil::CreateMode::Persistent));
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
        LOG_INFO(logger, "Removing table {} (this might take several minutes)", zookeeper_path);

        Strings children;
        code = zookeeper->tryGetChildren(zookeeper_path, children);
        if (code == Coordination::Error::ZNONODE)
        {
            LOG_WARNING(logger, "Table {} is already finished removing by another replica right now", remote_replica_path);
        }
        else
        {
            for (const auto & child : children)
                if (child != "dropped")
                    zookeeper->tryRemoveRecursive(zookeeper_path + "/" + child);

            ops.clear();
            responses.clear();
            ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/dropped", -1));
            ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path, -1));
            code = zookeeper->tryMulti(ops, responses);

            if (code == Coordination::Error::ZNONODE)
            {
                LOG_WARNING(logger, "Table {} is already finished removing by another replica right now", remote_replica_path);
            }
            else if (code == Coordination::Error::ZNOTEMPTY)
            {
                LOG_ERROR(logger, "Table was not completely removed from ZooKeeper, {} still exists and may contain some garbage.",
                          zookeeper_path);
            }
            else if (code != Coordination::Error::ZOK)
            {
                /// It is still possible that ZooKeeper session is expired or server is killed in the middle of the delete operation.
                zkutil::KeeperMultiException::check(code, ops, responses);
            }
            else
            {
                LOG_INFO(logger, "Table {} was successfully removed from ZooKeeper", zookeeper_path);
            }
        }
    }
}


/** Verify that list of columns and table storage_settings_ptr match those specified in ZK (/ metadata).
    * If not, throw an exception.
    */
void StorageReplicatedMergeTree::checkTableStructure(const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot)
{
    auto zookeeper = getZooKeeper();

    ReplicatedMergeTreeTableMetadata old_metadata(*this, metadata_snapshot);

    Coordination::Stat metadata_stat;
    String metadata_str = zookeeper->get(zookeeper_prefix + "/metadata", &metadata_stat);
    auto metadata_from_zk = ReplicatedMergeTreeTableMetadata::parse(metadata_str);
    old_metadata.checkEquals(metadata_from_zk, metadata_snapshot->getColumns(), global_context);

    Coordination::Stat columns_stat;
    auto columns_from_zk = ColumnsDescription::parse(zookeeper->get(zookeeper_prefix + "/columns", &columns_stat));

    const ColumnsDescription & old_columns = metadata_snapshot->getColumns();
    if (columns_from_zk != old_columns)
    {
        throw Exception("Table columns structure in ZooKeeper is different from local table structure", ErrorCodes::INCOMPATIBLE_COLUMNS);
    }
}

void StorageReplicatedMergeTree::setTableStructure(
ColumnsDescription new_columns, const ReplicatedMergeTreeTableMetadata::Diff & metadata_diff)
{
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    if (new_columns != new_metadata.columns)
    {
        new_metadata.columns = new_columns;

        new_metadata.column_ttls_by_name.clear();
        for (const auto & [name, ast] : new_metadata.columns.getColumnTTLs())
        {
            auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, new_metadata.columns, global_context, new_metadata.primary_key);
            new_metadata.column_ttls_by_name[name] = new_ttl_entry;
        }

        /// The type of partition key expression may change
        if (new_metadata.partition_key.definition_ast != nullptr)
            new_metadata.partition_key.recalculateWithNewColumns(new_metadata.columns, global_context);
    }

    if (!metadata_diff.empty())
    {
        if (metadata_diff.sorting_key_changed)
        {
            ParserNotEmptyExpressionList parser(false);
            auto new_sorting_key_expr_list = parseQuery(parser, metadata_diff.new_sorting_key, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

            ASTPtr order_by_ast;
            if (new_sorting_key_expr_list->children.size() == 1)
                order_by_ast = new_sorting_key_expr_list->children[0];
            else
            {
                auto tuple = makeASTFunction("tuple");
                tuple->arguments->children = new_sorting_key_expr_list->children;
                order_by_ast = tuple;
            }
            auto & sorting_key = new_metadata.sorting_key;
            auto & primary_key = new_metadata.primary_key;

            sorting_key.recalculateWithNewAST(order_by_ast, new_metadata.columns, global_context);

            if (primary_key.definition_ast == nullptr)
            {
                /// Primary and sorting key become independent after this ALTER so we have to
                /// save the old ORDER BY expression as the new primary key.
                auto old_sorting_key_ast = old_metadata.getSortingKey().definition_ast;
                primary_key = KeyDescription::getKeyFromAST(
                    old_sorting_key_ast, new_metadata.columns, global_context);
            }
        }

        if (metadata_diff.skip_indices_changed)
            new_metadata.secondary_indices = IndicesDescription::parse(metadata_diff.new_skip_indices, new_columns, global_context);

        if (metadata_diff.constraints_changed)
            new_metadata.constraints = ConstraintsDescription::parse(metadata_diff.new_constraints);

        if (metadata_diff.ttl_table_changed)
        {
            ParserTTLExpressionList parser;
            auto ttl_for_table_ast = parseQuery(parser, metadata_diff.new_ttl_table, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
            new_metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                ttl_for_table_ast, new_metadata.columns, global_context, new_metadata.primary_key);
        }
    }

    /// Even if the primary/sorting/partition keys didn't change we must reinitialize it
    /// because primary/partition key column types might have changed.
    checkTTLExpressions(new_metadata, old_metadata);
    setProperties(new_metadata, old_metadata);

    auto table_id = getStorageID();
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(global_context, table_id, new_metadata);
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
    if (zookeeper->tryGet(replica_path + "/parts/" + part_name, unused, &stat))
        res = stat.ctime / 1000;

    return res;
}


void StorageReplicatedMergeTree::checkParts(bool skip_sanity_checks)
{
    auto zookeeper = getZooKeeper();

    Strings expected_parts_vec = zookeeper->getChildren(replica_path + "/parts");

    /// Parts in ZK.
    NameSet expected_parts(expected_parts_vec.begin(), expected_parts_vec.end());

    /// There are no PreCommitted parts at startup.
    auto parts = getDataParts({MergeTreeDataPartState::Committed, MergeTreeDataPartState::Outdated});

    /** Local parts that are not in ZK.
      * In very rare cases they may cover missing parts
      * and someone may think that pushing them to zookeeper is good idea.
      * But actually we can't precisely determine that ALL missing parts
      * covered by this unexpected part. So missing parts will be downloaded.
      */
    DataParts unexpected_parts;

    /// Collect unexpected parts
    for (const auto & part : parts)
        if (!expected_parts.count(part->name))
            unexpected_parts.insert(part); /// this parts we will place to detached with ignored_ prefix

    /// Which parts should be taken from other replicas.
    Strings parts_to_fetch;

    for (const String & missing_name : expected_parts)
        if (!getActiveContainingPart(missing_name))
            parts_to_fetch.push_back(missing_name);

    /** To check the adequacy, for the parts that are in the FS, but not in ZK, we will only consider not the most recent parts.
      * Because unexpected new parts usually arise only because they did not have time to enroll in ZK with a rough restart of the server.
      * It also occurs from deduplicated parts that did not have time to retire.
      */
    size_t unexpected_parts_nonnew = 0;
    UInt64 unexpected_parts_nonnew_rows = 0;
    UInt64 unexpected_parts_rows = 0;
    for (const auto & part : unexpected_parts)
    {
        if (part->info.level > 0)
        {
            ++unexpected_parts_nonnew;
            unexpected_parts_nonnew_rows += part->rows_count;
        }

        unexpected_parts_rows += part->rows_count;
    }

    /// Additional helpful statistics
    auto get_blocks_count_in_data_part = [&] (const String & part_name) -> UInt64
    {
        MergeTreePartInfo part_info;
        if (MergeTreePartInfo::tryParsePartName(part_name, &part_info, format_version))
            return part_info.getBlocksCount();

        LOG_ERROR(log, "Unexpected part name: {}", part_name);
        return 0;
    };

    UInt64 parts_to_fetch_blocks = 0;
    for (const String & name : parts_to_fetch)
        parts_to_fetch_blocks += get_blocks_count_in_data_part(name);

    std::stringstream sanity_report;
    sanity_report << "There are "
        << unexpected_parts.size() << " unexpected parts with " << unexpected_parts_rows << " rows ("
        << unexpected_parts_nonnew << " of them is not just-written with " << unexpected_parts_rows << " rows), "
        << parts_to_fetch.size() << " missing parts (with " << parts_to_fetch_blocks << " blocks).";

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
    bool insane = unexpected_parts_rows > total_rows_on_filesystem * storage_settings_ptr->replicated_max_ratio_of_wrong_parts;

    if (insane && !skip_sanity_checks)
    {
        std::stringstream why;
        why << "The local set of parts of table " << getStorageID().getNameForLogs() << " doesn't look like the set of parts "
            << "in ZooKeeper: "
            << formatReadableQuantity(unexpected_parts_rows) << " rows of " << formatReadableQuantity(total_rows_on_filesystem)
            << " total rows in filesystem are suspicious.";

        throw Exception(why.str() + " " + sanity_report.str(), ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);
    }

    if (unexpected_parts_nonnew_rows > 0)
        LOG_WARNING(log, sanity_report.str());

    /// Add to the queue jobs to pick up the missing parts from other replicas and remove from ZK the information that we have them.
    std::vector<std::future<Coordination::ExistsResponse>> exists_futures;
    exists_futures.reserve(parts_to_fetch.size());
    for (const String & part_name : parts_to_fetch)
    {
        String part_path = replica_path + "/parts/" + part_name;
        exists_futures.emplace_back(zookeeper->asyncExists(part_path));
    }

    std::vector<std::future<Coordination::MultiResponse>> enqueue_futures;
    enqueue_futures.reserve(parts_to_fetch.size());
    for (size_t i = 0; i < parts_to_fetch.size(); ++i)
    {
        const String & part_name = parts_to_fetch[i];
        LOG_ERROR(log, "Removing locally missing part from ZooKeeper and queueing a fetch: {}", part_name);

        Coordination::Requests ops;

        time_t part_create_time = 0;
        Coordination::ExistsResponse exists_resp = exists_futures[i].get();
        if (exists_resp.error == Coordination::Error::ZOK)
        {
            part_create_time = exists_resp.stat.ctime / 1000;
            removePartFromZooKeeper(part_name, ops, exists_resp.stat.numChildren > 0);
        }

        LogEntry log_entry;
        log_entry.type = LogEntry::GET_PART;
        log_entry.source_replica = "";
        log_entry.new_part_name = part_name;
        log_entry.create_time = part_create_time;

        /// We assume that this occurs before the queue is loaded (queue.initialize).
        ops.emplace_back(zkutil::makeCreateRequest(
            replica_path + "/queue/queue-", log_entry.toString(), zkutil::CreateMode::PersistentSequential));

        enqueue_futures.emplace_back(zookeeper->asyncMulti(ops));
    }

    for (auto & future : enqueue_futures)
        future.get();

    /// Remove extra local parts.
    for (const DataPartPtr & part : unexpected_parts)
    {
        LOG_ERROR(log, "Renaming unexpected part {} to ignored_{}", part->name, part->name);
        forgetPartAndMoveToDetached(part, "ignored", true);
    }
}


void StorageReplicatedMergeTree::checkPartChecksumsAndAddCommitOps(const zkutil::ZooKeeperPtr & zookeeper,
    const DataPartPtr & part, Coordination::Requests & ops, String part_name, NameSet * absent_replicas_paths)
{
    if (part_name.empty())
        part_name = part->name;

    auto local_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(
        part->getColumns(), part->checksums);

    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);
    bool has_been_already_added = false;

    for (const String & replica : replicas)
    {
        String current_part_path = zookeeper_path + "/replicas/" + replica + "/parts/" + part_name;

        String part_zk_str;
        if (!zookeeper->tryGet(current_part_path, part_zk_str))
        {
            if (absent_replicas_paths)
                absent_replicas_paths->emplace(current_part_path);

            continue;
        }

        ReplicatedMergeTreePartHeader replica_part_header;
        if (!part_zk_str.empty())
            replica_part_header = ReplicatedMergeTreePartHeader::fromString(part_zk_str);
        else
        {
            Coordination::Stat columns_stat_before, columns_stat_after;
            String columns_str;
            String checksums_str;
            /// Let's check that the node's version with the columns did not change while we were reading the checksums.
            /// This ensures that the columns and the checksum refer to the same
            if (!zookeeper->tryGet(current_part_path + "/columns", columns_str, &columns_stat_before) ||
                !zookeeper->tryGet(current_part_path + "/checksums", checksums_str) ||
                !zookeeper->exists(current_part_path + "/columns", &columns_stat_after) ||
                columns_stat_before.version != columns_stat_after.version)
            {
                LOG_INFO(log, "Not checking checksums of part {} with replica {} because part changed while we were reading its checksums", part_name, replica);
                continue;
            }

            replica_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksumsZNodes(
                columns_str, checksums_str);
        }

        if (replica_part_header.getColumnsHash() != local_part_header.getColumnsHash())
        {
            LOG_INFO(log, "Not checking checksums of part {} with replica {} because columns are different", part_name, replica);
            continue;
        }

        replica_part_header.getChecksums().checkEqual(local_part_header.getChecksums(), true);

        if (replica == replica_name)
            has_been_already_added = true;

        /// If we verify checksums in "sequential manner" (i.e. recheck absence of checksums on other replicas when commit)
        /// then it is enough to verify checksums on at least one replica since checksums on other replicas must be the same.
        if (absent_replicas_paths)
        {
            absent_replicas_paths->clear();
            break;
        }
    }

    if (!has_been_already_added)
    {
        const auto storage_settings_ptr = getSettings();
        String part_path = replica_path + "/parts/" + part_name;

        //ops.emplace_back(zkutil::makeCheckRequest(
        //    zookeeper_path + "/columns", expected_columns_version));

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
                part_path + "/columns", part->getColumns().toString(), zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path + "/checksums", getChecksumsForZooKeeper(part->checksums), zkutil::CreateMode::Persistent));
        }
    }
    else
    {
        LOG_WARNING(log, "checkPartAndAddToZooKeeper: node {} already exists. Will not commit any nodes.", replica_path + "/parts/" + part_name);
    }
}

MergeTreeData::DataPartsVector StorageReplicatedMergeTree::checkPartChecksumsAndCommit(Transaction & transaction,
    const DataPartPtr & part)
{
    auto zookeeper = getZooKeeper();

    while (true)
    {
        Coordination::Requests ops;
        NameSet absent_part_paths_on_replicas;

        /// Checksums are checked here and `ops` is filled. In fact, the part is added to ZK just below, when executing `multi`.
        checkPartChecksumsAndAddCommitOps(zookeeper, part, ops, part->name, &absent_part_paths_on_replicas);

        /// Do not commit if the part is obsolete, we have just briefly checked its checksums
        if (transaction.isEmpty())
            return {};

        /// Will check that the part did not suddenly appear on skipped replicas
        if (!absent_part_paths_on_replicas.empty())
        {
            Coordination::Requests new_ops;
            for (const String & part_path : absent_part_paths_on_replicas)
            {
                new_ops.emplace_back(zkutil::makeCreateRequest(part_path, "", zkutil::CreateMode::Persistent));
                new_ops.emplace_back(zkutil::makeRemoveRequest(part_path, -1));
            }

            /// Add check ops at the beginning
            new_ops.insert(new_ops.end(), ops.begin(), ops.end());
            ops = std::move(new_ops);
        }

        try
        {
            zookeeper->multi(ops);
            return transaction.commit();
        }
        catch (const zkutil::KeeperMultiException & e)
        {
            size_t num_check_ops = 2 * absent_part_paths_on_replicas.size();
            size_t failed_op_index = e.failed_op_index;

            if (failed_op_index < num_check_ops && e.code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_INFO(log, "The part {} on a replica suddenly appeared, will recheck checksums", e.getPathForFirstFailedOp());
            }
            else
                throw;
        }
    }
}

String StorageReplicatedMergeTree::getChecksumsForZooKeeper(const MergeTreeDataPartChecksums & checksums) const
{
    return MinimalisticDataPartChecksums::getSerializedString(checksums,
        getSettings()->use_minimalistic_checksums_in_zookeeper);
}


bool StorageReplicatedMergeTree::executeLogEntry(LogEntry & entry)
{
    if (entry.type == LogEntry::DROP_RANGE)
    {
        executeDropRange(entry);
        return true;
    }

    if (entry.type == LogEntry::REPLACE_RANGE)
    {
        executeReplaceRange(entry);
        return true;
    }

    if (entry.type == LogEntry::GET_PART ||
        entry.type == LogEntry::MERGE_PARTS ||
        entry.type == LogEntry::MUTATE_PART)
    {
        /// If we already have this part or a part covering it, we do not need to do anything.
        /// The part may be still in the PreCommitted -> Committed transition so we first search
        /// among PreCommitted parts to definitely find the desired part if it exists.
        DataPartPtr existing_part = getPartIfExists(entry.new_part_name, {MergeTreeDataPartState::PreCommitted});
        if (!existing_part)
            existing_part = getActiveContainingPart(entry.new_part_name);

        /// Even if the part is locally, it (in exceptional cases) may not be in ZooKeeper. Let's check that it is there.
        if (existing_part && getZooKeeper()->exists(replica_path + "/parts/" + existing_part->name))
        {
            if (!(entry.type == LogEntry::GET_PART && entry.source_replica == replica_name))
            {
                LOG_DEBUG(log, "Skipping action for part {} because part {} already exists.", entry.new_part_name, existing_part->name);
            }
            return true;
        }
    }

    if (entry.type == LogEntry::GET_PART && entry.source_replica == replica_name)
        LOG_WARNING(log, "Part {} from own log doesn't exist.", entry.new_part_name);

    /// Perhaps we don't need this part, because during write with quorum, the quorum has failed (see below about `/quorum/failed_parts`).
    if (entry.quorum && getZooKeeper()->exists(zookeeper_path + "/quorum/failed_parts/" + entry.new_part_name))
    {
        LOG_DEBUG(log, "Skipping action for part {} because quorum for that part was failed.", entry.new_part_name);
        return true;    /// NOTE Deletion from `virtual_parts` is not done, but it is only necessary for merge.
    }

    bool do_fetch = false;
    if (entry.type == LogEntry::GET_PART)
    {
        do_fetch = true;
    }
    else if (entry.type == LogEntry::MERGE_PARTS)
    {
        /// Sometimes it's better to fetch merged part instead of merge
        /// For example when we don't have all source parts for merge
        do_fetch = !tryExecuteMerge(entry);
    }
    else if (entry.type == LogEntry::MUTATE_PART)
    {
        /// Sometimes it's better to fetch mutated part instead of merge
        do_fetch = !tryExecutePartMutation(entry);
    }
    else if (entry.type == LogEntry::ALTER_METADATA)
    {
        return executeMetadataAlter(entry);
    }
    else
    {
        throw Exception("Unexpected log entry type: " + toString(static_cast<int>(entry.type)), ErrorCodes::LOGICAL_ERROR);
    }

    if (do_fetch)
        return executeFetch(entry);

    return true;
}

bool StorageReplicatedMergeTree::tryExecuteMerge(const LogEntry & entry)
{
    // Log source part names just in case
    {
        std::stringstream source_parts_msg;
        for (auto i : ext::range(0, entry.source_parts.size()))
            source_parts_msg << (i != 0 ? ", " : "") << entry.source_parts[i];

        LOG_TRACE(log, "Executing log entry to merge parts {} to {}", source_parts_msg.str(), entry.new_part_name);
    }

    const auto storage_settings_ptr = getSettings();

    if (storage_settings_ptr->always_fetch_merged_part)
    {
        LOG_INFO(log, "Will fetch part {} because setting 'always_fetch_merged_part' is true", entry.new_part_name);
        return false;
    }

    DataPartsVector parts;
    bool have_all_parts = true;
    for (const String & name : entry.source_parts)
    {
        DataPartPtr part = getActiveContainingPart(name);
        if (!part)
        {
            have_all_parts = false;
            break;
        }
        if (part->name != name)
        {
            LOG_WARNING(log, "Part {} is covered by {} but should be merged into {}. This shouldn't happen often.", name, part->name, entry.new_part_name);
            have_all_parts = false;
            break;
        }
        parts.push_back(part);
    }

    if (!have_all_parts)
    {
        /// If you do not have all the necessary parts, try to take some already merged part from someone.
        LOG_DEBUG(log, "Don't have all parts for merge {}; will try to fetch it instead", entry.new_part_name);
        return false;
    }
    else if (entry.create_time + storage_settings_ptr->prefer_fetch_merged_part_time_threshold.totalSeconds() <= time(nullptr))
    {
        /// If entry is old enough, and have enough size, and part are exists in any replica,
        ///  then prefer fetching of merged part from replica.

        size_t sum_parts_bytes_on_disk = 0;
        for (const auto & part : parts)
            sum_parts_bytes_on_disk += part->getBytesOnDisk();

        if (sum_parts_bytes_on_disk >= storage_settings_ptr->prefer_fetch_merged_part_size_threshold)
        {
            String replica = findReplicaHavingPart(entry.new_part_name, true);    /// NOTE excessive ZK requests for same data later, may remove.
            if (!replica.empty())
            {
                LOG_DEBUG(log, "Prefer to fetch {} from replica {}", entry.new_part_name, replica);
                return false;
            }
        }
    }

    /// Start to make the main work
    size_t estimated_space_for_merge = MergeTreeDataMergerMutator::estimateNeededDiskSpace(parts);

    /// Can throw an exception while reserving space.
    IMergeTreeDataPart::TTLInfos ttl_infos;
    size_t max_volume_index = 0;
    for (auto & part_ptr : parts)
    {
        ttl_infos.update(part_ptr->ttl_infos);
        max_volume_index = std::max(max_volume_index, getStoragePolicy()->getVolumeIndexByDisk(part_ptr->volume->getDisk()));
    }
    ReservationPtr reserved_space = reserveSpacePreferringTTLRules(estimated_space_for_merge,
            ttl_infos, time(nullptr), max_volume_index);

    auto table_lock = lockForShare(RWLockImpl::NO_QUERY, storage_settings_ptr->lock_acquire_timeout_for_background_operations);
    StorageMetadataPtr metadata_snapshot = getInMemoryMetadataPtr();

    FutureMergedMutatedPart future_merged_part(parts, entry.new_part_type);
    if (future_merged_part.name != entry.new_part_name)
    {
        throw Exception("Future merged part name " + backQuote(future_merged_part.name) + " differs from part name in log entry: "
            + backQuote(entry.new_part_name), ErrorCodes::BAD_DATA_PART_NAME);
    }
    future_merged_part.updatePath(*this, reserved_space);

    auto table_id = getStorageID();
    MergeList::EntryPtr merge_entry = global_context.getMergeList().insert(table_id.database_name, table_id.table_name, future_merged_part);

    Transaction transaction(*this);
    MutableDataPartPtr part;

    Stopwatch stopwatch;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        writePartLog(
            PartLogElement::MERGE_PARTS, execution_status, stopwatch.elapsed(),
            entry.new_part_name, part, parts, merge_entry.get());
    };

    try
    {
        part = merger_mutator.mergePartsToTemporaryPart(
            future_merged_part, metadata_snapshot, *merge_entry,
            table_lock, entry.create_time, reserved_space, entry.deduplicate,
            entry.force_ttl);

        merger_mutator.renameMergedTemporaryPart(part, parts, &transaction);

        try
        {
            checkPartChecksumsAndCommit(transaction, part);
        }
        catch (const Exception & e)
        {
            if (MergeTreeDataPartChecksums::isBadChecksumsErrorCode(e.code()))
            {
                transaction.rollback();

                ProfileEvents::increment(ProfileEvents::DataAfterMergeDiffersFromReplica);

                LOG_ERROR(log, "{}. Data after merge is not byte-identical to data on another replicas. There could be several reasons: 1. Using newer version of compression library after server update. 2. Using another compression method. 3. Non-deterministic compression algorithm (highly unlikely). 4. Non-deterministic merge algorithm due to logical error in code. 5. Data corruption in memory due to bug in code. 6. Data corruption in memory due to hardware issue. 7. Manual modification of source data after server startup. 8. Manual modification of checksums stored in ZooKeeper. 9. Part format related settings like 'enable_mixed_granularity_parts' are different on different replicas. We will download merged part from replica to force byte-identical result.", getCurrentExceptionMessage(false));

                write_part_log(ExecutionStatus::fromCurrentException());

                tryRemovePartImmediately(std::move(part));
                /// No need to delete the part from ZK because we can be sure that the commit transaction
                /// didn't go through.

                return false;
            }

            throw;
        }

        /** Removing old parts from ZK and from the disk is delayed - see ReplicatedMergeTreeCleanupThread, clearOldParts.
          */

        /** With `ZSESSIONEXPIRED` or `ZOPERATIONTIMEOUT`, we can inadvertently roll back local changes to the parts.
          * This is not a problem, because in this case the merge will remain in the queue, and we will try again.
          */
        merge_selecting_task->schedule();
        ProfileEvents::increment(ProfileEvents::ReplicatedPartMerges);

        write_part_log({});

        return true;
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }
}

bool StorageReplicatedMergeTree::tryExecutePartMutation(const StorageReplicatedMergeTree::LogEntry & entry)
{
    const String & source_part_name = entry.source_parts.at(0);
    const auto storage_settings_ptr = getSettings();
    LOG_TRACE(log, "Executing log entry to mutate part {} to {}", source_part_name, entry.new_part_name);

    DataPartPtr source_part = getActiveContainingPart(source_part_name);
    if (!source_part)
    {
        LOG_DEBUG(log, "Source part {} for {} is not ready; will try to fetch it instead", source_part_name, entry.new_part_name);
        return false;
    }

    if (source_part->name != source_part_name)
    {
        throw Exception("Part " + source_part_name + " is covered by " + source_part->name
            + " but should be mutated to " + entry.new_part_name + ". This is a bug.",
            ErrorCodes::LOGICAL_ERROR);
    }

    /// TODO - some better heuristic?
    size_t estimated_space_for_result = MergeTreeDataMergerMutator::estimateNeededDiskSpace({source_part});

    if (entry.create_time + storage_settings_ptr->prefer_fetch_merged_part_time_threshold.totalSeconds() <= time(nullptr)
        && estimated_space_for_result >= storage_settings_ptr->prefer_fetch_merged_part_size_threshold)
    {
        /// If entry is old enough, and have enough size, and some replica has the desired part,
        /// then prefer fetching from replica.
        String replica = findReplicaHavingPart(entry.new_part_name, true);    /// NOTE excessive ZK requests for same data later, may remove.
        if (!replica.empty())
        {
            LOG_DEBUG(log, "Prefer to fetch {} from replica {}", entry.new_part_name, replica);
            return false;
        }
    }

    MergeTreePartInfo new_part_info = MergeTreePartInfo::fromPartName(
        entry.new_part_name, format_version);
    MutationCommands commands = queue.getMutationCommands(source_part, new_part_info.mutation);

    /// Once we mutate part, we must reserve space on the same disk, because mutations can possibly create hardlinks.
    /// Can throw an exception.
    ReservationPtr reserved_space = reserveSpace(estimated_space_for_result, source_part->volume);

    auto table_lock = lockForShare(
            RWLockImpl::NO_QUERY, storage_settings_ptr->lock_acquire_timeout_for_background_operations);
    StorageMetadataPtr metadata_snapshot = getInMemoryMetadataPtr();

    MutableDataPartPtr new_part;
    Transaction transaction(*this);

    FutureMergedMutatedPart future_mutated_part;
    future_mutated_part.parts.push_back(source_part);
    future_mutated_part.part_info = new_part_info;
    future_mutated_part.name = entry.new_part_name;
    future_mutated_part.updatePath(*this, reserved_space);
    future_mutated_part.type = source_part->getType();

    auto table_id = getStorageID();
    MergeList::EntryPtr merge_entry = global_context.getMergeList().insert(
        table_id.database_name, table_id.table_name, future_mutated_part);

    Stopwatch stopwatch;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        writePartLog(
            PartLogElement::MUTATE_PART, execution_status, stopwatch.elapsed(),
            entry.new_part_name, new_part, future_mutated_part.parts, merge_entry.get());
    };

    try
    {
        new_part = merger_mutator.mutatePartToTemporaryPart(
            future_mutated_part, metadata_snapshot, commands, *merge_entry,
            entry.create_time, global_context, reserved_space, table_lock);
        renameTempPartAndReplace(new_part, nullptr, &transaction);

        try
        {
            checkPartChecksumsAndCommit(transaction, new_part);
        }
        catch (const Exception & e)
        {
            if (MergeTreeDataPartChecksums::isBadChecksumsErrorCode(e.code()))
            {
                transaction.rollback();

                ProfileEvents::increment(ProfileEvents::DataAfterMutationDiffersFromReplica);

                LOG_ERROR(log, "{}. Data after mutation is not byte-identical to data on another replicas. We will download merged part from replica to force byte-identical result.", getCurrentExceptionMessage(false));

                write_part_log(ExecutionStatus::fromCurrentException());

                tryRemovePartImmediately(std::move(new_part));
                /// No need to delete the part from ZK because we can be sure that the commit transaction
                /// didn't go through.

                return false;
            }

            throw;
        }

        /** With `ZSESSIONEXPIRED` or `ZOPERATIONTIMEOUT`, we can inadvertently roll back local changes to the parts.
          * This is not a problem, because in this case the entry will remain in the queue, and we will try again.
          */
        merge_selecting_task->schedule();
        ProfileEvents::increment(ProfileEvents::ReplicatedPartMutations);
        write_part_log({});

        return true;
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }
}


bool StorageReplicatedMergeTree::executeFetch(LogEntry & entry)
{
    /// Looking for covering part. After that entry.actual_new_part_name may be filled.
    String replica = findReplicaHavingCoveringPart(entry, true);
    const auto storage_settings_ptr = getSettings();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    static std::atomic_uint total_fetches {0};
    if (storage_settings_ptr->replicated_max_parallel_fetches && total_fetches >= storage_settings_ptr->replicated_max_parallel_fetches)
    {
        throw Exception("Too many total fetches from replicas, maximum: " + storage_settings_ptr->replicated_max_parallel_fetches.toString(),
            ErrorCodes::TOO_MANY_FETCHES);
    }

    ++total_fetches;
    SCOPE_EXIT({--total_fetches;});

    if (storage_settings_ptr->replicated_max_parallel_fetches_for_table
        && current_table_fetches >= storage_settings_ptr->replicated_max_parallel_fetches_for_table)
    {
        throw Exception("Too many fetches from replicas for table, maximum: " + storage_settings_ptr->replicated_max_parallel_fetches_for_table.toString(),
            ErrorCodes::TOO_MANY_FETCHES);
    }

    ++current_table_fetches;
    SCOPE_EXIT({--current_table_fetches;});

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
                    throw Exception("Logical error: log entry with quorum but type is not GET_PART", ErrorCodes::LOGICAL_ERROR);

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

                Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

                Coordination::Requests ops;

                for (const auto & path_part : replicas)
                {
                    Coordination::Stat stat;
                    String path = zookeeper_path + "/replicas/" + path_part + "/host";
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
                    String quorum_path = zookeeper_path + "/quorum/status";
                    String quorum_str = zookeeper->get(quorum_path, &quorum_stat);
                    ReplicatedMergeTreeQuorumEntry quorum_entry;
                    quorum_entry.fromString(quorum_str);

                    if (quorum_entry.part_name == entry.new_part_name)
                    {
                        ops.emplace_back(zkutil::makeRemoveRequest(quorum_path, quorum_stat.version));

                        auto part_info = MergeTreePartInfo::fromPartName(entry.new_part_name, format_version);

                        if (part_info.min_block != part_info.max_block)
                            throw Exception("Logical error: log entry with quorum for part covering more than one block number",
                                ErrorCodes::LOGICAL_ERROR);

                        ops.emplace_back(zkutil::makeCreateRequest(
                            zookeeper_path + "/quorum/failed_parts/" + entry.new_part_name,
                            "",
                            zkutil::CreateMode::Persistent));

                        /// Deleting from `blocks`.
                        if (!entry.block_id.empty() && zookeeper->exists(zookeeper_path + "/blocks/" + entry.block_id))
                            ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/blocks/" + entry.block_id, -1));

                        Coordination::Responses responses;
                        auto code = zookeeper->tryMulti(ops, responses);

                        if (code == Coordination::Error::ZOK)
                        {
                            LOG_DEBUG(log, "Marked quorum for part {} as failed.", entry.new_part_name);
                            queue.removeFromVirtualParts(part_info);
                            return true;
                        }
                        else if (code == Coordination::Error::ZBADVERSION || code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
                        {
                            LOG_DEBUG(log, "State was changed or isn't expected when trying to mark quorum for part {} as failed. Code: {}", entry.new_part_name, Coordination::errorMessage(code));
                        }
                        else
                            throw Coordination::Exception(code);
                    }
                    else
                    {
                        LOG_WARNING(log, "No active replica has part {}, but that part needs quorum and /quorum/status contains entry about another part {}. It means that part was successfully written to {} replicas, but then all of them goes offline. Or it is a bug.", entry.new_part_name, quorum_entry.part_name, entry.quorum);
                    }
                }
            }

            if (replica.empty())
            {
                ProfileEvents::increment(ProfileEvents::ReplicatedPartFailedFetches);
                throw Exception("No active replica has part " + entry.new_part_name + " or covering part", ErrorCodes::NO_REPLICA_HAS_PART);
            }
        }

        try
        {
            String part_name = entry.actual_new_part_name.empty() ? entry.new_part_name : entry.actual_new_part_name;
            if (!fetchPart(part_name, metadata_snapshot, zookeeper_path + "/replicas/" + replica, false, entry.quorum))
                return false;
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


void StorageReplicatedMergeTree::executeDropRange(const LogEntry & entry)
{
    auto drop_range_info = MergeTreePartInfo::fromPartName(entry.new_part_name, format_version);
    queue.removePartProducingOpsInRange(getZooKeeper(), drop_range_info, entry);

    if (entry.detach)
        LOG_DEBUG(log, "Detaching parts.");
    else
        LOG_DEBUG(log, "Removing parts.");

    /// Delete the parts contained in the range to be deleted.
    /// It's important that no old parts remain (after the merge), because otherwise,
    ///  after adding a new replica, this new replica downloads them, but does not delete them.
    /// And, if you do not, the parts will come to life after the server is restarted.
    /// Therefore, we use all data parts.

    auto metadata_snapshot = getInMemoryMetadataPtr();
    DataPartsVector parts_to_remove;
    {
        auto data_parts_lock = lockParts();
        parts_to_remove = removePartsInRangeFromWorkingSet(drop_range_info, true, true, data_parts_lock);
    }

    if (entry.detach)
    {
        /// If DETACH clone parts to detached/ directory
        for (const auto & part : parts_to_remove)
        {
            LOG_INFO(log, "Detaching {}", part->relative_path);
            part->makeCloneInDetached("", metadata_snapshot);
        }
    }

    /// Forcibly remove parts from ZooKeeper
    tryRemovePartsFromZooKeeperWithRetries(parts_to_remove);

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
    auto & entry_replace = *entry.replace_range_entry;
    auto metadata_snapshot = getInMemoryMetadataPtr();

    MergeTreePartInfo drop_range = MergeTreePartInfo::fromPartName(entry_replace.drop_range_part_name, format_version);
    /// Range with only one block has special meaning ATTACH PARTITION
    bool replace = drop_range.getBlocksCount() > 1;

    queue.removePartProducingOpsInRange(getZooKeeper(), drop_range, entry);

    struct PartDescription
    {
        PartDescription(size_t index_, const String & src_part_name_, const String & new_part_name_, const String & checksum_hex_,
                        MergeTreeDataFormatVersion format_version)
            : index(index_),
            src_part_name(src_part_name_), src_part_info(MergeTreePartInfo::fromPartName(src_part_name_, format_version)),
            new_part_name(new_part_name_), new_part_info(MergeTreePartInfo::fromPartName(new_part_name_, format_version)),
            checksum_hex(checksum_hex_) {}

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
    };

    using PartDescriptionPtr = std::shared_ptr<PartDescription>;
    using PartDescriptions = std::vector<PartDescriptionPtr>;

    PartDescriptions all_parts;
    PartDescriptions parts_to_add;
    DataPartsVector parts_to_remove;

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
            if (!getActiveContainingPart(part_desc->new_part_info, MergeTreeDataPartState::Committed, data_parts_lock))
                parts_to_add.emplace_back(part_desc);
        }

        if (parts_to_add.empty() && replace)
            parts_to_remove = removePartsInRangeFromWorkingSet(drop_range, true, false, data_parts_lock);
    }

    if (parts_to_add.empty())
    {
        LOG_INFO(log, "All parts from REPLACE PARTITION command have been already attached");
        tryRemovePartsFromZooKeeperWithRetries(parts_to_remove);
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
        source_table = DatabaseCatalog::instance().tryGetTable(source_table_id, global_context);
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
            MergeTreeDataPartState::PreCommitted, MergeTreeDataPartState::Committed, MergeTreeDataPartState::Outdated};

        size_t num_clonable_parts = 0;
        for (PartDescriptionPtr & part_desc : parts_to_add)
        {
            auto src_part = src_data->getPartIfExists(part_desc->src_part_info, valid_states);
            if (!src_part)
            {
                LOG_DEBUG(log, "There is no part {} in {}", part_desc->src_part_name, source_table_id.getNameForLogs());
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
        if (adding_parts_active_set.getContainingPart(part_desc->new_part_info).empty())
        {
            throw Exception("Not found part " + part_desc->new_part_name +
                            " (or part covering it) neither source table neither remote replicas" , ErrorCodes::NO_REPLICA_HAS_PART);
        }
    }

    /// Filter covered parts
    PartDescriptions final_parts;
    {
        Strings final_part_names = adding_parts_active_set.getParts();

        for (const String & final_part_name : final_part_names)
        {
            auto part_desc = part_name_to_desc[final_part_name];
            if (!part_desc)
                throw Exception("There is no final part " + final_part_name + ". This is a bug", ErrorCodes::LOGICAL_ERROR);

            final_parts.emplace_back(part_desc);

            if (final_parts.size() > 1)
            {
                auto & prev = *final_parts[final_parts.size() - 2];
                auto & curr = *final_parts[final_parts.size() - 1];

                if (!prev.found_new_part_info.isDisjoint(curr.found_new_part_info))
                {
                    throw Exception("Intersected final parts detected: " + prev.found_new_part_name
                        + " and " + curr.found_new_part_name + ". It should be investigated.", ErrorCodes::INCORRECT_DATA);
                }
            }
        }
    }

    static const String TMP_PREFIX = "tmp_replace_from_";

    auto obtain_part = [&] (PartDescriptionPtr & part_desc)
    {
        if (part_desc->src_table_part)
        {

            if (part_desc->checksum_hex != part_desc->src_table_part->checksums.getTotalChecksumHex())
                throw Exception("Checksums of " + part_desc->src_table_part->name + " is suddenly changed", ErrorCodes::UNFINISHED);

            part_desc->res_part = cloneAndLoadDataPartOnSameDisk(
                part_desc->src_table_part, TMP_PREFIX + "clone_", part_desc->new_part_info, metadata_snapshot);
        }
        else if (!part_desc->replica.empty())
        {
            String source_replica_path = zookeeper_path + "/replicas/" + part_desc->replica;
            ReplicatedMergeTreeAddress address(getZooKeeper()->get(source_replica_path + "/host"));
            auto timeouts = ConnectionTimeouts::getHTTPTimeouts(global_context);
            auto [user, password] = global_context.getInterserverCredentials();
            String interserver_scheme = global_context.getInterserverScheme();

            if (interserver_scheme != address.scheme)
                throw Exception("Interserver schemas are different '" + interserver_scheme + "' != '" + address.scheme + "', can't fetch part from " + address.host, ErrorCodes::LOGICAL_ERROR);

            part_desc->res_part = fetcher.fetchPart(
                metadata_snapshot, part_desc->found_new_part_name, source_replica_path,
                address.host, address.replication_port, timeouts, user, password, interserver_scheme, false, TMP_PREFIX + "fetch_");

            /// TODO: check columns_version of fetched part

            ProfileEvents::increment(ProfileEvents::ReplicatedPartFetches);
        }
        else
            throw Exception("There is no receipt to produce part " + part_desc->new_part_name + ". This is bug", ErrorCodes::LOGICAL_ERROR);
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
        Transaction transaction(*this);

        Coordination::Requests ops;
        for (PartDescriptionPtr & part_desc : final_parts)
        {
            renameTempPartAndReplace(part_desc->res_part, nullptr, &transaction);
            getCommitPartOps(ops, part_desc->res_part);

            if (ops.size() > zkutil::MULTI_BATCH_SIZE)
            {
                zookeeper->multi(ops);
                ops.clear();
            }
        }

        if (!ops.empty())
            zookeeper->multi(ops);

        {
            auto data_parts_lock = lockParts();

            transaction.commit(&data_parts_lock);
            if (replace)
                parts_to_remove = removePartsInRangeFromWorkingSet(drop_range, true, false, data_parts_lock);
        }

        PartLog::addNewParts(global_context, res_parts, watch.elapsed());
    }
    catch (...)
    {
        PartLog::addNewParts(global_context, res_parts, watch.elapsed(), ExecutionStatus::fromCurrentException());
        throw;
    }

    tryRemovePartsFromZooKeeperWithRetries(parts_to_remove);
    res_parts.clear();
    parts_to_remove.clear();
    cleanup_thread.wakeup();

    return true;
}


void StorageReplicatedMergeTree::cloneReplica(const String & source_replica, Coordination::Stat source_is_lost_stat, zkutil::ZooKeeperPtr & zookeeper)
{
    LOG_INFO(log, "Will mimic {}", source_replica);

    String source_path = zookeeper_path + "/replicas/" + source_replica;

    /** TODO: it will be deleted! (It is only to support old version of CH server).
      * In current code, the replica is created in single transaction.
      * If the reference/master replica is not yet fully created, let's wait.
      */
    while (!zookeeper->exists(source_path + "/columns"))
    {
        LOG_INFO(log, "Waiting for replica {} to be fully created", source_path);

        zkutil::EventPtr event = std::make_shared<Poco::Event>();
        if (zookeeper->exists(source_path + "/columns", nullptr, event))
        {
            LOG_WARNING(log, "Oops, a watch has leaked");
            break;
        }

        event->wait();
    }

    /// The order of the following three actions is important.

    Strings source_queue_names;
    /// We are trying to get consistent /log_pointer and /queue state. Otherwise
    /// we can possibly duplicate entries in queue of cloned replica.
    while (true)
    {
        Coordination::Stat log_pointer_stat;
        String raw_log_pointer = zookeeper->get(source_path + "/log_pointer", &log_pointer_stat);

        Coordination::Requests ops;
        ops.push_back(zkutil::makeSetRequest(replica_path + "/log_pointer", raw_log_pointer, -1));

        /// For support old versions CH.
        if (source_is_lost_stat.version == -1)
        {
            /// We check that it was not suddenly upgraded to new version.
            /// Otherwise it can be upgraded and instantly become lost, but we cannot notice that.
            ops.push_back(zkutil::makeCreateRequest(source_path + "/is_lost", "0", zkutil::CreateMode::Persistent));
            ops.push_back(zkutil::makeRemoveRequest(source_path + "/is_lost", -1));
        }
        else /// The replica we clone should not suddenly become lost.
            ops.push_back(zkutil::makeCheckRequest(source_path + "/is_lost", source_is_lost_stat.version));

        Coordination::Responses responses;

        /// Let's remember the queue of the reference/master replica.
        source_queue_names = zookeeper->getChildren(source_path + "/queue");

        /// Check that our log pointer didn't changed while we read queue entries
        ops.push_back(zkutil::makeCheckRequest(source_path + "/log_pointer", log_pointer_stat.version));

        auto rc = zookeeper->tryMulti(ops, responses);

        if (rc == Coordination::Error::ZOK)
        {
            break;
        }
        else if (rc == Coordination::Error::ZNODEEXISTS)
        {
            throw Exception(
                "Can not clone replica, because the " + source_replica + " updated to new ClickHouse version",
                ErrorCodes::REPLICA_STATUS_CHANGED);
        }
        else if (responses[1]->error == Coordination::Error::ZBADVERSION)
        {
            /// If is_lost node version changed than source replica also lost,
            /// so we cannot clone from it.
            throw Exception(
                "Can not clone replica, because the " + source_replica + " became lost", ErrorCodes::REPLICA_STATUS_CHANGED);
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

    std::sort(source_queue_names.begin(), source_queue_names.end());

    Strings source_queue;
    for (const String & entry_name : source_queue_names)
    {
        String entry;
        if (!zookeeper->tryGet(source_path + "/queue/" + entry_name, entry))
            continue;
        source_queue.push_back(entry);
    }

    /// Add to the queue jobs to receive all the active parts that the reference/master replica has.
    Strings source_replica_parts = zookeeper->getChildren(source_path + "/parts");
    ActiveDataPartSet active_parts_set(format_version, source_replica_parts);

    Strings active_parts = active_parts_set.getParts();

    /// Remove local parts if source replica does not have them, because such parts will never be fetched by other replicas.
    Strings local_parts_in_zk = zookeeper->getChildren(replica_path + "/parts");
    Strings parts_to_remove_from_zk;
    for (const auto & part : local_parts_in_zk)
    {
        if (active_parts_set.getContainingPart(part).empty())
        {
            queue.remove(zookeeper, part);
            parts_to_remove_from_zk.emplace_back(part);
            LOG_WARNING(log, "Source replica does not have part {}. Removing it from ZooKeeper.", part);
        }
    }
    tryRemovePartsFromZooKeeperWithRetries(parts_to_remove_from_zk);

    auto local_active_parts = getDataParts();
    DataPartsVector parts_to_remove_from_working_set;
    for (const auto & part : local_active_parts)
    {
        if (active_parts_set.getContainingPart(part->name).empty())
        {
            parts_to_remove_from_working_set.emplace_back(part);
            LOG_WARNING(log, "Source replica does not have part {}. Removing it from working set.", part->name);
        }
    }
    removePartsFromWorkingSet(parts_to_remove_from_working_set, true);

    for (const String & name : active_parts)
    {
        LogEntry log_entry;
        log_entry.type = LogEntry::GET_PART;
        log_entry.source_replica = "";
        log_entry.new_part_name = name;
        log_entry.create_time = tryGetPartCreateTime(zookeeper, source_path, name);

        zookeeper->create(replica_path + "/queue/queue-", log_entry.toString(), zkutil::CreateMode::PersistentSequential);
    }

    LOG_DEBUG(log, "Queued {} parts to be fetched", active_parts.size());

    /// Add content of the reference/master replica queue to the queue.
    for (const String & entry : source_queue)
    {
        zookeeper->create(replica_path + "/queue/queue-", entry, zkutil::CreateMode::PersistentSequential);
    }

    LOG_DEBUG(log, "Copied {} queue entries", source_queue.size());
}


void StorageReplicatedMergeTree::cloneReplicaIfNeeded(zkutil::ZooKeeperPtr zookeeper)
{
    String res;
    if (zookeeper->tryGet(replica_path + "/is_lost", res))
    {
        if (res == "0")
            return;
    }
    else
    {
        /// Replica was created by old version of CH, so me must create "/is_lost".
        /// Note that in old version of CH there was no "lost" replicas possible.
        zookeeper->create(replica_path + "/is_lost", "0", zkutil::CreateMode::Persistent);
        return;
    }

    /// is_lost is "1": it means that we are in repair mode.

    String source_replica;
    Coordination::Stat source_is_lost_stat;
    source_is_lost_stat.version = -1;

    for (const String & source_replica_name : zookeeper->getChildren(zookeeper_path + "/replicas"))
    {
        String source_replica_path = zookeeper_path + "/replicas/" + source_replica_name;

        /// Do not clone from myself.
        if (source_replica_path != replica_path)
        {
            /// Do not clone from lost replicas.
            String source_replica_is_lost_value;
            if (!zookeeper->tryGet(source_replica_path + "/is_lost", source_replica_is_lost_value, &source_is_lost_stat)
                || source_replica_is_lost_value == "0")
            {
                source_replica = source_replica_name;
                break;
            }
        }
    }

    if (source_replica.empty())
        throw Exception("All replicas are lost", ErrorCodes::ALL_REPLICAS_LOST);

    /// Clear obsolete queue that we no longer need.
    zookeeper->removeChildren(replica_path + "/queue");

    /// Will do repair from the selected replica.
    cloneReplica(source_replica, source_is_lost_stat, zookeeper);
    /// If repair fails to whatever reason, the exception is thrown, is_lost will remain "1" and the replica will be repaired later.

    /// If replica is repaired successfully, we remove is_lost flag.
    zookeeper->set(replica_path + "/is_lost", "0");
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
        queue.pullLogsToQueue(getZooKeeper(), queue_updating_task->getWatchCallback());
        last_queue_update_finish_time.store(time(nullptr));
        queue_update_in_progress = false;
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

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
        queue_updating_task->scheduleAfter(QUEUE_UPDATE_ERROR_SLEEP_MS);
    }
}


void StorageReplicatedMergeTree::mutationsUpdatingTask()
{
    try
    {
        queue.updateMutations(getZooKeeper(), mutations_updating_task->getWatchCallback());
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


BackgroundProcessingPoolTaskResult StorageReplicatedMergeTree::queueTask()
{
    /// If replication queue is stopped exit immediately as we successfully executed the task
    if (queue.actions_blocker.isCancelled())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        return BackgroundProcessingPoolTaskResult::SUCCESS;
    }

    /// This object will mark the element of the queue as running.
    ReplicatedMergeTreeQueue::SelectedEntry selected;

    try
    {
        selected = queue.selectEntryToProcess(merger_mutator, *this);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    LogEntryPtr & entry = selected.first;

    if (!entry)
        return BackgroundProcessingPoolTaskResult::NOTHING_TO_DO;

    time_t prev_attempt_time = entry->last_attempt_time;

    bool res = queue.processEntry([this]{ return getZooKeeper(); }, entry, [&](LogEntryPtr & entry_to_process)
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
                LOG_INFO(log, e.displayText());
            }
            else if (e.code() == ErrorCodes::ABORTED)
            {
                /// Interrupted merge or downloading a part is not an error.
                LOG_INFO(log, e.message());
            }
            else if (e.code() == ErrorCodes::PART_IS_TEMPORARILY_LOCKED)
            {
                /// Part cannot be added temporarily
                LOG_INFO(log, e.displayText());
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

    /// We will go to sleep if the processing fails and if we have already processed this record recently.
    bool need_sleep = !res && (entry->last_attempt_time - prev_attempt_time < 10);

    /// If there was no exception, you do not need to sleep.
    return need_sleep ? BackgroundProcessingPoolTaskResult::ERROR : BackgroundProcessingPoolTaskResult::SUCCESS;
}


bool StorageReplicatedMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    return queue.isVirtualPart(part);
}

BackgroundProcessingPoolTaskResult StorageReplicatedMergeTree::movePartsTask()
{
    try
    {
        if (!selectPartsAndMove())
            return BackgroundProcessingPoolTaskResult::NOTHING_TO_DO;

        return BackgroundProcessingPoolTaskResult::SUCCESS;
    }
    catch (...)
    {
        tryLogCurrentException(log);
        return BackgroundProcessingPoolTaskResult::ERROR;
    }
}


void StorageReplicatedMergeTree::mergeSelectingTask()
{
    if (!is_leader)
        return;

    const auto storage_settings_ptr = getSettings();
    const bool deduplicate = false; /// TODO: read deduplicate option from table config
    const bool force_ttl = false;

    CreateMergeEntryResult create_result = CreateMergeEntryResult::Other;

    try
    {
        /// We must select parts for merge under merge_selecting_mutex because other threads
        /// (OPTIMIZE queries) can assign new merges.
        std::lock_guard merge_selecting_lock(merge_selecting_mutex);

        auto zookeeper = getZooKeeper();

        ReplicatedMergeTreeMergePredicate merge_pred = queue.getMergePredicate(zookeeper);

        /// If many merges is already queued, then will queue only small enough merges.
        /// Otherwise merge queue could be filled with only large merges,
        /// and in the same time, many small parts could be created and won't be merged.

        auto merges_and_mutations_queued = queue.countMergesAndPartMutations();
        size_t merges_and_mutations_sum = merges_and_mutations_queued.first + merges_and_mutations_queued.second;
        if (merges_and_mutations_sum >= storage_settings_ptr->max_replicated_merges_in_queue)
        {
            LOG_TRACE(log, "Number of queued merges ({}) and part mutations ({})"
                " is greater than max_replicated_merges_in_queue ({}), so won't select new parts to merge or mutate.",
                merges_and_mutations_queued.first,
                merges_and_mutations_queued.second,
                storage_settings_ptr->max_replicated_merges_in_queue);
        }
        else
        {
            UInt64 max_source_parts_size_for_merge = merger_mutator.getMaxSourcePartsSizeForMerge(
                storage_settings_ptr->max_replicated_merges_in_queue, merges_and_mutations_sum);
            UInt64 max_source_part_size_for_mutation = merger_mutator.getMaxSourcePartSizeForMutation();

            FutureMergedMutatedPart future_merged_part;
            if (max_source_parts_size_for_merge > 0 &&
                merger_mutator.selectPartsToMerge(future_merged_part, false, max_source_parts_size_for_merge, merge_pred, nullptr))
            {
                create_result = createLogEntryToMergeParts(zookeeper, future_merged_part.parts,
                    future_merged_part.name, future_merged_part.type, deduplicate, force_ttl, nullptr, merge_pred.getVersion());
            }
            /// If there are many mutations in queue, it may happen, that we cannot enqueue enough merges to merge all new parts
            else if (max_source_part_size_for_mutation > 0 && queue.countMutations() > 0
                     && merges_and_mutations_queued.second < storage_settings_ptr->max_replicated_mutations_in_queue)
            {
                /// Choose a part to mutate.
                DataPartsVector data_parts = getDataPartsVector();
                for (const auto & part : data_parts)
                {
                    if (part->getBytesOnDisk() > max_source_part_size_for_mutation)
                        continue;

                    std::optional<std::pair<Int64, int>> desired_mutation_version = merge_pred.getDesiredMutationVersion(part);
                    if (!desired_mutation_version)
                        continue;

                    create_result = createLogEntryToMutatePart(*part,
                        desired_mutation_version->first, desired_mutation_version->second, merge_pred.getVersion());

                    if (create_result == CreateMergeEntryResult::Ok)
                        break;
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    if (!is_leader)
        return;

    if (create_result != CreateMergeEntryResult::Ok
        && create_result != CreateMergeEntryResult::LogUpdated)
    {
        merge_selecting_task->scheduleAfter(MERGE_SELECTING_SLEEP_MS);
    }
    else
    {
        merge_selecting_task->schedule();
    }
}


void StorageReplicatedMergeTree::mutationsFinalizingTask()
{
    bool needs_reschedule = false;

    try
    {
        needs_reschedule = queue.tryFinalizeMutations(getZooKeeper());
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
    const MergeTreeDataPartType & merged_part_type,
    bool deduplicate,
    bool force_ttl,
    ReplicatedMergeTreeLogEntryData * out_log_entry,
    int32_t log_version)
{
    std::vector<std::future<Coordination::ExistsResponse>> exists_futures;
    exists_futures.reserve(parts.size());
    for (const auto & part : parts)
        exists_futures.emplace_back(zookeeper->asyncExists(replica_path + "/parts/" + part->name));

    bool all_in_zk = true;
    for (size_t i = 0; i < parts.size(); ++i)
    {
        /// If there is no information about part in ZK, we will not merge it.
        if (exists_futures[i].get().error == Coordination::Error::ZNONODE)
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
    entry.new_part_type = merged_part_type;
    entry.deduplicate = deduplicate;
    entry.force_ttl = force_ttl;
    entry.create_time = time(nullptr);

    for (const auto & part : parts)
        entry.source_parts.push_back(part->name);

    Coordination::Requests ops;
    Coordination::Responses responses;

    ops.emplace_back(zkutil::makeCreateRequest(
        zookeeper_path + "/log/log-", entry.toString(),
        zkutil::CreateMode::PersistentSequential));

    ops.emplace_back(zkutil::makeSetRequest(
        zookeeper_path + "/log", "", log_version)); /// Check and update version.

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
    const IMergeTreeDataPart & part, Int64 mutation_version, int32_t alter_version, int32_t log_version)
{
    auto zookeeper = getZooKeeper();

    /// If there is no information about part in ZK, we will not mutate it.
    if (!zookeeper->exists(replica_path + "/parts/" + part.name))
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
    entry.create_time = time(nullptr);
    entry.alter_version = alter_version;

    Coordination::Requests ops;
    Coordination::Responses responses;

    ops.emplace_back(zkutil::makeCreateRequest(
        zookeeper_path + "/log/log-", entry.toString(),
        zkutil::CreateMode::PersistentSequential));

    ops.emplace_back(zkutil::makeSetRequest(
        zookeeper_path + "/log", "", log_version)); /// Check and update version.

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


void StorageReplicatedMergeTree::removePartFromZooKeeper(const String & part_name, Coordination::Requests & ops, bool has_children)
{
    String part_path = replica_path + "/parts/" + part_name;

    if (has_children)
    {
        ops.emplace_back(zkutil::makeRemoveRequest(part_path + "/checksums", -1));
        ops.emplace_back(zkutil::makeRemoveRequest(part_path + "/columns", -1));
    }
    ops.emplace_back(zkutil::makeRemoveRequest(part_path, -1));
}


void StorageReplicatedMergeTree::removePartAndEnqueueFetch(const String & part_name)
{
    auto zookeeper = getZooKeeper();

    String part_path = replica_path + "/parts/" + part_name;

    Coordination::Requests ops;

    time_t part_create_time = 0;
    Coordination::Stat stat;
    if (zookeeper->exists(part_path, &stat))
    {
        part_create_time = stat.ctime / 1000;
        removePartFromZooKeeper(part_name, ops, stat.numChildren > 0);
    }

    LogEntryPtr log_entry = std::make_shared<LogEntry>();
    log_entry->type = LogEntry::GET_PART;
    log_entry->create_time = part_create_time;
    log_entry->source_replica = "";
    log_entry->new_part_name = part_name;

    ops.emplace_back(zkutil::makeCreateRequest(
        replica_path + "/queue/queue-", log_entry->toString(),
        zkutil::CreateMode::PersistentSequential));

    auto results = zookeeper->multi(ops);

    String path_created = dynamic_cast<const Coordination::CreateResponse &>(*results.back()).path_created;
    log_entry->znode_name = path_created.substr(path_created.find_last_of('/') + 1);
    queue.insert(zookeeper, log_entry);
}


void StorageReplicatedMergeTree::enterLeaderElection()
{
    auto callback = [this]()
    {
        LOG_INFO(log, "Became leader");

        is_leader = true;
        merge_selecting_task->activateAndSchedule();
    };

    try
    {
        leader_election = std::make_shared<zkutil::LeaderElection>(
            global_context.getSchedulePool(),
            zookeeper_path + "/leader_election",
            *current_zookeeper,    /// current_zookeeper lives for the lifetime of leader_election,
                                   ///  since before changing `current_zookeeper`, `leader_election` object is destroyed in `partialShutdown` method.
            callback,
            replica_name);
    }
    catch (...)
    {
        leader_election = nullptr;
        throw;
    }
}

void StorageReplicatedMergeTree::exitLeaderElection()
{
    if (!leader_election)
        return;

    /// Shut down the leader election thread to avoid suddenly becoming the leader again after
    /// we have stopped the merge_selecting_thread, but before we have deleted the leader_election object.
    leader_election->shutdown();

    if (is_leader)
    {
        LOG_INFO(log, "Stopped being leader");

        is_leader = false;
        merge_selecting_task->deactivate();
    }

    /// Delete the node in ZK only after we have stopped the merge_selecting_thread - so that only one
    /// replica assigns merges at any given time.
    leader_election = nullptr;
}


String StorageReplicatedMergeTree::findReplicaHavingPart(const String & part_name, bool active)
{
    auto zookeeper = getZooKeeper();
    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

    /// Select replicas in uniformly random order.
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);

    for (const String & replica : replicas)
    {
        /// We don't interested in ourself.
        if (replica == replica_name)
            continue;

        if (zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/parts/" + part_name) &&
            (!active || zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active")))
            return replica;

        /// Obviously, replica could become inactive or even vanish after return from this method.
    }

    return {};
}


String StorageReplicatedMergeTree::findReplicaHavingCoveringPart(LogEntry & entry, bool active)
{
    auto zookeeper = getZooKeeper();
    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

    /// Select replicas in uniformly random order.
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);

    for (const String & replica : replicas)
    {
        if (replica == replica_name)
            continue;

        if (active && !zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
            continue;

        String largest_part_found;
        Strings parts = zookeeper->getChildren(zookeeper_path + "/replicas/" + replica + "/parts");
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
    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

    /// Select replicas in uniformly random order.
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);

    String largest_part_found;
    String largest_replica_found;

    for (const String & replica : replicas)
    {
        if (replica == replica_name)
            continue;

        if (active && !zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
            continue;

        Strings parts = zookeeper->getChildren(zookeeper_path + "/replicas/" + replica + "/parts");
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
void StorageReplicatedMergeTree::updateQuorum(const String & part_name)
{
    auto zookeeper = getZooKeeper();

    /// Information on which replicas a part has been added, if the quorum has not yet been reached.
    const String quorum_status_path = zookeeper_path + "/quorum/status";
    /// The name of the previous part for which the quorum was reached.
    const String quorum_last_part_path = zookeeper_path + "/quorum/last_part";

    String value;
    Coordination::Stat stat;

    /// If there is no node, then all quorum INSERTs have already reached the quorum, and nothing is needed.
    while (zookeeper->tryGet(quorum_status_path, value, &stat))
    {
        ReplicatedMergeTreeQuorumEntry quorum_entry;
        quorum_entry.fromString(value);

        if (quorum_entry.part_name != part_name)
        {
            /// The quorum has already been achieved. Moreover, another INSERT with a quorum has already started.
            break;
        }

        quorum_entry.replicas.insert(replica_name);

        if (quorum_entry.replicas.size() >= quorum_entry.required_number_of_replicas)
        {
            /// The quorum is reached. Delete the node, and update information about the last part that was successfully written with quorum.

            Coordination::Requests ops;
            Coordination::Responses responses;

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
                throw Coordination::Exception(code, quorum_status_path);
        }
        else
        {
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
                throw Coordination::Exception(code, quorum_status_path);
        }
    }
}


void StorageReplicatedMergeTree::cleanLastPartNode(const String & partition_id)
{
    auto zookeeper = getZooKeeper();

    /// The name of the previous part for which the quorum was reached.
    const String quorum_last_part_path = zookeeper_path + "/quorum/last_part";

    /// Delete information from "last_part" node.

    while (true)
    {
        Coordination::Stat added_parts_stat;
        String old_added_parts = zookeeper->get(quorum_last_part_path, &added_parts_stat);

        ReplicatedMergeTreeQuorumAddedParts parts_with_quorum(format_version);

        if (!old_added_parts.empty())
            parts_with_quorum.fromString(old_added_parts);

        /// Delete information about particular partition.
        if (!parts_with_quorum.added_parts.count(partition_id))
        {
            /// There is no information about interested part.
            break;
        }

        parts_with_quorum.added_parts.erase(partition_id);

        String new_added_parts = parts_with_quorum.toString();

        auto code = zookeeper->trySet(quorum_last_part_path, new_added_parts, added_parts_stat.version);

        if (code == Coordination::Error::ZOK)
        {
            break;
        }
        else if (code == Coordination::Error::ZNONODE)
        {
            /// Node is deleted. It is impossible, but it is Ok.
            break;
        }
        else if (code == Coordination::Error::ZBADVERSION)
        {
            /// Node was updated meanwhile. We must re-read it and repeat all the actions.
            continue;
        }
        else
            throw Coordination::Exception(code, quorum_last_part_path);
    }
}


bool StorageReplicatedMergeTree::fetchPart(const String & part_name, const StorageMetadataPtr & metadata_snapshot,
    const String & source_replica_path, bool to_detached, size_t quorum)
{
    const auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

    if (auto part = getPartIfExists(part_info, {IMergeTreeDataPart::State::Outdated, IMergeTreeDataPart::State::Deleting}))
    {
        LOG_DEBUG(log, "Part {} should be deleted after previous attempt before fetch", part->name);
        /// Force immediate parts cleanup to delete the part that was left from the previous fetch attempt.
        cleanup_thread.wakeup();
        return false;
    }

    {
        std::lock_guard lock(currently_fetching_parts_mutex);
        if (!currently_fetching_parts.insert(part_name).second)
        {
            LOG_DEBUG(log, "Part {} is already fetching right now", part_name);
            return false;
        }
    }

    SCOPE_EXIT
    ({
        std::lock_guard lock(currently_fetching_parts_mutex);
        currently_fetching_parts.erase(part_name);
    });

    LOG_DEBUG(log, "Fetching part {} from {}", part_name, source_replica_path);

    TableLockHolder table_lock_holder;
    if (!to_detached)
        table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

    /// Logging
    Stopwatch stopwatch;
    MutableDataPartPtr part;
    DataPartsVector replaced_parts;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        writePartLog(
            PartLogElement::DOWNLOAD_PART, execution_status, stopwatch.elapsed(),
            part_name, part, replaced_parts, nullptr);
    };

    DataPartPtr part_to_clone;
    {
        /// If the desired part is a result of a part mutation, try to find the source part and compare
        /// its checksums to the checksums of the desired part. If they match, we can just clone the local part.

        /// If we have the source part, its part_info will contain covered_part_info.
        auto covered_part_info = part_info;
        covered_part_info.mutation = 0;
        auto source_part = getActiveContainingPart(covered_part_info);

        if (source_part)
        {
            MinimalisticDataPartChecksums source_part_checksums;
            source_part_checksums.computeTotalChecksums(source_part->checksums);

            MinimalisticDataPartChecksums desired_checksums;
            auto zookeeper = getZooKeeper();
            String part_path = source_replica_path + "/parts/" + part_name;
            String part_znode = zookeeper->get(part_path);
            if (!part_znode.empty())
                desired_checksums = ReplicatedMergeTreePartHeader::fromString(part_znode).getChecksums();
            else
            {
                String desired_checksums_str = zookeeper->get(part_path + "/checksums");
                desired_checksums = MinimalisticDataPartChecksums::deserializeFrom(desired_checksums_str);
            }

            if (source_part_checksums == desired_checksums)
            {
                LOG_TRACE(log, "Found local part {} with the same checksums as {}", source_part->name, part_name);
                part_to_clone = source_part;
            }
        }

    }

    std::function<MutableDataPartPtr()> get_part;
    if (part_to_clone)
    {
        get_part = [&, part_to_clone]()
        {
            return cloneAndLoadDataPartOnSameDisk(part_to_clone, "tmp_clone_", part_info, metadata_snapshot);
        };
    }
    else
    {
        ReplicatedMergeTreeAddress address(getZooKeeper()->get(source_replica_path + "/host"));
        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(global_context);
        auto user_password = global_context.getInterserverCredentials();
        String interserver_scheme = global_context.getInterserverScheme();

        get_part = [&, address, timeouts, user_password, interserver_scheme]()
        {
            if (interserver_scheme != address.scheme)
                throw Exception("Interserver schemes are different: '" + interserver_scheme
                    + "' != '" + address.scheme + "', can't fetch part from " + address.host,
                    ErrorCodes::LOGICAL_ERROR);

            return fetcher.fetchPart(
                metadata_snapshot, part_name, source_replica_path,
                address.host, address.replication_port,
                timeouts, user_password.first, user_password.second, interserver_scheme, to_detached);
        };
    }

    try
    {
        part = get_part();

        if (!to_detached)
        {
            Transaction transaction(*this);
            renameTempPartAndReplace(part, nullptr, &transaction);

            /** NOTE
              * Here, an error occurs if ALTER occurred with a change in the column type or column deletion,
              *  and the part on remote server has not yet been modified.
              * After a while, one of the following attempts to make `fetchPart` succeed.
              */
            replaced_parts = checkPartChecksumsAndCommit(transaction, part);

            /** If a quorum is tracked for this part, you must update it.
              * If you do not have time, in case of losing the session, when you restart the server - see the `ReplicatedMergeTreeRestartingThread::updateQuorumIfWeHavePart` method.
              */
            if (quorum)
                updateQuorum(part_name);

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
            part->renameTo("detached/" + part_name, true);
        }
    }
    catch (...)
    {
        if (!to_detached)
            write_part_log(ExecutionStatus::fromCurrentException());

        throw;
    }

    ProfileEvents::increment(ProfileEvents::ReplicatedPartFetches);

    if (part_to_clone)
        LOG_DEBUG(log, "Cloned part {} from {}{}", part_name, part_to_clone->name, to_detached ? " (to 'detached' directory)" : "");
    else
        LOG_DEBUG(log, "Fetched part {} from {}{}", part_name, source_replica_path, to_detached ? " (to 'detached' directory)" : "");

    return true;
}


void StorageReplicatedMergeTree::startup()
{
    if (is_readonly)
        return;

    try
    {
        queue.initialize(getDataParts());

        data_parts_exchange_endpoint = std::make_shared<DataPartsExchange::Service>(*this);
        global_context.getInterserverIOHandler().addEndpoint(data_parts_exchange_endpoint->getId(replica_path), data_parts_exchange_endpoint);

        /// In this thread replica will be activated.
        restarting_thread.start();

        /// Wait while restarting_thread initializes LeaderElection (and so on) or makes first attmept to do it
        startup_event.wait();

        /// If we don't separate create/start steps, race condition will happen
        /// between the assignment of queue_task_handle and queueTask that use the queue_task_handle.
        {
            auto lock = queue.lockQueue();
            auto & pool = global_context.getBackgroundPool();
            queue_task_handle = pool.createTask([this] { return queueTask(); });
            pool.startTask(queue_task_handle);
        }

        startBackgroundMovesIfNeeded();
    }
    catch (...)
    {
        /// Exception safety: failed "startup" does not require a call to "shutdown" from the caller.
        /// And it should be able to safely destroy table after exception in "startup" method.
        /// It means that failed "startup" must not create any background tasks that we will have to wait.
        try
        {
            shutdown();
        }
        catch (...)
        {
            std::terminate();
        }

        /// Note: after failed "startup", the table will be in a state that only allows to destroy the object.
        throw;
    }
}


void StorageReplicatedMergeTree::shutdown()
{
    /// Cancel fetches, merges and mutations to force the queue_task to finish ASAP.
    fetcher.blocker.cancelForever();
    merger_mutator.merges_blocker.cancelForever();
    parts_mover.moves_blocker.cancelForever();

    restarting_thread.shutdown();

    if (queue_task_handle)
        global_context.getBackgroundPool().removeTask(queue_task_handle);

    {
        /// Queue can trigger queue_task_handle itself. So we ensure that all
        /// queue processes finished and after that reset queue_task_handle.
        auto lock = queue.lockQueue();
        queue_task_handle.reset();

        /// Cancel logs pulling after background task were cancelled. It's still
        /// required because we can trigger pullLogsToQueue during manual OPTIMIZE,
        /// MUTATE, etc. query.
        queue.pull_log_blocker.cancelForever();
    }

    if (move_parts_task_handle)
        global_context.getBackgroundMovePool().removeTask(move_parts_task_handle);
    move_parts_task_handle.reset();

    if (data_parts_exchange_endpoint)
    {
        global_context.getInterserverIOHandler().removeEndpointIfExists(data_parts_exchange_endpoint->getId(replica_path));
        /// Ask all parts exchange handlers to finish asap. New ones will fail to start
        data_parts_exchange_endpoint->blocker.cancelForever();
        /// Wait for all of them
        std::unique_lock lock(data_parts_exchange_endpoint->rwlock);
    }
    data_parts_exchange_endpoint.reset();

    /// We clear all old parts after stopping all background operations. It's
    /// important, because background operations can produce temporary parts
    /// which will remove themselves in their descrutors. If so, we may have
    /// race condition between our remove call and background process.
    clearOldPartsFromFilesystem(true);
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

    for (const auto & data_part : getDataParts())
    {
        max_added_blocks[data_part->info.partition_id]
            = std::max(max_added_blocks[data_part->info.partition_id], data_part->info.max_block);
    }

    auto zookeeper = getZooKeeper();

    const String quorum_status_path = zookeeper_path + "/quorum/status";

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
    if (zookeeper->tryGet(zookeeper_path + "/quorum/last_part", added_parts_str))
    {
        if (!added_parts_str.empty())
        {
            ReplicatedMergeTreeQuorumAddedParts part_with_quorum(format_version);
            part_with_quorum.fromString(added_parts_str);

            auto added_parts = part_with_quorum.added_parts;

            for (const auto & added_part : added_parts)
                if (!getActiveContainingPart(added_part.second))
                    throw Exception(
                        "Replica doesn't have part " + added_part.second
                            + " which was successfully written to quorum of other replicas."
                              " Send query to another replica or disable 'select_sequential_consistency' setting.",
                        ErrorCodes::REPLICA_IS_NOT_IN_QUORUM);

            for (const auto & max_block : part_with_quorum.getMaxInsertedBlocks())
                max_added_blocks[max_block.first] = max_block.second;
        }
    }
    return max_added_blocks;
}

Pipes StorageReplicatedMergeTree::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned num_streams)
{
    /** The `select_sequential_consistency` setting has two meanings:
    * 1. To throw an exception if on a replica there are not all parts which have been written down on quorum of remaining replicas.
    * 2. Do not read parts that have not yet been written to the quorum of the replicas.
    * For this you have to synchronously go to ZooKeeper.
    */
    if (context.getSettingsRef().select_sequential_consistency)
    {
        auto max_added_blocks = getMaxAddedBlocks();
        return reader.read(column_names, metadata_snapshot, query_info, context, max_block_size, num_streams, &max_added_blocks);
    }

    return reader.read(column_names, metadata_snapshot, query_info, context, max_block_size, num_streams);
}


template <class Func>
void StorageReplicatedMergeTree::foreachCommittedParts(const Func & func) const
{
    auto max_added_blocks = getMaxAddedBlocks();
    auto lock = lockParts();
    for (const auto & part : getDataPartsStateRange(DataPartState::Committed))
    {
        if (part->isEmpty())
            continue;

        auto blocks_iterator = max_added_blocks.find(part->info.partition_id);
        if (blocks_iterator == max_added_blocks.end() || part->info.max_block > blocks_iterator->second)
            continue;

        func(part);
    }
}

std::optional<UInt64> StorageReplicatedMergeTree::totalRows() const
{
    UInt64 res = 0;
    foreachCommittedParts([&res](auto & part) { res += part->rows_count; });
    return res;
}

std::optional<UInt64> StorageReplicatedMergeTree::totalBytes() const
{
    UInt64 res = 0;
    foreachCommittedParts([&res](auto & part) { res += part->getBytesOnDisk(); });
    return res;
}


void StorageReplicatedMergeTree::assertNotReadonly() const
{
    if (is_readonly)
        throw Exception("Table is in readonly mode", ErrorCodes::TABLE_IS_READ_ONLY);
}


BlockOutputStreamPtr StorageReplicatedMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & context)
{
    const auto storage_settings_ptr = getSettings();
    assertNotReadonly();

    const Settings & query_settings = context.getSettingsRef();
    bool deduplicate = storage_settings_ptr->replicated_deduplication_window != 0 && query_settings.insert_deduplicate;

    return std::make_shared<ReplicatedMergeTreeBlockOutputStream>(
        *this, metadata_snapshot, query_settings.insert_quorum,
        query_settings.insert_quorum_timeout.totalMilliseconds(),
        query_settings.max_partitions_per_insert_block,
        deduplicate);
}


bool StorageReplicatedMergeTree::optimize(
    const ASTPtr &,
    const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Context & query_context)
{
    assertNotReadonly();

    if (!is_leader)
        throw Exception("OPTIMIZE cannot be done on this replica because it is not a leader", ErrorCodes::NOT_A_LEADER);

    constexpr size_t max_retries = 10;

    std::vector<ReplicatedMergeTreeLogEntryData> merge_entries;
    {
        auto zookeeper = getZooKeeper();

        auto handle_noop = [&] (const String & message)
        {
            if (query_context.getSettingsRef().optimize_throw_if_noop)
                throw Exception(message, ErrorCodes::CANNOT_ASSIGN_OPTIMIZE);
            return false;
        };

        bool force_ttl = (final && metadata_snapshot->hasAnyTTL());
        const auto storage_settings_ptr = getSettings();

        if (!partition && final)
        {
            DataPartsVector data_parts = getDataPartsVector();
            std::unordered_set<String> partition_ids;

            for (const DataPartPtr & part : data_parts)
                partition_ids.emplace(part->info.partition_id);

            UInt64 disk_space = getStoragePolicy()->getMaxUnreservedFreeSpace();

            for (const String & partition_id : partition_ids)
            {
                size_t try_no = 0;
                for (; try_no < max_retries; ++try_no)
                {
                    /// We must select parts for merge under merge_selecting_mutex because other threads
                    /// (merge_selecting_thread or OPTIMIZE queries) could assign new merges.
                    std::lock_guard merge_selecting_lock(merge_selecting_mutex);
                    ReplicatedMergeTreeMergePredicate can_merge = queue.getMergePredicate(zookeeper);

                    FutureMergedMutatedPart future_merged_part;
                    bool selected = merger_mutator.selectAllPartsToMergeWithinPartition(
                        future_merged_part, disk_space, can_merge, partition_id, true, nullptr);

                    if (!selected)
                        break;

                    ReplicatedMergeTreeLogEntryData merge_entry;
                    CreateMergeEntryResult create_result = createLogEntryToMergeParts(
                        zookeeper, future_merged_part.parts,
                        future_merged_part.name, future_merged_part.type, deduplicate, force_ttl,
                        &merge_entry, can_merge.getVersion());

                    if (create_result == CreateMergeEntryResult::MissingPart)
                        return handle_noop("Can't create merge queue node in ZooKeeper, because some parts are missing");

                    if (create_result == CreateMergeEntryResult::LogUpdated)
                        continue;

                    merge_entries.push_back(std::move(merge_entry));
                    break;
                }
                if (try_no == max_retries)
                    return handle_noop("Can't create merge queue node in ZooKeeper, because log was updated in every of "
                        + toString(max_retries) + " tries");
            }
        }
        else
        {
            size_t try_no = 0;
            for (; try_no < max_retries; ++try_no)
            {
                std::lock_guard merge_selecting_lock(merge_selecting_mutex);
                ReplicatedMergeTreeMergePredicate can_merge = queue.getMergePredicate(zookeeper);

                FutureMergedMutatedPart future_merged_part;
                String disable_reason;
                bool selected = false;
                if (!partition)
                {
                    selected = merger_mutator.selectPartsToMerge(
                        future_merged_part, true, storage_settings_ptr->max_bytes_to_merge_at_max_space_in_pool, can_merge, &disable_reason);
                }
                else
                {

                    UInt64 disk_space = getStoragePolicy()->getMaxUnreservedFreeSpace();
                    String partition_id = getPartitionIDFromQuery(partition, query_context);
                    selected = merger_mutator.selectAllPartsToMergeWithinPartition(
                        future_merged_part, disk_space, can_merge, partition_id, final, &disable_reason);
                }

                if (!selected)
                {
                    std::stringstream message;
                    message << "Cannot select parts for optimization";
                    if (!disable_reason.empty())
                        message << ": " << disable_reason;
                    LOG_INFO(log, message.str());
                    return handle_noop(message.str());
                }

                ReplicatedMergeTreeLogEntryData merge_entry;
                CreateMergeEntryResult create_result = createLogEntryToMergeParts(
                    zookeeper, future_merged_part.parts,
                    future_merged_part.name, future_merged_part.type, deduplicate, force_ttl,
                    &merge_entry, can_merge.getVersion());

                if (create_result == CreateMergeEntryResult::MissingPart)
                    return handle_noop("Can't create merge queue node in ZooKeeper, because some parts are missing");

                if (create_result == CreateMergeEntryResult::LogUpdated)
                    continue;

                merge_entries.push_back(std::move(merge_entry));
                break;
            }
            if (try_no == max_retries)
                return handle_noop("Can't create merge queue node in ZooKeeper, because log was updated in every of "
                    + toString(max_retries) + " tries");
        }
    }

    if (query_context.getSettingsRef().replication_alter_partitions_sync != 0)
    {
        /// NOTE Table lock must not be held while waiting. Some combination of R-W-R locks from different threads will yield to deadlock.
        for (auto & merge_entry : merge_entries)
            waitForAllReplicasToProcessLogEntry(merge_entry, false);
    }

    return true;
}

bool StorageReplicatedMergeTree::executeMetadataAlter(const StorageReplicatedMergeTree::LogEntry & entry)
{
    auto zookeeper = getZooKeeper();

    auto columns_from_entry = ColumnsDescription::parse(entry.columns_str);
    auto metadata_from_entry = ReplicatedMergeTreeTableMetadata::parse(entry.metadata_str);

    MergeTreeData::DataParts parts;

    /// If metadata nodes have changed, we will update table structure locally.
    Coordination::Requests requests;
    requests.emplace_back(zkutil::makeSetRequest(replica_path + "/columns", entry.columns_str, -1));
    requests.emplace_back(zkutil::makeSetRequest(replica_path + "/metadata", entry.metadata_str, -1));

    zookeeper->multi(requests);

    {
        auto lock = lockForAlter(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);
        LOG_INFO(log, "Metadata changed in ZooKeeper. Applying changes locally.");

        auto metadata_diff = ReplicatedMergeTreeTableMetadata(*this, getInMemoryMetadataPtr()).checkAndFindDiff(metadata_from_entry);
        setTableStructure(std::move(columns_from_entry), metadata_diff);
        metadata_version = entry.alter_version;

        LOG_INFO(log, "Applied changes to the metadata of the table. Current metadata version: {}", metadata_version);
    }

    /// This transaction may not happen, but it's OK, because on the next retry we will eventually create/update this node
    zookeeper->createOrUpdate(replica_path + "/metadata_version", std::to_string(metadata_version), zkutil::CreateMode::Persistent);

    return true;
}


void StorageReplicatedMergeTree::alter(
    const AlterCommands & params, const Context & query_context, TableLockHolder & table_lock_holder)
{
    assertNotReadonly();

    auto table_id = getStorageID();

    if (params.isSettingsAlter())
    {
        /// We don't replicate storage_settings_ptr ALTER. It's local operation.
        /// Also we don't upgrade alter lock to table structure lock.
        StorageInMemoryMetadata future_metadata = getInMemoryMetadata();
        params.apply(future_metadata, query_context);

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

    auto zookeeper = getZooKeeper();

    std::optional<ReplicatedMergeTreeLogEntryData> alter_entry;
    std::optional<String> mutation_znode;

    while (true)
    {
        /// Clear nodes from previous iteration
        alter_entry.emplace();
        mutation_znode.reset();

        /// We can safely read structure, because we guarded with alter_intention_lock
        if (is_readonly)
            throw Exception("Can't ALTER readonly table", ErrorCodes::TABLE_IS_READ_ONLY);

        auto current_metadata = getInMemoryMetadataPtr();

        StorageInMemoryMetadata future_metadata = *current_metadata;
        params.apply(future_metadata, query_context);

        ReplicatedMergeTreeTableMetadata future_metadata_in_zk(*this, current_metadata);
        if (ast_to_str(future_metadata.sorting_key.definition_ast) != ast_to_str(current_metadata->sorting_key.definition_ast))
            future_metadata_in_zk.sorting_key = serializeAST(*future_metadata.sorting_key.expression_list_ast);

        if (ast_to_str(future_metadata.partition_key.definition_ast) != ast_to_str(current_metadata->partition_key.definition_ast))
            future_metadata_in_zk.partition_key = serializeAST(*future_metadata.partition_key.expression_list_ast);

        if (ast_to_str(future_metadata.table_ttl.definition_ast) != ast_to_str(current_metadata->table_ttl.definition_ast))
            future_metadata_in_zk.ttl_table = serializeAST(*future_metadata.table_ttl.definition_ast);

        String new_indices_str = future_metadata.secondary_indices.toString();
        if (new_indices_str != current_metadata->secondary_indices.toString())
            future_metadata_in_zk.skip_indices = new_indices_str;

        String new_constraints_str = future_metadata.constraints.toString();
        if (new_constraints_str != current_metadata->constraints.toString())
            future_metadata_in_zk.constraints = new_constraints_str;

        Coordination::Requests ops;

        String new_metadata_str = future_metadata_in_zk.toString();
        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/metadata", new_metadata_str, metadata_version));

        String new_columns_str = future_metadata.columns.toString();
        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/columns", new_columns_str, -1));

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
        int new_metadata_version = metadata_version + 1;

        alter_entry->type = LogEntry::ALTER_METADATA;
        alter_entry->source_replica = replica_name;
        alter_entry->metadata_str = new_metadata_str;
        alter_entry->columns_str = new_columns_str;
        alter_entry->alter_version = new_metadata_version;
        alter_entry->create_time = time(nullptr);

        auto maybe_mutation_commands = params.getMutationCommands(
            *current_metadata, query_context.getSettingsRef().materialize_ttl_after_modify, query_context);
        alter_entry->have_mutation = !maybe_mutation_commands.empty();

        ops.emplace_back(zkutil::makeCreateRequest(
            zookeeper_path + "/log/log-", alter_entry->toString(), zkutil::CreateMode::PersistentSequential));

        std::optional<EphemeralLocksInAllPartitions> lock_holder;

        /// Now we will prepare mutations record.
        /// This code pretty same with mutate() function but process results slightly differently.
        if (alter_entry->have_mutation)
        {
            String mutations_path = zookeeper_path + "/mutations";

            ReplicatedMergeTreeMutationEntry mutation_entry;
            mutation_entry.source_replica = replica_name;
            mutation_entry.commands = maybe_mutation_commands;
            mutation_entry.alter_version = new_metadata_version;
            Coordination::Stat mutations_stat;
            zookeeper->get(mutations_path, &mutations_stat);

            lock_holder.emplace(
                zookeeper_path + "/block_numbers", "block-", zookeeper_path + "/temp", *zookeeper);

            for (const auto & lock : lock_holder->getLocks())
                mutation_entry.block_numbers[lock.partition_id] = lock.number;

            mutation_entry.create_time = time(nullptr);

            ops.emplace_back(zkutil::makeSetRequest(mutations_path, String(), mutations_stat.version));
            ops.emplace_back(
                zkutil::makeCreateRequest(mutations_path + "/", mutation_entry.toString(), zkutil::CreateMode::PersistentSequential));
        }

        Coordination::Responses results;
        Coordination::Error rc = zookeeper->tryMulti(ops, results);

        if (rc == Coordination::Error::ZOK)
        {
            if (alter_entry->have_mutation)
            {
                /// ALTER_METADATA record in replication /log
                String alter_path = dynamic_cast<const Coordination::CreateResponse &>(*results[2]).path_created;
                alter_entry->znode_name = alter_path.substr(alter_path.find_last_of('/') + 1);

                /// ReplicatedMergeTreeMutationEntry record in /mutations
                String mutation_path = dynamic_cast<const Coordination::CreateResponse &>(*results.back()).path_created;
                mutation_znode = mutation_path.substr(mutation_path.find_last_of('/') + 1);
            }
            else
            {
                /// ALTER_METADATA record in replication /log
                String alter_path = dynamic_cast<const Coordination::CreateResponse &>(*results.back()).path_created;
                alter_entry->znode_name = alter_path.substr(alter_path.find_last_of('/') + 1);
            }
            break;
        }
        else if (rc == Coordination::Error::ZBADVERSION)
        {
            if (results[0]->error != Coordination::Error::ZOK)
                throw Exception("Metadata on replica is not up to date with common metadata in Zookeeper. Cannot alter",
                    ErrorCodes::CANNOT_ASSIGN_ALTER);

            continue;
        }
        else
        {
            throw Coordination::Exception("Alter cannot be assigned because of Zookeeper error", rc);
        }
    }

    table_lock_holder.reset();

    std::vector<String> unwaited;
    if (query_context.getSettingsRef().replication_alter_partitions_sync == 2)
    {
        LOG_DEBUG(log, "Updated shared metadata nodes in ZooKeeper. Waiting for replicas to apply changes.");
        unwaited = waitForAllReplicasToProcessLogEntry(*alter_entry, false);
    }
    else if (query_context.getSettingsRef().replication_alter_partitions_sync == 1)
    {
        LOG_DEBUG(log, "Updated shared metadata nodes in ZooKeeper. Waiting for replicas to apply changes.");
        waitForReplicaToProcessLogEntry(replica_name, *alter_entry);
    }

    if (!unwaited.empty())
        throw Exception("Some replicas doesn't finish metadata alter", ErrorCodes::UNFINISHED);

    if (mutation_znode)
    {
        LOG_DEBUG(log, "Metadata changes applied. Will wait for data changes.");
        waitMutation(*mutation_znode, query_context.getSettingsRef().replication_alter_partitions_sync);
        LOG_DEBUG(log, "Data changes applied.");
    }
}

Pipes StorageReplicatedMergeTree::alterPartition(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    const PartitionCommands & commands,
    const Context & query_context)
{
    PartitionCommandsResultInfo result;
    for (const PartitionCommand & command : commands)
    {
        PartitionCommandsResultInfo current_command_results;
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
                checkPartitionCanBeDropped(command.partition);
                dropPartition(query, command.partition, command.detach, query_context);
                break;

            case PartitionCommand::DROP_DETACHED_PARTITION:
                dropDetached(command.partition, command.part, query_context);
                break;

            case PartitionCommand::ATTACH_PARTITION:
                current_command_results = attachPartition(command.partition, metadata_snapshot, command.part, query_context);
                break;
            case PartitionCommand::MOVE_PARTITION:
            {
                switch (*command.move_destination_type)
                {
                    case PartitionCommand::MoveDestinationType::DISK:
                        movePartitionToDisk(command.partition, command.move_destination_name, command.part, query_context);
                        break;

                    case PartitionCommand::MoveDestinationType::VOLUME:
                        movePartitionToVolume(command.partition, command.move_destination_name, command.part, query_context);
                        break;

                    case PartitionCommand::MoveDestinationType::TABLE:
                        checkPartitionCanBeDropped(command.partition);
                        String dest_database = query_context.resolveDatabase(command.to_database);
                        auto dest_storage = DatabaseCatalog::instance().getTable({dest_database, command.to_table}, query_context);
                        movePartitionToTable(dest_storage, command.partition, query_context);
                        break;
                }
            }
            break;

            case PartitionCommand::REPLACE_PARTITION:
            {
                checkPartitionCanBeDropped(command.partition);
                String from_database = query_context.resolveDatabase(command.from_database);
                auto from_storage = DatabaseCatalog::instance().getTable({from_database, command.from_table}, query_context);
                replacePartitionFrom(from_storage, command.partition, command.replace, query_context);
            }
            break;

            case PartitionCommand::FETCH_PARTITION:
                fetchPartition(command.partition, metadata_snapshot, command.from_zookeeper_path, query_context);
                break;

            case PartitionCommand::FREEZE_PARTITION:
            {
                auto lock = lockForShare(query_context.getCurrentQueryId(), query_context.getSettingsRef().lock_acquire_timeout);
                current_command_results = freezePartition(command.partition, metadata_snapshot, command.with_name, query_context, lock);
            }
            break;

            case PartitionCommand::FREEZE_ALL_PARTITIONS:
            {
                auto lock = lockForShare(query_context.getCurrentQueryId(), query_context.getSettingsRef().lock_acquire_timeout);
                current_command_results = freezeAll(command.with_name, metadata_snapshot, query_context, lock);
            }
            break;
        }
        for (auto & command_result : current_command_results)
            command_result.command_type = command.typeToString();
        result.insert(result.end(), current_command_results.begin(), current_command_results.end());
    }

    if (query_context.getSettingsRef().alter_partition_verbose_result)
        return convertCommandsResultToSource(result);

    return {};
}


/// If new version returns ordinary name, else returns part name containing the first and last month of the month
static String getPartNamePossiblyFake(MergeTreeDataFormatVersion format_version, const MergeTreePartInfo & part_info)
{
    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// The date range is all month long.
        const auto & lut = DateLUT::instance();
        time_t start_time = lut.YYYYMMDDToDate(parse<UInt32>(part_info.partition_id + "01"));
        DayNum left_date = lut.toDayNum(start_time);
        DayNum right_date = DayNum(static_cast<size_t>(left_date) + lut.daysInMonth(start_time) - 1);
        return part_info.getPartNameV0(left_date, right_date);
    }

    return part_info.getPartName();
}

bool StorageReplicatedMergeTree::getFakePartCoveringAllPartsInPartition(const String & partition_id, MergeTreePartInfo & part_info)
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
        auto zookeeper = getZooKeeper();
        auto block_number_lock = allocateBlockNumber(partition_id, zookeeper);
        right = block_number_lock->getNumber();
        block_number_lock->unlock();
        mutation_version = queue.getCurrentMutationVersion(partition_id, right);
    }

    /// Empty partition.
    if (right == 0)
        return false;

    --right;

    /// Artificial high level is chosen, to make this part "covering" all parts inside.
    part_info = MergeTreePartInfo(partition_id, left, right, MergeTreePartInfo::MAX_LEVEL, mutation_version);
    return true;
}


void StorageReplicatedMergeTree::dropPartition(const ASTPtr &, const ASTPtr & partition, bool detach, const Context & query_context)
{
    assertNotReadonly();
    if (!is_leader)
        throw Exception("DROP PARTITION cannot be done on this replica because it is not a leader", ErrorCodes::NOT_A_LEADER);

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();

    String partition_id = getPartitionIDFromQuery(partition, query_context);

    LogEntry entry;
    if (dropPartsInPartition(*zookeeper, partition_id, entry, detach))
    {
        /// If necessary, wait until the operation is performed on itself or on all replicas.
        if (query_context.getSettingsRef().replication_alter_partitions_sync != 0)
        {
            if (query_context.getSettingsRef().replication_alter_partitions_sync == 1)
                waitForReplicaToProcessLogEntry(replica_name, entry);
            else
                waitForAllReplicasToProcessLogEntry(entry);
        }
    }

    /// Cleaning possibly stored information about parts from /quorum/last_part node in ZooKeeper.
    cleanLastPartNode(partition_id);
}


void StorageReplicatedMergeTree::truncate(
    const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder & table_lock)
{
    table_lock.release();   /// Truncate is done asynchronously.

    assertNotReadonly();
    if (!is_leader)
        throw Exception("TRUNCATE cannot be done on this replica because it is not a leader", ErrorCodes::NOT_A_LEADER);

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();

    Strings partitions = zookeeper->getChildren(zookeeper_path + "/block_numbers");

    for (String & partition_id : partitions)
    {
        LogEntry entry;

        if (dropPartsInPartition(*zookeeper, partition_id, entry, false))
            waitForAllReplicasToProcessLogEntry(entry);
    }
}


PartitionCommandsResultInfo StorageReplicatedMergeTree::attachPartition(
    const ASTPtr & partition,
    const StorageMetadataPtr & metadata_snapshot,
    bool attach_part,
    const Context & query_context)
{
    assertNotReadonly();

    PartitionCommandsResultInfo results;
    PartsTemporaryRename renamed_parts(*this, "detached/");
    MutableDataPartsVector loaded_parts = tryLoadPartsToAttach(partition, attach_part, query_context, renamed_parts);

    ReplicatedMergeTreeBlockOutputStream output(*this, metadata_snapshot, 0, 0, 0, false);   /// TODO Allow to use quorum here.
    for (size_t i = 0; i < loaded_parts.size(); ++i)
    {
        String old_name = loaded_parts[i]->name;
        output.writeExistingPart(loaded_parts[i]);
        renamed_parts.old_and_new_names[i].first.clear();
        LOG_DEBUG(log, "Attached part {} as {}", old_name, loaded_parts[i]->name);
        results.push_back(PartitionCommandResultInfo{
            .partition_id = loaded_parts[i]->info.partition_id,
            .part_name = loaded_parts[i]->name,
            .old_part_name = old_name,
        });
    }
    return results;
}


void StorageReplicatedMergeTree::checkTableCanBeDropped() const
{
    auto table_id = getStorageID();
    global_context.checkTableCanBeDropped(table_id.database_name, table_id.table_name, getTotalActiveSizeInBytes());
}

void StorageReplicatedMergeTree::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    MergeTreeData::rename(new_path_to_table_data, new_table_id);

    /// Update table name in zookeeper
    if (!is_readonly)
    {
        /// We don't do it for readonly tables, because it will be updated on next table startup.
        /// It is also Ok to skip ZK error for the same reason.
        try
        {
            auto zookeeper = getZooKeeper();
            zookeeper->set(replica_path + "/host", getReplicatedMergeTreeAddress().toString());
        }
        catch (Coordination::Exception & e)
        {
            LOG_WARNING(log, "Cannot update the value of 'host' node (replica address) in ZooKeeper: {}", e.displayText());
        }
    }

    /// TODO: You can update names of loggers.
}


bool StorageReplicatedMergeTree::existsNodeCached(const std::string & path)
{
    {
        std::lock_guard lock(existing_nodes_cache_mutex);
        if (existing_nodes_cache.count(path))
            return true;
    }

    bool res = getZooKeeper()->exists(path);

    if (res)
    {
        std::lock_guard lock(existing_nodes_cache_mutex);
        existing_nodes_cache.insert(path);
    }

    return res;
}


std::optional<EphemeralLockInZooKeeper>
StorageReplicatedMergeTree::allocateBlockNumber(
    const String & partition_id, zkutil::ZooKeeperPtr & zookeeper, const String & zookeeper_block_id_path)
{
    /// Lets check for duplicates in advance, to avoid superfluous block numbers allocation
    Coordination::Requests deduplication_check_ops;
    if (!zookeeper_block_id_path.empty())
    {
        deduplication_check_ops.emplace_back(zkutil::makeCreateRequest(zookeeper_block_id_path, "", zkutil::CreateMode::Persistent));
        deduplication_check_ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_block_id_path, -1));
    }

    String block_numbers_path = zookeeper_path + "/block_numbers";
    String partition_path = block_numbers_path + "/" + partition_id;

    if (!existsNodeCached(partition_path))
    {
        Coordination::Requests ops;
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

    EphemeralLockInZooKeeper lock;
    /// 2 RTT
    try
    {
        lock = EphemeralLockInZooKeeper(
            partition_path + "/block-", zookeeper_path + "/temp", *zookeeper, &deduplication_check_ops);
    }
    catch (const zkutil::KeeperMultiException & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS && e.getPathForFirstFailedOp() == zookeeper_block_id_path)
            return {};

        throw Exception("Cannot allocate block number in ZooKeeper: " + e.displayText(), ErrorCodes::KEEPER_EXCEPTION);
    }
    catch (const Coordination::Exception & e)
    {
        throw Exception("Cannot allocate block number in ZooKeeper: " + e.displayText(), ErrorCodes::KEEPER_EXCEPTION);
    }

    return {std::move(lock)};
}


Strings StorageReplicatedMergeTree::waitForAllReplicasToProcessLogEntry(
    const ReplicatedMergeTreeLogEntryData & entry, bool wait_for_non_active)
{
    LOG_DEBUG(log, "Waiting for all replicas to process {}", entry.znode_name);

    auto zookeeper = getZooKeeper();
    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
    Strings unwaited;
    for (const String & replica : replicas)
    {
        if (wait_for_non_active || zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
        {
            if (!waitForReplicaToProcessLogEntry(replica, entry, wait_for_non_active))
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


bool StorageReplicatedMergeTree::waitForReplicaToProcessLogEntry(
    const String & replica, const ReplicatedMergeTreeLogEntryData & entry, bool wait_for_non_active)
{
    String entry_str = entry.toString();
    String log_node_name;

    /** Two types of entries can be passed to this function
      * 1. (more often) From `log` directory - a common log, from where replicas copy entries to their queue.
      * 2. From the `queue` directory of one of the replicas.
      *
      * The problem is that the numbers (`sequential` node) of the queue elements in `log` and in `queue` do not match.
      * (And the numbers of the same log element for different replicas do not match in the `queue`.)
      *
      * Therefore, you should consider these cases separately.
      */

    /** First, you need to wait until replica takes `queue` element from the `log` to its queue,
      *  if it has not been done already (see the `pullLogsToQueue` function).
      *
      * To do this, check its node `log_pointer` - the maximum number of the element taken from `log` + 1.
      */

    const auto & check_replica_become_inactive = [this, &replica]()
    {
        return !getZooKeeper()->exists(zookeeper_path + "/replicas/" + replica + "/is_active");
    };
    constexpr auto event_wait_timeout_ms = 1000;

    if (startsWith(entry.znode_name, "log-"))
    {
        /** In this case, just take the number from the node name `log-xxxxxxxxxx`.
          */

        UInt64 log_index = parse<UInt64>(entry.znode_name.substr(entry.znode_name.size() - 10));
        log_node_name = entry.znode_name;

        LOG_DEBUG(log, "Waiting for {} to pull {} to queue", replica, log_node_name);

        /// Let's wait until entry gets into the replica queue.
        while (wait_for_non_active || !check_replica_become_inactive())
        {
            zkutil::EventPtr event = std::make_shared<Poco::Event>();

            String log_pointer = getZooKeeper()->get(zookeeper_path + "/replicas/" + replica + "/log_pointer", nullptr, event);
            if (!log_pointer.empty() && parse<UInt64>(log_pointer) > log_index)
                break;

            if (wait_for_non_active)
                event->wait();
            else
                event->tryWait(event_wait_timeout_ms);
        }
    }
    else if (startsWith(entry.znode_name, "queue-"))
    {
        /** In this case, the number of `log` node is unknown. You need look through everything from `log_pointer` to the end,
          *  looking for a node with the same content. And if we do not find it - then the replica has already taken this entry in its queue.
          */

        String log_pointer = getZooKeeper()->get(zookeeper_path + "/replicas/" + replica + "/log_pointer");

        Strings log_entries = getZooKeeper()->getChildren(zookeeper_path + "/log");
        UInt64 log_index = 0;
        bool found = false;

        for (const String & log_entry_name : log_entries)
        {
            log_index = parse<UInt64>(log_entry_name.substr(log_entry_name.size() - 10));

            if (!log_pointer.empty() && log_index < parse<UInt64>(log_pointer))
                continue;

            String log_entry_str;
            bool exists = getZooKeeper()->tryGet(zookeeper_path + "/log/" + log_entry_name, log_entry_str);
            if (exists && entry_str == log_entry_str)
            {
                found = true;
                log_node_name = log_entry_name;
                break;
            }
        }

        if (found)
        {
            LOG_DEBUG(log, "Waiting for {} to pull {} to queue", replica, log_node_name);

            /// Let's wait until the entry gets into the replica queue.
            while (wait_for_non_active || !check_replica_become_inactive())
            {
                zkutil::EventPtr event = std::make_shared<Poco::Event>();

                String log_pointer_new = getZooKeeper()->get(zookeeper_path + "/replicas/" + replica + "/log_pointer", nullptr, event);
                if (!log_pointer_new.empty() && parse<UInt64>(log_pointer_new) > log_index)
                    break;

                if (wait_for_non_active)
                    event->wait();
                else
                    event->tryWait(event_wait_timeout_ms);
            }
        }
    }
    else
        throw Exception("Logical error: unexpected name of log node: " + entry.znode_name, ErrorCodes::LOGICAL_ERROR);

    if (!log_node_name.empty())
        LOG_DEBUG(log, "Looking for node corresponding to {} in {} queue", log_node_name, replica);
    else
        LOG_DEBUG(log, "Looking for corresponding node in {} queue", replica);

    /** Second - find the corresponding entry in the queue of the specified replica.
      * Its number may match neither the `log` node nor the `queue` node of the current replica (for us).
      * Therefore, we search by comparing the content.
      */

    Strings queue_entries = getZooKeeper()->getChildren(zookeeper_path + "/replicas/" + replica + "/queue");
    String queue_entry_to_wait_for;

    for (const String & entry_name : queue_entries)
    {
        String queue_entry_str;
        bool exists = getZooKeeper()->tryGet(zookeeper_path + "/replicas/" + replica + "/queue/" + entry_name, queue_entry_str);
        if (exists && queue_entry_str == entry_str)
        {
            queue_entry_to_wait_for = entry_name;
            break;
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
    String path_to_wait_on = zookeeper_path + "/replicas/" + replica + "/queue/" + queue_entry_to_wait_for;
    if (wait_for_non_active)
        return getZooKeeper()->waitForDisappear(path_to_wait_on);

    return getZooKeeper()->waitForDisappear(path_to_wait_on, check_replica_become_inactive);
}


void StorageReplicatedMergeTree::getStatus(Status & res, bool with_zk_fields)
{
    auto zookeeper = tryGetZooKeeper();
    const auto storage_settings_ptr = getSettings();

    res.is_leader = is_leader;
    res.can_become_leader = storage_settings_ptr->replicated_can_become_leader;
    res.is_readonly = is_readonly;
    res.is_session_expired = !zookeeper || zookeeper->expired();

    res.queue = queue.getStatus();
    res.absolute_delay = getAbsoluteDelay(); /// NOTE: may be slightly inconsistent with queue status.

    res.parts_to_check = part_check_thread.size();

    res.zookeeper_path = zookeeper_path;
    res.replica_name = replica_name;
    res.replica_path = replica_path;
    res.columns_version = -1;

    res.log_max_index = 0;
    res.log_pointer = 0;
    res.total_replicas = 0;
    res.active_replicas = 0;

    if (with_zk_fields && !res.is_session_expired)
    {
        try
        {
            auto log_entries = zookeeper->getChildren(zookeeper_path + "/log");

            if (log_entries.empty())
            {
                res.log_max_index = 0;
            }
            else
            {
                const String & last_log_entry = *std::max_element(log_entries.begin(), log_entries.end());
                res.log_max_index = parse<UInt64>(last_log_entry.substr(strlen("log-")));
            }

            String log_pointer_str = zookeeper->get(replica_path + "/log_pointer");
            res.log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);

            auto all_replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
            res.total_replicas = all_replicas.size();

            res.active_replicas = 0;
            for (const String & replica : all_replicas)
                if (zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
                    ++res.active_replicas;
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

    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

    for (const auto & replica : replicas)
    {
        if (replica == replica_name)
            continue;

        /// Skip dead replicas.
        if (!zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
            continue;

        String value;
        if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/min_unprocessed_insert_time", value))
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
    const Context & query_context)
{
    String partition_id = getPartitionIDFromQuery(partition, query_context);

    String from = from_;
    if (from.back() == '/')
        from.resize(from.size() - 1);

    LOG_INFO(log, "Will fetch partition {} from shard {}", partition_id, from_);

    /** Let's check that there is no such partition in the `detached` directory (where we will write the downloaded parts).
      * Unreliable (there is a race condition) - such a partition may appear a little later.
      */
    Poco::DirectoryIterator dir_end;
    for (const std::string & path : getDataPaths())
    {
        for (Poco::DirectoryIterator dir_it{path + "detached/"}; dir_it != dir_end; ++dir_it)
        {
            MergeTreePartInfo part_info;
            if (MergeTreePartInfo::tryParsePartName(dir_it.name(), &part_info, format_version)
                && part_info.partition_id == partition_id)
                throw Exception("Detached partition " + partition_id + " already exists.", ErrorCodes::PARTITION_ALREADY_EXISTS);
        }

    }

    zkutil::Strings replicas;
    zkutil::Strings active_replicas;
    String best_replica;

    {
        auto zookeeper = getZooKeeper();

        /// List of replicas of source shard.
        replicas = zookeeper->getChildren(from + "/replicas");

        /// Leave only active replicas.
        active_replicas.reserve(replicas.size());

        for (const String & replica : replicas)
            if (zookeeper->exists(from + "/replicas/" + replica + "/is_active"))
                active_replicas.push_back(replica);

        if (active_replicas.empty())
            throw Exception("No active replicas for shard " + from, ErrorCodes::NO_ACTIVE_REPLICAS);

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
            String current_replica_path = from + "/replicas/" + replica;

            String log_pointer_str = zookeeper->get(current_replica_path + "/log_pointer");
            Int64 log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);

            Coordination::Stat stat;
            zookeeper->get(current_replica_path + "/queue", &stat);
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
        throw Exception("Logical error: cannot choose best replica.", ErrorCodes::LOGICAL_ERROR);

    LOG_INFO(log, "Found {} replicas, {} of them are active. Selected {} to fetch from.", replicas.size(), active_replicas.size(), best_replica);

    String best_replica_path = from + "/replicas/" + best_replica;

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

        if (try_no >= query_context.getSettings().max_fetch_partition_retries_count)
            throw Exception("Too many retries to fetch parts from " + best_replica_path, ErrorCodes::TOO_MANY_RETRIES_TO_FETCH_PARTS);

        Strings parts = getZooKeeper()->getChildren(best_replica_path + "/parts");
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
                throw Exception("Partition " + partition_id + " on " + best_replica_path + " doesn't exist", ErrorCodes::PARTITION_DOESNT_EXIST);
        }
        else
        {
            for (const String & missing_part : missing_parts)
            {
                String containing_part = active_parts_set.getContainingPart(missing_part);
                if (!containing_part.empty())
                    parts_to_fetch.push_back(containing_part);
                else
                    LOG_WARNING(log, "Part {} on replica {} has been vanished.", missing_part, best_replica_path);
            }
        }

        LOG_INFO(log, "Parts to fetch: {}", parts_to_fetch.size());

        missing_parts.clear();
        for (const String & part : parts_to_fetch)
        {
            try
            {
                fetchPart(part, metadata_snapshot, best_replica_path, true, 0);
            }
            catch (const DB::Exception & e)
            {
                if (e.code() != ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER && e.code() != ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
                    && e.code() != ErrorCodes::CANNOT_READ_ALL_DATA)
                    throw;

                LOG_INFO(log, e.displayText());
                missing_parts.push_back(part);
            }
        }

        ++try_no;
    } while (!missing_parts.empty());
}


void StorageReplicatedMergeTree::mutate(const MutationCommands & commands, const Context & query_context)
{
    /// Overview of the mutation algorithm.
    ///
    /// When the client executes a mutation, this method is called. It acquires block numbers in all
    /// partitions, saves them in the mutation entry and writes the mutation entry to a new ZK node in
    /// the /mutations folder. This block numbers are needed to determine which parts should be mutated and
    /// which shouldn't (parts inserted after the mutation will have the block number higher than the
    /// block number acquired by the mutation in that partition and so will not be mutatied).
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
    /// (see tryExecutePartMutation() function). When a replica encounters a MUTATE_PART command, it is
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

    ReplicatedMergeTreeMutationEntry entry;
    entry.source_replica = replica_name;
    entry.commands = commands;

    String mutations_path = zookeeper_path + "/mutations";

    /// Update the mutations_path node when creating the mutation and check its version to ensure that
    /// nodes for mutations are created in the same order as the corresponding block numbers.
    /// Should work well if the number of concurrent mutation requests is small.
    while (true)
    {
        auto zookeeper = getZooKeeper();

        Coordination::Stat mutations_stat;
        zookeeper->get(mutations_path, &mutations_stat);

        EphemeralLocksInAllPartitions block_number_locks(
            zookeeper_path + "/block_numbers", "block-", zookeeper_path + "/temp", *zookeeper);

        for (const auto & lock : block_number_locks.getLocks())
            entry.block_numbers[lock.partition_id] = lock.number;

        entry.create_time = time(nullptr);

        Coordination::Requests requests;
        requests.emplace_back(zkutil::makeSetRequest(mutations_path, String(), mutations_stat.version));
        requests.emplace_back(zkutil::makeCreateRequest(
            mutations_path + "/", entry.toString(), zkutil::CreateMode::PersistentSequential));

        Coordination::Responses responses;
        Coordination::Error rc = zookeeper->tryMulti(requests, responses);

        if (rc == Coordination::Error::ZOK)
        {
            const String & path_created =
                dynamic_cast<const Coordination::CreateResponse *>(responses[1].get())->path_created;
            entry.znode_name = path_created.substr(path_created.find_last_of('/') + 1);
            LOG_TRACE(log, "Created mutation with ID {}", entry.znode_name);
            break;
        }
        else if (rc == Coordination::Error::ZBADVERSION)
        {
            LOG_TRACE(log, "Version conflict when trying to create a mutation node, retrying...");
            continue;
        }
        else
            throw Coordination::Exception("Unable to create a mutation znode", rc);
    }

    waitMutation(entry.znode_name, query_context.getSettingsRef().mutations_sync);
}

void StorageReplicatedMergeTree::waitMutation(const String & znode_name, size_t mutations_sync) const
{
    if (!mutations_sync)
        return;

    /// we have to wait
    auto zookeeper = getZooKeeper();
    Strings replicas;
    if (mutations_sync == 2) /// wait for all replicas
        replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
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

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();

    LOG_TRACE(log, "Killing mutation {}", mutation_id);

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
        global_context.getMergeList().cancelPartMutations(partition_id, block_number);
    }
    return CancellationCode::CancelSent;
}


void StorageReplicatedMergeTree::clearOldPartsAndRemoveFromZK()
{
    auto table_lock = lockForShare(
            RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);
    auto zookeeper = getZooKeeper();

    DataPartsVector parts = grabOldParts();
    if (parts.empty())
        return;

    DataPartsVector parts_to_delete_only_from_filesystem;    // Only duplicates
    DataPartsVector parts_to_delete_completely;              // All parts except duplicates
    DataPartsVector parts_to_retry_deletion;                 // Parts that should be retried due to network problems
    DataPartsVector parts_to_remove_from_filesystem;         // Parts removed from ZK

    for (const auto & part : parts)
    {
        if (!part->is_duplicate)
            parts_to_delete_completely.emplace_back(part);
        else
            parts_to_delete_only_from_filesystem.emplace_back(part);
    }
    parts.clear();

    auto remove_parts_from_filesystem = [log=log] (const DataPartsVector & parts_to_remove)
    {
        for (const auto & part : parts_to_remove)
        {
            try
            {
                part->remove();
            }
            catch (...)
            {
                tryLogCurrentException(log, "There is a problem with deleting part " + part->name + " from filesystem");
            }
        }
    };

    /// Delete duplicate parts from filesystem
    if (!parts_to_delete_only_from_filesystem.empty())
    {
        remove_parts_from_filesystem(parts_to_delete_only_from_filesystem);
        removePartsFinally(parts_to_delete_only_from_filesystem);

        LOG_DEBUG(log, "Removed {} old duplicate parts", parts_to_delete_only_from_filesystem.size());
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
        if (part_names_to_retry_deletion.count(part->name) == 0)
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
        remove_parts_from_filesystem(parts_to_remove_from_filesystem);
        removePartsFinally(parts_to_remove_from_filesystem);

        LOG_DEBUG(log, "Removed {} old parts", parts_to_remove_from_filesystem.size());
    }
}


bool StorageReplicatedMergeTree::tryRemovePartsFromZooKeeperWithRetries(DataPartsVector & parts, size_t max_retries)
{
    Strings part_names_to_remove;
    for (const auto & part : parts)
        part_names_to_remove.emplace_back(part->name);

    return tryRemovePartsFromZooKeeperWithRetries(part_names_to_remove, max_retries);
}

bool StorageReplicatedMergeTree::tryRemovePartsFromZooKeeperWithRetries(const Strings & part_names, size_t max_retries)
{
    size_t num_tries = 0;
    bool success = false;

    while (!success && (max_retries == 0 || num_tries < max_retries))
    {
        try
        {
            ++num_tries;
            success = true;

            auto zookeeper = getZooKeeper();

            std::vector<std::future<Coordination::ExistsResponse>> exists_futures;
            exists_futures.reserve(part_names.size());
            for (const String & part_name : part_names)
            {
                String part_path = replica_path + "/parts/" + part_name;
                exists_futures.emplace_back(zookeeper->asyncExists(part_path));
            }

            std::vector<std::future<Coordination::MultiResponse>> remove_futures;
            remove_futures.reserve(part_names.size());
            for (size_t i = 0; i < part_names.size(); ++i)
            {
                Coordination::ExistsResponse exists_resp = exists_futures[i].get();
                if (exists_resp.error == Coordination::Error::ZOK)
                {
                    Coordination::Requests ops;
                    removePartFromZooKeeper(part_names[i], ops, exists_resp.stat.numChildren > 0);
                    remove_futures.emplace_back(zookeeper->tryAsyncMulti(ops));
                }
            }

            for (auto & future : remove_futures)
            {
                auto response = future.get();

                if (response.error == Coordination::Error::ZOK || response.error == Coordination::Error::ZNONODE)
                    continue;

                if (Coordination::isHardwareError(response.error))
                {
                    success = false;
                    continue;
                }

                throw Coordination::Exception(response.error);
            }
        }
        catch (Coordination::Exception & e)
        {
            success = false;

            if (Coordination::isHardwareError(e.code))
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            else
                throw;
        }

        if (!success && num_tries < max_retries)
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    return success;
}

void StorageReplicatedMergeTree::removePartsFromZooKeeper(
    zkutil::ZooKeeperPtr & zookeeper, const Strings & part_names, NameSet * parts_should_be_retried)
{
    std::vector<std::future<Coordination::ExistsResponse>> exists_futures;
    std::vector<std::future<Coordination::MultiResponse>> remove_futures;
    exists_futures.reserve(part_names.size());
    remove_futures.reserve(part_names.size());
    try
    {
        /// Exception can be thrown from loop
        /// if zk session will be dropped
        for (const String & part_name : part_names)
        {
            String part_path = replica_path + "/parts/" + part_name;
            exists_futures.emplace_back(zookeeper->asyncExists(part_path));
        }

        for (size_t i = 0; i < part_names.size(); ++i)
        {
            Coordination::ExistsResponse exists_resp = exists_futures[i].get();
            if (exists_resp.error == Coordination::Error::ZOK)
            {
                Coordination::Requests ops;
                removePartFromZooKeeper(part_names[i], ops, exists_resp.stat.numChildren > 0);
                remove_futures.emplace_back(zookeeper->tryAsyncMulti(ops));
            }
            else
            {
                LOG_DEBUG(log, "There is no part {} in ZooKeeper, it was only in filesystem", part_names[i]);
                // emplace invalid future so that the total number of futures is the same as part_names.size();
                remove_futures.emplace_back();
            }
        }
    }
    catch (const Coordination::Exception & e)
    {
        if (parts_should_be_retried && Coordination::isHardwareError(e.code))
            parts_should_be_retried->insert(part_names.begin(), part_names.end());
        throw;
    }

    for (size_t i = 0; i < remove_futures.size(); ++i)
    {
        auto & future = remove_futures[i];

        if (!future.valid())
            continue;

        auto response = future.get();
        if (response.error == Coordination::Error::ZOK)
            continue;
        else if (response.error == Coordination::Error::ZNONODE)
        {
            LOG_DEBUG(log, "There is no part {} in ZooKeeper, it was only in filesystem", part_names[i]);
            continue;
        }
        else if (Coordination::isHardwareError(response.error))
        {
            if (parts_should_be_retried)
                parts_should_be_retried->insert(part_names[i]);
            continue;
        }
        else
            LOG_WARNING(log, "Cannot remove part {} from ZooKeeper: {}", part_names[i], Coordination::errorMessage(response.error));
    }
}


void StorageReplicatedMergeTree::clearBlocksInPartition(
    zkutil::ZooKeeper & zookeeper, const String & partition_id, Int64 min_block_num, Int64 max_block_num)
{
    Strings blocks;
    if (Coordination::Error::ZOK != zookeeper.tryGetChildren(zookeeper_path + "/blocks", blocks))
        throw Exception(zookeeper_path + "/blocks doesn't exist", ErrorCodes::NOT_FOUND_NODE);

    String partition_prefix = partition_id + "_";
    zkutil::AsyncResponses<Coordination::GetResponse> get_futures;
    for (const String & block_id : blocks)
    {
        if (startsWith(block_id, partition_prefix))
        {
            String path = zookeeper_path + "/blocks/" + block_id;
            get_futures.emplace_back(path, zookeeper.asyncTryGet(path));
        }
    }

    zkutil::AsyncResponses<Coordination::RemoveResponse> to_delete_futures;
    for (auto & pair : get_futures)
    {
        const String & path = pair.first;
        auto result = pair.second.get();

        if (result.error == Coordination::Error::ZNONODE)
            continue;

        ReadBufferFromString buf(result.data);
        Int64 block_num = 0;
        bool parsed = tryReadIntText(block_num, buf) && buf.eof();
        if (!parsed || (min_block_num <= block_num && block_num <= max_block_num))
            to_delete_futures.emplace_back(path, zookeeper.asyncTryRemove(path));
    }

    for (auto & pair : to_delete_futures)
    {
        const String & path = pair.first;
        Coordination::Error rc = pair.second.get().error;
        if (rc == Coordination::Error::ZNOTEMPTY)
        {
             /// Can happen if there are leftover block nodes with children created by previous server versions.
            zookeeper.removeRecursive(path);
        }
        else if (rc != Coordination::Error::ZOK)
            LOG_WARNING(log, "Error while deleting ZooKeeper path `{}`: {}, ignoring.", path, Coordination::errorMessage(rc));
    }

    LOG_TRACE(log, "Deleted {} deduplication block IDs in partition ID {}", to_delete_futures.size(), partition_id);
}

void StorageReplicatedMergeTree::replacePartitionFrom(
    const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context)
{
    /// First argument is true, because we possibly will add new data to current table.
    auto lock1 = lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
    auto lock2 = source_table->lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);

    auto source_metadata_snapshot = source_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    Stopwatch watch;
    MergeTreeData & src_data = checkStructureAndGetMergeTreeData(source_table, source_metadata_snapshot, metadata_snapshot);
    String partition_id = getPartitionIDFromQuery(partition, context);

    DataPartsVector src_all_parts = src_data.getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    DataPartsVector src_parts;
    MutableDataPartsVector dst_parts;
    Strings block_id_paths;
    Strings part_checksums;
    std::vector<EphemeralLockInZooKeeper> ephemeral_locks;

    LOG_DEBUG(log, "Cloning {} parts", src_all_parts.size());

    static const String TMP_PREFIX = "tmp_replace_from_";
    auto zookeeper = getZooKeeper();

    /// Firstly, generate last block number and compute drop_range
    /// NOTE: Even if we make ATTACH PARTITION instead of REPLACE PARTITION drop_range will not be empty, it will contain a block.
    /// So, such case has special meaning, if drop_range contains only one block it means that nothing to drop.
    MergeTreePartInfo drop_range;
    drop_range.partition_id = partition_id;
    drop_range.max_block = allocateBlockNumber(partition_id, zookeeper)->getNumber();
    drop_range.min_block = replace ? 0 : drop_range.max_block;
    drop_range.level = std::numeric_limits<decltype(drop_range.level)>::max();

    String drop_range_fake_part_name = getPartNamePossiblyFake(format_version, drop_range);

    if (drop_range.getBlocksCount() > 1)
    {
        /// We have to prohibit merges in drop_range, since new merge log entry appeared after this REPLACE FROM entry
        ///  could produce new merged part instead in place of just deleted parts.
        /// It is better to prohibit them on leader replica (like DROP PARTITION makes),
        ///  but it is inconvenient for a user since he could actually use source table from this replica.
        /// Therefore prohibit merges on the initializer server now and on the remaining servers when log entry will be executed.
        /// It does not provides strong guarantees, but is suitable for intended use case (assume merges are quite rare).

        {
            std::lock_guard merge_selecting_lock(merge_selecting_mutex);
            queue.disableMergesInBlockRange(drop_range_fake_part_name);
        }
    }

    for (const auto & src_part : src_all_parts)
    {
        /// We also make some kind of deduplication to avoid duplicated parts in case of ATTACH PARTITION
        /// Assume that merges in the partition are quite rare
        /// Save deduplication block ids with special prefix replace_partition

        if (!canReplacePartition(src_part))
            throw Exception(
                "Cannot replace partition '" + partition_id + "' because part '" + src_part->name + "' has inconsistent granularity with table",
                ErrorCodes::LOGICAL_ERROR);

        String hash_hex = src_part->checksums.getTotalChecksumHex();

        if (replace)
            LOG_INFO(log, "Trying to replace {} with hash_hex {}", src_part->name, hash_hex);
        else
            LOG_INFO(log, "Trying to attach {} with hash_hex {}", src_part->name, hash_hex);

        String block_id_path = replace ? "" : (zookeeper_path + "/blocks/" + partition_id + "_replace_from_" + hash_hex);

        auto lock = allocateBlockNumber(partition_id, zookeeper, block_id_path);
        if (!lock)
        {
            LOG_INFO(log, "Part {} (hash {}) has been already attached", src_part->name, hash_hex);
            continue;
        }

        UInt64 index = lock->getNumber();
        MergeTreePartInfo dst_part_info(partition_id, index, index, src_part->info.level);
        auto dst_part = cloneAndLoadDataPartOnSameDisk(src_part, TMP_PREFIX, dst_part_info, metadata_snapshot);

        src_parts.emplace_back(src_part);
        dst_parts.emplace_back(dst_part);
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

    /// We are almost ready to commit changes, remove fetches and merges from drop range
    queue.removePartProducingOpsInRange(zookeeper, drop_range, entry);

    /// Remove deduplication block_ids of replacing parts
    if (replace)
        clearBlocksInPartition(*zookeeper, drop_range.partition_id, drop_range.max_block, drop_range.max_block);

    DataPartsVector parts_to_remove;
    Coordination::Responses op_results;

    try
    {
        Coordination::Requests ops;
        for (size_t i = 0; i < dst_parts.size(); ++i)
        {
            getCommitPartOps(ops, dst_parts[i], block_id_paths[i]);
            ephemeral_locks[i].getUnlockOps(ops);

            if (ops.size() > zkutil::MULTI_BATCH_SIZE)
            {
                /// It is unnecessary to add parts to working set until we commit log entry
                zookeeper->multi(ops);
                ops.clear();
            }
        }

        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/log", "", -1));  /// Just update version
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential));

        Transaction transaction(*this);
        {
            auto data_parts_lock = lockParts();

            for (MutableDataPartPtr & part : dst_parts)
                renameTempPartAndReplace(part, nullptr, &transaction, data_parts_lock);
        }

        op_results = zookeeper->multi(ops);

        {
            auto data_parts_lock = lockParts();

            transaction.commit(&data_parts_lock);
            if (replace)
                parts_to_remove = removePartsInRangeFromWorkingSet(drop_range, true, false, data_parts_lock);
        }

        PartLog::addNewParts(global_context, dst_parts, watch.elapsed());
    }
    catch (...)
    {
        PartLog::addNewParts(global_context, dst_parts, watch.elapsed(), ExecutionStatus::fromCurrentException());
        throw;
    }

    String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*op_results.back()).path_created;
    entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

    for (auto & lock : ephemeral_locks)
        lock.assumeUnlocked();

    /// Forcibly remove replaced parts from ZooKeeper
    tryRemovePartsFromZooKeeperWithRetries(parts_to_remove);

    /// Speedup removing of replaced parts from filesystem
    parts_to_remove.clear();
    cleanup_thread.wakeup();

    /// If necessary, wait until the operation is performed on all replicas.
    if (context.getSettingsRef().replication_alter_partitions_sync > 1)
    {
        lock2.reset();
        lock1.reset();
        waitForAllReplicasToProcessLogEntry(entry);
    }
}

void StorageReplicatedMergeTree::movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, const Context & query_context)
{
    auto lock1 = lockForShare(query_context.getCurrentQueryId(), query_context.getSettingsRef().lock_acquire_timeout);
    auto lock2 = dest_table->lockForShare(query_context.getCurrentQueryId(), query_context.getSettingsRef().lock_acquire_timeout);

    auto dest_table_storage = std::dynamic_pointer_cast<StorageReplicatedMergeTree>(dest_table);
    if (!dest_table_storage)
        throw Exception("Table " + getStorageID().getNameForLogs() + " supports movePartitionToTable only for ReplicatedMergeTree family of table engines."
                        " Got " + dest_table->getName(), ErrorCodes::NOT_IMPLEMENTED);
    if (dest_table_storage->getStoragePolicy() != this->getStoragePolicy())
        throw Exception("Destination table " + dest_table_storage->getStorageID().getNameForLogs() +
                        " should have the same storage policy of source table " + getStorageID().getNameForLogs() + ". " +
                        getStorageID().getNameForLogs() + ": " + this->getStoragePolicy()->getName() + ", " +
                        getStorageID().getNameForLogs() + ": " + dest_table_storage->getStoragePolicy()->getName(), ErrorCodes::LOGICAL_ERROR);

    auto dest_metadata_snapshot = dest_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    Stopwatch watch;
    MergeTreeData & src_data = dest_table_storage->checkStructureAndGetMergeTreeData(*this, metadata_snapshot, dest_metadata_snapshot);
    auto src_data_id = src_data.getStorageID();
    String partition_id = getPartitionIDFromQuery(partition, query_context);

    DataPartsVector src_all_parts = src_data.getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    DataPartsVector src_parts;
    MutableDataPartsVector dst_parts;
    Strings block_id_paths;
    Strings part_checksums;
    std::vector<EphemeralLockInZooKeeper> ephemeral_locks;

    LOG_DEBUG(log, "Cloning {} parts", src_all_parts.size());

    static const String TMP_PREFIX = "tmp_move_from_";
    auto zookeeper = getZooKeeper();

    /// A range for log entry to remove parts from the source table (myself).

    MergeTreePartInfo drop_range;
    drop_range.partition_id = partition_id;
    drop_range.max_block = allocateBlockNumber(partition_id, zookeeper)->getNumber();
    drop_range.min_block = 0;
    drop_range.level = std::numeric_limits<decltype(drop_range.level)>::max();

    String drop_range_fake_part_name = getPartNamePossiblyFake(format_version, drop_range);

    if (drop_range.getBlocksCount() > 1)
    {
        std::lock_guard merge_selecting_lock(merge_selecting_mutex);
        queue.disableMergesInBlockRange(drop_range_fake_part_name);
    }

    /// Clone parts into destination table.

    for (const auto & src_part : src_all_parts)
    {
        if (!dest_table_storage->canReplacePartition(src_part))
            throw Exception(
                "Cannot move partition '" + partition_id + "' because part '" + src_part->name + "' has inconsistent granularity with table",
                ErrorCodes::LOGICAL_ERROR);

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
        auto dst_part = dest_table_storage->cloneAndLoadDataPartOnSameDisk(src_part, TMP_PREFIX, dst_part_info, dest_metadata_snapshot);

        src_parts.emplace_back(src_part);
        dst_parts.emplace_back(dst_part);
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
        MergeTreePartInfo drop_range_dest;
        drop_range_dest.partition_id = drop_range.partition_id;
        drop_range_dest.max_block = drop_range.max_block;
        drop_range_dest.min_block = drop_range.max_block;
        drop_range_dest.level = drop_range.level;

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

    queue.removePartProducingOpsInRange(zookeeper, drop_range, entry);

    clearBlocksInPartition(*zookeeper, drop_range.partition_id, drop_range.max_block, drop_range.max_block);

    DataPartsVector parts_to_remove;
    Coordination::Responses op_results;

    try
    {
        Coordination::Requests ops;
        for (size_t i = 0; i < dst_parts.size(); ++i)
        {
            dest_table_storage->getCommitPartOps(ops, dst_parts[i], block_id_paths[i]);
            ephemeral_locks[i].getUnlockOps(ops);

            if (ops.size() > zkutil::MULTI_BATCH_SIZE)
            {
                zookeeper->multi(ops);
                ops.clear();
            }
        }

        ops.emplace_back(zkutil::makeCreateRequest(dest_table_storage->zookeeper_path + "/log/log-",
                                                   entry.toString(), zkutil::CreateMode::PersistentSequential));

        {
            Transaction transaction(*dest_table_storage);

            auto src_data_parts_lock = lockParts();
            auto dest_data_parts_lock = dest_table_storage->lockParts();

            std::mutex mutex;
            DataPartsLock lock(mutex);

            for (MutableDataPartPtr & part : dst_parts)
                dest_table_storage->renameTempPartAndReplace(part, nullptr, &transaction, lock);

            op_results = zookeeper->multi(ops);

            parts_to_remove = removePartsInRangeFromWorkingSet(drop_range, true, false, lock);
            transaction.commit(&lock);
        }

        PartLog::addNewParts(global_context, dst_parts, watch.elapsed());
    }
    catch (...)
    {
        PartLog::addNewParts(global_context, dst_parts, watch.elapsed(), ExecutionStatus::fromCurrentException());
        throw;
    }

    String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*op_results.back()).path_created;
    entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

    for (auto & lock : ephemeral_locks)
        lock.assumeUnlocked();

    tryRemovePartsFromZooKeeperWithRetries(parts_to_remove);

    parts_to_remove.clear();
    cleanup_thread.wakeup();

    if (query_context.getSettingsRef().replication_alter_partitions_sync > 1)
    {
        lock2.reset();
        dest_table_storage->waitForAllReplicasToProcessLogEntry(entry);
    }

    Coordination::Requests ops_dest;

    ops_dest.emplace_back(zkutil::makeCreateRequest(
        zookeeper_path + "/log/log-", entry_delete.toString(), zkutil::CreateMode::PersistentSequential));
    ops_dest.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/log", "", -1));  /// Just update version

    op_results = zookeeper->multi(ops_dest);

    log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*op_results.front()).path_created;
    entry_delete.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

    if (query_context.getSettingsRef().replication_alter_partitions_sync > 1)
    {
        lock1.reset();
        waitForAllReplicasToProcessLogEntry(entry_delete);
    }

    /// Cleaning possibly stored information about parts from /quorum/last_part node in ZooKeeper.
    cleanLastPartNode(partition_id);
}

void StorageReplicatedMergeTree::getCommitPartOps(
    Coordination::Requests & ops,
    MutableDataPartPtr & part,
    const String & block_id_path) const
{
    const String & part_name = part->name;
    const auto storage_settings_ptr = getSettings();

    if (!block_id_path.empty())
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
            replica_path + "/parts/" + part->name,
            ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(part->getColumns(), part->checksums).toString(),
            zkutil::CreateMode::Persistent));
    }
    else
    {
        ops.emplace_back(zkutil::makeCreateRequest(
            replica_path + "/parts/" + part->name,
            "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            replica_path + "/parts/" + part->name + "/columns",
            part->getColumns().toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            replica_path + "/parts/" + part->name + "/checksums",
            getChecksumsForZooKeeper(part->checksums),
            zkutil::CreateMode::Persistent));
    }
}

ReplicatedMergeTreeAddress StorageReplicatedMergeTree::getReplicatedMergeTreeAddress() const
{
    auto host_port = global_context.getInterserverIOAddress();
    auto table_id = getStorageID();

    ReplicatedMergeTreeAddress res;
    res.host = host_port.first;
    res.replication_port = host_port.second;
    res.queries_port = global_context.getTCPPort();
    res.database = table_id.database_name;
    res.table = table_id.table_name;
    res.scheme = global_context.getInterserverScheme();
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
        return data_parts_exchange_endpoint ? data_parts_exchange_endpoint->blocker.cancel() : ActionLock();

    if (action_type == ActionLocks::ReplicationQueue)
        return queue.actions_blocker.cancel();

    if (action_type == ActionLocks::PartsMove)
        return parts_mover.moves_blocker.cancel();

    return {};
}


bool StorageReplicatedMergeTree::waitForShrinkingQueueSize(size_t queue_size, UInt64 max_wait_milliseconds)
{
    Stopwatch watch;

    /// Let's fetch new log entries firstly
    queue.pullLogsToQueue(getZooKeeper());

    {
        auto lock = queue.lockQueue();
        if (!queue_task_handle)
            return false;

        /// This is significant, because the execution of this task could be delayed at BackgroundPool.
        /// And we force it to be executed.
        queue_task_handle->signalReadyToRun();
    }

    Poco::Event target_size_event;
    auto callback = [&target_size_event, queue_size] (size_t new_queue_size)
    {
        if (new_queue_size <= queue_size)
            target_size_event.set();
    };
    const auto handler = queue.addSubscriber(std::move(callback));

    while (!target_size_event.tryWait(50))
    {
        if (max_wait_milliseconds && watch.elapsedMilliseconds() > max_wait_milliseconds)
            return false;

        if (partial_shutdown_called)
            throw Exception("Shutdown is called for table", ErrorCodes::ABORTED);
    }

    return true;
}


bool StorageReplicatedMergeTree::dropPartsInPartition(
    zkutil::ZooKeeper & zookeeper, String & partition_id, StorageReplicatedMergeTree::LogEntry & entry, bool detach)
{
    MergeTreePartInfo drop_range_info;
    if (!getFakePartCoveringAllPartsInPartition(partition_id, drop_range_info))
    {
        LOG_INFO(log, "Will not drop partition {}, it is empty.", partition_id);
        return false;
    }

    clearBlocksInPartition(zookeeper, partition_id, drop_range_info.min_block, drop_range_info.max_block);

    /** Forbid to choose the parts to be deleted for merging.
      * Invariant: after the `DROP_RANGE` entry appears in the log, merge of deleted parts will not appear in the log.
      */
    String drop_range_fake_part_name = getPartNamePossiblyFake(format_version, drop_range_info);
    {
        std::lock_guard merge_selecting_lock(merge_selecting_mutex);
        queue.disableMergesInBlockRange(drop_range_fake_part_name);
    }

    LOG_DEBUG(log, "Disabled merges covered by range {}", drop_range_fake_part_name);

    /// Finally, having achieved the necessary invariants, you can put an entry in the log.
    entry.type = LogEntry::DROP_RANGE;
    entry.source_replica = replica_name;
    entry.new_part_name = drop_range_fake_part_name;
    entry.detach = detach;
    entry.create_time = time(nullptr);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential));
    ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/log", "", -1));  /// Just update version.
    Coordination::Responses responses = zookeeper.multi(ops);

    String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*responses.front()).path_created;
    entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

    return true;
}


CheckResults StorageReplicatedMergeTree::checkData(const ASTPtr & query, const Context & context)
{
    CheckResults results;
    DataPartsVector data_parts;
    if (const auto & check_query = query->as<ASTCheckQuery &>(); check_query.partition)
    {
        String partition_id = getPartitionIDFromQuery(check_query.partition, context);
        data_parts = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    }
    else
        data_parts = getDataPartsVector();

    for (auto & part : data_parts)
    {
        try
        {
            results.push_back(part_check_thread.checkPart(part->name));
        }
        catch (const Exception & ex)
        {
            results.emplace_back(part->name, false, "Check of part finished with error: '" + ex.message() + "'");
        }
    }
    return results;
}

bool StorageReplicatedMergeTree::canUseAdaptiveGranularity() const
{
    const auto storage_settings_ptr = getSettings();
    return storage_settings_ptr->index_granularity_bytes != 0 &&
        (storage_settings_ptr->enable_mixed_granularity_parts ||
            (!has_non_adaptive_index_granularity_parts && !other_replicas_fixed_granularity));
}


MutationCommands StorageReplicatedMergeTree::getFirtsAlterMutationCommandsForPart(const DataPartPtr & part) const
{
    return queue.getFirstAlterMutationCommandsForPart(part);
}


void StorageReplicatedMergeTree::startBackgroundMovesIfNeeded()
{
    if (areBackgroundMovesNeeded() && !move_parts_task_handle)
    {
        auto & pool = global_context.getBackgroundMovePool();
        move_parts_task_handle = pool.createTask([this] { return movePartsTask(); });
        pool.startTask(move_parts_task_handle);
    }
}

}
