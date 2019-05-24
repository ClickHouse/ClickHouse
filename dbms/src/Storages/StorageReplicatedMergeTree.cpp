#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/FieldVisitors.h>
#include <Common/Macros.h>
#include <Common/formatReadable.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadPool.h>

#include <Storages/AlterCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumAddedParts.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/VirtualColumnUtils.h>

#include <Databases/IDatabase.h>

#include <Parsers/formatAST.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>

#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>

#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/PartLog.h>

#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/copyData.h>

#include <Poco/DirectoryIterator.h>

#include <ext/range.h>
#include <ext/scope_guard.h>

#include <ctime>
#include <thread>
#include <future>


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
}

namespace CurrentMetrics
{
    extern const Metric LeaderReplica;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int INCORRECT_DATA;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int REPLICA_IS_ALREADY_EXIST;
    extern const int NO_SUCH_REPLICA;
    extern const int NO_REPLICA_HAS_PART;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_UNEXPECTED_DATA_PARTS;
    extern const int ABORTED;
    extern const int REPLICA_IS_NOT_IN_QUORUM;
    extern const int TABLE_IS_READ_ONLY;
    extern const int NOT_FOUND_NODE;
    extern const int NO_ACTIVE_REPLICAS;
    extern const int LEADERSHIP_CHANGED;
    extern const int TABLE_IS_READ_ONLY;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int PARTITION_ALREADY_EXISTS;
    extern const int TOO_MANY_RETRIES_TO_FETCH_PARTS;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int PARTITION_DOESNT_EXIST;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int UNEXPECTED_FILE_IN_DATA_PART;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int UNFINISHED;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int TOO_MANY_FETCHES;
    extern const int BAD_DATA_PART_NAME;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int INCORRECT_FILE_NAME;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int KEEPER_EXCEPTION;
    extern const int ALL_REPLICAS_LOST;
    extern const int REPLICA_STATUS_CHANGED;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsFetch;
    extern const StorageActionBlockType PartsSend;
    extern const StorageActionBlockType ReplicationQueue;
}


static const auto QUEUE_UPDATE_ERROR_SLEEP_MS     = 1 * 1000;
static const auto MERGE_SELECTING_SLEEP_MS        = 5 * 1000;
static const auto MUTATIONS_FINALIZING_SLEEP_MS   = 1 * 1000;

/** There are three places for each part, where it should be
  * 1. In the RAM, data_parts, all_data_parts.
  * 2. In the filesystem (FS), the directory with the data of the table.
  * 3. in ZooKeeper (ZK).
  *
  * When adding a part, it must be added immediately to these three places.
  * This is done like this
  * - [FS] first write the part into a temporary directory on the filesystem;
  * - [FS] rename the temporary part to the result on the filesystem;
  * - [RAM] immediately afterwards add it to the `data_parts`, and remove from `data_parts` any parts covered by this one;
  * - [RAM] also set the `Transaction` object, which in case of an exception (in next point),
  *   rolls back the changes in `data_parts` (from the previous point) back;
  * - [ZK] then send a transaction (multi) to add a part to ZooKeeper (and some more actions);
  * - [FS, ZK] by the way, removing the covered (old) parts from filesystem, from ZooKeeper and from `all_data_parts`
  *   is delayed, after a few minutes.
  *
  * There is no atomicity here.
  * It could be possible to achieve atomicity using undo/redo logs and a flag in `DataPart` when it is completely ready.
  * But it would be inconvenient - I would have to write undo/redo logs for each `Part` in ZK, and this would increase already large number of interactions.
  *
  * Instead, we are forced to work in a situation where at any time
  *  (from another thread, or after server restart), there may be an unfinished transaction.
  *  (note - for this the part should be in RAM)
  * From these cases the most frequent one is when the part is already in the data_parts, but it's not yet in ZooKeeper.
  * This case must be distinguished from the case where such a situation is achieved due to some kind of damage to the state.
  *
  * Do this with the threshold for the time.
  * If the part is young enough, its lack in ZooKeeper will be perceived optimistically - as if it just did not have time to be added there
  *  - as if the transaction has not yet been executed, but will soon be executed.
  * And if the part is old, its absence in ZooKeeper will be perceived as an unfinished transaction that needs to be rolled back.
  *
  * PS. Perhaps it would be better to add a flag to the DataPart that a part is inserted into ZK.
  * But here it's too easy to get confused with the consistency of this flag.
  */
extern const int MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER = 5 * 60;


/** For randomized selection of replicas. */
/// avoid error: non-local variable 'DB::rng' declared '__thread' needs dynamic initialization
#ifndef __APPLE__
thread_local
#endif
    pcg64 rng{randomSeed()};


void StorageReplicatedMergeTree::setZooKeeper(zkutil::ZooKeeperPtr zookeeper)
{
    std::lock_guard lock(current_zookeeper_mutex);
    current_zookeeper = zookeeper;
}

zkutil::ZooKeeperPtr StorageReplicatedMergeTree::tryGetZooKeeper()
{
    std::lock_guard lock(current_zookeeper_mutex);
    return current_zookeeper;
}

zkutil::ZooKeeperPtr StorageReplicatedMergeTree::getZooKeeper()
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    return res;
}


StorageReplicatedMergeTree::StorageReplicatedMergeTree(
    const String & zookeeper_path_,
    const String & replica_name_,
    bool attach,
    const String & path_,
    const String & database_name_,
    const String & table_name_,
    const ColumnsDescription & columns_,
    const IndicesDescription & indices_,
    Context & context_,
    const String & date_column_name,
    const ASTPtr & partition_by_ast_,
    const ASTPtr & order_by_ast_,
    const ASTPtr & primary_key_ast_,
    const ASTPtr & sample_by_ast_,
    const ASTPtr & ttl_table_ast_,
    const MergingParams & merging_params_,
    const MergeTreeSettings & settings_,
    bool has_force_restore_data_flag)
        : MergeTreeData(database_name_, table_name_,
            path_ + escapeForFileName(table_name_) + '/',
            columns_, indices_,
            context_, date_column_name, partition_by_ast_, order_by_ast_, primary_key_ast_,
            sample_by_ast_, ttl_table_ast_, merging_params_,
            settings_, true, attach,
            [this] (const std::string & name) { enqueuePartForCheck(name); }),
        zookeeper_path(global_context.getMacros()->expand(zookeeper_path_, database_name_, table_name_)),
        replica_name(global_context.getMacros()->expand(replica_name_, database_name_, table_name_)),
        reader(*this), writer(*this), merger_mutator(*this, global_context.getBackgroundPool()), queue(*this), fetcher(*this),
        cleanup_thread(*this), alter_thread(*this), part_check_thread(*this), restarting_thread(*this)
{
    if (path_.empty())
        throw Exception("ReplicatedMergeTree storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;
    replica_path = zookeeper_path + "/replicas/" + replica_name;

    queue_updating_task = global_context.getSchedulePool().createTask(database_name + "." + table_name + " (StorageReplicatedMergeTree::queueUpdatingTask)", [this]{ queueUpdatingTask(); });

    mutations_updating_task = global_context.getSchedulePool().createTask(database_name + "." + table_name + " (StorageReplicatedMergeTree::mutationsUpdatingTask)", [this]{ mutationsUpdatingTask(); });

    merge_selecting_task = global_context.getSchedulePool().createTask(database_name + "." + table_name + " (StorageReplicatedMergeTree::mergeSelectingTask)", [this] { mergeSelectingTask(); });
    /// Will be activated if we win leader election.
    merge_selecting_task->deactivate();

    mutations_finalizing_task = global_context.getSchedulePool().createTask(database_name + "." + table_name + " (StorageReplicatedMergeTree::mutationsFinalizingTask)", [this] { mutationsFinalizingTask(); });

    if (global_context.hasZooKeeper())
        current_zookeeper = global_context.getZooKeeper();

    bool skip_sanity_checks = false;

    if (current_zookeeper && current_zookeeper->exists(replica_path + "/flags/force_restore_data"))
    {
        skip_sanity_checks = true;
        current_zookeeper->remove(replica_path + "/flags/force_restore_data");

        LOG_WARNING(log, "Skipping the limits on severity of changes to data parts and columns (flag "
            << replica_path << "/flags/force_restore_data).");
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
        return;
    }

    if (!attach)
    {
        if (!getDataParts().empty())
            throw Exception("Data directory for table already containing data parts - probably it was unclean DROP table or manual intervention. You must either clear directory by hand or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);

        createTableIfNotExists();

        checkTableStructure(false, false);
        createReplica();
    }
    else
    {
        checkTableStructure(skip_sanity_checks, true);
        checkParts(skip_sanity_checks);

        /// Temporary directories contain unfinalized results of Merges or Fetches (after forced restart)
        ///  and don't allow to reinitialize them, so delete each of them immediately
        clearOldTemporaryDirectories(0);
    }

    createNewZooKeeperNodes();
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

    /// ALTERs of the metadata node.
    zookeeper->createIfNotExists(replica_path + "/metadata", String());
}


void StorageReplicatedMergeTree::createTableIfNotExists()
{
    auto zookeeper = getZooKeeper();

    if (zookeeper->exists(zookeeper_path))
        return;

    LOG_DEBUG(log, "Creating table " << zookeeper_path);

    zookeeper->createAncestors(zookeeper_path);

    /// We write metadata of table so that the replicas can check table parameters with them.
    String metadata = ReplicatedMergeTreeTableMetadata(*this).toString();

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "",
        zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", metadata,
        zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/columns", getColumns().toString(),
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
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "",
        zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    auto code = zookeeper->tryMulti(ops, responses);
    if (code && code != Coordination::ZNODEEXISTS)
        throw Coordination::Exception(code);
}


/** Verify that list of columns and table settings match those specified in ZK (/ metadata).
    * If not, throw an exception.
    */
void StorageReplicatedMergeTree::checkTableStructure(bool skip_sanity_checks, bool allow_alter)
{
    auto zookeeper = getZooKeeper();

    ReplicatedMergeTreeTableMetadata old_metadata(*this);

    Coordination::Stat metadata_stat;
    String metadata_str = zookeeper->get(zookeeper_path + "/metadata", &metadata_stat);
    auto metadata_from_zk = ReplicatedMergeTreeTableMetadata::parse(metadata_str);
    auto metadata_diff = old_metadata.checkAndFindDiff(metadata_from_zk, allow_alter);
    metadata_version = metadata_stat.version;

    Coordination::Stat columns_stat;
    auto columns_from_zk = ColumnsDescription::parse(zookeeper->get(zookeeper_path + "/columns", &columns_stat));
    columns_version = columns_stat.version;

    const ColumnsDescription & old_columns = getColumns();
    if (columns_from_zk != old_columns || !metadata_diff.empty())
    {
        if (allow_alter &&
            (skip_sanity_checks ||
             old_columns.getOrdinary().sizeOfDifference(columns_from_zk.getOrdinary()) +
             old_columns.getMaterialized().sizeOfDifference(columns_from_zk.getMaterialized()) <= 2))
        {
            LOG_WARNING(log, "Table structure in ZooKeeper is a little different from local table structure. Assuming ALTER.");

            /// We delay setting table structure till startup() because otherwise new table metadata file can
            /// be overwritten in DatabaseOrdinary::createTable.
            set_table_structure_at_startup = [columns_from_zk, metadata_diff, this]()
            {
                /// Without any locks, because table has not been created yet.
                setTableStructure(std::move(columns_from_zk), metadata_diff);
            };
        }
        else
        {
            throw Exception("Table structure in ZooKeeper is too different from local table structure",
                            ErrorCodes::INCOMPATIBLE_COLUMNS);
        }
    }
}


void StorageReplicatedMergeTree::setTableStructure(ColumnsDescription new_columns, const ReplicatedMergeTreeTableMetadata::Diff & metadata_diff)
{
    ASTPtr new_primary_key_ast = primary_key_ast;
    ASTPtr new_order_by_ast = order_by_ast;
    auto new_indices = getIndices();
    ASTPtr new_ttl_table_ast = ttl_table_ast;
    IDatabase::ASTModifier storage_modifier;
    if (!metadata_diff.empty())
    {
        if (metadata_diff.sorting_key_changed)
        {
            ParserNotEmptyExpressionList parser(false);
            auto new_sorting_key_expr_list = parseQuery(parser, metadata_diff.new_sorting_key, 0);

            if (new_sorting_key_expr_list->children.size() == 1)
                new_order_by_ast = new_sorting_key_expr_list->children[0];
            else
            {
                auto tuple = makeASTFunction("tuple");
                tuple->arguments->children = new_sorting_key_expr_list->children;
                new_order_by_ast = tuple;
            }

            if (!primary_key_ast)
            {
                /// Primary and sorting key become independent after this ALTER so we have to
                /// save the old ORDER BY expression as the new primary key.
                new_primary_key_ast = order_by_ast->clone();
            }
        }

        if (metadata_diff.skip_indices_changed)
            new_indices = IndicesDescription::parse(metadata_diff.new_skip_indices);

        if (metadata_diff.ttl_table_changed)
        {
            ParserExpression parser;
            new_ttl_table_ast = parseQuery(parser, metadata_diff.new_ttl_table, 0);
        }

        storage_modifier = [&](IAST & ast)
        {
            auto & storage_ast = ast.as<ASTStorage &>();

            if (!storage_ast.order_by)
                throw Exception(
                    "ALTER MODIFY ORDER BY of default-partitioned tables is not supported",
                    ErrorCodes::LOGICAL_ERROR);

            if (new_primary_key_ast.get() != primary_key_ast.get())
                storage_ast.set(storage_ast.primary_key, new_primary_key_ast);

            if (new_ttl_table_ast.get() != ttl_table_ast.get())
                storage_ast.set(storage_ast.ttl_table, new_ttl_table_ast);

            storage_ast.set(storage_ast.order_by, new_order_by_ast);
        };
    }

    global_context.getDatabase(database_name)->alterTable(global_context, table_name, new_columns, new_indices, storage_modifier);

    /// Even if the primary/sorting keys didn't change we must reinitialize it
    /// because primary key column types might have changed.
    setPrimaryKeyIndicesAndColumns(new_order_by_ast, new_primary_key_ast, new_columns, new_indices);
    setTTLExpressions(new_columns.getColumnTTLs(), new_ttl_table_ast);
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


void StorageReplicatedMergeTree::createReplica()
{
    auto zookeeper = getZooKeeper();

    LOG_DEBUG(log, "Creating replica " << replica_path);

    int32_t code;

    do
    {
        Coordination::Stat replicas_stat;
        String last_added_replica = zookeeper->get(zookeeper_path + "/replicas", &replicas_stat);

        /// If it is not the first replica, we will mark it as "lost", to immediately repair (clone) from existing replica.
        String is_lost_value = last_added_replica.empty() ? "0" : "1";

        Coordination::Requests ops;
        Coordination::Responses resps;
        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/log_pointer", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/queue", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/parts", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/flags", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/is_lost", is_lost_value, zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/columns", getColumns().toString(), zkutil::CreateMode::Persistent));
        /// Check version of /replicas to see if there are any replicas created at the same moment of time.
        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name, replicas_stat.version));

        code = zookeeper->tryMulti(ops, resps);
        if (code == Coordination::Error::ZNODEEXISTS)
            throw Exception("Replica " + replica_path + " already exists.", ErrorCodes::REPLICA_IS_ALREADY_EXIST);
        else if (code == Coordination::Error::ZBADVERSION)
            LOG_ERROR(log, "Retrying createReplica(), because some other replicas were created at the same time");
        else
            zkutil::KeeperMultiException::check(code, ops, resps);
    } while (code == Coordination::Error::ZBADVERSION);
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

        LOG_ERROR(log, "Unexpected part name: " << part_name);
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

    bool insane = unexpected_parts_rows > total_rows_on_filesystem * settings.replicated_max_ratio_of_wrong_parts;

    if (insane && !skip_sanity_checks)
    {
        std::stringstream why;
        why << "The local set of parts of table " << database_name << "." << table_name << " doesn't look like the set of parts "
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
        LOG_ERROR(log, "Removing locally missing part from ZooKeeper and queueing a fetch: " << part_name);

        Coordination::Requests ops;

        time_t part_create_time = 0;
        Coordination::ExistsResponse exists_resp = exists_futures[i].get();
        if (!exists_resp.error)
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
        LOG_ERROR(log, "Renaming unexpected part " << part->name << " to ignored_" + part->name);
        forgetPartAndMoveToDetached(part, "ignored_", true);
    }
}


void StorageReplicatedMergeTree::checkPartChecksumsAndAddCommitOps(const zkutil::ZooKeeperPtr & zookeeper,
    const DataPartPtr & part, Coordination::Requests & ops, String part_name, NameSet * absent_replicas_paths)
{
    if (part_name.empty())
        part_name = part->name;

    check(part->columns);
    int expected_columns_version = columns_version;

    auto local_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(
        part->columns, part->checksums);

    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
    std::shuffle(replicas.begin(), replicas.end(), rng);
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
                LOG_INFO(log, "Not checking checksums of part " << part_name << " with replica " << replica
                    << " because part changed while we were reading its checksums");
                continue;
            }

            replica_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksumsZNodes(
                columns_str, checksums_str);
        }

        if (replica_part_header.getColumnsHash() != local_part_header.getColumnsHash())
        {
            LOG_INFO(log, "Not checking checksums of part " << part_name << " with replica " << replica
                << " because columns are different");
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
        String part_path = replica_path + "/parts/" + part_name;

        ops.emplace_back(zkutil::makeCheckRequest(
            zookeeper_path + "/columns", expected_columns_version));

        if (settings.use_minimalistic_part_header_in_zookeeper)
        {
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path, local_part_header.toString(), zkutil::CreateMode::Persistent));
        }
        else
        {
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path, "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path + "/columns", part->columns.toString(), zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path + "/checksums", getChecksumsForZooKeeper(part->checksums), zkutil::CreateMode::Persistent));
        }
    }
    else
    {
        LOG_WARNING(log, "checkPartAndAddToZooKeeper: node " << replica_path + "/parts/" + part_name << " already exists."
            << " Will not commit any nodes.");
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

            if (failed_op_index < num_check_ops && e.code == Coordination::ZNODEEXISTS)
            {
                LOG_INFO(log, "The part " << e.getPathForFirstFailedOp() << " on a replica suddenly appeared, will recheck checksums");
            }
            else
                throw;
        }
    }
}

String StorageReplicatedMergeTree::getChecksumsForZooKeeper(const MergeTreeDataPartChecksums & checksums) const
{
    return MinimalisticDataPartChecksums::getSerializedString(checksums,
                                                              static_cast<bool>(settings.use_minimalistic_checksums_in_zookeeper));
}


bool StorageReplicatedMergeTree::executeLogEntry(LogEntry & entry)
{
    if (entry.type == LogEntry::DROP_RANGE)
    {
        executeDropRange(entry);
        return true;
    }

    if (entry.type == LogEntry::CLEAR_COLUMN)
    {
        executeClearColumnInPartition(entry);
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
                LOG_DEBUG(log, "Skipping action for part " << entry.new_part_name << " because part " + existing_part->name + " already exists.");
            }
            return true;
        }
    }

    if (entry.type == LogEntry::GET_PART && entry.source_replica == replica_name)
        LOG_WARNING(log, "Part " << entry.new_part_name << " from own log doesn't exist.");

    /// Perhaps we don't need this part, because during write with quorum, the quorum has failed (see below about `/quorum/failed_parts`).
    if (entry.quorum && getZooKeeper()->exists(zookeeper_path + "/quorum/failed_parts/" + entry.new_part_name))
    {
        LOG_DEBUG(log, "Skipping action for part " << entry.new_part_name << " because quorum for that part was failed.");
        return true;    /// NOTE Deletion from `virtual_parts` is not done, but it is only necessary for merge.
    }

    bool do_fetch = false;
    if (entry.type == LogEntry::GET_PART)
    {
        do_fetch = true;
    }
    else if (entry.type == LogEntry::MERGE_PARTS)
    {
        do_fetch = !tryExecuteMerge(entry);
    }
    else if (entry.type == LogEntry::MUTATE_PART)
    {
        do_fetch = !tryExecutePartMutation(entry);
    }
    else
    {
        throw Exception("Unexpected log entry type: " + toString(static_cast<int>(entry.type)), ErrorCodes::LOGICAL_ERROR);
    }

    if (do_fetch)
        return executeFetch(entry);

    return true;
}


void StorageReplicatedMergeTree::writePartLog(
    PartLogElement::Type type, const ExecutionStatus & execution_status, UInt64 elapsed_ns,
    const String & new_part_name,
    const DataPartPtr & result_part,
    const DataPartsVector & source_parts,
    const MergeListEntry * merge_entry)
{
    try
    {
        auto part_log = global_context.getPartLog(database_name);
        if (!part_log)
            return;

        PartLogElement part_log_elem;

        part_log_elem.event_type = type;

        part_log_elem.error = static_cast<UInt16>(execution_status.code);
        part_log_elem.exception = execution_status.message;

        part_log_elem.event_time = time(nullptr);
        /// TODO: Stop stopwatch in outer code to exclude ZK timings and so on
        part_log_elem.duration_ms = elapsed_ns / 10000000;

        part_log_elem.database_name = database_name;
        part_log_elem.table_name = table_name;
        part_log_elem.partition_id = MergeTreePartInfo::fromPartName(new_part_name, format_version).partition_id;
        part_log_elem.part_name = new_part_name;

        if (result_part)
        {
            part_log_elem.bytes_compressed_on_disk = result_part->bytes_on_disk;
            part_log_elem.rows = result_part->rows_count;
        }

        part_log_elem.source_part_names.reserve(source_parts.size());
        for (const auto & source_part : source_parts)
            part_log_elem.source_part_names.push_back(source_part->name);

        if (merge_entry)
        {
            part_log_elem.rows_read = (*merge_entry)->rows_read;
            part_log_elem.bytes_read_uncompressed = (*merge_entry)->bytes_read_uncompressed;

            part_log_elem.rows = (*merge_entry)->rows_written;
            part_log_elem.bytes_uncompressed = (*merge_entry)->bytes_written_uncompressed;
        }

        part_log->add(part_log_elem);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}


bool StorageReplicatedMergeTree::tryExecuteMerge(const LogEntry & entry)
{
    // Log source part names just in case
    {
        std::stringstream log_message;
        log_message << "Executing log entry to merge parts ";
        for (auto i : ext::range(0, entry.source_parts.size()))
            log_message << (i != 0 ? ", " : "") << entry.source_parts[i];
        log_message << " to " << entry.new_part_name;

        LOG_TRACE(log, log_message.rdbuf());
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
            LOG_WARNING(log, "Part " << name << " is covered by " << part->name
                << " but should be merged into " << entry.new_part_name << ". This shouldn't happen often.");
            have_all_parts = false;
            break;
        }
        parts.push_back(part);
    }

    if (!have_all_parts)
    {
        /// If you do not have all the necessary parts, try to take some already merged part from someone.
        LOG_DEBUG(log, "Don't have all parts for merge " << entry.new_part_name << "; will try to fetch it instead");
        return false;
    }
    else if (entry.create_time + settings.prefer_fetch_merged_part_time_threshold.totalSeconds() <= time(nullptr))
    {
        /// If entry is old enough, and have enough size, and part are exists in any replica,
        ///  then prefer fetching of merged part from replica.

        size_t sum_parts_bytes_on_disk = 0;
        for (const auto & part : parts)
            sum_parts_bytes_on_disk += part->bytes_on_disk;

        if (sum_parts_bytes_on_disk >= settings.prefer_fetch_merged_part_size_threshold)
        {
            String replica = findReplicaHavingPart(entry.new_part_name, true);    /// NOTE excessive ZK requests for same data later, may remove.
            if (!replica.empty())
            {
                LOG_DEBUG(log, "Prefer to fetch " << entry.new_part_name << " from replica " << replica);
                return false;
            }
        }
    }

    /// Start to make the main work

    size_t estimated_space_for_merge = MergeTreeDataMergerMutator::estimateNeededDiskSpace(parts);

    /// Can throw an exception.
    DiskSpaceMonitor::ReservationPtr reserved_space = DiskSpaceMonitor::reserve(full_path, estimated_space_for_merge);

    auto table_lock = lockStructureForShare(false, RWLockImpl::NO_QUERY);

    FutureMergedMutatedPart future_merged_part(parts);
    if (future_merged_part.name != entry.new_part_name)
    {
        throw Exception("Future merged part name `" + future_merged_part.name + "` differs from part name in log entry: `"
                        + entry.new_part_name + "`", ErrorCodes::BAD_DATA_PART_NAME);
    }

    MergeList::EntryPtr merge_entry = global_context.getMergeList().insert(database_name, table_name, future_merged_part);

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
            future_merged_part, *merge_entry, entry.create_time, reserved_space.get(), entry.deduplicate);

        merger_mutator.renameMergedTemporaryPart(part, parts, &transaction);
        removeEmptyColumnsFromPart(part);

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

                LOG_ERROR(log, getCurrentExceptionMessage(false) << ". "
                    "Data after merge is not byte-identical to data on another replicas. "
                    "There could be several reasons: "
                    "1. Using newer version of compression library after server update. "
                    "2. Using another compression method. "
                    "3. Non-deterministic compression algorithm (highly unlikely). "
                    "4. Non-deterministic merge algorithm due to logical error in code. "
                    "5. Data corruption in memory due to bug in code. "
                    "6. Data corruption in memory due to hardware issue. "
                    "7. Manual modification of source data after server startup. "
                    "8. Manual modification of checksums stored in ZooKeeper. "
                    "We will download merged part from replica to force byte-identical result.");

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
    LOG_TRACE(log, "Executing log entry to mutate part " << source_part_name << " to " << entry.new_part_name);

    DataPartPtr source_part = getActiveContainingPart(source_part_name);
    if (!source_part)
    {
        LOG_DEBUG(log, "Source part " + source_part_name + " for " << entry.new_part_name << " is not ready; will try to fetch it instead");
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

    if (entry.create_time + settings.prefer_fetch_merged_part_time_threshold.totalSeconds() <= time(nullptr)
        && estimated_space_for_result >= settings.prefer_fetch_merged_part_size_threshold)
    {
        /// If entry is old enough, and have enough size, and some replica has the desired part,
        /// then prefer fetching from replica.
        String replica = findReplicaHavingPart(entry.new_part_name, true);    /// NOTE excessive ZK requests for same data later, may remove.
        if (!replica.empty())
        {
            LOG_DEBUG(log, "Prefer to fetch " << entry.new_part_name << " from replica " << replica);
            return false;
        }
    }

    MergeTreePartInfo new_part_info = MergeTreePartInfo::fromPartName(
        entry.new_part_name, format_version);
    MutationCommands commands = queue.getMutationCommands(source_part, new_part_info.mutation);

    /// Can throw an exception.
    DiskSpaceMonitor::ReservationPtr reserved_space = DiskSpaceMonitor::reserve(full_path, estimated_space_for_result);

    auto table_lock = lockStructureForShare(false, RWLockImpl::NO_QUERY);

    MutableDataPartPtr new_part;
    Transaction transaction(*this);

    FutureMergedMutatedPart future_mutated_part;
    future_mutated_part.parts.push_back(source_part);
    future_mutated_part.part_info = new_part_info;
    future_mutated_part.name = entry.new_part_name;

    MergeList::EntryPtr merge_entry = global_context.getMergeList().insert(
        database_name, table_name, future_mutated_part);

    Stopwatch stopwatch;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        writePartLog(
            PartLogElement::MUTATE_PART, execution_status, stopwatch.elapsed(),
            entry.new_part_name, new_part, future_mutated_part.parts, merge_entry.get());
    };

    try
    {
        new_part = merger_mutator.mutatePartToTemporaryPart(future_mutated_part, commands, *merge_entry, global_context);
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

                LOG_ERROR(log, getCurrentExceptionMessage(false) << ". "
                    "Data after mutation is not byte-identical to data on another replicas. "
                    "We will download merged part from replica to force byte-identical result.");

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
    String replica = findReplicaHavingCoveringPart(entry, true);

    static std::atomic_uint total_fetches {0};
    if (settings.replicated_max_parallel_fetches && total_fetches >= settings.replicated_max_parallel_fetches)
    {
        throw Exception("Too many total fetches from replicas, maximum: " + settings.replicated_max_parallel_fetches.toString(),
            ErrorCodes::TOO_MANY_FETCHES);
    }

    ++total_fetches;
    SCOPE_EXIT({--total_fetches;});

    if (settings.replicated_max_parallel_fetches_for_table && current_table_fetches >= settings.replicated_max_parallel_fetches_for_table)
    {
        throw Exception("Too many fetches from replicas for table, maximum: " + settings.replicated_max_parallel_fetches_for_table.toString(),
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

                LOG_DEBUG(log, "No active replica has part " << entry.new_part_name << " which needs to be written with quorum."
                    " Will try to mark that quorum as failed.");

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

                for (size_t i = 0, size = replicas.size(); i < size; ++i)
                {
                    Coordination::Stat stat;
                    String path = zookeeper_path + "/replicas/" + replicas[i] + "/host";
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

                        if (code == Coordination::ZOK)
                        {
                            LOG_DEBUG(log, "Marked quorum for part " << entry.new_part_name << " as failed.");
                            queue.removeFromVirtualParts(part_info);
                            return true;
                        }
                        else if (code == Coordination::ZBADVERSION || code == Coordination::ZNONODE || code == Coordination::ZNODEEXISTS)
                        {
                            LOG_DEBUG(log, "State was changed or isn't expected when trying to mark quorum for part "
                                << entry.new_part_name << " as failed. Code: " << zkutil::ZooKeeper::error2string(code));
                        }
                        else
                            throw Coordination::Exception(code);
                    }
                    else
                    {
                        LOG_WARNING(log, "No active replica has part " << entry.new_part_name
                            << ", but that part needs quorum and /quorum/status contains entry about another part " << quorum_entry.part_name
                            << ". It means that part was successfully written to " << entry.quorum
                            << " replicas, but then all of them goes offline."
                            << " Or it is a bug.");
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
            if (!fetchPart(entry.actual_new_part_name, zookeeper_path + "/replicas/" + replica, false, entry.quorum))
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
        /** If you can not download the part you need for some merge, it's better not to try to get other parts for this merge,
          * but try to get already merged part. To do this, move the action to get the remaining parts
          * for this merge at the end of the queue.
          */
        try
        {
            auto parts_for_merge = queue.moveSiblingPartsForMergeToEndOfQueue(entry.new_part_name);

            if (!parts_for_merge.empty() && replica.empty())
            {
                LOG_INFO(log, "No active replica has part " << entry.new_part_name << ". Will fetch merged part instead.");
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

    LOG_DEBUG(log, (entry.detach ? "Detaching" : "Removing") << " parts.");

    /// Delete the parts contained in the range to be deleted.
    /// It's important that no old parts remain (after the merge), because otherwise,
    ///  after adding a new replica, this new replica downloads them, but does not delete them.
    /// And, if you do not, the parts will come to life after the server is restarted.
    /// Therefore, we use all data parts.

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
            LOG_INFO(log, "Detaching " << part->relative_path);
            part->makeCloneInDetached("");
        }
    }

    /// Forcibly remove parts from ZooKeeper
    tryRemovePartsFromZooKeeperWithRetries(parts_to_remove);

    LOG_INFO(log, (entry.detach ? "Detached " : "Removed ") << parts_to_remove.size() << " parts inside " << entry.new_part_name << ".");

    /// We want to remove dropped parts from disk as soon as possible
    /// To be removed a partition should have zero refcount, therefore call the cleanup thread at exit
    parts_to_remove.clear();
    cleanup_thread.wakeup();
}


void StorageReplicatedMergeTree::executeClearColumnInPartition(const LogEntry & entry)
{
    LOG_INFO(log, "Clear column " << entry.column_name << " in parts inside " << entry.new_part_name << " range");

    auto entry_part_info = MergeTreePartInfo::fromPartName(entry.new_part_name, format_version);

    /// We don't change table structure, only data in some parts
    /// To disable reading from these parts, we will sequentially acquire write lock for each part inside alterDataPart()
    /// If we will lock the whole table here, a deadlock can occur. For example, if use use Buffer table (CLICKHOUSE-3238)
    auto lock_read_structure = lockStructureForShare(false, RWLockImpl::NO_QUERY);

    auto zookeeper = getZooKeeper();

    AlterCommand alter_command;
    alter_command.type = AlterCommand::DROP_COLUMN;
    alter_command.column_name = entry.column_name;

    auto new_columns = getColumns();
    auto new_indices = getIndices();
    ASTPtr ignored_order_by_ast;
    ASTPtr ignored_primary_key_ast;
    ASTPtr ignored_ttl_table_ast;
    alter_command.apply(new_columns, new_indices, ignored_order_by_ast, ignored_primary_key_ast, ignored_ttl_table_ast);

    size_t modified_parts = 0;
    auto parts = getDataParts();
    auto columns_for_parts = new_columns.getAllPhysical();

    /// Check there are no merges in range again
    /// TODO: Currently, there are no guarantees that a merge covering entry_part_info will happen during the execution.
    /// To solve this problem we could add read/write flags for each part in future_parts
    ///  and make more sophisticated checks for merges in shouldExecuteLogEntry().
    /// But this feature will be useless when the mutation feature is implemented.
    queue.checkThereAreNoConflictsInRange(entry_part_info, entry);

    for (const auto & part : parts)
    {
        if (!entry_part_info.contains(part->info))
            continue;

        LOG_DEBUG(log, "Clearing column " << entry.column_name << " in part " << part->name);

        MergeTreeData::AlterDataPartTransactionPtr transaction(new MergeTreeData::AlterDataPartTransaction(part));
        alterDataPart(columns_for_parts, new_indices.indices, false, transaction);
        if (!transaction->isValid())
            continue;

        updatePartHeaderInZooKeeperAndCommit(zookeeper, *transaction);

        ++modified_parts;
    }

    LOG_DEBUG(log, "Cleared column " << entry.column_name << " in " << modified_parts << " parts");

    /// Recalculate columns size (not only for the modified column)
    recalculateColumnSizes();
}


bool StorageReplicatedMergeTree::executeReplaceRange(const LogEntry & entry)
{
    Stopwatch watch;
    auto & entry_replace = *entry.replace_range_entry;

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

    auto table_lock_holder_dst_table = lockStructureForShare(false, RWLockImpl::NO_QUERY);

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
    TableStructureReadLockHolder table_lock_holder_src_table;
    String source_table_name = entry_replace.from_database + "." + entry_replace.from_table;

    auto clone_data_parts_from_source_table = [&] () -> size_t
    {
        source_table = global_context.tryGetTable(entry_replace.from_database, entry_replace.from_table);
        if (!source_table)
        {
            LOG_DEBUG(log, "Can't use " << source_table_name << " as source table for REPLACE PARTITION command. It does not exist.");
            return 0;
        }

        MergeTreeData * src_data = nullptr;
        try
        {
            src_data = &checkStructureAndGetMergeTreeData(source_table);
        }
        catch (Exception &)
        {
            LOG_INFO(log, "Can't use " << source_table_name << " as source table for REPLACE PARTITION command. Will fetch all parts."
                           << " Reason: " << getCurrentExceptionMessage(false));
            return 0;
        }

        table_lock_holder_src_table = source_table->lockStructureForShare(false, RWLockImpl::NO_QUERY);

        DataPartStates valid_states{MergeTreeDataPartState::PreCommitted, MergeTreeDataPartState::Committed,
                                                   MergeTreeDataPartState::Outdated};

        size_t num_clonable_parts = 0;
        for (PartDescriptionPtr & part_desc : parts_to_add)
        {
            auto src_part = src_data->getPartIfExists(part_desc->src_part_info, valid_states);
            if (!src_part)
            {
                LOG_DEBUG(log, "There is no part " << part_desc->src_part_name << " in " << source_table_name);
                continue;
            }

            String checksum_hex;
            {
                std::shared_lock<std::shared_mutex> part_lock(src_part->columns_lock);
                checksum_hex = src_part->checksums.getTotalChecksumHex();
            }

            if (checksum_hex != part_desc->checksum_hex)
            {
                LOG_DEBUG(log, "Part " << part_desc->src_part_name << " of " << source_table_name << " has inappropriate checksum");
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
    LOG_DEBUG(log, "Found " << num_clonable_parts << " parts that could be cloned (of " << parts_to_add.size() << " required parts)");

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
            LOG_DEBUG(log, "Part " << part_desc->new_part_name << " is not found on remote replicas");

            /// Fallback to covering part
            replica = findReplicaHavingCoveringPart(part_desc->new_part_name, true, found_part_name);

            if (replica.empty())
            {
                /// It is not fail, since adjacent parts could cover current part
                LOG_DEBUG(log, "Parts covering " << part_desc->new_part_name << " are not found on remote replicas");
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
            std::shared_lock<std::shared_mutex> part_lock(part_desc->src_table_part->columns_lock);

            if (part_desc->checksum_hex != part_desc->src_table_part->checksums.getTotalChecksumHex())
                throw Exception("Checksums of " + part_desc->src_table_part->name + " is suddenly changed", ErrorCodes::UNFINISHED);

            part_desc->res_part = cloneAndLoadDataPart(
                part_desc->src_table_part, TMP_PREFIX + "clone_", part_desc->new_part_info);
        }
        else if (!part_desc->replica.empty())
        {
            String source_replica_path = zookeeper_path + "/replicas/" + part_desc->replica;
            ReplicatedMergeTreeAddress address(getZooKeeper()->get(source_replica_path + "/host"));
            auto timeouts = ConnectionTimeouts::getHTTPTimeouts(global_context);
            auto [user, password] = global_context.getInterserverCredentials();
            String interserver_scheme = global_context.getInterserverScheme();

            if (interserver_scheme != address.scheme)
                throw Exception("Interserver schemes are different '" + interserver_scheme + "' != '" + address.scheme + "', can't fetch part from " + address.host, ErrorCodes::LOGICAL_ERROR);

            part_desc->res_part = fetcher.fetchPart(part_desc->found_new_part_name, source_replica_path,
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
    LOG_INFO(log, "Will mimic " << source_replica);

    String source_path = zookeeper_path + "/replicas/" + source_replica;

    /** TODO: it will be deleted! (It is only to support old version of CH server).
      * In current code, the replica is created in single transaction.
      * If the reference/master replica is not yet fully created, let's wait.
      */
    while (!zookeeper->exists(source_path + "/columns"))
    {
        LOG_INFO(log, "Waiting for replica " << source_path << " to be fully created");

        zkutil::EventPtr event = std::make_shared<Poco::Event>();
        if (zookeeper->exists(source_path + "/columns", nullptr, event))
        {
            LOG_WARNING(log, "Oops, a watch has leaked");
            break;
        }

        event->wait();
    }

    /// The order of the following three actions is important. Entries in the log can be duplicated, but they can not be lost.

    String raw_log_pointer = zookeeper->get(source_path + "/log_pointer");

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
    else    /// The replica we clone should not suddenly become lost.
        ops.push_back(zkutil::makeCheckRequest(source_path + "/is_lost", source_is_lost_stat.version));

    Coordination::Responses resp;

    auto error = zookeeper->tryMulti(ops, resp);
    if (error == Coordination::Error::ZBADVERSION)
        throw Exception("Can not clone replica, because the " + source_replica + " became lost", ErrorCodes::REPLICA_STATUS_CHANGED);
    else if (error == Coordination::Error::ZNODEEXISTS)
        throw Exception("Can not clone replica, because the " + source_replica + " updated to new ClickHouse version", ErrorCodes::REPLICA_STATUS_CHANGED);
    else
        zkutil::KeeperMultiException::check(error, ops, resp);

    /// Let's remember the queue of the reference/master replica.
    Strings source_queue_names = zookeeper->getChildren(source_path + "/queue");
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
    Strings parts = zookeeper->getChildren(source_path + "/parts");
    ActiveDataPartSet active_parts_set(format_version, parts);

    Strings active_parts = active_parts_set.getParts();
    for (const String & name : active_parts)
    {
        LogEntry log_entry;
        log_entry.type = LogEntry::GET_PART;
        log_entry.source_replica = "";
        log_entry.new_part_name = name;
        log_entry.create_time = tryGetPartCreateTime(zookeeper, source_path, name);

        zookeeper->create(replica_path + "/queue/queue-", log_entry.toString(), zkutil::CreateMode::PersistentSequential);
    }

    LOG_DEBUG(log, "Queued " << active_parts.size() << " parts to be fetched");

    /// Add content of the reference/master replica queue to the queue.
    for (const String & entry : source_queue)
    {
        zookeeper->create(replica_path + "/queue/queue-", entry, zkutil::CreateMode::PersistentSequential);
    }

    LOG_DEBUG(log, "Copied " << source_queue.size() << " queue entries");
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

        if (e.code == Coordination::ZSESSIONEXPIRED)
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

        if (e.code == Coordination::ZSESSIONEXPIRED)
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


void StorageReplicatedMergeTree::mergeSelectingTask()
{
    if (!is_leader)
        return;

    const bool deduplicate = false; /// TODO: read deduplicate option from table config

    bool success = false;

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
        size_t merges_and_mutations_queued = queue.countMergesAndPartMutations();
        if (merges_and_mutations_queued >= settings.max_replicated_merges_in_queue)
        {
            LOG_TRACE(log, "Number of queued merges and part mutations (" << merges_and_mutations_queued
                << ") is greater than max_replicated_merges_in_queue ("
                << settings.max_replicated_merges_in_queue << "), so won't select new parts to merge or mutate.");
        }
        else
        {
            UInt64 max_source_parts_size = merger_mutator.getMaxSourcePartsSize(
                settings.max_replicated_merges_in_queue, merges_and_mutations_queued);

            if (max_source_parts_size > 0)
            {
                FutureMergedMutatedPart future_merged_part;
                if (merger_mutator.selectPartsToMerge(future_merged_part, false, max_source_parts_size, merge_pred))
                {
                    success = createLogEntryToMergeParts(zookeeper, future_merged_part.parts, future_merged_part.name, deduplicate);
                }
                else if (queue.countMutations() > 0)
                {
                    /// Choose a part to mutate.

                    DataPartsVector data_parts = getDataPartsVector();
                    for (const auto & part : data_parts)
                    {
                        if (part->bytes_on_disk > max_source_parts_size)
                            continue;

                        std::optional<Int64> desired_mutation_version = merge_pred.getDesiredMutationVersion(part);
                        if (!desired_mutation_version)
                            continue;

                        if (createLogEntryToMutatePart(*part, *desired_mutation_version))
                        {
                            success = true;
                            break;
                        }
                    }
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

    if (!success)
        merge_selecting_task->scheduleAfter(MERGE_SELECTING_SLEEP_MS);
    else
        merge_selecting_task->schedule();

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
        mutations_finalizing_task->scheduleAfter(MUTATIONS_FINALIZING_SLEEP_MS);
}


bool StorageReplicatedMergeTree::createLogEntryToMergeParts(
    zkutil::ZooKeeperPtr & zookeeper,
    const DataPartsVector & parts,
    const String & merged_name,
    bool deduplicate,
    ReplicatedMergeTreeLogEntryData * out_log_entry)
{
    std::vector<std::future<Coordination::ExistsResponse>> exists_futures;
    exists_futures.reserve(parts.size());
    for (const auto & part : parts)
        exists_futures.emplace_back(zookeeper->asyncExists(replica_path + "/parts/" + part->name));

    bool all_in_zk = true;
    for (size_t i = 0; i < parts.size(); ++i)
    {
        /// If there is no information about part in ZK, we will not merge it.
        if (exists_futures[i].get().error == Coordination::ZNONODE)
        {
            all_in_zk = false;

            const auto & part = parts[i];
            if (part->modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < time(nullptr))
            {
                LOG_WARNING(log, "Part " << part->name << " (that was selected for merge)"
                    << " with age " << (time(nullptr) - part->modification_time)
                    << " seconds exists locally but not in ZooKeeper."
                    << " Won't do merge with that part and will check it.");
                enqueuePartForCheck(part->name);
            }
        }
    }

    if (!all_in_zk)
        return false;

    ReplicatedMergeTreeLogEntryData entry;
    entry.type = LogEntry::MERGE_PARTS;
    entry.source_replica = replica_name;
    entry.new_part_name = merged_name;
    entry.deduplicate = deduplicate;
    entry.create_time = time(nullptr);

    for (const auto & part : parts)
        entry.source_parts.push_back(part->name);

    String path_created = zookeeper->create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);
    entry.znode_name = path_created.substr(path_created.find_last_of('/') + 1);

    if (out_log_entry)
        *out_log_entry = entry;

    return true;
}


bool StorageReplicatedMergeTree::createLogEntryToMutatePart(const MergeTreeDataPart & part, Int64 mutation_version)
{
    auto zookeeper = getZooKeeper();

    /// If there is no information about part in ZK, we will not mutate it.
    if (!zookeeper->exists(replica_path + "/parts/" + part.name))
    {
        if (part.modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < time(nullptr))
        {
            LOG_WARNING(log, "Part " << part.name << " (that was selected for mutation)"
                << " with age " << (time(nullptr) - part.modification_time)
                << " seconds exists locally but not in ZooKeeper."
                << " Won't mutate that part and will check it.");
            enqueuePartForCheck(part.name);
        }

        return false;
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

    zookeeper->create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);
    return true;
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

    String path_created = dynamic_cast<const Coordination::CreateResponse &>(*results[0]).path_created;
    log_entry->znode_name = path_created.substr(path_created.find_last_of('/') + 1);
    queue.insert(zookeeper, log_entry);
}


void StorageReplicatedMergeTree::enterLeaderElection()
{
    auto callback = [this]()
    {
        CurrentMetrics::add(CurrentMetrics::LeaderReplica);
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
        CurrentMetrics::sub(CurrentMetrics::LeaderReplica);
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
    std::shuffle(replicas.begin(), replicas.end(), rng);

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
    std::shuffle(replicas.begin(), replicas.end(), rng);

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
                    LOG_INFO(log, "Will not fetch part " << largest_part_found << " covering " << entry.new_part_name << ". " << reject_reason);
                    return {};
                }
            }
            else
            {
                entry.actual_new_part_name = entry.new_part_name;
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
    std::shuffle(replicas.begin(), replicas.end(), rng);

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
            parts_with_quorum.added_parts[part_info.partition_id] = part_name;

            String new_added_parts = parts_with_quorum.toString();

            ops.emplace_back(zkutil::makeRemoveRequest(quorum_status_path, stat.version));
            ops.emplace_back(zkutil::makeSetRequest(quorum_last_part_path, new_added_parts, added_parts_stat.version));
            auto code = zookeeper->tryMulti(ops, responses);

            if (code == Coordination::ZOK)
            {
                break;
            }
            else if (code == Coordination::ZNONODE)
            {
                /// The quorum has already been achieved.
                break;
            }
            else if (code == Coordination::ZBADVERSION)
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

            if (code == Coordination::ZOK)
            {
                break;
            }
            else if (code == Coordination::ZNONODE)
            {
                /// The quorum has already been achieved.
                break;
            }
            else if (code == Coordination::ZBADVERSION)
            {
                /// Node was updated meanwhile. We must re-read it and repeat all the actions.
                continue;
            }
            else
                throw Coordination::Exception(code, quorum_status_path);
        }
    }
}


bool StorageReplicatedMergeTree::fetchPart(const String & part_name, const String & source_replica_path, bool to_detached, size_t quorum)
{
    const auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

    if (auto part = getPartIfExists(part_info, {MergeTreeDataPart::State::Outdated, MergeTreeDataPart::State::Deleting}))
    {
        LOG_DEBUG(log, "Part " << part->name << " should be deleted after previous attempt before fetch");
        /// Force immediate parts cleanup to delete the part that was left from the previous fetch attempt.
        cleanup_thread.wakeup();
        return false;
    }

    {
        std::lock_guard lock(currently_fetching_parts_mutex);
        if (!currently_fetching_parts.insert(part_name).second)
        {
            LOG_DEBUG(log, "Part " << part_name << " is already fetching right now");
            return false;
        }
    }

    SCOPE_EXIT
    ({
        std::lock_guard lock(currently_fetching_parts_mutex);
        currently_fetching_parts.erase(part_name);
    });

    LOG_DEBUG(log, "Fetching part " << part_name << " from " << source_replica_path);

    TableStructureReadLockHolder table_lock_holder;
    if (!to_detached)
        table_lock_holder = lockStructureForShare(true, RWLockImpl::NO_QUERY);

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
                LOG_TRACE(log, "Found local part " << source_part->name << " with the same checksums as " << part_name);
                part_to_clone = source_part;
            }
        }

    }

    std::function<MutableDataPartPtr()> get_part;
    if (part_to_clone)
    {
        get_part = [&, part_to_clone]()
        {
            return cloneAndLoadDataPart(part_to_clone, "tmp_clone_", part_info);
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
                part_name, source_replica_path,
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
                LOG_DEBUG(log, "Part " << replaced_part->name << " is rendered obsolete by fetching part " << part_name);
                ProfileEvents::increment(ProfileEvents::ObsoleteReplicatedParts);
            }

            write_part_log({});
        }
        else
        {
            part->renameTo("detached/" + part_name);
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
        LOG_DEBUG(log, "Cloned part " << part_name << " from " << part_to_clone->name << (to_detached ? " (to 'detached' directory)" : ""));
    else
        LOG_DEBUG(log, "Fetched part " << part_name << " from " << source_replica_path << (to_detached ? " (to 'detached' directory)" : ""));

    return true;
}


void StorageReplicatedMergeTree::startup()
{
    if (is_readonly)
        return;

    if (set_table_structure_at_startup)
        set_table_structure_at_startup();

    queue.initialize(
        zookeeper_path, replica_path,
        database_name + "." + table_name + " (ReplicatedMergeTreeQueue)",
        getDataParts());

    StoragePtr ptr = shared_from_this();
    InterserverIOEndpointPtr data_parts_exchange_endpoint = std::make_shared<DataPartsExchange::Service>(*this, ptr);
    data_parts_exchange_endpoint_holder = std::make_shared<InterserverIOEndpointHolder>(
        data_parts_exchange_endpoint->getId(replica_path), data_parts_exchange_endpoint, global_context.getInterserverIOHandler());

    queue_task_handle = global_context.getBackgroundPool().addTask([this] { return queueTask(); });

    /// In this thread replica will be activated.
    restarting_thread.start();

    /// Wait while restarting_thread initializes LeaderElection (and so on) or makes first attmept to do it
    startup_event.wait();
}


void StorageReplicatedMergeTree::shutdown()
{
    /// Cancel fetches, merges and mutations to force the queue_task to finish ASAP.
    fetcher.blocker.cancelForever();
    merger_mutator.actions_blocker.cancelForever();

    restarting_thread.shutdown();

    if (queue_task_handle)
        global_context.getBackgroundPool().removeTask(queue_task_handle);
    queue_task_handle.reset();

    if (data_parts_exchange_endpoint_holder)
    {
        data_parts_exchange_endpoint_holder->getBlocker().cancelForever();
        data_parts_exchange_endpoint_holder = nullptr;
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


BlockInputStreams StorageReplicatedMergeTree::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned num_streams)
{
    const Settings & settings = context.getSettingsRef();

    /** The `select_sequential_consistency` setting has two meanings:
    * 1. To throw an exception if on a replica there are not all parts which have been written down on quorum of remaining replicas.
    * 2. Do not read parts that have not yet been written to the quorum of the replicas.
    * For this you have to synchronously go to ZooKeeper.
    */
    if (settings.select_sequential_consistency)
    {
        ReplicatedMergeTreeQuorumAddedParts::PartitionIdToMaxBlock max_added_blocks;

        for (const auto & data_part : getDataParts())
        {
            max_added_blocks[data_part->info.partition_id] = std::max(max_added_blocks[data_part->info.partition_id], data_part->info.max_block);
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
                        throw Exception("Replica doesn't have part " + added_part.second + " which was successfully written to quorum of other replicas."
                            " Send query to another replica or disable 'select_sequential_consistency' setting.", ErrorCodes::REPLICA_IS_NOT_IN_QUORUM);

                for (const auto & max_block : part_with_quorum.getMaxInsertedBlocks())
                        max_added_blocks[max_block.first] = max_block.second;
            }
        }

        return reader.read(column_names, query_info, context, max_block_size, num_streams, &max_added_blocks);
    }

    return reader.read(column_names, query_info, context, max_block_size, num_streams);
}


void StorageReplicatedMergeTree::assertNotReadonly() const
{
    if (is_readonly)
        throw Exception("Table is in readonly mode", ErrorCodes::TABLE_IS_READ_ONLY);
}


BlockOutputStreamPtr StorageReplicatedMergeTree::write(const ASTPtr & /*query*/, const Context & context)
{
    assertNotReadonly();

    const Settings & query_settings = context.getSettingsRef();
    bool deduplicate = settings.replicated_deduplication_window != 0 && query_settings.insert_deduplicate;

    return std::make_shared<ReplicatedMergeTreeBlockOutputStream>(*this,
        query_settings.insert_quorum, query_settings.insert_quorum_timeout.totalMilliseconds(), query_settings.max_partitions_per_insert_block, deduplicate);
}


bool StorageReplicatedMergeTree::optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & query_context)
{
    assertNotReadonly();

    if (!is_leader)
    {
        sendRequestToLeaderReplica(query, query_context);
        return true;
    }

    std::vector<ReplicatedMergeTreeLogEntryData> merge_entries;
    {
        /// We must select parts for merge under merge_selecting_mutex because other threads
        /// (merge_selecting_thread or OPTIMIZE queries) could assign new merges.
        std::lock_guard merge_selecting_lock(merge_selecting_mutex);

        auto zookeeper = getZooKeeper();
        ReplicatedMergeTreeMergePredicate can_merge = queue.getMergePredicate(zookeeper);

        auto handle_noop = [&] (const String & message)
        {
            if (query_context.getSettingsRef().optimize_throw_if_noop)
                throw Exception(message, ErrorCodes::CANNOT_ASSIGN_OPTIMIZE);
            return false;
        };

        if (!partition && final)
        {
            DataPartsVector data_parts = getDataPartsVector();
            std::unordered_set<String> partition_ids;

            for (const DataPartPtr & part : data_parts)
                partition_ids.emplace(part->info.partition_id);

            UInt64 disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

            for (const String & partition_id : partition_ids)
            {
                FutureMergedMutatedPart future_merged_part;
                bool selected = merger_mutator.selectAllPartsToMergeWithinPartition(
                    future_merged_part, disk_space, can_merge, partition_id, true, nullptr);
                ReplicatedMergeTreeLogEntryData merge_entry;
                if (selected &&
                    !createLogEntryToMergeParts(zookeeper, future_merged_part.parts, future_merged_part.name, deduplicate, &merge_entry))
                    return handle_noop("Can't create merge queue node in ZooKeeper");
                if (merge_entry.type != ReplicatedMergeTreeLogEntryData::Type::EMPTY)
                    merge_entries.push_back(std::move(merge_entry));
            }
        }
        else
        {
            FutureMergedMutatedPart future_merged_part;
            String disable_reason;
            bool selected = false;
            if (!partition)
            {
                selected = merger_mutator.selectPartsToMerge(
                    future_merged_part, true, settings.max_bytes_to_merge_at_max_space_in_pool, can_merge, &disable_reason);
            }
            else
            {
                UInt64 disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);
                String partition_id = getPartitionIDFromQuery(partition, query_context);
                selected = merger_mutator.selectAllPartsToMergeWithinPartition(
                    future_merged_part, disk_space, can_merge, partition_id, final, &disable_reason);
            }

            if (!selected)
            {
                LOG_INFO(log, "Cannot select parts for optimization" + (disable_reason.empty() ? "" : ": " + disable_reason));
                return handle_noop(disable_reason);
            }

            ReplicatedMergeTreeLogEntryData merge_entry;
            if (!createLogEntryToMergeParts(zookeeper, future_merged_part.parts, future_merged_part.name, deduplicate, &merge_entry))
                return handle_noop("Can't create merge queue node in ZooKeeper");
            if (merge_entry.type != ReplicatedMergeTreeLogEntryData::Type::EMPTY)
                merge_entries.push_back(std::move(merge_entry));
        }
    }

    /// TODO: Bad setting name for such purpose
    if (query_context.getSettingsRef().replication_alter_partitions_sync != 0)
    {
        for (auto & merge_entry : merge_entries)
            waitForAllReplicasToProcessLogEntry(merge_entry);
    }

    return true;
}


void StorageReplicatedMergeTree::alter(
    const AlterCommands & params, const String & /*database_name*/, const String & /*table_name*/,
    const Context & query_context, TableStructureWriteLockHolder & table_lock_holder)
{
    assertNotReadonly();

    LOG_DEBUG(log, "Doing ALTER");

    /// Alter is done by modifying the metadata nodes in ZK that are shared between all replicas
    /// (/columns, /metadata). We set contents of the shared nodes to the new values and wait while
    /// replicas asynchronously apply changes (see ReplicatedMergeTreeAlterThread.cpp) and modify
    /// their respective replica metadata nodes (/replicas/<replica>/columns, /replicas/<replica>/metadata).

    struct ChangedNode
    {
        ChangedNode(const String & table_path_, String name_, String new_value_)
            : table_path(table_path_), name(std::move(name_)), shared_path(table_path + "/" + name)
            , new_value(std::move(new_value_))
        {}

        const String & table_path;
        String name;

        String shared_path;

        String getReplicaPath(const String & replica) const
        {
            return table_path + "/replicas/" + replica + "/" + name;
        }

        String new_value;
        int32_t new_version = -1; /// Initialization is to suppress (useless) false positive warning found by cppcheck.
    };

    std::vector<ChangedNode> changed_nodes;

    {
        /// Just to read current structure. Alter will be done in separate thread.
        auto table_lock = lockStructureForShare(false, query_context.getCurrentQueryId());

        if (is_readonly)
            throw Exception("Can't ALTER readonly table", ErrorCodes::TABLE_IS_READ_ONLY);

        checkAlter(params, query_context);

        ColumnsDescription new_columns = getColumns();
        IndicesDescription new_indices = getIndices();
        ASTPtr new_order_by_ast = order_by_ast;
        ASTPtr new_primary_key_ast = primary_key_ast;
        ASTPtr new_ttl_table_ast = ttl_table_ast;
        params.apply(new_columns, new_indices, new_order_by_ast, new_primary_key_ast, new_ttl_table_ast);

        String new_columns_str = new_columns.toString();
        if (new_columns_str != getColumns().toString())
            changed_nodes.emplace_back(zookeeper_path, "columns", new_columns_str);

        ReplicatedMergeTreeTableMetadata new_metadata(*this);
        if (new_order_by_ast.get() != order_by_ast.get())
            new_metadata.sorting_key = serializeAST(*extractKeyExpressionList(new_order_by_ast));

        if (new_ttl_table_ast.get() != ttl_table_ast.get())
            new_metadata.ttl_table = serializeAST(*new_ttl_table_ast);

        String new_indices_str = new_indices.toString();
        if (new_indices_str != getIndices().toString())
            new_metadata.skip_indices = new_indices_str;

        String new_metadata_str = new_metadata.toString();
        if (new_metadata_str != ReplicatedMergeTreeTableMetadata(*this).toString())
            changed_nodes.emplace_back(zookeeper_path, "metadata", new_metadata_str);

        /// Modify shared metadata nodes in ZooKeeper.
        Coordination::Requests ops;
        for (const auto & node : changed_nodes)
            ops.emplace_back(zkutil::makeSetRequest(node.shared_path, node.new_value, -1));

        Coordination::Responses results = getZooKeeper()->multi(ops);

        for (size_t i = 0; i < changed_nodes.size(); ++i)
            changed_nodes[i].new_version = dynamic_cast<const Coordination::SetResponse &>(*results[i]).stat.version;
    }

    LOG_DEBUG(log, "Updated shared metadata nodes in ZooKeeper. Waiting for replicas to apply changes.");

    table_lock_holder.release();

    /// Wait until all replicas will apply ALTER.

    for (const auto & node : changed_nodes)
    {
        Coordination::Stat stat;
        /// Subscribe to change of shared ZK metadata nodes, to finish waiting if someone will do another ALTER.
        if (!getZooKeeper()->exists(node.shared_path, &stat, alter_query_event))
            throw Exception(node.shared_path + " doesn't exist", ErrorCodes::NOT_FOUND_NODE);

        if (stat.version != node.new_version)
        {
            LOG_WARNING(log, node.shared_path + " changed before this ALTER finished; " +
                "overlapping ALTER-s are fine but use caution with nontransitive changes");
            return;
        }
    }

    Strings replicas = getZooKeeper()->getChildren(zookeeper_path + "/replicas");

    std::set<String> inactive_replicas;
    std::set<String> timed_out_replicas;

    time_t replication_alter_columns_timeout = query_context.getSettingsRef().replication_alter_columns_timeout;

    for (const String & replica : replicas)
    {
        LOG_DEBUG(log, "Waiting for " << replica << " to apply changes");

        while (!partial_shutdown_called)
        {
            auto zookeeper = getZooKeeper();

            /// Replica could be inactive.
            if (!zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
            {
                LOG_WARNING(log, "Replica " << replica << " is not active during ALTER query."
                    " ALTER will be done asynchronously when replica becomes active.");

                inactive_replicas.emplace(replica);
                break;
            }

            struct ReplicaNode
            {
                explicit ReplicaNode(String path_) : path(std::move(path_)) {}

                String path;
                String value;
                int32_t version = -1;
            };

            std::vector<ReplicaNode> replica_nodes;
            for (const auto & node : changed_nodes)
                replica_nodes.emplace_back(node.getReplicaPath(replica));

            bool replica_was_removed = false;
            for (auto & node : replica_nodes)
            {
                Coordination::Stat stat;

                /// Replica could has been removed.
                if (!zookeeper->tryGet(node.path, node.value, &stat))
                {
                    LOG_WARNING(log, replica << " was removed");
                    replica_was_removed = true;
                    break;
                }

                node.version = stat.version;
            }

            if (replica_was_removed)
                break;

            bool alter_was_applied = true;
            for (size_t i = 0; i < replica_nodes.size(); ++i)
            {
                if (replica_nodes[i].value != changed_nodes[i].new_value)
                {
                    alter_was_applied = false;
                    break;
                }
            }

            /// The ALTER has been successfully applied.
            if (alter_was_applied)
                break;

            for (const auto & node : changed_nodes)
            {
                Coordination::Stat stat;
                if (!zookeeper->exists(node.shared_path, &stat))
                    throw Exception(node.shared_path + " doesn't exist", ErrorCodes::NOT_FOUND_NODE);

                if (stat.version != node.new_version)
                {
                    LOG_WARNING(log, node.shared_path + " changed before this ALTER finished; "
                        "overlapping ALTER-s are fine but use caution with nontransitive changes");
                    return;
                }
            }

            bool replica_nodes_changed_concurrently = false;
            for (const auto & replica_node : replica_nodes)
            {
                Coordination::Stat stat;
                if (!zookeeper->exists(replica_node.path, &stat, alter_query_event))
                {
                    LOG_WARNING(log, replica << " was removed");
                    replica_was_removed = true;
                    break;
                }

                if (stat.version != replica_node.version)
                {
                    replica_nodes_changed_concurrently = true;
                    break;
                }
            }

            if (replica_was_removed)
                break;

            if (replica_nodes_changed_concurrently)
                continue;

            /// Now wait for replica nodes to change.

            if (!replication_alter_columns_timeout)
            {
                alter_query_event->wait();
                /// Everything is fine.
            }
            else if (alter_query_event->tryWait(replication_alter_columns_timeout * 1000))
            {
                /// Everything is fine.
            }
            else
            {
                LOG_WARNING(log, "Timeout when waiting for replica " << replica << " to apply ALTER."
                    " ALTER will be done asynchronously.");

                timed_out_replicas.emplace(replica);
                break;
            }
        }

        if (partial_shutdown_called)
            throw Exception("Alter is not finished because table shutdown was called. Alter will be done after table restart.",
                ErrorCodes::UNFINISHED);

        if (!inactive_replicas.empty() || !timed_out_replicas.empty())
        {
            std::stringstream exception_message;
            exception_message << "Alter is not finished because";

            if (!inactive_replicas.empty())
            {
                exception_message << " some replicas are inactive right now";

                for (auto it = inactive_replicas.begin(); it != inactive_replicas.end(); ++it)
                    exception_message << (it == inactive_replicas.begin() ? ": " : ", ") << *it;
            }

            if (!timed_out_replicas.empty() && !inactive_replicas.empty())
                exception_message << " and";

            if (!timed_out_replicas.empty())
            {
                exception_message << " timeout when waiting for some replicas";

                for (auto it = timed_out_replicas.begin(); it != timed_out_replicas.end(); ++it)
                    exception_message << (it == timed_out_replicas.begin() ? ": " : ", ") << *it;

                exception_message << " (replication_alter_columns_timeout = " << replication_alter_columns_timeout << ")";
            }

            exception_message << ". Alter will be done asynchronously.";

            throw Exception(exception_message.str(), ErrorCodes::UNFINISHED);
        }
    }

    LOG_DEBUG(log, "ALTER finished");
}

void StorageReplicatedMergeTree::alterPartition(const ASTPtr & query, const PartitionCommands & commands, const Context & query_context)
{
    for (const PartitionCommand & command : commands)
    {
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
                checkPartitionCanBeDropped(command.partition);
                dropPartition(query, command.partition, command.detach, query_context);
                break;

            case PartitionCommand::ATTACH_PARTITION:
                attachPartition(command.partition, command.part, query_context);
                break;

            case PartitionCommand::REPLACE_PARTITION:
            {
                checkPartitionCanBeDropped(command.partition);
                String from_database = command.from_database.empty() ? query_context.getCurrentDatabase() : command.from_database;
                auto from_storage = query_context.getTable(from_database, command.from_table);
                replacePartitionFrom(from_storage, command.partition, command.replace, query_context);
            }
            break;

            case PartitionCommand::FETCH_PARTITION:
                fetchPartition(command.partition, command.from_zookeeper_path, query_context);
                break;

            case PartitionCommand::FREEZE_PARTITION:
            {
                auto lock = lockStructureForShare(false, query_context.getCurrentQueryId());
                freezePartition(command.partition, command.with_name, query_context);
            }
            break;

            case PartitionCommand::CLEAR_COLUMN:
                clearColumnInPartition(command.partition, command.column_name, query_context);
                break;

            case PartitionCommand::FREEZE_ALL_PARTITIONS:
            {
                auto lock = lockStructureForShare(false, query_context.getCurrentQueryId());
                freezeAll(command.with_name, query_context);
            }
            break;
        }
    }
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


void StorageReplicatedMergeTree::clearColumnInPartition(
    const ASTPtr & partition, const Field & column_name, const Context & query_context)
{
    assertNotReadonly();

    /// We don't block merges, so anyone can manage this task (not only leader)

    String partition_id = getPartitionIDFromQuery(partition, query_context);
    MergeTreePartInfo drop_range_info;

    if (!getFakePartCoveringAllPartsInPartition(partition_id, drop_range_info))
    {
        LOG_INFO(log, "Will not clear partition " << partition_id << ", it is empty.");
        return;
    }

    /// We allocated new block number for this part, so new merges can't merge clearing parts with new ones

    LogEntry entry;
    entry.type = LogEntry::CLEAR_COLUMN;
    entry.new_part_name = getPartNamePossiblyFake(format_version, drop_range_info);
    entry.column_name = column_name.safeGet<String>();
    entry.create_time = time(nullptr);

    String log_znode_path = getZooKeeper()->create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);
    entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

    /// If necessary, wait until the operation is performed on itself or on all replicas.
    if (query_context.getSettingsRef().replication_alter_partitions_sync != 0)
    {
        if (query_context.getSettingsRef().replication_alter_partitions_sync == 1)
            waitForReplicaToProcessLogEntry(replica_name, entry);
        else
            waitForAllReplicasToProcessLogEntry(entry);
    }
}


void StorageReplicatedMergeTree::dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & query_context)
{
    assertNotReadonly();

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();

    if (!is_leader)
    {
        // TODO: we can manually reconstruct the query from outside the |dropPartition()| and remove the |query| argument from interface.
        //       It's the only place where we need this argument.
        sendRequestToLeaderReplica(query, query_context);
        return;
    }

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
}


void StorageReplicatedMergeTree::truncate(const ASTPtr & query, const Context & query_context)
{
    assertNotReadonly();

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();

    if (!is_leader)
    {
        sendRequestToLeaderReplica(query, query_context);
        return;
    }

    Strings partitions = zookeeper->getChildren(zookeeper_path + "/block_numbers");

    for (String & partition_id : partitions)
    {
        LogEntry entry;

        if (dropPartsInPartition(*zookeeper, partition_id, entry, false))
            waitForAllReplicasToProcessLogEntry(entry);
    }
}


void StorageReplicatedMergeTree::attachPartition(const ASTPtr & partition, bool attach_part, const Context & query_context)
{
    // TODO: should get some locks to prevent race with 'alter … modify column'

    assertNotReadonly();

    String partition_id;

    if (attach_part)
        partition_id = partition->as<ASTLiteral &>().value.safeGet<String>();
    else
        partition_id = getPartitionIDFromQuery(partition, query_context);

    String source_dir = "detached/";

    /// Let's compose a list of parts that should be added.
    Strings parts;
    if (attach_part)
    {
        parts.push_back(partition_id);
    }
    else
    {
        LOG_DEBUG(log, "Looking for parts for partition " << partition_id << " in " << source_dir);
        ActiveDataPartSet active_parts(format_version);

        std::set<String> part_names;
        for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
        {
            String name = it.name();
            MergeTreePartInfo part_info;
            if (!MergeTreePartInfo::tryParsePartName(name, &part_info, format_version))
                continue;
            if (part_info.partition_id != partition_id)
                continue;
            LOG_DEBUG(log, "Found part " << name);
            active_parts.add(name);
            part_names.insert(name);
        }
        LOG_DEBUG(log, active_parts.size() << " of them are active");
        parts = active_parts.getParts();

        /// Inactive parts rename so they can not be attached in case of repeated ATTACH.
        for (const auto & name : part_names)
        {
            String containing_part = active_parts.getContainingPart(name);
            if (!containing_part.empty() && containing_part != name)
                Poco::File(full_path + source_dir + name).renameTo(full_path + source_dir + "inactive_" + name);
        }
    }

    /// Synchronously check that added parts exist and are not broken. We will write checksums.txt if it does not exist.
    LOG_DEBUG(log, "Checking parts");
    std::vector<MutableDataPartPtr> loaded_parts;
    for (const String & part : parts)
    {
        LOG_DEBUG(log, "Checking part " << part);
        loaded_parts.push_back(loadPartAndFixMetadata(source_dir + part));
    }

    ReplicatedMergeTreeBlockOutputStream output(*this, 0, 0, 0, false);   /// TODO Allow to use quorum here.
    for (auto & part : loaded_parts)
    {
        String old_name = part->name;
        output.writeExistingPart(part);
        LOG_DEBUG(log, "Attached part " << old_name << " as " << part->name);
    }
}


void StorageReplicatedMergeTree::checkTableCanBeDropped() const
{
    /// Consider only synchronized data
    const_cast<StorageReplicatedMergeTree &>(*this).recalculateColumnSizes();
    global_context.checkTableCanBeDropped(database_name, table_name, getTotalActiveSizeInBytes());
}


void StorageReplicatedMergeTree::checkPartitionCanBeDropped(const ASTPtr & partition)
{
    const_cast<StorageReplicatedMergeTree &>(*this).recalculateColumnSizes();

    const String partition_id = getPartitionIDFromQuery(partition, global_context);
    auto parts_to_remove = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);

    UInt64 partition_size = 0;

    for (const auto & part : parts_to_remove)
        partition_size += part->bytes_on_disk;

    global_context.checkPartitionCanBeDropped(database_name, table_name, partition_size);
}


void StorageReplicatedMergeTree::drop()
{
    {
        auto zookeeper = tryGetZooKeeper();

        if (is_readonly || !zookeeper)
            throw Exception("Can't drop readonly replicated table (need to drop data in ZooKeeper as well)", ErrorCodes::TABLE_IS_READ_ONLY);

        shutdown();

        if (zookeeper->expired())
            throw Exception("Table was not dropped because ZooKeeper session has expired.", ErrorCodes::TABLE_WAS_NOT_DROPPED);

        LOG_INFO(log, "Removing replica " << replica_path);
        replica_is_active_node = nullptr;
        zookeeper->tryRemoveRecursive(replica_path);

        /// Check that `zookeeper_path` exists: it could have been deleted by another replica after execution of previous line.
        Strings replicas;
        if (zookeeper->tryGetChildren(zookeeper_path + "/replicas", replicas) == Coordination::ZOK && replicas.empty())
        {
            LOG_INFO(log, "Removing table " << zookeeper_path << " (this might take several minutes)");
            zookeeper->tryRemoveRecursive(zookeeper_path);
        }
    }

    dropAllData();
}


void StorageReplicatedMergeTree::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
    std::string new_full_path = new_path_to_db + escapeForFileName(new_table_name) + '/';

    setPath(new_full_path);

    database_name = new_database_name;
    table_name = new_table_name;
    full_path = new_full_path;

    /// Update table name in zookeeper
    auto zookeeper = getZooKeeper();
    zookeeper->set(replica_path + "/host", getReplicatedMergeTreeAddress().toString());

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
        int code = zookeeper->tryMulti(ops, responses);
        if (code && code != Coordination::ZNODEEXISTS)
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
        if (e.code == Coordination::ZNODEEXISTS && e.getPathForFirstFailedOp() == zookeeper_block_id_path)
            return {};

        throw Exception("Cannot allocate block number in ZooKeeper: " + e.displayText(), ErrorCodes::KEEPER_EXCEPTION);
    }
    catch (const Coordination::Exception & e)
    {
        throw Exception("Cannot allocate block number in ZooKeeper: " + e.displayText(), ErrorCodes::KEEPER_EXCEPTION);
    }

    return {std::move(lock)};
}


void StorageReplicatedMergeTree::waitForAllReplicasToProcessLogEntry(const ReplicatedMergeTreeLogEntryData & entry)
{
    LOG_DEBUG(log, "Waiting for all replicas to process " << entry.znode_name);

    Strings replicas = getZooKeeper()->getChildren(zookeeper_path + "/replicas");
    for (const String & replica : replicas)
        waitForReplicaToProcessLogEntry(replica, entry);

    LOG_DEBUG(log, "Finished waiting for all replicas to process " << entry.znode_name);
}


void StorageReplicatedMergeTree::waitForReplicaToProcessLogEntry(const String & replica, const ReplicatedMergeTreeLogEntryData & entry)
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

    if (startsWith(entry.znode_name, "log-"))
    {
        /** In this case, just take the number from the node name `log-xxxxxxxxxx`.
          */

        UInt64 log_index = parse<UInt64>(entry.znode_name.substr(entry.znode_name.size() - 10));
        log_node_name = entry.znode_name;

        LOG_DEBUG(log, "Waiting for " << replica << " to pull " << log_node_name << " to queue");

        /// Let's wait until entry gets into the replica queue.
        while (true)
        {
            zkutil::EventPtr event = std::make_shared<Poco::Event>();

            String log_pointer = getZooKeeper()->get(zookeeper_path + "/replicas/" + replica + "/log_pointer", nullptr, event);
            if (!log_pointer.empty() && parse<UInt64>(log_pointer) > log_index)
                break;

            event->wait();
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
            LOG_DEBUG(log, "Waiting for " << replica << " to pull " << log_node_name << " to queue");

            /// Let's wait until the entry gets into the replica queue.
            while (true)
            {
                zkutil::EventPtr event = std::make_shared<Poco::Event>();

                String log_pointer_new = getZooKeeper()->get(zookeeper_path + "/replicas/" + replica + "/log_pointer", nullptr, event);
                if (!log_pointer_new.empty() && parse<UInt64>(log_pointer_new) > log_index)
                    break;

                event->wait();
            }
        }
    }
    else
        throw Exception("Logical error: unexpected name of log node: " + entry.znode_name, ErrorCodes::LOGICAL_ERROR);

    if (!log_node_name.empty())
        LOG_DEBUG(log, "Looking for node corresponding to " << log_node_name << " in " << replica << " queue");
    else
        LOG_DEBUG(log, "Looking for corresponding node in " << replica << " queue");

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
        LOG_DEBUG(log, "No corresponding node found. Assuming it has been already processed." " Found " << queue_entries.size() << " nodes.");
        return;
    }

    LOG_DEBUG(log, "Waiting for " << queue_entry_to_wait_for << " to disappear from " << replica << " queue");

    /// Third - wait until the entry disappears from the replica queue.
    getZooKeeper()->waitForDisappear(zookeeper_path + "/replicas/" + replica + "/queue/" + queue_entry_to_wait_for);
}


void StorageReplicatedMergeTree::getStatus(Status & res, bool with_zk_fields)
{
    auto zookeeper = tryGetZooKeeper();

    res.is_leader = is_leader;
    res.can_become_leader = settings.replicated_can_become_leader;
    res.is_readonly = is_readonly;
    res.is_session_expired = !zookeeper || zookeeper->expired();

    res.queue = queue.getStatus();
    res.absolute_delay = getAbsoluteDelay(); /// NOTE: may be slightly inconsistent with queue status.

    res.parts_to_check = part_check_thread.size();

    res.zookeeper_path = zookeeper_path;
    res.replica_name = replica_name;
    res.replica_path = replica_path;
    res.columns_version = columns_version;

    if (res.is_session_expired || !with_zk_fields)
    {
        res.log_max_index = 0;
        res.log_pointer = 0;
        res.total_replicas = 0;
        res.active_replicas = 0;
    }
    else
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
}


/// TODO: Probably it is better to have queue in ZK with tasks for leader (like DDL)
void StorageReplicatedMergeTree::sendRequestToLeaderReplica(const ASTPtr & query, const Context & query_context)
{
    auto live_replicas = getZooKeeper()->getChildren(zookeeper_path + "/leader_election");
    if (live_replicas.empty())
        throw Exception("No active replicas", ErrorCodes::NO_ACTIVE_REPLICAS);

    std::sort(live_replicas.begin(), live_replicas.end());
    const auto leader = getZooKeeper()->get(zookeeper_path + "/leader_election/" + live_replicas.front());

    if (leader == replica_name)
        throw Exception("Leader was suddenly changed or logical error.", ErrorCodes::LEADERSHIP_CHANGED);

    /// SECONDARY_QUERY here means, that we received query from DDLWorker
    /// there is no sense to send query to leader, because he will receive it from own DDLWorker
    if (query_context.getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        LOG_DEBUG(log, "Not leader replica received query from DDLWorker, skipping it.");
        return;
    }

    ReplicatedMergeTreeAddress leader_address(getZooKeeper()->get(zookeeper_path + "/replicas/" + leader + "/host"));

    /// TODO: add setters and getters interface for database and table fields of AST
    auto new_query = query->clone();
    if (auto * alter = new_query->as<ASTAlterQuery>())
    {
        alter->database = leader_address.database;
        alter->table = leader_address.table;
    }
    else if (auto * optimize = new_query->as<ASTOptimizeQuery>())
    {
        optimize->database = leader_address.database;
        optimize->table = leader_address.table;
    }
    else if (auto * drop = new_query->as<ASTDropQuery>(); drop->kind == ASTDropQuery::Kind::Truncate)
    {
        drop->database = leader_address.database;
        drop->table    = leader_address.table;
    }
    else
        throw Exception("Can't proxy this query. Unsupported query type", ErrorCodes::NOT_IMPLEMENTED);

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(global_context.getSettingsRef());

    const auto & query_settings = query_context.getSettingsRef();
    const auto & query_client_info = query_context.getClientInfo();
    String user = query_client_info.current_user;
    String password = query_client_info.current_password;

    if (auto address = findClusterAddress(leader_address); address)
    {
        user = address->user;
        password = address->password;
    }

    Connection connection(
        leader_address.host,
        leader_address.queries_port,
        leader_address.database,
        user, password, timeouts, "Follower replica");

    std::stringstream new_query_ss;
    formatAST(*new_query, new_query_ss, false, true);
    RemoteBlockInputStream stream(connection, new_query_ss.str(), {}, global_context, &query_settings);
    NullBlockOutputStream output({});

    copyData(stream, output);
    return;
}


std::optional<Cluster::Address> StorageReplicatedMergeTree::findClusterAddress(const ReplicatedMergeTreeAddress & leader_address) const
{
    for (auto & iter : global_context.getClusters().getContainer())
    {
        const auto & shards = iter.second->getShardsAddresses();

        for (size_t shard_num = 0; shard_num < shards.size(); ++shard_num)
        {
            for (size_t replica_num = 0; replica_num < shards[shard_num].size(); ++replica_num)
            {
                const Cluster::Address & address = shards[shard_num][replica_num];
                /// user is actually specified, not default
                if (address.host_name == leader_address.host && address.port == leader_address.queries_port && address.user_specified)
                    return address;
            }
        }
    }
    return {};
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

    /** Relative delay is the maximum difference of absolute delay from any other replica,
      *  (if this replica lags behind any other live replica, or zero, otherwise).
      * Calculated only if the absolute delay is large enough.
      */

    if (out_absolute_delay < static_cast<time_t>(settings.min_relative_delay_to_yield_leadership))
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


void StorageReplicatedMergeTree::fetchPartition(const ASTPtr & partition, const String & from_, const Context & query_context)
{
    String partition_id = getPartitionIDFromQuery(partition, query_context);

    String from = from_;
    if (from.back() == '/')
        from.resize(from.size() - 1);

    LOG_INFO(log, "Will fetch partition " << partition_id << " from shard " << from_);

    /** Let's check that there is no such partition in the `detached` directory (where we will write the downloaded parts).
      * Unreliable (there is a race condition) - such a partition may appear a little later.
      */
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it{getFullPath() + "detached/"}; dir_it != dir_end; ++dir_it)
    {
        MergeTreePartInfo part_info;
        if (MergeTreePartInfo::tryParsePartName(dir_it.name(), &part_info, format_version)
              && part_info.partition_id == partition_id)
            throw Exception("Detached partition " + partition_id + " already exists.", ErrorCodes::PARTITION_ALREADY_EXISTS);
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

    LOG_INFO(log, "Found " << replicas.size() << " replicas, " << active_replicas.size() << " of them are active."
        << " Selected " << best_replica << " to fetch from.");

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
            LOG_INFO(log, "Some of parts (" << missing_parts.size() << ") are missing. Will try to fetch covering parts.");

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
                    LOG_WARNING(log, "Part " << missing_part << " on replica " << best_replica_path << " has been vanished.");
            }
        }

        LOG_INFO(log, "Parts to fetch: " << parts_to_fetch.size());

        missing_parts.clear();
        for (const String & part : parts_to_fetch)
        {
            try
            {
                fetchPart(part, best_replica_path, true, 0);
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


void StorageReplicatedMergeTree::mutate(const MutationCommands & commands, const Context &)
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
    /// is governed by the same settings. TODO: support a single "merge-mutation" operation when the data
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
        int32_t rc = zookeeper->tryMulti(requests, responses);

        if (rc == Coordination::ZOK)
        {
            const String & path_created =
                dynamic_cast<const Coordination::CreateResponse *>(responses[1].get())->path_created;
            entry.znode_name = path_created.substr(path_created.find_last_of('/') + 1);
            LOG_TRACE(log, "Created mutation with ID " << entry.znode_name);
            break;
        }
        else if (rc == Coordination::ZBADVERSION)
        {
            LOG_TRACE(log, "Version conflict when trying to create a mutation node, retrying...");
            continue;
        }
        else
            throw Coordination::Exception("Unable to create a mutation znode", rc);
    }
}

std::vector<MergeTreeMutationStatus> StorageReplicatedMergeTree::getMutationsStatus() const
{
    return queue.getMutationsStatus();
}

CancellationCode StorageReplicatedMergeTree::killMutation(const String & mutation_id)
{
    assertNotReadonly();

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();

    LOG_TRACE(log, "Killing mutation " << mutation_id);

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
    /// Critical section is not required (since grabOldParts() returns unique part set on each call)

    auto table_lock = lockStructureForShare(false, RWLockImpl::NO_QUERY);
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
        for (auto & part : parts_to_remove)
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

        LOG_DEBUG(log, "Removed " << parts_to_delete_only_from_filesystem.size() << " old duplicate parts");
    }

    /// Delete normal parts from ZooKeeper
    NameSet part_names_to_retry_deletion;
    try
    {
        Strings part_names_to_delete_completely;
        for (const auto & part : parts_to_delete_completely)
            part_names_to_delete_completely.emplace_back(part->name);

        LOG_DEBUG(log, "Removing " << parts_to_delete_completely.size() << " old parts from ZooKeeper");
        removePartsFromZooKeeper(zookeeper, part_names_to_delete_completely, &part_names_to_retry_deletion);
    }
    catch (...)
    {
        LOG_ERROR(log, "There is a problem with deleting parts from ZooKeeper: " << getCurrentExceptionMessage(true));
    }

    /// Part names that were reliably deleted from ZooKeeper should be deleted from filesystem
    auto num_reliably_deleted_parts = parts_to_delete_completely.size() - part_names_to_retry_deletion.size();
    LOG_DEBUG(log, "Removed " << num_reliably_deleted_parts << " old parts from ZooKeeper. Removing them from filesystem.");

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
        LOG_DEBUG(log, "Will retry deletion of " << parts_to_retry_deletion.size() << " parts in the next time");
    }

    /// Remove parts from filesystem and finally from data_parts
    if (!parts_to_remove_from_filesystem.empty())
    {
        remove_parts_from_filesystem(parts_to_remove_from_filesystem);
        removePartsFinally(parts_to_remove_from_filesystem);

        LOG_DEBUG(log, "Removed " << parts_to_remove_from_filesystem.size() << " old parts");
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
                if (!exists_resp.error)
                {
                    Coordination::Requests ops;
                    removePartFromZooKeeper(part_names[i], ops, exists_resp.stat.numChildren > 0);
                    remove_futures.emplace_back(zookeeper->tryAsyncMulti(ops));
                }
            }

            for (auto & future : remove_futures)
            {
                auto response = future.get();

                if (response.error == 0 || response.error == Coordination::ZNONODE)
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
    exists_futures.reserve(part_names.size());
    for (const String & part_name : part_names)
    {
        String part_path = replica_path + "/parts/" + part_name;
        exists_futures.emplace_back(zookeeper->asyncExists(part_path));
    }

    std::vector<std::future<Coordination::MultiResponse>> remove_futures;
    remove_futures.reserve(part_names.size());
    try
    {
        for (size_t i = 0; i < part_names.size(); ++i)
        {
            Coordination::ExistsResponse exists_resp = exists_futures[i].get();
            if (!exists_resp.error)
            {
                Coordination::Requests ops;
                removePartFromZooKeeper(part_names[i], ops, exists_resp.stat.numChildren > 0);
                remove_futures.emplace_back(zookeeper->tryAsyncMulti(ops));
            }
            else
            {
                LOG_DEBUG(log,
                    "There is no part " << part_names[i] << " in ZooKeeper, it was only in filesystem");
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
        if (response.error == Coordination::ZOK)
            continue;
        else if (response.error == Coordination::ZNONODE)
        {
            LOG_DEBUG(log,
                "There is no part " << part_names[i] << " in ZooKeeper, it was only in filesystem");
            continue;
        }
        else if (Coordination::isHardwareError(response.error))
        {
            if (parts_should_be_retried)
                parts_should_be_retried->insert(part_names[i]);
            continue;
        }
        else
            LOG_WARNING(log, "Cannot remove part " << part_names[i] << " from ZooKeeper: "
                << zkutil::ZooKeeper::error2string(response.error));
    }
}


void StorageReplicatedMergeTree::clearBlocksInPartition(
    zkutil::ZooKeeper & zookeeper, const String & partition_id, Int64 min_block_num, Int64 max_block_num)
{
    Strings blocks;
    if (zookeeper.tryGetChildren(zookeeper_path + "/blocks", blocks))
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

        if (result.error == Coordination::ZNONODE)
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
        int32_t rc = pair.second.get().error;
        if (rc == Coordination::ZNOTEMPTY)
        {
             /// Can happen if there are leftover block nodes with children created by previous server versions.
            zookeeper.removeRecursive(path);
        }
        else if (rc)
            LOG_WARNING(log,
                "Error while deleting ZooKeeper path `" << path << "`: " + zkutil::ZooKeeper::error2string(rc) << ", ignoring.");
    }

    LOG_TRACE(log, "Deleted " << to_delete_futures.size() << " deduplication block IDs in partition ID " << partition_id);
}

void StorageReplicatedMergeTree::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace,
                                                      const Context & context)
{
    auto lock1 = lockStructureForShare(false, context.getCurrentQueryId());
    auto lock2 = source_table->lockStructureForShare(false, context.getCurrentQueryId());

    Stopwatch watch;
    MergeTreeData & src_data = checkStructureAndGetMergeTreeData(source_table);
    String partition_id = getPartitionIDFromQuery(partition, context);

    DataPartsVector src_all_parts = src_data.getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    DataPartsVector src_parts;
    MutableDataPartsVector dst_parts;
    Strings block_id_paths;
    Strings part_checksums;
    std::vector<EphemeralLockInZooKeeper> ephemeral_locks;

    LOG_DEBUG(log, "Cloning " << src_all_parts.size() << " parts");

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
            queue.disableMergesInRange(drop_range_fake_part_name);
        }
    }

    for (size_t i = 0; i < src_all_parts.size(); ++i)
    {
        /// We also make some kind of deduplication to avoid duplicated parts in case of ATTACH PARTITION
        /// Assume that merges in the partition are quite rare
        /// Save deduplication block ids with special prefix replace_partition

        auto & src_part = src_all_parts[i];
        String hash_hex = src_part->checksums.getTotalChecksumHex();
        String block_id_path = replace ? "" : (zookeeper_path + "/blocks/" + partition_id + "_replace_from_" + hash_hex);

        auto lock = allocateBlockNumber(partition_id, zookeeper, block_id_path);
        if (!lock)
        {
            LOG_INFO(log, "Part " << src_part->name << " (hash " << hash_hex << ") has been already attached");
            continue;
        }

        UInt64 index = lock->getNumber();
        MergeTreePartInfo dst_part_info(partition_id, index, index, src_part->info.level);
        auto dst_part = cloneAndLoadDataPart(src_part, TMP_PREFIX, dst_part_info);

        src_parts.emplace_back(src_part);
        dst_parts.emplace_back(dst_part);
        ephemeral_locks.emplace_back(std::move(*lock));
        block_id_paths.emplace_back(block_id_path);
        part_checksums.emplace_back(hash_hex);
    }

    ReplicatedMergeTreeLogEntryData entry;
    {
        entry.type = ReplicatedMergeTreeLogEntryData::REPLACE_RANGE;
        entry.source_replica = replica_name;
        entry.create_time = time(nullptr);
        entry.replace_range_entry = std::make_shared<ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry>();

        auto & entry_replace = *entry.replace_range_entry;
        entry_replace.drop_range_part_name = drop_range_fake_part_name;
        entry_replace.from_database = src_data.database_name;
        entry_replace.from_table = src_data.table_name;
        for (const auto & part : src_parts)
            entry_replace.src_part_names.emplace_back(part->name);
        for (const auto & part : dst_parts)
            entry_replace.new_part_names.emplace_back(part->name);
        for (const String & checksum : part_checksums)
            entry_replace.part_names_checksums.emplace_back(checksum);
        entry_replace.columns_version = columns_version;
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
        waitForAllReplicasToProcessLogEntry(entry);
}

void StorageReplicatedMergeTree::getCommitPartOps(
    Coordination::Requests & ops,
    MutableDataPartPtr & part,
    const String & block_id_path) const
{
    const String & part_name = part->name;

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

    ops.emplace_back(zkutil::makeCheckRequest(
        zookeeper_path + "/columns",
        columns_version));

    if (settings.use_minimalistic_part_header_in_zookeeper)
    {
        ops.emplace_back(zkutil::makeCreateRequest(
            replica_path + "/parts/" + part->name,
            ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(part->columns, part->checksums).toString(),
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
            part->columns.toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            replica_path + "/parts/" + part->name + "/checksums",
            getChecksumsForZooKeeper(part->checksums),
            zkutil::CreateMode::Persistent));
    }
}

void StorageReplicatedMergeTree::updatePartHeaderInZooKeeperAndCommit(
    const zkutil::ZooKeeperPtr & zookeeper,
    AlterDataPartTransaction & transaction)
{
    String part_path = replica_path + "/parts/" + transaction.getPartName();

    bool need_delete_columns_and_checksums_nodes = false;
    try
    {
        if (settings.use_minimalistic_part_header_in_zookeeper)
        {
            auto part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(
                transaction.getNewColumns(), transaction.getNewChecksums());
            Coordination::Stat stat;
            zookeeper->set(part_path, part_header.toString(), -1, &stat);

            need_delete_columns_and_checksums_nodes = stat.numChildren > 0;
        }
        else
        {
            Coordination::Requests ops;
            ops.emplace_back(zkutil::makeSetRequest(
                    part_path, String(), -1));
            ops.emplace_back(zkutil::makeSetRequest(
                    part_path + "/columns", transaction.getNewColumns().toString(), -1));
            ops.emplace_back(zkutil::makeSetRequest(
                    part_path + "/checksums", getChecksumsForZooKeeper(transaction.getNewChecksums()), -1));
            zookeeper->multi(ops);
        }
    }
    catch (const Coordination::Exception & e)
    {
        /// The part does not exist in ZK. We will add to queue for verification - maybe the part is superfluous, and it must be removed locally.
        if (e.code == Coordination::ZNONODE)
            enqueuePartForCheck(transaction.getPartName());

        throw;
    }

    /// Apply file changes.
    transaction.commit();

    /// Legacy <part_path>/columns and <part_path>/checksums znodes are not needed anymore and can be deleted.
    if (need_delete_columns_and_checksums_nodes)
    {
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeRemoveRequest(part_path + "/columns", -1));
        ops.emplace_back(zkutil::makeRemoveRequest(part_path + "/checksums", -1));
        zookeeper->multi(ops);
    }
}

ReplicatedMergeTreeAddress StorageReplicatedMergeTree::getReplicatedMergeTreeAddress() const
{
    auto host_port = global_context.getInterserverIOAddress();

    ReplicatedMergeTreeAddress res;
    res.host = host_port.first;
    res.replication_port = host_port.second;
    res.queries_port = global_context.getTCPPort();
    res.database = database_name;
    res.table = table_name;
    res.scheme = global_context.getInterserverScheme();
    return res;
}

ActionLock StorageReplicatedMergeTree::getActionLock(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return merger_mutator.actions_blocker.cancel();

    if (action_type == ActionLocks::PartsFetch)
        return fetcher.blocker.cancel();

    if (action_type == ActionLocks::PartsSend)
        return data_parts_exchange_endpoint_holder ? data_parts_exchange_endpoint_holder->getBlocker().cancel() : ActionLock();

    if (action_type == ActionLocks::ReplicationQueue)
        return queue.actions_blocker.cancel();

    return {};
}


bool StorageReplicatedMergeTree::waitForShrinkingQueueSize(size_t queue_size, UInt64 max_wait_milliseconds)
{
    /// Let's fetch new log entries firstly
    queue.pullLogsToQueue(getZooKeeper());

    Stopwatch watch;
    Poco::Event event;
    std::atomic<bool> cond_reached{false};

    auto callback = [&event, &cond_reached, queue_size] (size_t new_queue_size)
    {
        if (new_queue_size <= queue_size)
            cond_reached.store(true, std::memory_order_relaxed);

        event.set();
    };

    auto handler = queue.addSubscriber(std::move(callback));

    while (true)
    {
        event.tryWait(50);

        if (max_wait_milliseconds && watch.elapsedMilliseconds() > max_wait_milliseconds)
            break;

        if (cond_reached)
            break;

        if (partial_shutdown_called)
            throw Exception("Shutdown is called for table", ErrorCodes::ABORTED);
    }

    return cond_reached.load(std::memory_order_relaxed);
}


bool StorageReplicatedMergeTree::dropPartsInPartition(
    zkutil::ZooKeeper & zookeeper, String & partition_id, StorageReplicatedMergeTree::LogEntry & entry, bool detach)
{
    MergeTreePartInfo drop_range_info;
    if (!getFakePartCoveringAllPartsInPartition(partition_id, drop_range_info))
    {
        LOG_INFO(log, "Will not drop partition " << partition_id << ", it is empty.");
        return false;
    }

    clearBlocksInPartition(zookeeper, partition_id, drop_range_info.min_block, drop_range_info.max_block);

    /** Forbid to choose the parts to be deleted for merging.
      * Invariant: after the `DROP_RANGE` entry appears in the log, merge of deleted parts will not appear in the log.
      */
    String drop_range_fake_part_name = getPartNamePossiblyFake(format_version, drop_range_info);
    {
        std::lock_guard merge_selecting_lock(merge_selecting_mutex);
        queue.disableMergesInRange(drop_range_fake_part_name);
    }

    LOG_DEBUG(log, "Disabled merges covered by range " << drop_range_fake_part_name);

    /// Finally, having achieved the necessary invariants, you can put an entry in the log.
    entry.type = LogEntry::DROP_RANGE;
    entry.source_replica = replica_name;
    entry.new_part_name = drop_range_fake_part_name;
    entry.detach = detach;
    entry.create_time = time(nullptr);

    String log_znode_path = zookeeper.create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);
    entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

    return true;
}

}
