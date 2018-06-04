#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/KeeperException.h>

#include <Common/FieldVisitors.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>

#include <Databases/IDatabase.h>

#include <Parsers/formatAST.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTLiteral.h>

#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>

#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/PartLog.h>

#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/copyData.h>

#include <Common/Macros.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/formatReadable.h>
#include <Common/setThreadName.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>

#include <Poco/DirectoryIterator.h>

#include <common/ThreadPool.h>

#include <ext/range.h>
#include <ext/scope_guard.h>

#include <cfenv>
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
    extern const int METADATA_MISMATCH;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int TOO_MANY_FETCHES;
    extern const int BAD_DATA_PART_NAME;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int INCORRECT_FILE_NAME;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int KEEPER_EXCEPTION;
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

/** There are three places for each part, where it should be
  * 1. In the RAM, MergeTreeData::data_parts, all_data_parts.
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
    std::lock_guard<std::mutex> lock(current_zookeeper_mutex);
    current_zookeeper = zookeeper;
}

zkutil::ZooKeeperPtr StorageReplicatedMergeTree::tryGetZooKeeper()
{
    std::lock_guard<std::mutex> lock(current_zookeeper_mutex);
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
    const String & path_, const String & database_name_, const String & name_,
    const ColumnsDescription & columns_,
    Context & context_,
    const ASTPtr & primary_expr_ast_,
    const ASTPtr & secondary_sorting_expr_list_,
    const String & date_column_name,
    const ASTPtr & partition_expr_ast_,
    const ASTPtr & sampling_expression_,
    const MergeTreeData::MergingParams & merging_params_,
    const MergeTreeSettings & settings_,
    bool has_force_restore_data_flag)
    : context(context_),
    database_name(database_name_),
    table_name(name_), full_path(path_ + escapeForFileName(table_name) + '/'),
    zookeeper_path(context.getMacros()->expand(zookeeper_path_)),
    replica_name(context.getMacros()->expand(replica_name_)),
    data(database_name, table_name,
        full_path, columns_,
        context_, primary_expr_ast_, secondary_sorting_expr_list_, date_column_name, partition_expr_ast_,
        sampling_expression_, merging_params_,
        settings_, true, attach,
        [this] (const std::string & name) { enqueuePartForCheck(name); }),
    reader(data), writer(data), merger_mutator(data, context.getBackgroundPool()), queue(*this),
    fetcher(data),
    shutdown_event(false), part_check_thread(*this),
    log(&Logger::get(database_name + "." + table_name + " (StorageReplicatedMergeTree)"))
{
    if (path_.empty())
        throw Exception("ReplicatedMergeTree storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;
    replica_path = zookeeper_path + "/replicas/" + replica_name;

    queue_updating_task = context.getSchedulePool().createTask(database_name + "." + table_name + " (StorageReplicatedMergeTree::queueUpdatingTask)", [this]{ queueUpdatingTask(); });

    mutations_updating_task = context.getSchedulePool().createTask(database_name + "." + table_name + " (StorageReplicatedMergeTree::mutationsUpdatingTask)", [this]{ mutationsUpdatingTask(); });

    merge_selecting_task = context.getSchedulePool().createTask(database_name + "." + table_name + " (StorageReplicatedMergeTree::mergeSelectingTask)", [this] { mergeSelectingTask(); });
    /// Will be activated if we win leader election.
    merge_selecting_task->deactivate();

    if (context.hasZooKeeper())
        current_zookeeper = context.getZooKeeper();

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

    data.loadDataParts(skip_sanity_checks);

    if (!current_zookeeper)
    {
        if (!attach)
            throw Exception("Can't create replicated table without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

        /// Do not activate the replica. It will be readonly.
        LOG_ERROR(log, "No ZooKeeper: table will be in readonly mode.");
        is_readonly = true;
        return;
    }

    if (!attach)
    {
        if (!data.getDataParts().empty())
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
        data.clearOldTemporaryDirectories(0);
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
}


static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return "";
    std::stringstream ss;
    formatAST(*ast, ss, false, true);
    return ss.str();
}


namespace
{
    /** The basic parameters of table engine for saving in ZooKeeper.
      * Lets you verify that they match local ones.
      */
    struct TableMetadata
    {
        const MergeTreeData & data;

        explicit TableMetadata(const MergeTreeData & data_)
            : data(data_) {}

        void write(WriteBuffer & out) const
        {
            out << "metadata format version: 1" << "\n"
                << "date column: ";

            if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
                out << data.minmax_idx_columns[data.minmax_idx_date_column_pos] << "\n";
            else
                out << "\n";

            out << "sampling expression: " << formattedAST(data.sampling_expression) << "\n"
                << "index granularity: " << data.index_granularity << "\n"
                << "mode: " << static_cast<int>(data.merging_params.mode) << "\n"
                << "sign column: " << data.merging_params.sign_column << "\n"
                << "primary key: " << formattedAST(data.primary_expr_ast) << "\n";

            if (data.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
            {
                out << "data format version: " << data.format_version.toUnderType() << "\n";
                out << "partition key: " << formattedAST(data.partition_expr_ast) << "\n";
            }
        }

        String toString() const
        {
            WriteBufferFromOwnString out;
            write(out);
            return out.str();
        }

        void check(ReadBuffer & in) const
        {
            /// TODO Can be made less cumbersome.

            in >> "metadata format version: 1";

            in >> "\ndate column: ";
            String read_date_column;
            in >> read_date_column;

            if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
            {
                const String & local_date_column = data.minmax_idx_columns[data.minmax_idx_date_column_pos];
                if (local_date_column != read_date_column)
                    throw Exception("Existing table metadata in ZooKeeper differs in date index column."
                        " Stored in ZooKeeper: " + read_date_column + ", local: " + local_date_column,
                        ErrorCodes::METADATA_MISMATCH);
            }
            else if (!read_date_column.empty())
                throw Exception(
                    "Existing table metadata in ZooKeeper differs in date index column."
                    " Stored in ZooKeeper: " + read_date_column + ", local is custom-partitioned.",
                    ErrorCodes::METADATA_MISMATCH);

            in >> "\nsampling expression: ";
            String read_sample_expression;
            String local_sample_expression = formattedAST(data.sampling_expression);
            in >> read_sample_expression;

            if (read_sample_expression != local_sample_expression)
                throw Exception("Existing table metadata in ZooKeeper differs in sample expression."
                    " Stored in ZooKeeper: " + read_sample_expression + ", local: " + local_sample_expression,
                    ErrorCodes::METADATA_MISMATCH);

            in >> "\nindex granularity: ";
            size_t read_index_granularity = 0;
            in >> read_index_granularity;

            if (read_index_granularity != data.index_granularity)
                throw Exception("Existing table metadata in ZooKeeper differs in index granularity."
                    " Stored in ZooKeeper: " + DB::toString(read_index_granularity) + ", local: " + DB::toString(data.index_granularity),
                    ErrorCodes::METADATA_MISMATCH);

            in >> "\nmode: ";
            int read_mode = 0;
            in >> read_mode;

            if (read_mode != static_cast<int>(data.merging_params.mode))
                throw Exception("Existing table metadata in ZooKeeper differs in mode of merge operation."
                    " Stored in ZooKeeper: " + DB::toString(read_mode) + ", local: "
                    + DB::toString(static_cast<int>(data.merging_params.mode)),
                    ErrorCodes::METADATA_MISMATCH);

            in >> "\nsign column: ";
            String read_sign_column;
            in >> read_sign_column;

            if (read_sign_column != data.merging_params.sign_column)
                throw Exception("Existing table metadata in ZooKeeper differs in sign column."
                    " Stored in ZooKeeper: " + read_sign_column + ", local: " + data.merging_params.sign_column,
                    ErrorCodes::METADATA_MISMATCH);

            in >> "\nprimary key: ";
            String read_primary_key;
            String local_primary_key = formattedAST(data.primary_expr_ast);
            in >> read_primary_key;

            /// NOTE: You can make a less strict check of match expressions so that tables do not break from small changes
            ///    in formatAST code.
            if (read_primary_key != local_primary_key)
                throw Exception("Existing table metadata in ZooKeeper differs in primary key."
                    " Stored in ZooKeeper: " + read_primary_key + ", local: " + local_primary_key,
                    ErrorCodes::METADATA_MISMATCH);

            in >> "\n";
            MergeTreeDataFormatVersion read_data_format_version;
            if (in.eof())
                read_data_format_version = 0;
            else
            {
                in >> "data format version: ";
                in >> read_data_format_version.toUnderType();
            }

            if (read_data_format_version != data.format_version)
                throw Exception("Existing table metadata in ZooKeeper differs in data format version."
                    " Stored in ZooKeeper: " + DB::toString(read_data_format_version.toUnderType()) +
                    ", local: " + DB::toString(data.format_version.toUnderType()),
                    ErrorCodes::METADATA_MISMATCH);

            if (data.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
            {
                in >> "\npartition key: ";
                String read_partition_key;
                String local_partition_key = formattedAST(data.partition_expr_ast);
                in >> read_partition_key;

                if (read_partition_key != local_partition_key)
                    throw Exception(
                        "Existing table metadata in ZooKeeper differs in partition key expression."
                        " Stored in ZooKeeper: " + read_partition_key + ", local: " + local_partition_key,
                        ErrorCodes::METADATA_MISMATCH);

                in >> "\n";
            }

            assertEOF(in);
        }

        void check(const String & s) const
        {
            ReadBufferFromString in(s);
            check(in);
        }
    };
}


void StorageReplicatedMergeTree::createTableIfNotExists()
{
    auto zookeeper = getZooKeeper();

    if (zookeeper->exists(zookeeper_path))
        return;

    LOG_DEBUG(log, "Creating table " << zookeeper_path);

    zookeeper->createAncestors(zookeeper_path);

    /// We write metadata of table so that the replicas can check table parameters with them.
    String metadata = TableMetadata(data).toString();

    zkutil::Requests ops;
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

    zkutil::Responses responses;
    auto code = zookeeper->tryMulti(ops, responses);
    if (code && code != ZooKeeperImpl::ZooKeeper::ZNODEEXISTS)
        throw zkutil::KeeperException(code);
}


/** Verify that list of columns and table settings match those specified in ZK (/ metadata).
    * If not, throw an exception.
    */
void StorageReplicatedMergeTree::checkTableStructure(bool skip_sanity_checks, bool allow_alter)
{
    auto zookeeper = getZooKeeper();

    String metadata_str = zookeeper->get(zookeeper_path + "/metadata");
    TableMetadata(data).check(metadata_str);

    zkutil::Stat stat;
    auto columns_from_zk = ColumnsDescription::parse(zookeeper->get(zookeeper_path + "/columns", &stat));
    columns_version = stat.version;

    const ColumnsDescription & old_columns = getColumns();
    if (columns_from_zk != old_columns)
    {
        if (allow_alter &&
            (skip_sanity_checks ||
             old_columns.ordinary.sizeOfDifference(columns_from_zk.ordinary) +
             old_columns.materialized.sizeOfDifference(columns_from_zk.materialized) <= 2))
        {
            LOG_WARNING(log, "Table structure in ZooKeeper is a little different from local table structure. Assuming ALTER.");

            /// Without any locks, because table has not been created yet.
            context.getDatabase(database_name)->alterTable(context, table_name, columns_from_zk, {});

            setColumns(std::move(columns_from_zk));
        }
        else
        {
            throw Exception("Table structure in ZooKeeper is too different from local table structure",
                            ErrorCodes::INCOMPATIBLE_COLUMNS);
        }
    }
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
    zkutil::Stat stat;
    String unused;
    if (zookeeper->tryGet(replica_path + "/parts/" + part_name, unused, &stat))
        res = stat.ctime / 1000;

    return res;
}


void StorageReplicatedMergeTree::createReplica()
{
    auto zookeeper = getZooKeeper();

    LOG_DEBUG(log, "Creating replica " << replica_path);

    /// Create an empty replica. We'll create `columns` node at the end - we'll use it as a sign that replica creation is complete.
    zkutil::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(replica_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/log_pointer", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/queue", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/parts", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/flags", "", zkutil::CreateMode::Persistent));

    try
    {
        zookeeper->multi(ops);
    }
    catch (const zkutil::KeeperException & e)
    {
        if (e.code == ZooKeeperImpl::ZooKeeper::ZNODEEXISTS)
            throw Exception("Replica " + replica_path + " already exists.", ErrorCodes::REPLICA_IS_ALREADY_EXIST);

        throw;
    }

    /** You need to change the data of nodes/replicas to anything, so that the thread that removes old entries in the log,
      *  stumbled over this change and does not delete the entries we have not yet read.
      */
    zookeeper->set(zookeeper_path + "/replicas", "last added replica: " + replica_name);

    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

    /** "Reference" replica, from which we take information about the set of parts, queue and pointer to the log.
      * Take random replica created earlier than this.
      */
    String source_replica;

    zkutil::Stat stat;
    zookeeper->exists(replica_path, &stat);
    auto my_create_time = stat.czxid;

    std::shuffle(replicas.begin(), replicas.end(), rng);
    for (const String & replica : replicas)
    {
        if (!zookeeper->exists(zookeeper_path + "/replicas/" + replica, &stat))
            throw Exception("Replica " + zookeeper_path + "/replicas/" + replica + " was removed from right under our feet.",
                            ErrorCodes::NO_SUCH_REPLICA);
        if (stat.czxid < my_create_time)
        {
            source_replica = replica;
            break;
        }
    }

    if (source_replica.empty())
    {
        LOG_INFO(log, "This is the first replica");
    }
    else
    {
        LOG_INFO(log, "Will mimic " << source_replica);

        String source_path = zookeeper_path + "/replicas/" + source_replica;

        /** If the reference/master replica is not yet fully created, let's wait.
          * NOTE: If something went wrong while creating it, we can hang around forever.
          *    You can create an ephemeral node at the time of creation to make sure that the replica is created, and not abandoned.
          *    The same can be done for the table. You can automatically delete a replica/table node,
          *     if you see that it was not created up to the end, and the one who created it died.
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

        /// Copy reference to the log from `reference/master` replica.
        zookeeper->set(replica_path + "/log_pointer", zookeeper->get(source_path + "/log_pointer"));

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
        ActiveDataPartSet active_parts_set(data.format_version, parts);

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

        /// It will then be loaded into the queue variable in `queue.initialize` method.

        LOG_DEBUG(log, "Copied " << source_queue.size() << " queue entries");
    }

    zookeeper->create(replica_path + "/columns", getColumns().toString(), zkutil::CreateMode::Persistent);
}


void StorageReplicatedMergeTree::checkParts(bool skip_sanity_checks)
{
    auto zookeeper = getZooKeeper();

    Strings expected_parts_vec = zookeeper->getChildren(replica_path + "/parts");

    /// Parts in ZK.
    NameSet expected_parts(expected_parts_vec.begin(), expected_parts_vec.end());

    /// There are no PreCommitted parts at startup.
    auto parts = data.getDataParts({MergeTreeDataPartState::Committed, MergeTreeDataPartState::Outdated});

    /// Local parts that are not in ZK.
    MergeTreeData::DataParts unexpected_parts;

    for (const auto & part : parts)
    {
        if (expected_parts.count(part->name))
            expected_parts.erase(part->name);
        else
            unexpected_parts.insert(part);
    }

    /// Which local parts to added into ZK.
    MergeTreeData::DataPartsVector parts_to_add;
    UInt64 parts_to_add_rows = 0;

    /// Which parts should be taken from other replicas.
    Strings parts_to_fetch;

    for (const String & missing_name : expected_parts)
    {
        /// If locally some part is missing, but there is a part covering it, you can replace it in ZK with the covering one.
        auto containing = data.getActiveContainingPart(missing_name);
        if (containing)
        {
            LOG_ERROR(log, "Ignoring missing local part " << missing_name << " because part " << containing->name << " exists");
            if (unexpected_parts.count(containing))
            {
                parts_to_add.push_back(containing);
                unexpected_parts.erase(containing);
                parts_to_add_rows += containing->rows_count;
            }
        }
        else
        {
            LOG_ERROR(log, "Fetching missing part " << missing_name);
            parts_to_fetch.push_back(missing_name);
        }
    }

    for (const String & name : parts_to_fetch)
        expected_parts.erase(name);


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
        if (MergeTreePartInfo::tryParsePartName(part_name, &part_info, data.format_version))
            return part_info.getBlocksCount();

        LOG_ERROR(log, "Unexpected part name: " << part_name);
        return 0;
    };

    UInt64 parts_to_fetch_blocks = 0;
    for (const String & name : parts_to_fetch)
        parts_to_fetch_blocks += get_blocks_count_in_data_part(name);

    UInt64 expected_parts_blocks = 0;
    for (const String & name : expected_parts)
        expected_parts_blocks += get_blocks_count_in_data_part(name);

    std::stringstream sanity_report;
    sanity_report << "There are "
        << unexpected_parts.size() << " unexpected parts with " << unexpected_parts_rows << " rows ("
        << unexpected_parts_nonnew << " of them is not just-written with " << unexpected_parts_rows << " rows), "
        << parts_to_add.size() << " unexpectedly merged parts with " << parts_to_add_rows << " rows, "
        << expected_parts.size() << " missing obsolete parts (with " << expected_parts_blocks << " blocks), "
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

    UInt64 total_suspicious_rows = parts_to_add_rows + unexpected_parts_rows;
    UInt64 total_suspicious_rows_no_new = parts_to_add_rows + unexpected_parts_nonnew_rows;

    bool insane = total_suspicious_rows > total_rows_on_filesystem * data.settings.replicated_max_ratio_of_wrong_parts;

    if (insane && !skip_sanity_checks)
    {
        std::stringstream why;
        why << "The local set of parts of table " << database_name  << "." << table_name << " doesn't look like the set of parts "
            << "in ZooKeeper: "
            << formatReadableQuantity(total_suspicious_rows) << " rows of " << formatReadableQuantity(total_rows_on_filesystem)
            << " total rows in filesystem are suspicious.";

        throw Exception(why.str() + " " + sanity_report.str(), ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);
    }

    if (total_suspicious_rows_no_new > 0)
        LOG_WARNING(log, sanity_report.str());

    /// Add information to the ZK about the parts that cover the missing parts.
    for (const MergeTreeData::DataPartPtr & part : parts_to_add)
    {
        LOG_ERROR(log, "Adding unexpected local part to ZooKeeper: " << part->name);

        zkutil::Requests ops;
        checkPartChecksumsAndAddCommitOps(zookeeper, part, ops);
        zookeeper->multi(ops);
    }

    /// Remove from ZK information about the parts covered by the newly added ones.
    {
        for (const String & name : expected_parts)
            LOG_ERROR(log, "Removing unexpectedly merged local part from ZooKeeper: " << name);

        removePartsFromZooKeeper(zookeeper, Strings(expected_parts.begin(), expected_parts.end()));
    }

    /// Add to the queue job to pick up the missing parts from other replicas and remove from ZK the information that we have them.
    for (const String & name : parts_to_fetch)
    {
        LOG_ERROR(log, "Removing missing part from ZooKeeper and queueing a fetch: " << name);

        LogEntry log_entry;
        log_entry.type = LogEntry::GET_PART;
        log_entry.source_replica = "";
        log_entry.new_part_name = name;
        log_entry.create_time = tryGetPartCreateTime(zookeeper, replica_path, name);

        /// We assume that this occurs before the queue is loaded (queue.initialize).
        zkutil::Requests ops;
        removePartFromZooKeeper(name, ops);
        ops.emplace_back(zkutil::makeCreateRequest(
            replica_path + "/queue/queue-", log_entry.toString(), zkutil::CreateMode::PersistentSequential));
        zookeeper->multi(ops);
    }

    /// Remove extra local parts.
    for (const MergeTreeData::DataPartPtr & part : unexpected_parts)
    {
        LOG_ERROR(log, "Renaming unexpected part " << part->name << " to ignored_" + part->name);
        data.forgetPartAndMoveToDetached(part, "ignored_", true);
    }
}


void StorageReplicatedMergeTree::checkPartChecksumsAndAddCommitOps(const zkutil::ZooKeeperPtr & zookeeper,
    const MergeTreeData::DataPartPtr & part, zkutil::Requests & ops, String part_name, NameSet * absent_replicas_paths)
{
    if (part_name.empty())
        part_name = part->name;

    check(part->columns);
    int expected_columns_version = columns_version;

    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
    std::shuffle(replicas.begin(), replicas.end(), rng);
    String expected_columns_str = part->columns.toString();
    bool has_been_alredy_added = false;

    for (const String & replica : replicas)
    {
        zkutil::Stat stat_before, stat_after;
        String current_part_path = zookeeper_path + "/replicas/" + replica + "/parts/" + part_name;

        String columns_str;
        if (!zookeeper->tryGet(current_part_path + "/columns", columns_str, &stat_before))
        {
            if (absent_replicas_paths)
                absent_replicas_paths->emplace(current_part_path);

            continue;
        }

        if (columns_str != expected_columns_str)
        {
            LOG_INFO(log, "Not checking checksums of part " << part_name << " with replica " << replica
                << " because columns are different");
            continue;
        }

        String checksums_str;
        /// Let's check that the node's version with the columns did not change while we were reading the checksums.
        /// This ensures that the columns and the checksum refer to the same data.
        if (!zookeeper->tryGet(current_part_path + "/checksums", checksums_str) ||
            !zookeeper->exists(current_part_path + "/columns", &stat_after) ||
            stat_before.version != stat_after.version)
        {
            LOG_INFO(log, "Not checking checksums of part " << part_name << " with replica " << replica
                << " because part changed while we were reading its checksums");
            continue;
        }

        auto zk_checksums = MinimalisticDataPartChecksums::deserializeFrom(checksums_str);
        zk_checksums.checkEqual(part->checksums, true);

        if (replica == replica_name)
            has_been_alredy_added = true;

        /// If we verify checksums in "sequential manner" (i.e. recheck absence of checksums on other replicas when commit)
        /// then it is enough to verify checksums on at least one replica since checksums on other replicas must be the same.
        if (absent_replicas_paths)
        {
            absent_replicas_paths->clear();
            break;
        }
    }

    if (!has_been_alredy_added)
    {
        String part_path = replica_path + "/parts/" + part_name;

        ops.emplace_back(zkutil::makeCheckRequest(
            zookeeper_path + "/columns", expected_columns_version));
        ops.emplace_back(zkutil::makeCreateRequest(
            part_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            part_path + "/columns", part->columns.toString(), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            part_path + "/checksums", getChecksumsForZooKeeper(part->checksums), zkutil::CreateMode::Persistent));
    }
    else
    {
        LOG_WARNING(log, "checkPartAndAddToZooKeeper: node " << replica_path + "/parts/" + part_name << " already exists."
            << " Will not commit any nodes.");
    }
}

MergeTreeData::DataPartsVector StorageReplicatedMergeTree::checkPartChecksumsAndCommit(MergeTreeData::Transaction & transaction,
    const MergeTreeData::DataPartPtr & part)
{
    auto zookeeper = getZooKeeper();

    while (true)
    {
        zkutil::Requests ops;
        NameSet absent_part_paths_on_replicas;

        /// Checksums are checked here and `ops` is filled. In fact, the part is added to ZK just below, when executing `multi`.
        checkPartChecksumsAndAddCommitOps(zookeeper, part, ops, part->name, &absent_part_paths_on_replicas);

        /// Do not commit if the part is obsolete, we have just briefly checked its checksums
        if (transaction.isEmpty())
            return {};

        /// Will check that the part did not suddenly appear on skipped replicas
        if (!absent_part_paths_on_replicas.empty())
        {
            zkutil::Requests new_ops;
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

            if (failed_op_index < num_check_ops && e.code == ZooKeeperImpl::ZooKeeper::ZNODEEXISTS)
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
                                                              static_cast<bool>(data.settings.use_minimalistic_checksums_in_zookeeper));
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
        MergeTreeData::DataPartPtr existing_part = data.getPartIfExists(entry.new_part_name, {MergeTreeDataPartState::PreCommitted});
        if (!existing_part)
            existing_part = data.getActiveContainingPart(entry.new_part_name);

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
        throw Exception("Unexpected log entry type: " + toString(static_cast<int>(entry.type)));
    }

    if (do_fetch)
        return executeFetch(entry);

    return true;
}


void StorageReplicatedMergeTree::writePartLog(
    PartLogElement::Type type, const ExecutionStatus & execution_status, UInt64 elapsed_ns,
    const String & new_part_name,
    const MergeTreeData::DataPartPtr & result_part,
    const MergeTreeData::DataPartsVector & source_parts,
    const MergeListEntry * merge_entry) const
{
    try
    {
        auto part_log = context.getPartLog(database_name);
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

            part_log_elem.rows_read = (*merge_entry)->bytes_read_uncompressed;
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

    MergeTreeData::DataPartsVector parts;
    bool have_all_parts = true;
    for (const String & name : entry.source_parts)
    {
        MergeTreeData::DataPartPtr part = data.getActiveContainingPart(name);
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
    else if (entry.create_time + data.settings.prefer_fetch_merged_part_time_threshold.totalSeconds() <= time(nullptr))
    {
        /// If entry is old enough, and have enough size, and part are exists in any replica,
        ///  then prefer fetching of merged part from replica.

        size_t sum_parts_bytes_on_disk = 0;
        for (const auto & part : parts)
            sum_parts_bytes_on_disk += part->bytes_on_disk;

        if (sum_parts_bytes_on_disk >= data.settings.prefer_fetch_merged_part_size_threshold)
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

    auto table_lock = lockStructure(false, __PRETTY_FUNCTION__);

    MergeList::EntryPtr merge_entry = context.getMergeList().insert(database_name, table_name, entry.new_part_name, parts);
    size_t aio_threshold = context.getSettings().min_bytes_to_use_direct_io;

    MergeTreeDataMergerMutator::FuturePart future_merged_part(parts);
    if (future_merged_part.name != entry.new_part_name)
    {
        throw Exception("Future merged part name `" + future_merged_part.name +  "` differs from part name in log entry: `"
                        + entry.new_part_name + "`", ErrorCodes::BAD_DATA_PART_NAME);
    }

    MergeTreeData::Transaction transaction;
    MergeTreeData::MutableDataPartPtr part;

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
            future_merged_part, *merge_entry, aio_threshold, entry.create_time, reserved_space.get(), entry.deduplicate);

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

    MergeTreeData::DataPartPtr source_part = data.getActiveContainingPart(source_part_name);
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

    if (entry.create_time + data.settings.prefer_fetch_merged_part_time_threshold.totalSeconds() <= time(nullptr)
        && estimated_space_for_result >= data.settings.prefer_fetch_merged_part_size_threshold)
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
        entry.new_part_name, data.format_version);
    MutationCommands commands = queue.getMutationCommands(source_part, new_part_info.mutation);

    /// Can throw an exception.
    DiskSpaceMonitor::ReservationPtr reserved_space = DiskSpaceMonitor::reserve(full_path, estimated_space_for_result);

    auto table_lock = lockStructure(false, __PRETTY_FUNCTION__);

    MergeTreeData::MutableDataPartPtr new_part;
    MergeTreeData::Transaction transaction;

    MergeTreeDataMergerMutator::FuturePart future_mutated_part;
    future_mutated_part.parts.push_back(source_part);
    future_mutated_part.part_info = new_part_info;
    future_mutated_part.name = entry.new_part_name;

    Stopwatch stopwatch;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        writePartLog(
            PartLogElement::MUTATE_PART, execution_status, stopwatch.elapsed(),
            entry.new_part_name, new_part, future_mutated_part.parts, nullptr);
    };

    try
    {
        new_part = merger_mutator.mutatePartToTemporaryPart(future_mutated_part, commands.commands, context);
        data.renameTempPartAndReplace(new_part, nullptr, &transaction);

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
    if (data.settings.replicated_max_parallel_fetches && total_fetches >= data.settings.replicated_max_parallel_fetches)
    {
        throw Exception("Too many total fetches from replicas, maximum: " + data.settings.replicated_max_parallel_fetches.toString(),
            ErrorCodes::TOO_MANY_FETCHES);
    }

    ++total_fetches;
    SCOPE_EXIT({--total_fetches;});

    if (data.settings.replicated_max_parallel_fetches_for_table && current_table_fetches >= data.settings.replicated_max_parallel_fetches_for_table)
    {
        throw Exception("Too many fetches from replicas for table, maximum: " + data.settings.replicated_max_parallel_fetches_for_table.toString(),
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

                zkutil::Requests ops;

                for (size_t i = 0, size = replicas.size(); i < size; ++i)
                {
                    zkutil::Stat stat;
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
                    zkutil::Stat quorum_stat;
                    String quorum_path = zookeeper_path + "/quorum/status";
                    String quorum_str = zookeeper->get(quorum_path, &quorum_stat);
                    ReplicatedMergeTreeQuorumEntry quorum_entry;
                    quorum_entry.fromString(quorum_str);

                    if (quorum_entry.part_name == entry.new_part_name)
                    {
                        ops.emplace_back(zkutil::makeRemoveRequest(quorum_path, quorum_stat.version));

                        auto part_info = MergeTreePartInfo::fromPartName(entry.new_part_name, data.format_version);

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

                        zkutil::Responses responses;
                        auto code = zookeeper->tryMulti(ops, responses);

                        if (code == ZooKeeperImpl::ZooKeeper::ZOK)
                        {
                            LOG_DEBUG(log, "Marked quorum for part " << entry.new_part_name << " as failed.");
                            queue.removeFromVirtualParts(part_info);
                            return true;
                        }
                        else if (code == ZooKeeperImpl::ZooKeeper::ZBADVERSION || code == ZooKeeperImpl::ZooKeeper::ZNONODE || code == ZooKeeperImpl::ZooKeeper::ZNODEEXISTS)
                        {
                            LOG_DEBUG(log, "State was changed or isn't expected when trying to mark quorum for part "
                                << entry.new_part_name << " as failed. Code: " << zkutil::ZooKeeper::error2string(code));
                        }
                        else
                            throw zkutil::KeeperException(code);
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
    auto drop_range_info = MergeTreePartInfo::fromPartName(entry.new_part_name, data.format_version);
    queue.removePartProducingOpsInRange(getZooKeeper(), drop_range_info);

    LOG_DEBUG(log, (entry.detach ? "Detaching" : "Removing") << " parts.");

    /// Delete the parts contained in the range to be deleted.
    /// It's important that no old parts remain (after the merge), because otherwise,
    ///  after adding a new replica, this new replica downloads them, but does not delete them.
    /// And, if you do not, the parts will come to life after the server is restarted.
    /// Therefore, we use all data parts.

    MergeTreeData::DataPartsVector parts_to_remove;
    {
        auto data_parts_lock = data.lockParts();
        parts_to_remove = data.removePartsInRangeFromWorkingSet(drop_range_info, true, true, data_parts_lock);
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
    cleanup_thread->schedule();
}


void StorageReplicatedMergeTree::executeClearColumnInPartition(const LogEntry & entry)
{
    LOG_INFO(log, "Clear column " << entry.column_name << " in parts inside " << entry.new_part_name << " range");

    auto entry_part_info = MergeTreePartInfo::fromPartName(entry.new_part_name, data.format_version);

    /// We don't change table structure, only data in some parts
    /// To disable reading from these parts, we will sequentially acquire write lock for each part inside alterDataPart()
    /// If we will lock the whole table here, a deadlock can occur. For example, if use use Buffer table (CLICKHOUSE-3238)
    auto lock_read_structure = lockStructure(false, __PRETTY_FUNCTION__);

    auto zookeeper = getZooKeeper();

    AlterCommand alter_command;
    alter_command.type = AlterCommand::DROP_COLUMN;
    alter_command.column_name = entry.column_name;

    auto new_columns = getColumns();
    alter_command.apply(new_columns);

    size_t modified_parts = 0;
    auto parts = data.getDataParts();
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

        auto transaction = data.alterDataPart(part, columns_for_parts, data.primary_expr_ast, false);
        if (!transaction)
            continue;

        /// Update part metadata in ZooKeeper.
        zkutil::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(
            replica_path + "/parts/" + part->name + "/columns", transaction->getNewColumns().toString(), -1));
        ops.emplace_back(zkutil::makeSetRequest(
            replica_path + "/parts/" + part->name + "/checksums", getChecksumsForZooKeeper(transaction->getNewChecksums()), -1));

        zookeeper->multi(ops);

        transaction->commit();
        ++modified_parts;
    }

    LOG_DEBUG(log, "Cleared column " << entry.column_name << " in " << modified_parts << " parts");

    /// Recalculate columns size (not only for the modified column)
    data.recalculateColumnSizes();
}


bool StorageReplicatedMergeTree::executeReplaceRange(const LogEntry & entry)
{
    Stopwatch watch;
    auto & entry_replace = *entry.replace_range_entry;

    MergeTreePartInfo drop_range = MergeTreePartInfo::fromPartName(entry_replace.drop_range_part_name, data.format_version);
    /// Range with only one block has special meaning ATTACH PARTITION
    bool replace = drop_range.getBlocksCount() > 1;

    queue.removePartProducingOpsInRange(getZooKeeper(), drop_range);

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

        /// Part which will be comitted
        MergeTreeData::MutableDataPartPtr res_part;

        /// We could find a covering part
        MergeTreePartInfo found_new_part_info;
        String found_new_part_name;

        /// Hold pointer to part in source table if will clone it from local table
        MergeTreeData::DataPartPtr src_table_part;

        /// A replica that will be used to fetch part
        String replica;
    };

    using PartDescriptionPtr = std::shared_ptr<PartDescription>;
    using PartDescriptions = std::vector<PartDescriptionPtr>;

    PartDescriptions all_parts;
    PartDescriptions parts_to_add;
    MergeTreeData::DataPartsVector parts_to_remove;

    auto structure_lock_dst_table = lockStructure(false, __PRETTY_FUNCTION__);

    for (size_t i = 0; i < entry_replace.new_part_names.size(); ++i)
    {
        all_parts.emplace_back(std::make_shared<PartDescription>(i,
            entry_replace.src_part_names.at(i),
            entry_replace.new_part_names.at(i),
            entry_replace.part_names_checksums.at(i),
            data.format_version));
    }

    /// What parts we should add? Or we have already added all required parts (we an replica-intializer)
    {
        auto data_parts_lock = data.lockParts();

        for (const PartDescriptionPtr & part_desc : all_parts)
        {
            if (!data.getActiveContainingPart(part_desc->new_part_info, MergeTreeDataPartState::Committed, data_parts_lock))
                parts_to_add.emplace_back(part_desc);
        }

        if (parts_to_add.empty() && replace)
            parts_to_remove = data.removePartsInRangeFromWorkingSet(drop_range, true, false, data_parts_lock);
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
    TableStructureReadLockPtr structure_lock_src_table;
    String source_table_name = entry_replace.from_database + "." + entry_replace.from_table;

    auto clone_data_parts_from_source_table = [&] () -> size_t
    {
        source_table = context.tryGetTable(entry_replace.from_database, entry_replace.from_table);
        if (!source_table)
        {
            LOG_DEBUG(log, "Can't use " << source_table_name << " as source table for REPLACE PARTITION command. It does not exist.");
            return 0;
        }

        MergeTreeData * src_data = nullptr;
        try
        {
            src_data = data.checkStructureAndGetMergeTreeData(source_table);
        }
        catch (Exception & e)
        {
            LOG_INFO(log, "Can't use " << source_table_name << " as source table for REPLACE PARTITION command. Will fetch all parts."
                           << " Reason: " << getCurrentExceptionMessage(false));
            return 0;
        }

        structure_lock_src_table = source_table->lockStructure(false, __PRETTY_FUNCTION__);

        MergeTreeData::DataPartStates valid_states{MergeTreeDataPartState::PreCommitted, MergeTreeDataPartState::Committed,
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

    ActiveDataPartSet adding_parts_active_set(data.format_version);
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
            LOG_DEBUG(log, "Part " <<  part_desc->new_part_name << " is not found on remote replicas");

            /// Fallback to covering part
            replica = findReplicaHavingCoveringPart(part_desc->new_part_name, true, found_part_name);

            if (replica.empty())
            {
                /// It is not fail, since adjacent parts could cover current part
                LOG_DEBUG(log, "Parts covering " <<  part_desc->new_part_name << " are not found on remote replicas");
                continue;
            }
        }
        else
        {
            found_part_name = part_desc->new_part_name;
        }

        part_desc->found_new_part_name = found_part_name;
        part_desc->found_new_part_info = MergeTreePartInfo::fromPartName(found_part_name, data.format_version);
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
                                    + " and " + curr.found_new_part_name + ". It should be investigated.");
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

            part_desc->res_part = data.cloneAndLoadDataPart(
                part_desc->src_table_part, TMP_PREFIX + "clone_", part_desc->new_part_info);
        }
        else if (!part_desc->replica.empty())
        {
            String replica_path = zookeeper_path + "/replicas/" + part_desc->replica;
            ReplicatedMergeTreeAddress address(getZooKeeper()->get(replica_path + "/host"));
            auto timeouts = ConnectionTimeouts::getHTTPTimeouts(context.getSettingsRef());

            part_desc->res_part = fetcher.fetchPart(part_desc->found_new_part_name, replica_path,
                                                    address.host, address.replication_port, timeouts, false, TMP_PREFIX + "fetch_");

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

    MergeTreeData::MutableDataPartsVector res_parts;
    for (PartDescriptionPtr & part_desc : final_parts)
        res_parts.emplace_back(part_desc->res_part);

    try
    {
        /// Commit parts
        auto zookeeper = getZooKeeper();
        MergeTreeData::Transaction transaction;

        zkutil::Requests ops;
        for (PartDescriptionPtr & part_desc : final_parts)
        {
            data.renameTempPartAndReplace(part_desc->res_part, nullptr, &transaction);
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
            auto data_parts_lock = data.lockParts();

            transaction.commit(&data_parts_lock);
            if (replace)
                parts_to_remove = data.removePartsInRangeFromWorkingSet(drop_range, true, false, data_parts_lock);
        }

        PartLog::addNewParts(this->context, res_parts, watch.elapsed());
    }
    catch (...)
    {
        PartLog::addNewParts(this->context, res_parts, watch.elapsed(), ExecutionStatus::fromCurrentException());
        throw;
    }

    tryRemovePartsFromZooKeeperWithRetries(parts_to_remove);
    res_parts.clear();
    parts_to_remove.clear();
    cleanup_thread->schedule();

    return true;
}


void StorageReplicatedMergeTree::queueUpdatingTask()
{
    //most probably this check is not relevant
    if (shutdown_called)
        return;

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
    catch (const zkutil::KeeperException & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == ZooKeeperImpl::ZooKeeper::ZSESSIONEXPIRED)
            return;

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
    catch (const zkutil::KeeperException & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == ZooKeeperImpl::ZooKeeper::ZSESSIONEXPIRED)
            return;

        mutations_updating_task->scheduleAfter(QUEUE_UPDATE_ERROR_SLEEP_MS);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        mutations_updating_task->scheduleAfter(QUEUE_UPDATE_ERROR_SLEEP_MS);
    }
}


bool StorageReplicatedMergeTree::queueTask()
{
    /// If replication queue is stopped exit immediately as we successfully executed the task
    if (queue.actions_blocker.isCancelled())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        return true;
    }

    /// This object will mark the element of the queue as running.
    ReplicatedMergeTreeQueue::SelectedEntry selected;

    try
    {
        selected = queue.selectEntryToProcess(merger_mutator, data);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    LogEntryPtr & entry = selected.first;

    if (!entry)
        return false;

    time_t prev_attempt_time = entry->last_attempt_time;

    bool res = queue.processEntry([this]{ return getZooKeeper(); }, entry, [&](LogEntryPtr & entry)
    {
        try
        {
            return executeLogEntry(*entry);
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
    return !need_sleep;
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
        std::lock_guard<std::mutex> merge_selecting_lock(merge_selecting_mutex);

        auto zookeeper = getZooKeeper();

        ReplicatedMergeTreeMergePredicate merge_pred = queue.getMergePredicate(zookeeper);

        /// If many merges is already queued, then will queue only small enough merges.
        /// Otherwise merge queue could be filled with only large merges,
        /// and in the same time, many small parts could be created and won't be merged.
        size_t merges_and_mutations_queued = merge_pred.countMergesAndPartMutations();
        if (merges_and_mutations_queued >= data.settings.max_replicated_merges_in_queue)
        {
            LOG_TRACE(log, "Number of queued merges and part mutations (" << merges_and_mutations_queued
                << ") is greater than max_replicated_merges_in_queue ("
                << data.settings.max_replicated_merges_in_queue << "), so won't select new parts to merge or mutate.");
        }
        else
        {
            size_t max_source_parts_size = merger_mutator.getMaxSourcePartsSize(
                data.settings.max_replicated_merges_in_queue, merges_and_mutations_queued);

            if (max_source_parts_size > 0)
            {
                MergeTreeDataMergerMutator::FuturePart future_merged_part;
                if (merger_mutator.selectPartsToMerge(future_merged_part, false, max_source_parts_size, merge_pred))
                {
                    success = createLogEntryToMergeParts(zookeeper, future_merged_part.parts, future_merged_part.name, deduplicate);
                }
                else if (merge_pred.countMutations() > 0)
                {
                    /// Choose a part to mutate.

                    MergeTreeData::DataPartsVector data_parts = data.getDataPartsVector();
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


bool StorageReplicatedMergeTree::createLogEntryToMergeParts(
    zkutil::ZooKeeperPtr & zookeeper,
    const MergeTreeData::DataPartsVector & parts,
    const String & merged_name,
    bool deduplicate,
    ReplicatedMergeTreeLogEntryData * out_log_entry)
{
    bool all_in_zk = true;
    for (const auto & part : parts)
    {
        /// If there is no information about part in ZK, we will not merge it.
        if (!zookeeper->exists(replica_path + "/parts/" + part->name))
        {
            all_in_zk = false;

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

    const String & partition_id = parts[0]->info.partition_id;
    for (size_t i = 0; i + 1 < parts.size(); ++i)
    {
        /// Remove the unnecessary entries about non-existent blocks.
        for (Int64 number = parts[i]->info.max_block + 1; number <= parts[i + 1]->info.min_block - 1; ++number)
        {
            zookeeper->tryRemove(zookeeper_path + "/block_numbers/" + partition_id + "/block-" + padIndex(number));
        }
    }

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


void StorageReplicatedMergeTree::removePartFromZooKeeper(const String & part_name, zkutil::Requests & ops)
{
    String part_path = replica_path + "/parts/" + part_name;

    ops.emplace_back(zkutil::makeRemoveRequest(part_path + "/checksums", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(part_path + "/columns", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(part_path, -1));
}


void StorageReplicatedMergeTree::removePartAndEnqueueFetch(const String & part_name)
{
    auto zookeeper = getZooKeeper();

    String part_path = replica_path + "/parts/" + part_name;

    LogEntryPtr log_entry = std::make_shared<LogEntry>();
    log_entry->type = LogEntry::GET_PART;
    log_entry->create_time = tryGetPartCreateTime(zookeeper, replica_path, part_name);
    log_entry->source_replica = "";
    log_entry->new_part_name = part_name;

    zkutil::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(
        replica_path + "/queue/queue-", log_entry->toString(),
        zkutil::CreateMode::PersistentSequential));

    removePartFromZooKeeper(part_name, ops);

    auto results = zookeeper->multi(ops);

    String path_created = dynamic_cast<const zkutil::CreateResponse &>(*results[0]).path_created;
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
        merge_selecting_task->activate();
        merge_selecting_task->schedule();
    };

    try
    {
        leader_election = std::make_shared<zkutil::LeaderElection>(
            context.getSchedulePool(),
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
                || MergeTreePartInfo::contains(part_on_replica, entry.new_part_name, data.format_version))
            {
                if (largest_part_found.empty()
                    || MergeTreePartInfo::contains(part_on_replica, largest_part_found, data.format_version))
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
                || MergeTreePartInfo::contains(part_on_replica, part_name, data.format_version))
            {
                if (largest_part_found.empty()
                    || MergeTreePartInfo::contains(part_on_replica, largest_part_found, data.format_version))
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
    zkutil::Stat stat;

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

            zkutil::Requests ops;
            zkutil::Responses responses;
            ops.emplace_back(zkutil::makeRemoveRequest(quorum_status_path, stat.version));
            ops.emplace_back(zkutil::makeSetRequest(quorum_last_part_path, part_name, -1));
            auto code = zookeeper->tryMulti(ops, responses);

            if (code == ZooKeeperImpl::ZooKeeper::ZOK)
            {
                break;
            }
            else if (code == ZooKeeperImpl::ZooKeeper::ZNONODE)
            {
                /// The quorum has already been achieved.
                break;
            }
            else if (code == ZooKeeperImpl::ZooKeeper::ZBADVERSION)
            {
                /// Node was updated meanwhile. We must re-read it and repeat all the actions.
                continue;
            }
            else
                throw zkutil::KeeperException(code, quorum_status_path);
        }
        else
        {
            /// We update the node, registering there one more replica.
            auto code = zookeeper->trySet(quorum_status_path, quorum_entry.toString(), stat.version);

            if (code == ZooKeeperImpl::ZooKeeper::ZOK)
            {
                break;
            }
            else if (code == ZooKeeperImpl::ZooKeeper::ZNONODE)
            {
                /// The quorum has already been achieved.
                break;
            }
            else if (code == ZooKeeperImpl::ZooKeeper::ZBADVERSION)
            {
                /// Node was updated meanwhile. We must re-read it and repeat all the actions.
                continue;
            }
            else
                throw zkutil::KeeperException(code, quorum_status_path);
        }
    }
}


bool StorageReplicatedMergeTree::fetchPart(const String & part_name, const String & replica_path, bool to_detached, size_t quorum)
{
    if (auto part = data.getPartIfExists(part_name, {MergeTreeDataPart::State::Outdated, MergeTreeDataPart::State::Deleting}))
    {
        LOG_DEBUG(log, "Part " << part->getNameWithState() << " should be deleted after previous attempt before fetch");
        /// Force immediate parts cleanup to delete the part that was left from the previous fetch attempt.
        cleanup_thread->schedule();
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(currently_fetching_parts_mutex);
        if (!currently_fetching_parts.insert(part_name).second)
        {
            LOG_DEBUG(log, "Part " << part_name << " is already fetching right now");
            return false;
        }
    }

    SCOPE_EXIT
    ({
        std::lock_guard<std::mutex> lock(currently_fetching_parts_mutex);
        currently_fetching_parts.erase(part_name);
    });

    LOG_DEBUG(log, "Fetching part " << part_name << " from " << replica_path);

    TableStructureReadLockPtr table_lock;
    if (!to_detached)
        table_lock = lockStructure(true, __PRETTY_FUNCTION__);

    /// Logging
    Stopwatch stopwatch;
    MergeTreeData::MutableDataPartPtr part;
    MergeTreeData::DataPartsVector replaced_parts;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        writePartLog(
            PartLogElement::DOWNLOAD_PART, execution_status, stopwatch.elapsed(),
            part_name, part, replaced_parts, nullptr);
    };

    ReplicatedMergeTreeAddress address(getZooKeeper()->get(replica_path + "/host"));
    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(context.getSettingsRef());

    try
    {
        part = fetcher.fetchPart(part_name, replica_path, address.host, address.replication_port, timeouts, to_detached);

        if (!to_detached)
        {
            MergeTreeData::Transaction transaction;
            data.renameTempPartAndReplace(part, nullptr, &transaction);

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

    LOG_DEBUG(log, "Fetched part " << part_name << " from " << replica_path << (to_detached ? " (to 'detached' directory)" : ""));
    return true;
}


void StorageReplicatedMergeTree::startup()
{
    if (is_readonly)
        return;

    queue.initialize(
        zookeeper_path, replica_path,
        database_name + "." + table_name + " (ReplicatedMergeTreeQueue)",
        data.getDataParts(), current_zookeeper);

    queue.pullLogsToQueue(current_zookeeper);
    last_queue_update_finish_time.store(time(nullptr));
    /// NOTE: not updating last_queue_update_start_time because it must contain the time when
    /// the notification of queue change was received. In the beginning it is effectively infinite.

    StoragePtr ptr = shared_from_this();
    InterserverIOEndpointPtr data_parts_exchange_endpoint = std::make_shared<DataPartsExchange::Service>(data, ptr);
    data_parts_exchange_endpoint_holder = std::make_shared<InterserverIOEndpointHolder>(
        data_parts_exchange_endpoint->getId(replica_path), data_parts_exchange_endpoint, context.getInterserverIOHandler());

    /// In this thread replica will be activated.
    restarting_thread = std::make_unique<ReplicatedMergeTreeRestartingThread>(*this);

    /// Wait while restarting_thread initializes LeaderElection (and so on) or makes first attmept to do it
    startup_event.wait();
}


void StorageReplicatedMergeTree::shutdown()
{
    if (restarting_thread)
    {
        restarting_thread->stop();
        restarting_thread.reset();
    }

    if (data_parts_exchange_endpoint_holder)
    {
        data_parts_exchange_endpoint_holder->getBlocker().cancelForever();
        data_parts_exchange_endpoint_holder = nullptr;
    }

    fetcher.blocker.cancelForever();
}


StorageReplicatedMergeTree::~StorageReplicatedMergeTree()
{
    try
    {
        shutdown();
    }
    catch(...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


BlockInputStreams StorageReplicatedMergeTree::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    const Settings & settings = context.getSettingsRef();

    /** The `select_sequential_consistency` setting has two meanings:
    * 1. To throw an exception if on a replica there are not all parts which have been written down on quorum of remaining replicas.
    * 2. Do not read parts that have not yet been written to the quorum of the replicas.
    * For this you have to synchronously go to ZooKeeper.
    */
    Int64 max_block_number_to_read = 0;
    if (settings.select_sequential_consistency)
    {
        auto zookeeper = getZooKeeper();

        String last_part;
        zookeeper->tryGet(zookeeper_path + "/quorum/last_part", last_part);

        if (!last_part.empty() && !data.getActiveContainingPart(last_part))    /// TODO Disable replica for distributed queries.
            throw Exception("Replica doesn't have part " + last_part + " which was successfully written to quorum of other replicas."
                " Send query to another replica or disable 'select_sequential_consistency' setting.", ErrorCodes::REPLICA_IS_NOT_IN_QUORUM);

        if (last_part.empty())  /// If no part has been written with quorum.
        {
            String quorum_str;
            if (zookeeper->tryGet(zookeeper_path + "/quorum/status", quorum_str))
            {
                ReplicatedMergeTreeQuorumEntry quorum_entry;
                quorum_entry.fromString(quorum_str);
                auto part_info = MergeTreePartInfo::fromPartName(quorum_entry.part_name, data.format_version);
                max_block_number_to_read = part_info.min_block - 1;
            }
        }
        else
        {
            auto part_info = MergeTreePartInfo::fromPartName(last_part, data.format_version);
            max_block_number_to_read = part_info.max_block;
        }
    }

    return reader.read(
        column_names, query_info, context, processed_stage, max_block_size, num_streams, max_block_number_to_read);
}


void StorageReplicatedMergeTree::assertNotReadonly() const
{
    if (is_readonly)
        throw Exception("Table is in readonly mode", ErrorCodes::TABLE_IS_READ_ONLY);
}


BlockOutputStreamPtr StorageReplicatedMergeTree::write(const ASTPtr & /*query*/, const Settings & settings)
{
    assertNotReadonly();

    bool deduplicate = data.settings.replicated_deduplication_window != 0 && settings.insert_deduplicate;

    return std::make_shared<ReplicatedMergeTreeBlockOutputStream>(*this,
        settings.insert_quorum, settings.insert_quorum_timeout.totalMilliseconds(), deduplicate);
}


bool StorageReplicatedMergeTree::optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context)
{
    assertNotReadonly();

    if (!is_leader)
    {
        sendRequestToLeaderReplica(query, context.getSettingsRef());
        return true;
    }

    ReplicatedMergeTreeLogEntryData merge_entry;
    {
        /// We must select parts for merge under merge_selecting_mutex because other threads
        /// (merge_selecting_thread or OPTIMIZE queries) could assign new merges.
        std::lock_guard<std::mutex> merge_selecting_lock(merge_selecting_mutex);

        size_t disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

        MergeTreeDataMergerMutator::FuturePart future_merged_part;
        String disable_reason;
        bool selected = false;

        auto zookeeper = getZooKeeper();
        ReplicatedMergeTreeMergePredicate can_merge = queue.getMergePredicate(zookeeper);

        if (!partition)
        {
            selected = merger_mutator.selectPartsToMerge(
                future_merged_part, true, data.settings.max_bytes_to_merge_at_max_space_in_pool, can_merge, &disable_reason);
        }
        else
        {
            String partition_id = data.getPartitionIDFromQuery(partition, context);
            selected = merger_mutator.selectAllPartsToMergeWithinPartition(
                future_merged_part, disk_space, can_merge, partition_id, final, &disable_reason);
        }

        auto handle_noop = [&] (const String & message)
        {
            if (context.getSettingsRef().optimize_throw_if_noop)
                throw Exception(message, ErrorCodes::CANNOT_ASSIGN_OPTIMIZE);
            return false;
        };

        if (!selected)
        {
            LOG_INFO(log, "Cannot select parts for optimization" + (disable_reason.empty() ? "" : ": " + disable_reason));
            return handle_noop(disable_reason);
        }

        if (!createLogEntryToMergeParts(zookeeper, future_merged_part.parts, future_merged_part.name, deduplicate, &merge_entry))
            return handle_noop("Can't create merge queue node in ZooKeeper");
    }

    /// TODO: Bad setting name for such purpose
    if (context.getSettingsRef().replication_alter_partitions_sync != 0)
        waitForAllReplicasToProcessLogEntry(merge_entry);

    return true;
}


void StorageReplicatedMergeTree::alter(const AlterCommands & params,
    const String & /*database_name*/, const String & /*table_name*/, const Context & context)
{
    assertNotReadonly();

    LOG_DEBUG(log, "Doing ALTER");

    int new_columns_version = -1;   /// Initialization is to suppress (useless) false positive warning found by cppcheck.
    String new_columns_str;
    zkutil::Stat stat;

    {
        /// Just to read current structure. Alter will be done in separate thread.
        auto table_lock = lockStructure(false, __PRETTY_FUNCTION__);

        if (is_readonly)
            throw Exception("Can't ALTER readonly table", ErrorCodes::TABLE_IS_READ_ONLY);

        data.checkAlter(params);

        for (const AlterCommand & param : params)
            if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
                throw Exception("Modification of primary key is not supported for replicated tables", ErrorCodes::NOT_IMPLEMENTED);

        ColumnsDescription new_columns = data.getColumns();
        params.apply(new_columns);

        new_columns_str = new_columns.toString();

        /// Do ALTER.
        getZooKeeper()->set(zookeeper_path + "/columns", new_columns_str, -1, &stat);

        new_columns_version = stat.version;
    }

    LOG_DEBUG(log, "Updated columns in ZooKeeper. Waiting for replicas to apply changes.");

    /// Wait until all replicas will apply ALTER.

    /// Subscribe to change of columns, to finish waiting if someone will do another ALTER.
    if (!getZooKeeper()->exists(zookeeper_path + "/columns", &stat, alter_query_event))
        throw Exception(zookeeper_path + "/columns doesn't exist", ErrorCodes::NOT_FOUND_NODE);

    if (stat.version != new_columns_version)
    {
        LOG_WARNING(log, zookeeper_path + "/columns changed before this ALTER finished; "
            "overlapping ALTER-s are fine but use caution with nontransitive changes");
        return;
    }

    Strings replicas = getZooKeeper()->getChildren(zookeeper_path + "/replicas");

    std::set<String> inactive_replicas;
    std::set<String> timed_out_replicas;

    time_t replication_alter_columns_timeout = context.getSettingsRef().replication_alter_columns_timeout;

    for (const String & replica : replicas)
    {
        LOG_DEBUG(log, "Waiting for " << replica << " to apply changes");

        while (!shutdown_called)
        {
            /// Replica could be inactive.
            if (!getZooKeeper()->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
            {
                LOG_WARNING(log, "Replica " << replica << " is not active during ALTER query."
                    " ALTER will be done asynchronously when replica becomes active.");

                inactive_replicas.emplace(replica);
                break;
            }

            String replica_columns_str;

            /// Replica could has been removed.
            if (!getZooKeeper()->tryGet(zookeeper_path + "/replicas/" + replica + "/columns", replica_columns_str, &stat))
            {
                LOG_WARNING(log, replica << " was removed");
                break;
            }

            int replica_columns_version = stat.version;

            /// The ALTER has been successfully applied.
            if (replica_columns_str == new_columns_str)
                break;

            if (!getZooKeeper()->exists(zookeeper_path + "/columns", &stat))
                throw Exception(zookeeper_path + "/columns doesn't exist", ErrorCodes::NOT_FOUND_NODE);

            if (stat.version != new_columns_version)
            {
                LOG_WARNING(log, zookeeper_path + "/columns changed before ALTER finished; "
                    "overlapping ALTER-s are fine but use caution with nontransitive changes");
                return;
            }

            if (!getZooKeeper()->exists(zookeeper_path + "/replicas/" + replica + "/columns", &stat, alter_query_event))
            {
                LOG_WARNING(log, replica << " was removed");
                break;
            }

            if (stat.version != replica_columns_version)
                continue;

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

        if (shutdown_called)
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
      * This prohibits merges of deleted parts with the new inserted data.
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

    /// Artificial high level is choosen, to make this part "covering" all parts inside.
    part_info = MergeTreePartInfo(partition_id, left, right, MergeTreePartInfo::MAX_LEVEL, mutation_version);
    return true;
}


void StorageReplicatedMergeTree::clearColumnInPartition(
    const ASTPtr & partition, const Field & column_name, const Context & context)
{
    assertNotReadonly();

    /// We don't block merges, so anyone can manage this task (not only leader)

    String partition_id = data.getPartitionIDFromQuery(partition, context);
    MergeTreePartInfo drop_range_info;

    if (!getFakePartCoveringAllPartsInPartition(partition_id, drop_range_info))
    {
        LOG_INFO(log, "Will not clear partition " << partition_id << ", it is empty.");
        return;
    }

    /// We allocated new block number for this part, so new merges can't merge clearing parts with new ones

    LogEntry entry;
    entry.type = LogEntry::CLEAR_COLUMN;
    entry.new_part_name = getPartNamePossiblyFake(data.format_version, drop_range_info);
    entry.column_name = column_name.safeGet<String>();
    entry.create_time = time(nullptr);

    String log_znode_path = getZooKeeper()->create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);
    entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

    /// If necessary, wait until the operation is performed on itself or on all replicas.
    if (context.getSettingsRef().replication_alter_partitions_sync != 0)
    {
        if (context.getSettingsRef().replication_alter_partitions_sync == 1)
            waitForReplicaToProcessLogEntry(replica_name, entry);
        else
            waitForAllReplicasToProcessLogEntry(entry);
    }
}

void StorageReplicatedMergeTree::dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & context)
{
    assertNotReadonly();

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();

    if (!is_leader)
    {
        sendRequestToLeaderReplica(query, context.getSettingsRef());
        return;
    }

    String partition_id = data.getPartitionIDFromQuery(partition, context);
    MergeTreePartInfo drop_range_info;
    if (!getFakePartCoveringAllPartsInPartition(partition_id, drop_range_info))
    {
        LOG_INFO(log, "Will not drop partition " << partition_id << ", it is empty.");
        return;
    }

    clearBlocksInPartition(*zookeeper, partition_id, drop_range_info.min_block, drop_range_info.max_block);

    /** Forbid to choose the parts to be deleted for merging.
      * Invariant: after the `DROP_RANGE` entry appears in the log, merge of deleted parts will not appear in the log.
      */
    String drop_range_fake_part_name = getPartNamePossiblyFake(data.format_version, drop_range_info);
    {
        std::lock_guard<std::mutex> merge_selecting_lock(merge_selecting_mutex);
        queue.disableMergesInRange(drop_range_fake_part_name);
    }

    LOG_DEBUG(log, "Disabled merges covered by range " << drop_range_fake_part_name);

    /// Finally, having achieved the necessary invariants, you can put an entry in the log.
    LogEntry entry;
    entry.type = LogEntry::DROP_RANGE;
    entry.source_replica = replica_name;
    entry.new_part_name = drop_range_fake_part_name;
    entry.detach = detach;
    entry.create_time = time(nullptr);

    String log_znode_path = zookeeper->create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);
    entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

    /// If necessary, wait until the operation is performed on itself or on all replicas.
    if (context.getSettingsRef().replication_alter_partitions_sync != 0)
    {
        if (context.getSettingsRef().replication_alter_partitions_sync == 1)
            waitForReplicaToProcessLogEntry(replica_name, entry);
        else
            waitForAllReplicasToProcessLogEntry(entry);
    }
}


void StorageReplicatedMergeTree::attachPartition(const ASTPtr & partition, bool attach_part, const Context & context)
{
    assertNotReadonly();

    String partition_id;

    if (attach_part)
        partition_id = typeid_cast<const ASTLiteral &>(*partition).value.safeGet<String>();
    else
        partition_id = data.getPartitionIDFromQuery(partition, context);

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
        ActiveDataPartSet active_parts(data.format_version);

        std::set<String> part_names;
        for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
        {
            String name = it.name();
            MergeTreePartInfo part_info;
            if (!MergeTreePartInfo::tryParsePartName(name, &part_info, data.format_version))
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
    std::vector<MergeTreeData::MutableDataPartPtr> loaded_parts;
    for (const String & part : parts)
    {
        LOG_DEBUG(log, "Checking part " << part);
        loaded_parts.push_back(data.loadPartAndFixMetadata(source_dir + part));
    }

    ReplicatedMergeTreeBlockOutputStream output(*this, 0, 0, false);   /// TODO Allow to use quorum here.
    for (auto & part : loaded_parts)
    {
        String old_name = part->name;
        output.writeExistingPart(part);
        LOG_DEBUG(log, "Attached part " << old_name << " as " << part->name);
    }
}


bool StorageReplicatedMergeTree::checkTableCanBeDropped() const
{
    /// Consider only synchronized data
    const_cast<MergeTreeData &>(getData()).recalculateColumnSizes();
    context.checkTableCanBeDropped(database_name, table_name, getData().getTotalActiveSizeInBytes());
    return true;
}


void StorageReplicatedMergeTree::drop()
{
    {
        auto zookeeper = tryGetZooKeeper();

        if (is_readonly || !zookeeper)
            throw Exception("Can't drop readonly replicated table (need to drop data in ZooKeeper as well)", ErrorCodes::TABLE_IS_READ_ONLY);

        // checkTableCanBeDropped(); // uncomment to feel yourself safe

        shutdown();

        if (zookeeper->expired())
            throw Exception("Table was not dropped because ZooKeeper session has expired.", ErrorCodes::TABLE_WAS_NOT_DROPPED);

        LOG_INFO(log, "Removing replica " << replica_path);
        replica_is_active_node = nullptr;
        zookeeper->tryRemoveRecursive(replica_path);

        /// Check that `zookeeper_path` exists: it could have been deleted by another replica after execution of previous line.
        Strings replicas;
        if (zookeeper->tryGetChildren(zookeeper_path + "/replicas", replicas) == ZooKeeperImpl::ZooKeeper::ZOK && replicas.empty())
        {
            LOG_INFO(log, "Removing table " << zookeeper_path << " (this might take several minutes)");
            zookeeper->tryRemoveRecursive(zookeeper_path);
        }
    }

    data.dropAllData();
}


void StorageReplicatedMergeTree::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
    std::string new_full_path = new_path_to_db + escapeForFileName(new_table_name) + '/';

    data.setPath(new_full_path);

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
        std::lock_guard<std::mutex> lock(existing_nodes_cache_mutex);
        if (existing_nodes_cache.count(path))
            return true;
    }

    bool res = getZooKeeper()->exists(path);

    if (res)
    {
        std::lock_guard<std::mutex> lock(existing_nodes_cache_mutex);
        existing_nodes_cache.insert(path);
    }

    return res;
}


std::optional<AbandonableLockInZooKeeper>
StorageReplicatedMergeTree::allocateBlockNumber(
    const String & partition_id, zkutil::ZooKeeperPtr & zookeeper, const String & zookeeper_block_id_path)
{
    /// Lets check for duplicates in advance, to avoid superflous block numbers allocation
    zkutil::Requests deduplication_check_ops;
    if (!zookeeper_block_id_path.empty())
    {
        deduplication_check_ops.emplace_back(zkutil::makeCreateRequest(zookeeper_block_id_path, "", zkutil::CreateMode::Persistent));
        deduplication_check_ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_block_id_path, -1));
    }

    String block_numbers_path = zookeeper_path + "/block_numbers";
    String partition_path = block_numbers_path + "/" + partition_id;

    if (!existsNodeCached(partition_path))
    {
        zkutil::Requests ops;
        ops.push_back(zkutil::makeCreateRequest(partition_path, "", zkutil::CreateMode::Persistent));
        /// We increment data version of the block_numbers node so that it becomes possible
        /// to check in a ZK transaction that the set of partitions didn't change
        /// (unfortunately there is no CheckChildren op).
        ops.push_back(zkutil::makeSetRequest(block_numbers_path, "", -1));

        zkutil::Responses responses;
        int code = zookeeper->tryMulti(ops, responses);
        if (code && code != ZooKeeperImpl::ZooKeeper::ZNODEEXISTS)
            zkutil::KeeperMultiException::check(code, ops, responses);
    }

    AbandonableLockInZooKeeper lock;
    /// 2 RTT
    try
    {
        lock = AbandonableLockInZooKeeper(
            partition_path + "/block-", zookeeper_path + "/temp", *zookeeper, &deduplication_check_ops);
    }
    catch (const zkutil::KeeperMultiException & e)
    {
        if (e.code == ZooKeeperImpl::ZooKeeper::ZNODEEXISTS && e.getPathForFirstFailedOp() == zookeeper_block_id_path)
            return {};

        throw Exception("Cannot allocate block number in ZooKeeper: " + e.displayText(), ErrorCodes::KEEPER_EXCEPTION);
    }
    catch (const zkutil::KeeperException & e)
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

                String log_pointer = getZooKeeper()->get(zookeeper_path + "/replicas/" + replica + "/log_pointer", nullptr, event);
                if (!log_pointer.empty() && parse<UInt64>(log_pointer) > log_index)
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
void StorageReplicatedMergeTree::sendRequestToLeaderReplica(const ASTPtr & query, const Settings & settings)
{
    auto live_replicas = getZooKeeper()->getChildren(zookeeper_path + "/leader_election");
    if (live_replicas.empty())
        throw Exception("No active replicas", ErrorCodes::NO_ACTIVE_REPLICAS);

    std::sort(live_replicas.begin(), live_replicas.end());
    const auto leader = getZooKeeper()->get(zookeeper_path + "/leader_election/" + live_replicas.front());

    if (leader == replica_name)
        throw Exception("Leader was suddenly changed or logical error.", ErrorCodes::LEADERSHIP_CHANGED);

    ReplicatedMergeTreeAddress leader_address(getZooKeeper()->get(zookeeper_path + "/replicas/" + leader + "/host"));

    /// TODO: add setters and getters interface for database and table fields of AST
    auto new_query = query->clone();
    if (auto * alter = typeid_cast<ASTAlterQuery *>(new_query.get()))
    {
        alter->database = leader_address.database;
        alter->table = leader_address.table;
    }
    else if (auto * optimize = typeid_cast<ASTOptimizeQuery *>(new_query.get()))
    {
        optimize->database = leader_address.database;
        optimize->table = leader_address.table;
    }
    else
        throw Exception("Can't proxy this query. Unsupported query type", ErrorCodes::NOT_IMPLEMENTED);

    /// Query send with current user credentials

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(context.getSettingsRef());
    Connection connection(
        leader_address.host,
        leader_address.queries_port,
        leader_address.database,
        context.getClientInfo().current_user, context.getClientInfo().current_password, timeouts, "ClickHouse replica");

    RemoteBlockInputStream stream(connection, formattedAST(new_query), {}, context, &settings);
    NullBlockOutputStream output({});

    copyData(stream, output);
    return;
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

    if (out_absolute_delay < static_cast<time_t>(data.settings.min_relative_delay_to_yield_leadership))
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


void StorageReplicatedMergeTree::fetchPartition(const ASTPtr & partition, const String & from_, const Context & context)
{
    String partition_id = data.getPartitionIDFromQuery(partition, context);

    String from = from_;
    if (from.back() == '/')
        from.resize(from.size() - 1);

    LOG_INFO(log, "Will fetch partition " << partition_id << " from shard " << from_);

    /** Let's check that there is no such partition in the `detached` directory (where we will write the downloaded parts).
      * Unreliable (there is a race condition) - such a partition may appear a little later.
      */
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it{data.getFullPath() + "detached/"}; dir_it != dir_end; ++dir_it)
    {
        MergeTreePartInfo part_info;
        if (MergeTreePartInfo::tryParsePartName(dir_it.name(), &part_info, data.format_version)
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

            zkutil::Stat stat;
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

        if (try_no >= 5)
            throw Exception("Too many retries to fetch parts from " + best_replica_path, ErrorCodes::TOO_MANY_RETRIES_TO_FETCH_PARTS);

        Strings parts = getZooKeeper()->getChildren(best_replica_path + "/parts");
        ActiveDataPartSet active_parts_set(data.format_version, parts);
        Strings parts_to_fetch;

        if (missing_parts.empty())
        {
            parts_to_fetch = active_parts_set.getParts();

            /// Leaving only the parts of the desired partition.
            Strings parts_to_fetch_partition;
            for (const String & part : parts_to_fetch)
            {
                if (MergeTreePartInfo::fromPartName(part, data.format_version).partition_id == partition_id)
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
                if (e.code() != ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER && e.code() != ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS)
                    throw;

                LOG_INFO(log, e.displayText());
                missing_parts.push_back(part);
            }
        }

        ++try_no;
    } while (!missing_parts.empty());
}


void StorageReplicatedMergeTree::freezePartition(const ASTPtr & partition, const String & with_name, const Context & context)
{
    data.freezePartition(partition, with_name, context);
}


void StorageReplicatedMergeTree::mutate(const MutationCommands & commands, const Context &)
{
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

        zkutil::Stat mutations_stat;
        zookeeper->get(mutations_path, &mutations_stat);

        EphemeralLocksInAllPartitions block_number_locks(
            zookeeper_path + "/block_numbers", "block-", zookeeper_path + "/temp", *zookeeper);

        for (const auto & lock : block_number_locks.getLocks())
            entry.block_numbers[lock.partition_id] = lock.number;

        entry.create_time = time(nullptr);

        zkutil::Requests requests;
        requests.emplace_back(zkutil::makeSetRequest(mutations_path, String(), mutations_stat.version));
        requests.emplace_back(zkutil::makeCreateRequest(
            mutations_path + "/", entry.toString(), zkutil::CreateMode::PersistentSequential));

        zkutil::Responses responses;
        int32_t rc = zookeeper->tryMulti(requests, responses);

        if (rc == ZooKeeperImpl::ZooKeeper::ZOK)
            break;
        else if (rc == ZooKeeperImpl::ZooKeeper::ZBADVERSION)
        {
            LOG_TRACE(log, "Version conflict when trying to create a mutation node, retrying...");
            continue;
        }
        else
            throw zkutil::KeeperException("Unable to create a mutation znode", rc);
    }
}


void StorageReplicatedMergeTree::clearOldPartsAndRemoveFromZK()
{
    /// Critical section is not required (since grabOldParts() returns unique part set on each call)

    auto table_lock = lockStructure(false, __PRETTY_FUNCTION__);
    auto zookeeper = getZooKeeper();

    MergeTreeData::DataPartsVector parts = data.grabOldParts();
    if (parts.empty())
        return;

    MergeTreeData::DataPartsVector parts_to_delete_only_from_filesystem;    // Only duplicates
    MergeTreeData::DataPartsVector parts_to_delete_completely;              // All parts except duplicates
    MergeTreeData::DataPartsVector parts_to_retry_deletion;                 // Parts that should be retried due to network problems
    MergeTreeData::DataPartsVector parts_to_remove_from_filesystem;         // Parts removed from ZK

    for (const auto & part : parts)
    {
        if (!part->is_duplicate)
            parts_to_delete_completely.emplace_back(part);
        else
            parts_to_delete_only_from_filesystem.emplace_back(part);
    }
    parts.clear();

    auto remove_parts_from_filesystem = [log=log] (const MergeTreeData::DataPartsVector & parts_to_remove)
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
        data.removePartsFinally(parts_to_delete_only_from_filesystem);

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
        data.rollbackDeletingParts(parts_to_retry_deletion);
        LOG_DEBUG(log, "Will retry deletion of " << parts_to_retry_deletion.size() << " parts in the next time");
    }

    /// Remove parts from filesystem and finally from data_parts
    if (!parts_to_remove_from_filesystem.empty())
    {
        remove_parts_from_filesystem(parts_to_remove_from_filesystem);
        data.removePartsFinally(parts_to_remove_from_filesystem);

        LOG_DEBUG(log, "Removed " << parts_to_remove_from_filesystem.size() << " old parts");
    }
}


bool StorageReplicatedMergeTree::tryRemovePartsFromZooKeeperWithRetries(MergeTreeData::DataPartsVector & parts, size_t max_retries)
{
    Strings part_names_to_remove;
    for (const auto & part : parts)
        part_names_to_remove.emplace_back(part->name);

    return tryRemovePartsFromZooKeeperWithRetries(part_names_to_remove, max_retries);
}

bool StorageReplicatedMergeTree::tryRemovePartsFromZooKeeperWithRetries(const Strings & part_names, size_t max_retries)
{
    using MultiFuture = std::future<ZooKeeperImpl::ZooKeeper::MultiResponse>;

    size_t num_tries = 0;
    bool sucess = false;

    while (!sucess && (max_retries == 0 || num_tries < max_retries))
    {
        std::vector<MultiFuture> futures;
        futures.reserve(part_names.size());

        ++num_tries;
        sucess = true;

        try
        {
            auto zookeeper = getZooKeeper();

            for (const String & part_name : part_names)
            {
                zkutil::Requests ops;
                removePartFromZooKeeper(part_name, ops);

                futures.emplace_back(zookeeper->tryAsyncMulti(ops));
            }

            for (auto & future : futures)
            {
                auto response = future.get();

                if (response.error == 0 || response.error == ZooKeeperImpl::ZooKeeper::ZNONODE)
                    continue;

                if (zkutil::isHardwareError(response.error))
                {
                    sucess = false;
                    continue;
                }

                throw ZooKeeperImpl::Exception(response.error);
            }
        }
        catch (ZooKeeperImpl::Exception & e)
        {
            sucess = false;

            if (zkutil::isHardwareError(e.code))
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            else
                throw;
        }

        if (!sucess && num_tries < max_retries)
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    return sucess;
}

/// TODO: rewrite this code using async Multi ops after final ZooKeeper library update
void StorageReplicatedMergeTree::removePartsFromZooKeeper(zkutil::ZooKeeperPtr & zookeeper, const Strings & part_names,
                                                          NameSet * parts_should_be_retried)
{
    zkutil::Requests ops;
    auto it_first_node_in_batch = part_names.cbegin();

    for (auto it = part_names.cbegin(); it != part_names.cend(); ++it)
    {
        removePartFromZooKeeper(*it, ops);

        auto it_next = std::next(it);
        if (ops.size() >= zkutil::MULTI_BATCH_SIZE || it_next == part_names.cend())
        {
            zkutil::Responses unused_responses;
            auto code = zookeeper->tryMultiNoThrow(ops, unused_responses);
            ops.clear();

            if (code == ZooKeeperImpl::ZooKeeper::ZNONODE)
            {
                /// Fallback
                LOG_DEBUG(log, "There are no some part nodes in ZooKeeper, will remove part nodes sequentially");

                for (auto it_in_batch = it_first_node_in_batch; it_in_batch != it_next; ++it_in_batch)
                {
                    zkutil::Requests cur_ops;
                    removePartFromZooKeeper(*it_in_batch, cur_ops);
                    auto cur_code = zookeeper->tryMultiNoThrow(cur_ops, unused_responses);

                    if (cur_code == ZooKeeperImpl::ZooKeeper::ZNONODE)
                    {
                        LOG_DEBUG(log, "There is no part " << *it_in_batch << " in ZooKeeper, it was only in filesystem");
                    }
                    else if (parts_should_be_retried && zkutil::isHardwareError(cur_code))
                    {
                        parts_should_be_retried->emplace(*it_in_batch);
                    }
                    else if (cur_code)
                    {
                        LOG_WARNING(log, "Cannot remove part " << *it_in_batch << " from ZooKeeper: " << zkutil::ZooKeeper::error2string(cur_code));
                    }
                }
            }
            else if (parts_should_be_retried && zkutil::isHardwareError(code))
            {
                for (auto it_in_batch = it_first_node_in_batch; it_in_batch != it_next; ++it_in_batch)
                    parts_should_be_retried->emplace(*it_in_batch);
            }
            else if (code)
            {
                LOG_WARNING(log, "There was a problem with deleting " << (it_next - it_first_node_in_batch)
                    << " nodes from ZooKeeper: " << ::zkutil::ZooKeeper::error2string(code));
            }

            it_first_node_in_batch = it_next;
        }
    }
}


void StorageReplicatedMergeTree::clearBlocksInPartition(
    zkutil::ZooKeeper & zookeeper, const String & partition_id, Int64 min_block_num, Int64 max_block_num)
{
    Strings blocks;
    if (zookeeper.tryGetChildren(zookeeper_path + "/blocks", blocks))
        throw Exception(zookeeper_path + "/blocks doesn't exist", ErrorCodes::NOT_FOUND_NODE);

    String partition_prefix = partition_id + "_";
    std::vector<std::pair<String, std::future<zkutil::GetResponse>>> get_futures;
    for (const String & block_id : blocks)
    {
        if (startsWith(block_id, partition_prefix))
        {
            String path = zookeeper_path + "/blocks/" + block_id;
            get_futures.emplace_back(path, zookeeper.asyncTryGet(path));
        }
    }

    std::vector<std::pair<String, std::future<zkutil::RemoveResponse>>> to_delete_futures;
    for (auto & pair : get_futures)
    {
        const String & path = pair.first;
        auto result = pair.second.get();

        if (result.error == ZooKeeperImpl::ZooKeeper::ZNONODE)
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
        if (rc == ZooKeeperImpl::ZooKeeper::ZNOTEMPTY)
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
    auto lock1 = lockStructure(false, __PRETTY_FUNCTION__);
    auto lock2 = source_table->lockStructure(false, __PRETTY_FUNCTION__);

    Stopwatch watch;
    MergeTreeData * src_data = data.checkStructureAndGetMergeTreeData(source_table);
    String partition_id = data.getPartitionIDFromQuery(partition, context);

    MergeTreeData::DataPartsVector src_all_parts = src_data->getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    MergeTreeData::DataPartsVector src_parts;
    MergeTreeData::MutableDataPartsVector dst_parts;
    Strings block_id_paths;
    Strings part_checksums;
    std::vector<AbandonableLockInZooKeeper> abandonable_locks;

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

    String drop_range_fake_part_name = getPartNamePossiblyFake(data.format_version, drop_range);

    if (drop_range.getBlocksCount() > 1)
    {
        /// We have to prohibit merges in drop_range, since new merge log entry appeared after this REPLACE FROM entry
        ///  could produce new merged part instead in place of just deleted parts.
        /// It is better to prohibit them on leader replica (like DROP PARTITION makes),
        ///  but it is inconvenient for a user since he could actually use source table from this replica.
        /// Therefore prohibit merges on the initializer server now and on the remaining servers when log entry will be executed.
        /// It does not provides strong guarantees, but is suitable for intended use case (assume merges are quite rare).

        {
            std::lock_guard<std::mutex> merge_selecting_lock(merge_selecting_mutex);
            queue.disableMergesInRange(drop_range_fake_part_name);
        }
    }

    for (size_t i = 0; i < src_all_parts.size(); ++i)
    {
        /// We also make some kind of deduplication to avoid duplicated parts in case of ATTACH PARTITION
        /// Assume that merges in the partiton are quite rare
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
        auto dst_part = data.cloneAndLoadDataPart(src_part, TMP_PREFIX, dst_part_info);

        src_parts.emplace_back(src_part);
        dst_parts.emplace_back(dst_part);
        abandonable_locks.emplace_back(std::move(*lock));
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
        entry_replace.from_database = src_data->database_name;
        entry_replace.from_table = src_data->table_name;
        for (const auto & part : src_parts)
            entry_replace.src_part_names.emplace_back(part->name);
        for (const auto & part : dst_parts)
            entry_replace.new_part_names.emplace_back(part->name);
        for (const String & checksum : part_checksums)
            entry_replace.part_names_checksums.emplace_back(checksum);
        entry_replace.columns_version = columns_version;
    }

    /// We are almost ready to commit changes, remove fetches and merges from drop range
    queue.removePartProducingOpsInRange(zookeeper, drop_range);

    /// Remove deduplication block_ids of replacing parts
    if (replace)
        clearBlocksInPartition(*zookeeper, drop_range.partition_id, drop_range.max_block, drop_range.max_block);

    MergeTreeData::DataPartsVector parts_to_remove;
    zkutil::Responses op_results;

    try
    {
        zkutil::Requests ops;
        for (size_t i = 0; i < dst_parts.size(); ++i)
        {
            getCommitPartOps(ops, dst_parts[i], block_id_paths[i]);
            abandonable_locks[i].getUnlockOps(ops);

            if (ops.size() > zkutil::MULTI_BATCH_SIZE)
            {
                /// It is unnecessary to add parts to working set until we commit log entry
                zookeeper->multi(ops);
                ops.clear();
            }
        }

        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential));

        MergeTreeData::Transaction transaction;
        {
            auto data_parts_lock = data.lockParts();

            for (MergeTreeData::MutableDataPartPtr & part : dst_parts)
                data.renameTempPartAndReplace(part, nullptr, &transaction, data_parts_lock);
        }

        op_results = zookeeper->multi(ops);

        {
            auto data_parts_lock = data.lockParts();

            transaction.commit(&data_parts_lock);
            if (replace)
                parts_to_remove = data.removePartsInRangeFromWorkingSet(drop_range, true, false, data_parts_lock);
        }

        PartLog::addNewParts(this->context, dst_parts, watch.elapsed());
    }
    catch (...)
    {
        PartLog::addNewParts(this->context, dst_parts, watch.elapsed(), ExecutionStatus::fromCurrentException());
        throw;
    }

    String log_znode_path = dynamic_cast<const zkutil::CreateResponse &>(*op_results.back()).path_created;
    entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

    for (auto & lock : abandonable_locks)
        lock.assumeUnlocked();

    /// Forcibly remove replaced parts from ZooKeeper
    tryRemovePartsFromZooKeeperWithRetries(parts_to_remove);

    /// Speedup removing of replaced parts from filesystem
    parts_to_remove.clear();
    cleanup_thread->schedule();

    /// If necessary, wait until the operation is performed on all replicas.
    if (context.getSettingsRef().replication_alter_partitions_sync > 1)
        waitForAllReplicasToProcessLogEntry(entry);
}

void StorageReplicatedMergeTree::getCommitPartOps(
    zkutil::Requests & ops,
    MergeTreeData::MutableDataPartPtr & part,
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

    /// Information about the part, in the replica data.

    ops.emplace_back(zkutil::makeCheckRequest(
        zookeeper_path + "/columns",
        columns_version));
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

ReplicatedMergeTreeAddress StorageReplicatedMergeTree::getReplicatedMergeTreeAddress() const
{
    auto host_port = context.getInterserverIOAddress();

    ReplicatedMergeTreeAddress res;
    res.host = host_port.first;
    res.replication_port = host_port.second;
    res.queries_port = context.getTCPPort();
    res.database = database_name;
    res.table = table_name;
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

        if (shutdown_called)
            throw Exception("Shutdown is called for table", ErrorCodes::ABORTED);
    }

    return cond_reached.load(std::memory_order_relaxed);
}

}
