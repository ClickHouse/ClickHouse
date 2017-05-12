#include <zkutil/Types.h>
#include <zkutil/KeeperException.h>

#include <Core/FieldVisitors.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/ReshardingWorker.h>

#include <Databases/IDatabase.h>

#include <Parsers/formatAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/queryToString.h>

#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>

#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/PartLog.h>

#include <DataStreams/AddingConstColumnBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/copyData.h>

#include <Common/Macros.h>
#include <Common/VirtualColumnUtils.h>
#include <Common/formatReadable.h>
#include <Common/setThreadName.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils.h>

#include <Poco/DirectoryIterator.h>

#include <Common/ThreadPool.h>

#include <ext/range.hpp>
#include <ext/scope_guard.hpp>

#include <cfenv>
#include <ctime>
#include <thread>
#include <future>


namespace ProfileEvents
{
    extern const Event ReplicatedPartMerges;
    extern const Event ReplicatedPartFailedFetches;
    extern const Event ReplicatedPartFetchesOfMerged;
    extern const Event ObsoleteReplicatedParts;
    extern const Event ReplicatedPartFetches;
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
    extern const int TOO_MUCH_RETRIES_TO_FETCH_PARTS;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int PARTITION_DOESNT_EXIST;
    extern const int INCONSISTENT_TABLE_ACCROSS_SHARDS;
    extern const int INSUFFICIENT_SPACE_FOR_RESHARDING;
    extern const int RESHARDING_NO_WORKER;
    extern const int INVALID_PARTITIONS_INTERVAL;
    extern const int RESHARDING_INVALID_PARAMETERS;
    extern const int INVALID_SHARD_WEIGHT;
    extern const int DUPLICATE_SHARD_PATHS;
    extern const int RESHARDING_COORDINATOR_DELETED;
    extern const int RESHARDING_NO_SUCH_COORDINATOR;
    extern const int RESHARDING_NO_COORDINATOR_MEMBERSHIP;
    extern const int RESHARDING_ALREADY_SUBSCRIBED;
    extern const int RESHARDING_INVALID_QUERY;
    extern const int RWLOCK_NO_SUCH_LOCK;
    extern const int NO_SUCH_BARRIER;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int UNFINISHED;
    extern const int METADATA_MISMATCH;
    extern const int RESHARDING_NULLABLE_SHARDING_KEY;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int TOO_MUCH_FETCHES;
}


static const auto QUEUE_UPDATE_ERROR_SLEEP_MS     = 1 * 1000;
static const auto MERGE_SELECTING_SLEEP_MS        = 5 * 1000;

/** Some data numbers are assigned to the data blocks that are integers.
  * For blocks added in the usual way (INSERT), numbers are allocated in ascending order.
  * Merges are made for the range of block numbers on a numeric line
  *  if the block numbers `x`, `z` are involved in the merge, and there is a block with the number `y` that `x < y < z`, then the block with the number `y` is also involved in the merge.
  * This is required to save the properties of some operations that can be performed during a merge - for example, in CollapsingMergeTree.
  * In particular, this allows during the merge to know that in one part all the data was added earlier than all the data in the other part.
  *
  * Occasionally there is a need to add to the table some obviously old part of data,
  *  so that it is perceived as old in the logic of CollapsingMergeTree.
  * You can add such a part of data with a special ATTACH request.
  * And in this case, we need to allocate to this part numbers smaller than the numbers of all the other parts.
  * In this regard, the numbers of the regular parts added by INSERT do not start from zero, but from a larger number,
  *  and smaller numbers are considered "reserved".
  *
  * Why is this number 200?
  * The fact is that previously negative block numbers were not supported.
  * And also, the merge is done that way so that when you increase the number of parts, insertion of new parts slows down on purpose,
  *  until mergers have time to reduce the number of parts; and it was calculated for about 200 parts.
  * So, when you insert all the parts from the other table into the table, 200 is sure enough.
  * In turn, this number is chosen almost at random.
  */
extern const Int64 RESERVED_BLOCK_NUMBERS = 200;


/** There are three places for each part, where it should be
  * 1. In the RAM, MergeTreeData::data_parts, all_data_parts.
  * 2. In the file system (FS), the directory with the data of the table.
  * 3. in ZooKeeper (ZK).
  *
  * When adding a part, it must be added immediately to these three places.
  * This is done like this
  * - [FS] first write the part into a temporary directory on the file system;
  * - [FS] rename the temporary part to the result on the file system;
  * - [RAM] immediately afterwards add it to the `data_parts`, and remove from `data_parts` any parts covered by this one;
  * - [RAM] also set the `Transaction` object, which in case of an exception (in next point),
  *   rolls back the changes in `data_parts` (from the previous point) back;
  * - [ZK] then send a transaction (multi) to add a part to ZooKeeper (and some more actions);
  * - [FS, ZK] by the way, removing the covered (old) parts from file system, from ZooKeeper and from `all_data_parts`
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
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    Context & context_,
    ASTPtr & primary_expr_ast_,
    const String & date_column_name_,
    const ASTPtr & sampling_expression_,
    size_t index_granularity_,
    const MergeTreeData::MergingParams & merging_params_,
    bool has_force_restore_data_flag,
    const MergeTreeSettings & settings_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_}, context(context_),
    current_zookeeper(context.getZooKeeper()), database_name(database_name_),
    table_name(name_), full_path(path_ + escapeForFileName(table_name) + '/'),
    zookeeper_path(context.getMacros().expand(zookeeper_path_)),
    replica_name(context.getMacros().expand(replica_name_)),
    data(database_name, table_name,
        full_path, columns_,
        materialized_columns_, alias_columns_, column_defaults_,
        context_, primary_expr_ast_, date_column_name_,
        sampling_expression_, index_granularity_, merging_params_,
        settings_, database_name_ + "." + table_name, true, attach,
        [this] (const std::string & name) { enqueuePartForCheck(name); }),
    reader(data), writer(data, context), merger(data, context.getBackgroundPool()), fetcher(data), sharded_partition_uploader_client(*this),
    shutdown_event(false), part_check_thread(*this),
    log(&Logger::get(database_name + "." + table_name + " (StorageReplicatedMergeTree)"))
{
    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    replica_path = zookeeper_path + "/replicas/" + replica_name;

    bool skip_sanity_checks = false;

    try
    {
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
    }
    catch (const zkutil::KeeperException & e)
    {
        /// Failed to connect to ZK (this became known when trying to perform the first operation).
        if (e.code == ZCONNECTIONLOSS)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            current_zookeeper = nullptr;
        }
        else
            throw;
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
    }

    createNewZooKeeperNodes();

    String unreplicated_path = full_path + "unreplicated/";
    if (Poco::File(unreplicated_path).exists())
    {
        unreplicated_data = std::make_unique<MergeTreeData>(
            database_name, table_name,
            unreplicated_path, columns_,
            materialized_columns_, alias_columns_, column_defaults_,
            context_, primary_expr_ast_,
            date_column_name_, sampling_expression_, index_granularity_, merging_params_, settings_,
            database_name_ + "." + table_name + "[unreplicated]", /* require_part_metadata = */ false, /* attach = */ true);

        unreplicated_data->loadDataParts(skip_sanity_checks);

        if (unreplicated_data->getDataPartsVector().empty())
        {
            unreplicated_data.reset();
        }
        else
        {
            LOG_INFO(log, "Have unreplicated data");
            unreplicated_reader = std::make_unique<MergeTreeDataSelectExecutor>(*unreplicated_data);
            unreplicated_merger = std::make_unique<MergeTreeDataMerger>(*unreplicated_data, context.getBackgroundPool());
        }
    }

    queue.initialize(
        zookeeper_path, replica_path,
        database_name + "." + table_name + " (ReplicatedMergeTreeQueue)",
        data.getDataParts(), current_zookeeper);

    queue.pullLogsToQueue(current_zookeeper, nullptr);

    /// In this thread replica will be activated.
    restarting_thread = std::make_unique<ReplicatedMergeTreeRestartingThread>(*this);
}


void StorageReplicatedMergeTree::createNewZooKeeperNodes()
{
    auto zookeeper = getZooKeeper();

    /// Working with quorum.
    zookeeper->createIfNotExists(zookeeper_path + "/quorum", "");
    zookeeper->createIfNotExists(zookeeper_path + "/quorum/last_part", "");
    zookeeper->createIfNotExists(zookeeper_path + "/quorum/failed_parts", "");

    /// Tracking lag of replicas.
    zookeeper->createIfNotExists(replica_path + "/min_unprocessed_insert_time", "");
    zookeeper->createIfNotExists(replica_path + "/max_processed_insert_time", "");
}


StoragePtr StorageReplicatedMergeTree::create(
    const String & zookeeper_path_,
    const String & replica_name_,
    bool attach,
    const String & path_, const String & database_name_, const String & name_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    Context & context_,
    ASTPtr & primary_expr_ast_,
    const String & date_column_name_,
    const ASTPtr & sampling_expression_,
    size_t index_granularity_,
    const MergeTreeData::MergingParams & merging_params_,
    bool has_force_restore_data_flag_,
    const MergeTreeSettings & settings_)
{
    auto res = make_shared(
        zookeeper_path_, replica_name_, attach,
        path_, database_name_, name_,
        columns_, materialized_columns_, alias_columns_, column_defaults_,
        context_, primary_expr_ast_, date_column_name_,
        sampling_expression_, index_granularity_,
        merging_params_, has_force_restore_data_flag_, settings_);
    StoragePtr res_ptr = res;

    auto get_endpoint_holder = [&res](InterserverIOEndpointPtr endpoint)
    {
        return std::make_shared<InterserverIOEndpointHolder>(
            endpoint->getId(res->replica_path),
            endpoint,
            res->context.getInterserverIOHandler());
    };

    if (res->tryGetZooKeeper())
    {
        {
            InterserverIOEndpointPtr endpoint = std::make_shared<DataPartsExchange::Service>(res->data, res_ptr);
            res->endpoint_holder = get_endpoint_holder(endpoint);
        }

        /// Services for resharding.

        {
            InterserverIOEndpointPtr endpoint = std::make_shared<RemoteDiskSpaceMonitor::Service>(res->context);
            res->disk_space_monitor_endpoint_holder = get_endpoint_holder(endpoint);
        }

        {
            InterserverIOEndpointPtr endpoint = std::make_shared<ShardedPartitionUploader::Service>(res_ptr);
            res->sharded_partition_uploader_endpoint_holder = get_endpoint_holder(endpoint);
        }

        {
            InterserverIOEndpointPtr endpoint = std::make_shared<RemoteQueryExecutor::Service>(res->context);
            res->remote_query_executor_endpoint_holder = get_endpoint_holder(endpoint);
        }

        {
            InterserverIOEndpointPtr endpoint = std::make_shared<RemotePartChecker::Service>(res_ptr);
            res->remote_part_checker_endpoint_holder = get_endpoint_holder(endpoint);
        }
    }

    return res;
}


static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return "";
    std::stringstream ss;
    formatAST(*ast, ss, 0, false, true);
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

        TableMetadata(const MergeTreeData & data_)
            : data(data_) {}

        void write(WriteBuffer & out) const
        {
            out << "metadata format version: 1" << "\n"
                << "date column: " << data.date_column_name << "\n"
                << "sampling expression: " << formattedAST(data.sampling_expression) << "\n"
                << "index granularity: " << data.index_granularity << "\n"
                << "mode: " << static_cast<int>(data.merging_params.mode) << "\n"
                << "sign column: " << data.merging_params.sign_column << "\n"
                << "primary key: " << formattedAST(data.primary_expr_ast) << "\n";
        }

        String toString() const
        {
            String res;
            WriteBufferFromString out(res);
            write(out);
            return res;
        }

        void check(ReadBuffer & in) const
        {
            /// TODO Can be made less cumbersome.

            in >> "metadata format version: 1";

            in >> "\ndate column: ";
            String read_date_column_name;
            in >> read_date_column_name;

            if (read_date_column_name != data.date_column_name)
                throw Exception("Existing table metadata in ZooKeeper differs in date index column."
                    " Stored in ZooKeeper: " + read_date_column_name + ", local: " + data.date_column_name,
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

    auto acl = zookeeper->getDefaultACL();

    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path, "",
        acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/metadata", metadata,
        acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/columns", ColumnsDescription<false>{
        data.getColumnsListNonMaterialized(), data.materialized_columns,
        data.alias_columns, data.column_defaults}.toString(),
        acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/log", "",
        acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/blocks", "",
        acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/block_numbers", "",
        acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/nonincrement_block_numbers", "",
        acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/leader_election", "",
        acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/temp", "",
        acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/replicas", "",
        acl, zkutil::CreateMode::Persistent));

    auto code = zookeeper->tryMulti(ops);
    if (code != ZOK && code != ZNODEEXISTS)
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
    auto columns_desc = ColumnsDescription<true>::parse(zookeeper->get(zookeeper_path + "/columns", &stat));

    auto & columns = columns_desc.columns;
    auto & materialized_columns = columns_desc.materialized;
    auto & alias_columns = columns_desc.alias;
    auto & column_defaults = columns_desc.defaults;
    columns_version = stat.version;

    if (columns != data.getColumnsListNonMaterialized() ||
        materialized_columns != data.materialized_columns ||
        alias_columns != data.alias_columns ||
        column_defaults != data.column_defaults)
    {
        if (allow_alter &&
            (skip_sanity_checks ||
             data.getColumnsListNonMaterialized().sizeOfDifference(columns) +
             data.materialized_columns.sizeOfDifference(materialized_columns) <= 2))
        {
            LOG_WARNING(log, "Table structure in ZooKeeper is a little different from local table structure. Assuming ALTER.");

            /// Without any locks, because table has not been created yet.
            context.getDatabase(database_name)->alterTable(
                context, table_name,
                columns, materialized_columns, alias_columns, column_defaults, {});

            data.setColumnsList(columns);
            data.materialized_columns = std::move(materialized_columns);
            data.alias_columns = std::move(alias_columns);
            data.column_defaults = std::move(column_defaults);
        }
        else
        {
            throw Exception("Table structure in ZooKeeper is too much different from local table structure.",
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
    auto acl = zookeeper->getDefaultACL();
    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(replica_path, "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(replica_path + "/host", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(replica_path + "/log_pointer", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(replica_path + "/queue", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(replica_path + "/parts", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(replica_path + "/flags", "", acl, zkutil::CreateMode::Persistent));

    try
    {
        zookeeper->multi(ops);
    }
    catch (const zkutil::KeeperException & e)
    {
        if (e.code == ZNODEEXISTS)
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

    Stat stat;
    zookeeper->exists(replica_path, &stat);
    auto my_create_time = stat.czxid;

    std::random_shuffle(replicas.begin(), replicas.end());
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
        ActiveDataPartSet active_parts_set(parts);

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

    zookeeper->create(replica_path + "/columns", ColumnsDescription<false>{
            data.getColumnsListNonMaterialized(),
            data.materialized_columns,
            data.alias_columns,
            data.column_defaults
        }.toString(), zkutil::CreateMode::Persistent);
}


void StorageReplicatedMergeTree::checkParts(bool skip_sanity_checks)
{
    auto zookeeper = getZooKeeper();

    Strings expected_parts_vec = zookeeper->getChildren(replica_path + "/parts");

    /// Parts in ZK.
    NameSet expected_parts(expected_parts_vec.begin(), expected_parts_vec.end());

    MergeTreeData::DataParts parts = data.getAllDataParts();

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
    for (const auto & part : unexpected_parts)
        if (part->level > 0 || part->right < RESERVED_BLOCK_NUMBERS)
            ++unexpected_parts_nonnew;

    String sanity_report = "There are "
            + toString(unexpected_parts.size()) + " unexpected parts ("
            + toString(unexpected_parts_nonnew) + " of them is not just-written), "
            + toString(parts_to_add.size()) + " unexpectedly merged parts, "
            + toString(expected_parts.size()) + " missing obsolete parts, "
            + toString(parts_to_fetch.size()) + " missing parts";

    /** You can automatically synchronize data,
      *  if the number of errors of each of the four types is no more than the corresponding thresholds,
      *  or if the ratio of the total number of errors to the total number of parts (minimum - on the local file system or in ZK)
      *  no more than some ratio (for example 5%).
      *
      * A large number of mismatches in the data on the file system and the expected data
      *  may indicate a configuration error (the server accidentally connected as a replica not from that shard).
      * In this case, the protection mechanism does not allow the server to start.
      */

    size_t min_parts_local_or_expected = std::min(expected_parts_vec.size(), parts.size());
    size_t total_difference = parts_to_add.size() + unexpected_parts_nonnew + expected_parts.size() + parts_to_fetch.size();

    bool insane =
        (parts_to_add.size() > data.settings.replicated_max_unexpectedly_merged_parts
            || unexpected_parts_nonnew > data.settings.replicated_max_unexpected_parts
            || expected_parts.size() > data.settings.replicated_max_missing_obsolete_parts
            || parts_to_fetch.size() > data.settings.replicated_max_missing_active_parts)
        && (total_difference > min_parts_local_or_expected * data.settings.replicated_max_ratio_of_wrong_parts);

    if (insane && !skip_sanity_checks)
        throw Exception("The local set of parts of table " + getTableName() + " doesn't look like the set of parts in ZooKeeper. "
            + sanity_report, ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);

    if (total_difference > 0)
        LOG_WARNING(log, sanity_report);

    /// Add information to the ZK about the parts that cover the missing parts.
    for (const MergeTreeData::DataPartPtr & part : parts_to_add)
    {
        LOG_ERROR(log, "Adding unexpected local part to ZooKeeper: " << part->name);

        zkutil::Ops ops;
        checkPartAndAddToZooKeeper(part, ops);
        zookeeper->multi(ops);
    }

    /// Remove from ZK information about the parts covered by the newly added ones.
    for (const String & name : expected_parts)
    {
        LOG_ERROR(log, "Removing unexpectedly merged local part from ZooKeeper: " << name);

        zkutil::Ops ops;
        removePartFromZooKeeper(name, ops);
        zookeeper->multi(ops);
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
        zkutil::Ops ops;
        removePartFromZooKeeper(name, ops);
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            replica_path + "/queue/queue-", log_entry.toString(), zookeeper->getDefaultACL(), zkutil::CreateMode::PersistentSequential));
        zookeeper->multi(ops);
    }

    /// Remove extra local parts.
    for (const MergeTreeData::DataPartPtr & part : unexpected_parts)
    {
        LOG_ERROR(log, "Renaming unexpected part " << part->name << " to ignored_" + part->name);
        data.renameAndDetachPart(part, "ignored_", true);
    }
}


void StorageReplicatedMergeTree::checkPartAndAddToZooKeeper(
    const MergeTreeData::DataPartPtr & part, zkutil::Ops & ops, String part_name)
{
    auto zookeeper = getZooKeeper();

    if (part_name.empty())
        part_name = part->name;

    check(part->columns);
    int expected_columns_version = columns_version;

    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
    std::random_shuffle(replicas.begin(), replicas.end());
    String expected_columns_str = part->columns.toString();

    for (const String & replica : replicas)
    {
        zkutil::Stat stat_before, stat_after;
        String columns_str;
        if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/parts/" + part_name + "/columns", columns_str, &stat_before))
            continue;
        if (columns_str != expected_columns_str)
        {
            LOG_INFO(log, "Not checking checksums of part " << part_name << " with replica " << replica
                << " because columns are different");
            continue;
        }
        String checksums_str;
        /// Let's check that the node's version with the columns did not change while we were reading the checksums.
        /// This ensures that the columns and the checksum refer to the same data.
        if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/parts/" + part_name + "/checksums", checksums_str) ||
            !zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/parts/" + part_name + "/columns", &stat_after) ||
            stat_before.version != stat_after.version)
        {
            LOG_INFO(log, "Not checking checksums of part " << part_name << " with replica " << replica
                << " because part changed while we were reading its checksums");
            continue;
        }

        auto checksums = MergeTreeData::DataPart::Checksums::parse(checksums_str);
        checksums.checkEqual(part->checksums, true);
    }

    if (zookeeper->exists(replica_path + "/parts/" + part_name))
    {
        LOG_ERROR(log, "checkPartAndAddToZooKeeper: node " << replica_path + "/parts/" + part_name << " already exists");
        return;
    }

    auto acl = zookeeper->getDefaultACL();

    ops.emplace_back(std::make_unique<zkutil::Op::Check>(
        zookeeper_path + "/columns",
        expected_columns_version));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
        replica_path + "/parts/" + part_name,
        "",
        acl,
        zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
        replica_path + "/parts/" + part_name + "/columns",
        part->columns.toString(),
        acl,
        zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
        replica_path + "/parts/" + part_name + "/checksums",
        part->checksums.toString(),
        acl,
        zkutil::CreateMode::Persistent));
}


void StorageReplicatedMergeTree::addNewPartToZooKeeper(const MergeTreeData::DataPartPtr & part, zkutil::Ops & ops, String part_name)
{
    auto zookeeper = getZooKeeper();

    if (part_name.empty())
        part_name = part->name;

    check(part->columns);

    auto acl = zookeeper->getDefaultACL();

    ops.emplace_back(std::make_unique<zkutil::Op::Check>(
        zookeeper_path + "/columns",
        columns_version));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
        replica_path + "/parts/" + part_name,
        "",
        acl,
        zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
        replica_path + "/parts/" + part_name + "/columns",
        part->columns.toString(),
        acl,
        zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
        replica_path + "/parts/" + part_name + "/checksums",
        part->checksums.toString(),
        acl,
        zkutil::CreateMode::Persistent));
}


void StorageReplicatedMergeTree::pullLogsToQueue(zkutil::EventPtr next_update_event)
{
    if (queue.pullLogsToQueue(getZooKeeper(), next_update_event))
    {
        if (queue_task_handle)
            queue_task_handle->wake();
    }
}


bool StorageReplicatedMergeTree::executeLogEntry(const LogEntry & entry)
{
    if (entry.type == LogEntry::DROP_RANGE)
    {
        executeDropRange(entry);
        return true;
    }

    if (entry.type == LogEntry::GET_PART ||
        entry.type == LogEntry::MERGE_PARTS ||
        entry.type == LogEntry::ATTACH_PART)
    {
        /// If we already have this part or a part covering it, we do not need to do anything.
        MergeTreeData::DataPartPtr containing_part = data.getActiveContainingPart(entry.new_part_name);

        /// Even if the part is locally, it (in exceptional cases) may not be in ZooKeeper. Let's check that it is there.
        if (containing_part && getZooKeeper()->exists(replica_path + "/parts/" + containing_part->name))
        {
            if (!(entry.type == LogEntry::GET_PART && entry.source_replica == replica_name))
                LOG_DEBUG(log, "Skipping action for part " << entry.new_part_name << " - part already exists.");
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
    else if (entry.type == LogEntry::ATTACH_PART)
    {
        do_fetch = !executeAttachPart(entry);
    }
    else if (entry.type == LogEntry::MERGE_PARTS)
    {
        std::stringstream log_message;
        log_message << "Executing log entry to merge parts ";
        for (auto i : ext::range(0, entry.parts_to_merge.size()))
            log_message << (i != 0 ? ", " : "") << entry.parts_to_merge[i];
        log_message << " to " << entry.new_part_name;

        LOG_TRACE(log, log_message.rdbuf());

        MergeTreeData::DataPartsVector parts;
        bool have_all_parts = true;
        for (const String & name : entry.parts_to_merge)
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
            /// If you do not have all the necessary parts, try to take some already merged piece from someone.
            do_fetch = true;
            LOG_DEBUG(log, "Don't have all parts for merge " << entry.new_part_name << "; will try to fetch it instead");
        }
        else if (entry.create_time + data.settings.prefer_fetch_merged_part_time_threshold <= time(nullptr))
        {
            /// If entry is old enough, and have enough size, and part are exists in any replica,
            ///  then prefer fetching of merged part from replica.

            size_t sum_parts_size_in_bytes = 0;
            for (const auto & part : parts)
                sum_parts_size_in_bytes += part->size_in_bytes;

            if (sum_parts_size_in_bytes >= data.settings.prefer_fetch_merged_part_size_threshold)
            {
                String replica = findReplicaHavingPart(entry.new_part_name, true);    /// NOTE excessive ZK requests for same data later, may remove.
                if (!replica.empty())
                {
                    do_fetch = true;
                    LOG_DEBUG(log, "Preffering to fetch " << entry.new_part_name << " from replica");
                }
            }
        }

        if (!do_fetch)
        {
            size_t estimated_space_for_merge = MergeTreeDataMerger::estimateDiskSpaceForMerge(parts);

            /// Can throw an exception.
            DiskSpaceMonitor::ReservationPtr reserved_space = DiskSpaceMonitor::reserve(full_path, estimated_space_for_merge);

            auto table_lock = lockStructure(false);

            MergeList::EntryPtr merge_entry = context.getMergeList().insert(database_name, table_name, entry.new_part_name, parts);
            MergeTreeData::Transaction transaction;
            size_t aio_threshold = context.getSettings().min_bytes_to_use_direct_io;

            /// Logging
            Stopwatch stopwatch;

            auto part = merger.mergePartsToTemporaryPart(
                parts, entry.new_part_name, *merge_entry, aio_threshold, entry.create_time, reserved_space.get(), entry.deduplicate);

            zkutil::Ops ops;

            try
            {
                /// Checksums are checked here and `ops` is filled. In fact, the part is added to ZK just below, when executing `multi`.
                checkPartAndAddToZooKeeper(part, ops, entry.new_part_name);
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::CHECKSUM_DOESNT_MATCH
                    || e.code() == ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART)
                {
                    do_fetch = true;
                    part->remove();

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
                }
                else
                    throw;
            }

            if (!do_fetch)
            {
                merger.renameMergedTemporaryPart(parts, part, entry.new_part_name, &transaction);
                getZooKeeper()->multi(ops);        /// After long merge, get fresh ZK handle, because previous session may be expired.

                if (std::shared_ptr<PartLog> part_log = context.getPartLog())
                {
                    PartLogElement elem;
                    elem.event_time = time(0);

                    elem.merged_from.reserve(parts.size());
                    for (const auto & part : parts)
                        elem.merged_from.push_back(part->name);
                    elem.event_type = PartLogElement::MERGE_PARTS;
                    elem.size_in_bytes = part->size_in_bytes;

                    elem.database_name = part->storage.getDatabaseName();
                    elem.table_name = part->storage.getTableName();
                    elem.part_name = part->name;

                    elem.duration_ms = stopwatch.elapsed() / 1000000;

                    part_log->add(elem);

                    elem.duration_ms = 0;
                    elem.event_type = PartLogElement::REMOVE_PART;
                    elem.merged_from = Strings();

                    for (const auto & part : parts)
                    {
                        elem.part_name = part->name;
                        elem.size_in_bytes = part->size_in_bytes;
                        part_log->add(elem);
                    }
                }

                /** Removing old chunks from ZK and from the disk is delayed - see ReplicatedMergeTreeCleanupThread, clearOldParts.
                  */

                /** With `ZCONNECTIONLOSS` or `ZOPERATIONTIMEOUT`, we can inadvertently roll back local changes to the parts.
                  * This is not a problem, because in this case the merge will remain in the queue, and we will try again.
                  */
                transaction.commit();
                merge_selecting_event.set();

                ProfileEvents::increment(ProfileEvents::ReplicatedPartMerges);
            }
        }
    }
    else
    {
        throw Exception("Unexpected log entry type: " + toString(static_cast<int>(entry.type)));
    }

    if (do_fetch)
    {
        String replica = findReplicaHavingCoveringPart(entry, true);

        static std::atomic_uint total_fetches {0};
        if (data.settings.replicated_max_parallel_fetches && total_fetches >= data.settings.replicated_max_parallel_fetches)
        {
            throw Exception("Too much total fetches from replicas, maximum: " + toString(data.settings.replicated_max_parallel_fetches),
                ErrorCodes::TOO_MUCH_FETCHES);
        }

        ++total_fetches;
        SCOPE_EXIT({--total_fetches;});

        if (data.settings.replicated_max_parallel_fetches_for_table && current_table_fetches >= data.settings.replicated_max_parallel_fetches_for_table)
        {
            throw Exception("Too much fetches from replicas for table, maximum: " + toString(data.settings.replicated_max_parallel_fetches_for_table),
                ErrorCodes::TOO_MUCH_FETCHES);
        }

        ++current_table_fetches;
        SCOPE_EXIT({--current_table_fetches;});

        if (replica.empty() && entry.type == LogEntry::ATTACH_PART)
        {
            /** If ATTACH - a piece may not be here, because the replica, on which the part is, still did not have time to attach it.
                * In that case, you need to wait for this.
                */

            /// The part must be on the initiator replica.
            if (entry.source_replica.empty() || entry.source_replica == replica_name)
                throw Exception("Logical error: no source replica specified for ATTACH_PART log entry;"
                    " or trying to fetch part on source replica", ErrorCodes::LOGICAL_ERROR);

            throw Exception("No active replica has attached part " + entry.new_part_name + " or covering part yet", ErrorCodes::NO_REPLICA_HAS_PART);
        }

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

                    if (entry.block_id.empty())
                        throw Exception("Logical error: log entry with quorum has empty block_id", ErrorCodes::LOGICAL_ERROR);

                    LOG_DEBUG(log, "No active replica has part " << entry.new_part_name << " which needs to be written with quorum."
                        " Will try to mark that quorum as failed.");

                    /** Atomically:
                      * - if replicas do not become active;
                      * - if there is a `quorum` node with this part;
                      * - delete `quorum` node;
                      * - set `nonincrement_block_numbers` to resolve merges through the number of the lost part;
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

                    zkutil::Ops ops;

                    for (size_t i = 0, size = replicas.size(); i < size; ++i)
                    {
                        Stat stat;
                        String path = zookeeper_path + "/replicas/" + replicas[i] + "/host";
                        zookeeper->get(path, &stat);
                        ops.emplace_back(std::make_unique<zkutil::Op::Check>(path, stat.version));
                    }

                    /// We verify that while we were collecting versions, the replica with the necessary part did not come alive.
                    replica = findReplicaHavingPart(entry.new_part_name, true);

                    /// Also during this time a completely new replica could be created.
                    /// But if a part does not appear on the old, then it can not be on the new one either.

                    if (replica.empty())
                    {
                        Stat quorum_stat;
                        String quorum_path = zookeeper_path + "/quorum/status";
                        String quorum_str = zookeeper->get(quorum_path, &quorum_stat);
                        ReplicatedMergeTreeQuorumEntry quorum_entry;
                        quorum_entry.fromString(quorum_str);

                        if (quorum_entry.part_name == entry.new_part_name)
                        {
                            ops.emplace_back(std::make_unique<zkutil::Op::Remove>(quorum_path, quorum_stat.version));

                            const auto partition_str = entry.new_part_name.substr(0, 6);
                            ActiveDataPartSet::Part part_info;
                            ActiveDataPartSet::parsePartName(entry.new_part_name, part_info);

                            if (part_info.left != part_info.right)
                                throw Exception("Logical error: log entry with quorum for part covering more than one block number",
                                    ErrorCodes::LOGICAL_ERROR);

                            zookeeper->createIfNotExists(zookeeper_path + "/nonincrement_block_numbers/" + partition_str, "");

                            auto acl = zookeeper->getDefaultACL();

                            ops.emplace_back(std::make_unique<zkutil::Op::Create>(
                                zookeeper_path + "/nonincrement_block_numbers/" + partition_str + "/block-" + padIndex(part_info.left),
                                "",
                                acl,
                                zkutil::CreateMode::Persistent));

                            ops.emplace_back(std::make_unique<zkutil::Op::Create>(
                                zookeeper_path + "/quorum/failed_parts/" + entry.new_part_name,
                                "",
                                acl,
                                zkutil::CreateMode::Persistent));

                            /// Deleting from `blocks`.
                            if (zookeeper->exists(zookeeper_path + "/blocks/" + entry.block_id))
                            {
                                ops.emplace_back(std::make_unique<zkutil::Op::Remove>(zookeeper_path + "/blocks/" + entry.block_id + "/number", -1));
                                ops.emplace_back(std::make_unique<zkutil::Op::Remove>(zookeeper_path + "/blocks/" + entry.block_id + "/checksum", -1));
                                ops.emplace_back(std::make_unique<zkutil::Op::Remove>(zookeeper_path + "/blocks/" + entry.block_id, -1));
                            }

                            auto code = zookeeper->tryMulti(ops);

                            if (code == ZOK)
                            {
                                LOG_DEBUG(log, "Marked quorum for part " << entry.new_part_name << " as failed.");
                                return true;    /// NOTE Deletion from `virtual_parts` is not done, but it is only necessary for merges.
                            }
                            else if (code == ZBADVERSION || code == ZNONODE || code == ZNODEEXISTS)
                            {
                                LOG_DEBUG(log, "State was changed or isn't expected when trying to mark quorum for part "
                                    << entry.new_part_name << " as failed. Code: " << zerror(code));
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
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

            throw;
        }
    }

    return true;
}


void StorageReplicatedMergeTree::executeDropRange(const StorageReplicatedMergeTree::LogEntry & entry)
{
    LOG_INFO(log, (entry.detach ? "Detaching" : "Removing") << " parts inside " << entry.new_part_name << ".");

    queue.removeGetsAndMergesInRange(getZooKeeper(), entry.new_part_name);

    LOG_DEBUG(log, (entry.detach ? "Detaching" : "Removing") << " parts.");
    size_t removed_parts = 0;

    /// Delete the parts contained in the range to be deleted.
    /// It's important that no old parts remain (after the merge), because otherwise,
    ///  after adding a new replica, this new replica downloads them, but does not delete them.
    /// And, if you do not, the parts will come to life after the server is restarted.
    /// Therefore, we use getAllDataParts.
    auto parts = data.getAllDataParts();
    for (const auto & part : parts)
    {
        if (!ActiveDataPartSet::contains(entry.new_part_name, part->name))
            continue;

        LOG_DEBUG(log, "Removing part " << part->name);
        ++removed_parts;

        /// If you do not need to delete a part, it's more reliable to move the directory before making changes to ZooKeeper.
        if (entry.detach)
            data.renameAndDetachPart(part);

        zkutil::Ops ops;
        removePartFromZooKeeper(part->name, ops);
        auto code = getZooKeeper()->tryMulti(ops);

        /// If the part is already removed (for example, because it was never added to ZK due to crash,
        /// see ReplicatedMergeTreeBlockOutputStream), then Ok.
        if (code != ZOK && code != ZNONODE)
            throw zkutil::KeeperException(code);

        /// If the part needs to be removed, it is more reliable to delete the directory after the changes in ZooKeeper.
        if (!entry.detach)
            data.replaceParts({part}, {}, true);
    }

    LOG_INFO(log, (entry.detach ? "Detached " : "Removed ") << removed_parts << " parts inside " << entry.new_part_name << ".");
}


bool StorageReplicatedMergeTree::executeAttachPart(const StorageReplicatedMergeTree::LogEntry & entry)
{
    String source_path = (entry.attach_unreplicated ? "unreplicated/" : "detached/") + entry.source_part_name;

    LOG_INFO(log, "Attaching part " << entry.source_part_name << " from " << source_path << " as " << entry.new_part_name);

    if (!Poco::File(data.getFullPath() + source_path).exists())
    {
        LOG_INFO(log, "No part at " << source_path << ". Will fetch it instead");
        return false;
    }

    LOG_DEBUG(log, "Checking data");
    MergeTreeData::MutableDataPartPtr part = data.loadPartAndFixMetadata(source_path);

    zkutil::Ops ops;
    checkPartAndAddToZooKeeper(part, ops, entry.new_part_name);

    if (entry.attach_unreplicated && unreplicated_data)
    {
        MergeTreeData::DataPartPtr unreplicated_part = unreplicated_data->getPartIfExists(entry.source_part_name);
        if (unreplicated_part)
            unreplicated_data->detachPartInPlace(unreplicated_part);
        else
            LOG_WARNING(log, "Unreplicated part " << entry.source_part_name << " is already detached");
    }

    getZooKeeper()->multi(ops);

    /// NOTE: We can not use `renameTempPartAndAdd`, because the part is not temporary - if something goes wrong, you do not need to delete it.
    part->renameTo(entry.new_part_name);
    part->name = entry.new_part_name;
    ActiveDataPartSet::parsePartName(part->name, *part);

    data.attachPart(part);

    LOG_INFO(log, "Finished attaching part " << entry.new_part_name);

    /// New parts with other data may appear at the place of the deleted parts.
    context.resetCaches();

    return true;
}


void StorageReplicatedMergeTree::queueUpdatingThread()
{
    setThreadName("ReplMTQueueUpd");

    while (!shutdown_called)
    {
        last_queue_update_attempt_time.store(time(nullptr));
        try
        {
            pullLogsToQueue(queue_updating_event);
            last_successful_queue_update_attempt_time.store(time(nullptr));
            queue_updating_event->wait();
        }
        catch (const zkutil::KeeperException & e)
        {
            if (e.code == ZINVALIDSTATE)
                restarting_thread->wakeup();

            tryLogCurrentException(__PRETTY_FUNCTION__);
            queue_updating_event->tryWait(QUEUE_UPDATE_ERROR_SLEEP_MS);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            queue_updating_event->tryWait(QUEUE_UPDATE_ERROR_SLEEP_MS);
        }
    }

    LOG_DEBUG(log, "Queue updating thread finished");
}


bool StorageReplicatedMergeTree::queueTask()
{
    /// This object will mark the element of the queue as running.
    ReplicatedMergeTreeQueue::SelectedEntry selected;

    try
    {
        selected = queue.selectEntryToProcess(merger, data);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
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
            else
                tryLogCurrentException(__PRETTY_FUNCTION__);

            /** This exception will be written to the queue element, and it can be looked up using `system.replication_queue` table.
              * The thread that performs this action will sleep a few seconds after the exception.
              * See `queue.processEntry` function.
              */
            throw;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    });

    /// We will go to sleep if the processing fails and if we have already processed this record recently.
    bool need_sleep = !res && (entry->last_attempt_time - prev_attempt_time < 10);

    /// If there was no exception, you do not need to sleep.
    return !need_sleep;
}


bool StorageReplicatedMergeTree::canMergeParts(
    const MergeTreeData::DataPartPtr & left,
    const MergeTreeData::DataPartPtr & right,
    MemoizedPartsThatCouldBeMerged * memo)
{
    /** It can take a long time to determine whether it is possible to merge two adjacent parts.
      * Two adjacent parts can be merged if all block numbers between their numbers are not used (abandoned).
      * This means that another part can not be inserted between these parts.
      *
      * But if the numbers of adjacent blocks differ sufficiently strongly (usually if there are many "abandoned" blocks between them),
      *  then too many readings are made from ZooKeeper to find out if it's possible to merge them.
      *
      * Let's use a statement that if a couple of parts were possible to merge, and their merge is not yet planned,
      *  then now they can be merged, and we will remember this state (if the parameter `memo` is specified),
      *  not to make many times the same requests to ZooKeeper.
      *
      * TODO I wonder how this is combined with DROP PARTITION and then ATTACH PARTITION.
      */

    /// If any of the parts is already going to be merge into a larger one, do not agree to merge it.
    if (queue.partWillBeMergedOrMergesDisabled(left->name)
        || (left.get() != right.get() && queue.partWillBeMergedOrMergesDisabled(right->name)))
        return false;

    auto key = std::make_pair(left->name, right->name);
    if (memo && memo->count(key))
        return true;

    String month_name = left->name.substr(0, 6);
    auto zookeeper = getZooKeeper();

    /// You can not merge parts, among which is a part for which the quorum is unsatisfied.
    /// Note: theoretically, this could be resolved. But this will make logic more complex.
    String quorum_node_value;
    if (zookeeper->tryGet(zookeeper_path + "/quorum/status", quorum_node_value))
    {
        ReplicatedMergeTreeQuorumEntry quorum_entry;
        quorum_entry.fromString(quorum_node_value);

        ActiveDataPartSet::Part part_info;
        ActiveDataPartSet::parsePartName(quorum_entry.part_name, part_info);

        if (part_info.left != part_info.right)
            throw Exception("Logical error: part written with quorum covers more than one block numbers", ErrorCodes::LOGICAL_ERROR);

        if (left->right <= part_info.left && right->left >= part_info.right)
            return false;
    }

    /// Won't merge last_part even if quorum is satisfied, because we gonna check if replica has this part
    /// on SELECT execution.
    String quorum_last_part;
    if (zookeeper->tryGet(zookeeper_path + "/quorum/last_part", quorum_last_part) && quorum_last_part.empty() == false)
    {
        ActiveDataPartSet::Part part_info;
        ActiveDataPartSet::parsePartName(quorum_last_part, part_info);

        if (part_info.left != part_info.right)
            throw Exception("Logical error: part written with quorum covers more than one block numbers", ErrorCodes::LOGICAL_ERROR);

        if (left->right <= part_info.left && right->left >= part_info.right)
            return false;
    }

    /// You can merge the parts, if all the numbers between them are abandoned - do not correspond to any blocks.
    for (Int64 number = left->right + 1; number <= right->left - 1; ++number)
    {
        /** For numbers before RESERVED_BLOCK_NUMBERS AbandonableLock is not used
            *  - these numbers can not be "abandoned" - that is, not used for parts.
            * These are the part numbers that were added using `ALTER ... ATTACH`.
            * They should go without a gap (for each number there should be a part).
            * We check that for all such numbers there are parts,
            *  otherwise, through the "holes" - missing parts, you can not merge.
            */

        if (number < RESERVED_BLOCK_NUMBERS)
        {
            if (!data.hasBlockNumberInMonth(number, left->month))
                return false;
        }
        else
        {
            String path1 = zookeeper_path +                 "/block_numbers/" + month_name + "/block-" + padIndex(number);
            String path2 = zookeeper_path + "/nonincrement_block_numbers/" + month_name + "/block-" + padIndex(number);

            if (AbandonableLockInZooKeeper::check(path1, *zookeeper) != AbandonableLockInZooKeeper::ABANDONED &&
                AbandonableLockInZooKeeper::check(path2, *zookeeper) != AbandonableLockInZooKeeper::ABANDONED)
                return false;
        }
    }

    if (memo)
        memo->insert(key);

    return true;
}


void StorageReplicatedMergeTree::mergeSelectingThread()
{
    setThreadName("ReplMTMergeSel");
    LOG_DEBUG(log, "Merge selecting thread started");

    bool deduplicate = false; /// TODO: read deduplicate option from table config
    bool need_pull = true;

    MemoizedPartsThatCouldBeMerged memoized_parts_that_could_be_merged;

    auto can_merge = [&memoized_parts_that_could_be_merged, this]
        (const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right) -> bool
    {
        return canMergeParts(left, right, &memoized_parts_that_could_be_merged);
    };

    while (!shutdown_called && is_leader_node)
    {
        bool success = false;

        try
        {
            if (need_pull)
            {
                /// You need to load new entries into the queue before you select parts to merge.
                ///  (so we know which parts are already going to be merged).
                pullLogsToQueue();
                need_pull = false;
            }

            std::lock_guard<std::mutex> merge_selecting_lock(merge_selecting_mutex);

            /** If many merges is already queued, then will queue only small enough merges.
              * Otherwise merge queue could be filled with only large merges,
              *  and in the same time, many small parts could be created and won't be merged.
              */
            size_t merges_queued = queue.countMerges();

            if (merges_queued >= data.settings.max_replicated_merges_in_queue)
            {
                LOG_TRACE(log, "Number of queued merges (" << merges_queued
                    << ") is greater than max_replicated_merges_in_queue ("
                    << data.settings.max_replicated_merges_in_queue << "), so won't select new parts to merge.");
            }
            else
            {
                MergeTreeData::DataPartsVector parts;
                String merged_name;

                size_t max_parts_size_for_merge = merger.getMaxPartsSizeForMerge(data.settings.max_replicated_merges_in_queue, merges_queued);

                if (max_parts_size_for_merge > 0
                    && merger.selectPartsToMerge(
                        parts, merged_name, false,
                        max_parts_size_for_merge,
                        can_merge)
                    && createLogEntryToMergeParts(parts, merged_name, deduplicate))
                {
                    success = true;
                    need_pull = true;
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (shutdown_called || !is_leader_node)
            break;

        if (!success)
            merge_selecting_event.tryWait(MERGE_SELECTING_SLEEP_MS);
    }

    LOG_DEBUG(log, "Merge selecting thread finished");
}


bool StorageReplicatedMergeTree::createLogEntryToMergeParts(
    const MergeTreeData::DataPartsVector & parts, const String & merged_name, bool deduplicate, ReplicatedMergeTreeLogEntryData * out_log_entry)
{
    auto zookeeper = getZooKeeper();

    bool all_in_zk = true;
    for (const auto & part : parts)
    {
        /// If there is no information about part in ZK, we will not merge it.
        if (!zookeeper->exists(replica_path + "/parts/" + part->name))
        {
            all_in_zk = false;

            if (part->modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < time(0))
            {
                LOG_WARNING(log, "Part " << part->name << " (that was selected for merge)"
                    << " with age " << (time(0) - part->modification_time)
                    << " seconds exists locally but not in ZooKeeper."
                    << " Won't do merge with that part and will check it.");
                enqueuePartForCheck(part->name);
            }
        }
    }
    if (!all_in_zk)
        return false;

    LogEntry entry;
    entry.type = LogEntry::MERGE_PARTS;
    entry.source_replica = replica_name;
    entry.new_part_name = merged_name;
    entry.deduplicate = deduplicate;
    entry.create_time = time(0);

    for (const auto & part : parts)
        entry.parts_to_merge.push_back(part->name);

    String path_created = zookeeper->create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);
    entry.znode_name = path_created.substr(path_created.find_last_of('/') + 1);

    String month_name = parts[0]->name.substr(0, 6);
    for (size_t i = 0; i + 1 < parts.size(); ++i)
    {
        /// Remove the unnecessary entries about non-existent blocks.
        for (Int64 number = std::max(RESERVED_BLOCK_NUMBERS, parts[i]->right + 1); number <= parts[i + 1]->left - 1; ++number)
        {
            zookeeper->tryRemove(zookeeper_path +               "/block_numbers/" + month_name + "/block-" + padIndex(number));
            zookeeper->tryRemove(zookeeper_path + "/nonincrement_block_numbers/" + month_name + "/block-" + padIndex(number));
        }
    }

    if (out_log_entry)
        *out_log_entry = entry;

    return true;
}


void StorageReplicatedMergeTree::removePartFromZooKeeper(const String & part_name, zkutil::Ops & ops)
{
    String part_path = replica_path + "/parts/" + part_name;

    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(part_path + "/checksums", -1));
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(part_path + "/columns", -1));
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(part_path, -1));
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

    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
        replica_path + "/queue/queue-", log_entry->toString(), zookeeper->getDefaultACL(),
        zkutil::CreateMode::PersistentSequential));

    removePartFromZooKeeper(part_name, ops);

    auto results = zookeeper->multi(ops);

    String path_created = dynamic_cast<zkutil::Op::Create &>(*ops[0]).getPathCreated();
    log_entry->znode_name = path_created.substr(path_created.find_last_of('/') + 1);
    queue.insert(zookeeper, log_entry);
}


void StorageReplicatedMergeTree::becomeLeader()
{
    std::lock_guard<std::mutex> lock(leader_node_mutex);

    if (shutdown_called)
        return;

    LOG_INFO(log, "Became leader");
    is_leader_node = true;
    merge_selecting_thread = std::thread(&StorageReplicatedMergeTree::mergeSelectingThread, this);
}


String StorageReplicatedMergeTree::findReplicaHavingPart(const String & part_name, bool active)
{
    auto zookeeper = getZooKeeper();
    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

    /// Select replicas in uniformly random order.
    std::random_shuffle(replicas.begin(), replicas.end());

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


String StorageReplicatedMergeTree::findReplicaHavingCoveringPart(const LogEntry & entry, bool active)
{
    auto zookeeper = getZooKeeper();
    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

    /// Select replicas in uniformly random order.
    std::random_shuffle(replicas.begin(), replicas.end());

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
            if (part_on_replica == entry.new_part_name || ActiveDataPartSet::contains(part_on_replica, entry.new_part_name))
            {
                if (largest_part_found.empty() || ActiveDataPartSet::contains(part_on_replica, largest_part_found))
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

            zkutil::Ops ops;
            ops.emplace_back(std::make_unique<zkutil::Op::Remove>(quorum_status_path, stat.version));
            ops.emplace_back(std::make_unique<zkutil::Op::SetData>(quorum_last_part_path, part_name, -1));
            auto code = zookeeper->tryMulti(ops);

            if (code == ZOK)
            {
                break;
            }
            else if (code == ZNONODE)
            {
                /// The quorum has already been achieved.
                break;
            }
            else if (code == ZBADVERSION)
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

            if (code == ZOK)
            {
                break;
            }
            else if (code == ZNONODE)
            {
                /// The quorum has already been achieved.
                break;
            }
            else if (code == ZBADVERSION)
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
        table_lock = lockStructure(true);

    ReplicatedMergeTreeAddress address(getZooKeeper()->get(replica_path + "/host"));

    Stopwatch stopwatch;

    MergeTreeData::MutableDataPartPtr part = fetcher.fetchPart(
        part_name, replica_path, address.host, address.replication_port, to_detached);


    if (!to_detached)
    {
        zkutil::Ops ops;

        /** NOTE
          * Here, an error occurs if ALTER occurred with a change in the column type or column deletion,
          *  and the part on remote server has not yet been modified.
          * After a while, one of the following attempts to make `fetchPart` succeed.
          */
        checkPartAndAddToZooKeeper(part, ops, part_name);

        MergeTreeData::Transaction transaction;
        auto removed_parts = data.renameTempPartAndReplace(part, nullptr, &transaction);

        if (std::shared_ptr<PartLog> part_log = context.getPartLog())
        {
            PartLogElement elem;
            elem.event_time = time(0);
            elem.event_type = PartLogElement::DOWNLOAD_PART;
            elem.size_in_bytes = part->size_in_bytes;
            elem.duration_ms = stopwatch.elapsed() / 10000000;

            elem.merged_from.reserve(removed_parts.size());
            for (const auto & part : removed_parts)
            {
                elem.merged_from.push_back(part->name);
            }

            elem.database_name = part->storage.getDatabaseName();
            elem.table_name = part->storage.getTableName();
            elem.part_name = part->name;

            part_log->add(elem);

            elem.duration_ms = 0;
            elem.event_type = PartLogElement::REMOVE_PART;
            elem.merged_from = Strings();
            for (const auto & part : removed_parts)
            {
                elem.part_name = part->name;
                elem.size_in_bytes = part->size_in_bytes;
                part_log->add(elem);
            }
        }


        getZooKeeper()->multi(ops);
        transaction.commit();

        /** If a quorum is tracked for this part, you must update it.
          * If you do not have time, in case of losing the session, when you restart the server - see the `ReplicatedMergeTreeRestartingThread::updateQuorumIfWeHavePart` method.
          */
        if (quorum)
            updateQuorum(part_name);

        merge_selecting_event.set();

        for (const auto & removed_part : removed_parts)
        {
            LOG_DEBUG(log, "Part " << removed_part->name << " is rendered obsolete by fetching part " << part_name);
            ProfileEvents::increment(ProfileEvents::ObsoleteReplicatedParts);
        }
    }
    else
    {
        Poco::File(data.getFullPath() + "detached/tmp_" + part_name).renameTo(data.getFullPath() + "detached/" + part_name);
    }

    ProfileEvents::increment(ProfileEvents::ReplicatedPartFetches);

    LOG_DEBUG(log, "Fetched part " << part_name << " from " << replica_path << (to_detached ? " (to 'detached' directory)" : ""));
    return true;
}


void StorageReplicatedMergeTree::shutdown()
{
    /** This must be done before waiting for restarting_thread.
      * Because restarting_thread will wait for finishing of tasks in background pool,
      *  and parts are fetched in that tasks.
      */
    fetcher.cancel();

    if (restarting_thread)
    {
        restarting_thread->stop();
        restarting_thread.reset();
    }

    if (endpoint_holder)
    {
        endpoint_holder->cancel();
        endpoint_holder = nullptr;
    }

    if (disk_space_monitor_endpoint_holder)
    {
        disk_space_monitor_endpoint_holder->cancel();
        disk_space_monitor_endpoint_holder = nullptr;
    }
    disk_space_monitor_client.cancel();

    if (sharded_partition_uploader_endpoint_holder)
    {
        sharded_partition_uploader_endpoint_holder->cancel();
        sharded_partition_uploader_endpoint_holder = nullptr;
    }
    sharded_partition_uploader_client.cancel();

    if (remote_query_executor_endpoint_holder)
    {
        remote_query_executor_endpoint_holder->cancel();
        remote_query_executor_endpoint_holder = nullptr;
    }
    remote_query_executor_client.cancel();

    if (remote_part_checker_endpoint_holder)
    {
        remote_part_checker_endpoint_holder->cancel();
        remote_part_checker_endpoint_holder = nullptr;
    }
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
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    /** The table can have two kinds of data
      * - replicated data;
      * - old, non-replicable data - they lie separately and their integrity is not controlled.
      * And the table engine provides the ability to use "virtual columns".
      * One of them - `_replicated` allows you to determine which part of the data is read,
      *  or, if used in WHERE, select data from only one part.
      */

    Names virt_column_names;
    Names real_column_names;
    for (const auto & it : column_names)
        if (it == "_replicated")
            virt_column_names.push_back(it);
        else
            real_column_names.push_back(it);

    auto & select = typeid_cast<const ASTSelectQuery &>(*query);

    /// Try transferring some condition from WHERE to PREWHERE if enabled and viable
    if (settings.optimize_move_to_prewhere && select.where_expression && !select.prewhere_expression && !select.final())
        MergeTreeWhereOptimizer{query, context, data, real_column_names, log};

    Block virtual_columns_block;
    auto column = std::make_shared<ColumnUInt8>(2);
    ColumnPtr column_ptr = column;
    column->getData()[0] = 0;
    column->getData()[1] = 1;
    virtual_columns_block.insert(ColumnWithTypeAndName(column_ptr, std::make_shared<DataTypeUInt8>(), "_replicated"));

    /// If the `_replicated` column is requested, try indexing.
    if (!virt_column_names.empty())
        VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, context);

    std::multiset<UInt8> values = VirtualColumnUtils::extractSingleValueFromBlock<UInt8>(virtual_columns_block, "_replicated");

    BlockInputStreams res;

    size_t part_index = 0;

    /** The `parallel_replica_offset` and `parallel_replicas_count` settings allow you to read one part of the data from one replica, and the other from other replica.
      * For replicated data, the data is broken by the same mechanism as the SAMPLE section.
      * And for non-replicable data, since their integrity between replicas is not controlled,
      *  from the first (settings.parallel_replica_offset == 0) replica all data is selected, and from the rest - none.
      */

    if ((settings.parallel_replica_offset == 0) && unreplicated_reader && values.count(0))
    {
        res = unreplicated_reader->read(real_column_names, query,
                                        context, settings, processed_stage,
                                        max_block_size, threads, &part_index, 0);

        for (auto & virtual_column : virt_column_names)
        {
            if (virtual_column == "_replicated")
            {
                for (auto & stream : res)
                    stream = std::make_shared<AddingConstColumnBlockInputStream<UInt8>>(stream, std::make_shared<DataTypeUInt8>(), 0, "_replicated");
            }
        }
    }

    if (values.count(1))
    {
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

            if (!last_part.empty() && !data.getPartIfExists(last_part))    /// TODO Disable replica for distributed queries.
                throw Exception("Replica doesn't have part " + last_part + " which was successfully written to quorum of other replicas."
                    " Send query to another replica or disable 'select_sequential_consistency' setting.", ErrorCodes::REPLICA_IS_NOT_IN_QUORUM);

            if (last_part.empty())  /// If no part has been written with quorum.
            {
                String quorum_str;
                if (zookeeper->tryGet(zookeeper_path + "/quorum/status", quorum_str))
                {
                    ReplicatedMergeTreeQuorumEntry quorum_entry;
                    quorum_entry.fromString(quorum_str);
                    ActiveDataPartSet::Part part_info;
                    ActiveDataPartSet::parsePartName(quorum_entry.part_name, part_info);
                    max_block_number_to_read = part_info.left - 1;
                }
            }
            else
            {
                ActiveDataPartSet::Part part_info;
                ActiveDataPartSet::parsePartName(last_part, part_info);
                max_block_number_to_read = part_info.right;
            }
        }

        auto res2 = reader.read(
            real_column_names, query, context, settings, processed_stage, max_block_size, threads, &part_index, max_block_number_to_read);

        for (auto & virtual_column : virt_column_names)
        {
            if (virtual_column == "_replicated")
            {
                for (auto & stream : res2)
                    stream = std::make_shared<AddingConstColumnBlockInputStream<UInt8>>(stream, std::make_shared<DataTypeUInt8>(), 1, "_replicated");
            }
        }

        res.insert(res.end(), res2.begin(), res2.end());
    }

    return res;
}


void StorageReplicatedMergeTree::assertNotReadonly() const
{
    if (is_readonly)
        throw Exception("Table is in readonly mode", ErrorCodes::TABLE_IS_READ_ONLY);
}


BlockOutputStreamPtr StorageReplicatedMergeTree::write(ASTPtr query, const Settings & settings)
{
    assertNotReadonly();

    String insert_id;
    if (query)
        if (ASTInsertQuery * insert = typeid_cast<ASTInsertQuery *>(&*query))
            insert_id = insert->insert_id;

    return std::make_shared<ReplicatedMergeTreeBlockOutputStream>(*this, insert_id,
        settings.insert_quorum, settings.insert_quorum_timeout.totalMilliseconds());
}


bool StorageReplicatedMergeTree::optimize(const String & partition, bool final, bool deduplicate, const Settings & settings)
{
    /// If there is nonreplicated data, then merge them first.
    if (unreplicated_data)
    {
        std::lock_guard<std::mutex> lock(unreplicated_mutex);
        unreplicated_data->clearOldParts();

        MergeTreeData::DataPartsVector parts;
        String merged_name;
        auto always_can_merge = [](const MergeTreeData::DataPartPtr & a, const MergeTreeData::DataPartPtr & b) { return true; };

        if (unreplicated_merger->selectPartsToMerge(parts, merged_name, true, 0, always_can_merge))
        {
            MergeList::EntryPtr merge_entry = context.getMergeList().insert(database_name, table_name, merged_name, parts);

            /// Logging
            Stopwatch stopwatch;

            auto new_part = unreplicated_merger->mergePartsToTemporaryPart(
                parts, merged_name, *merge_entry, settings.min_bytes_to_use_direct_io, time(0), nullptr /*disk_reservation*/, deduplicate);

            unreplicated_merger->renameMergedTemporaryPart(parts, new_part, merged_name, nullptr);

            if (std::shared_ptr<PartLog> part_log = context.getPartLog())
            {
                PartLogElement elem;
                elem.event_time = time(0);

                elem.merged_from.reserve(parts.size());
                for (const auto & part : parts)
                    elem.merged_from.push_back(part->name);

                elem.event_type = PartLogElement::MERGE_PARTS;
                elem.size_in_bytes = new_part->size_in_bytes;

                elem.database_name = new_part->storage.getDatabaseName();
                elem.table_name = new_part->storage.getTableName();
                elem.part_name = new_part->name;

                elem.duration_ms = stopwatch.elapsed() / 1000000;

                part_log->add(elem);

                elem.duration_ms = 0;
                elem.event_type = PartLogElement::REMOVE_PART;
                elem.merged_from = Strings();

                for (const auto & part : parts)
                {
                    elem.part_name = part->name;
                    elem.size_in_bytes = part->size_in_bytes;
                    part_log->add(elem);
                }
            }
            return true;
        }
    }

    assertNotReadonly();

    if (!is_leader_node)
        throw Exception("Method OPTIMIZE for ReplicatedMergeTree could be called only on leader replica", ErrorCodes::NOT_IMPLEMENTED);

    auto can_merge = [this]
        (const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right)
    {
        return canMergeParts(left, right, nullptr);
    };

    pullLogsToQueue();

    ReplicatedMergeTreeLogEntryData merge_entry;
    {
        std::lock_guard<std::mutex> merge_selecting_lock(merge_selecting_mutex);

        MergeTreeData::DataPartsVector parts;
        String merged_name;

        size_t disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

        bool selected = false;

        if (partition.empty())
        {
            selected = merger.selectPartsToMerge(parts, merged_name, false, data.settings.max_bytes_to_merge_at_max_space_in_pool, can_merge);
        }
        else
        {
            DayNum_t month = MergeTreeData::getMonthFromName(partition);
            selected = merger.selectAllPartsToMergeWithinPartition(parts, merged_name, disk_space, can_merge, month, final);
        }

        if (!selected)
            return false;

        if (!createLogEntryToMergeParts(parts, merged_name, deduplicate, &merge_entry))
            return false;
    }

    waitForAllReplicasToProcessLogEntry(merge_entry);
    return true;
}


void StorageReplicatedMergeTree::alter(const AlterCommands & params,
    const String & database_name, const String & table_name, const Context & context)
{
    assertNotReadonly();

    LOG_DEBUG(log, "Doing ALTER");

    int new_columns_version;
    String new_columns_str;
    zkutil::Stat stat;

    {
        /// Just to read current structure. Alter will be done in separate thread.
        auto table_lock = lockStructure(false);

        if (is_readonly)
            throw Exception("Can't ALTER readonly table", ErrorCodes::TABLE_IS_READ_ONLY);

        data.checkAlter(params);

        for (const AlterCommand & param : params)
            if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
                throw Exception("Modification of primary key is not supported for replicated tables", ErrorCodes::NOT_IMPLEMENTED);

        NamesAndTypesList new_columns = data.getColumnsListNonMaterialized();
        NamesAndTypesList new_materialized_columns = data.materialized_columns;
        NamesAndTypesList new_alias_columns = data.alias_columns;
        ColumnDefaults new_column_defaults = data.column_defaults;
        params.apply(new_columns, new_materialized_columns, new_alias_columns, new_column_defaults);

        new_columns_str = ColumnsDescription<false>{
            new_columns, new_materialized_columns,
            new_alias_columns, new_column_defaults
        }.toString();

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


/// The name of an imaginary part covering all possible parts in the specified month with numbers in the specified range.
static String getFakePartNameForDrop(const String & month_name, UInt64 left, UInt64 right)
{
    /// The date range is all month long.
    const auto & lut = DateLUT::instance();
    time_t start_time = lut.YYYYMMDDToDate(parse<UInt32>(month_name + "01"));
    DayNum_t left_date = lut.toDayNum(start_time);
    DayNum_t right_date = DayNum_t(static_cast<size_t>(left_date) + lut.daysInMonth(start_time) - 1);

    /// Level - `right-left+1`: part could not have been formed as a result of such or more mergers.
    /// TODO This is not true for parts after ATTACH.
    return ActiveDataPartSet::getPartName(left_date, right_date, left, right, right - left + 1);
}


void StorageReplicatedMergeTree::dropUnreplicatedPartition(const Field & partition, const bool detach, const Settings & settings)
{
    if (!unreplicated_data)
        return;

    /// Asks to complete merges and does not allow them to start.
    /// This protects against the "revival" of data for a remote partition after the completion of the merge.
    auto merge_blocker = unreplicated_merger->cancel();
    auto structure_lock = lockStructure(true);

    const DayNum_t month = MergeTreeData::getMonthDayNum(partition);

    size_t removed_parts = 0;
    MergeTreeData::DataParts parts = unreplicated_data->getDataParts();

    for (const auto & part : parts)
    {
        if (part->month != month)
            continue;

        LOG_DEBUG(log, "Removing unreplicated part " << part->name);
        ++removed_parts;

        if (detach)
            unreplicated_data->renameAndDetachPart(part, "");
        else
            unreplicated_data->replaceParts({part}, {}, false);
    }

    LOG_INFO(log, (detach ? "Detached " : "Removed ") << removed_parts << " unreplicated parts inside " << applyVisitor(FieldVisitorToString(), partition) << ".");
}


void StorageReplicatedMergeTree::dropPartition(
    ASTPtr query, const Field & field, bool detach, bool unreplicated, const Settings & settings)
{
    if (unreplicated)
    {
        dropUnreplicatedPartition(field, detach, settings);
        return;
    }

    assertNotReadonly();

    String month_name = MergeTreeData::getMonthName(field);

    if (!is_leader_node)
    {
        /// Proxy request to the leader.

        auto live_replicas = getZooKeeper()->getChildren(zookeeper_path + "/leader_election");
        if (live_replicas.empty())
            throw Exception("No active replicas", ErrorCodes::NO_ACTIVE_REPLICAS);

        std::sort(live_replicas.begin(), live_replicas.end());
        const auto leader = getZooKeeper()->get(zookeeper_path + "/leader_election/" + live_replicas.front());

        if (leader == replica_name)
            throw Exception("Leader was suddenly changed or logical error.", ErrorCodes::LEADERSHIP_CHANGED);

        ReplicatedMergeTreeAddress leader_address(getZooKeeper()->get(zookeeper_path + "/replicas/" + leader + "/host"));

        auto new_query = query->clone();
        auto & alter = typeid_cast<ASTAlterQuery &>(*new_query);

        alter.database = leader_address.database;
        alter.table = leader_address.table;

        /// NOTE Works only if there is access from the default user without a password. You can fix it by adding a parameter to the server config.

        Connection connection(
            leader_address.host,
            leader_address.queries_port,
            leader_address.database,
            "", "", "ClickHouse replica");

        RemoteBlockInputStream stream(connection, formattedAST(new_query), &settings);
        NullBlockOutputStream output;

        copyData(stream, output);
        return;
    }

    auto number_and_exists = data.getMinBlockNumberForMonth(data.getMonthFromName(month_name));

    /// Even if there is no data in the partition, you still need to mark the range for deletion.
    /// - Because before executing DETACH, tasks for downloading parts to this partition can be executed.
    Int64 left = number_and_exists.second ? number_and_exists.first : RESERVED_BLOCK_NUMBERS;

    /** Let's skip one number in `block_numbers` for the month being deleted, and we will only delete parts until this number.
      * This prohibits merges of deleted parts with the new inserted data.
      * Invariant: merges of deleted parts with other parts do not appear in the log.
      * NOTE: If you need to similarly support a `DROP PART` request, you will have to think of some new mechanism for it,
      *     to guarantee this invariant.
      */
    Int64 right;

    {
        AbandonableLockInZooKeeper block_number_lock = allocateBlockNumber(month_name);
        right = block_number_lock.getNumber();
        block_number_lock.unlock();
    }

    /// This should never happen.
    if (right == 0)
        throw Exception("Logical error: newly allocated block number is zero", ErrorCodes::LOGICAL_ERROR);
    --right;

    String fake_part_name = getFakePartNameForDrop(month_name, left, right);

    /** Forbid to choose the parts to be deleted for merging.
      * Invariant: after the `DROP_RANGE` entry appears in the log, merge of deleted parts will not appear in the log.
      */
    {
        std::lock_guard<std::mutex> merge_selecting_lock(merge_selecting_mutex);
        queue.disableMergesInRange(fake_part_name);
    }

    LOG_DEBUG(log, "Disabled merges in range " << left << " - " << right << " for month " << month_name);

    /// Finally, having achieved the necessary invariants, you can put an entry in the log.
    LogEntry entry;
    entry.type = LogEntry::DROP_RANGE;
    entry.source_replica = replica_name;
    entry.new_part_name = fake_part_name;
    entry.detach = detach;
    String log_znode_path = getZooKeeper()->create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);
    entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);
    entry.create_time = time(0);

    /// If necessary, wait until the operation is performed on itself or on all replicas.
    if (settings.replication_alter_partitions_sync != 0)
    {
        if (settings.replication_alter_partitions_sync == 1)
            waitForReplicaToProcessLogEntry(replica_name, entry);
        else
            waitForAllReplicasToProcessLogEntry(entry);
    }
}


void StorageReplicatedMergeTree::attachPartition(ASTPtr query, const Field & field, bool unreplicated, bool attach_part, const Settings & settings)
{
    assertNotReadonly();

    String partition;

    if (attach_part)
        partition = field.safeGet<String>();
    else
        partition = MergeTreeData::getMonthName(field);

    String source_dir = (unreplicated ? "unreplicated/" : "detached/");

    /// Let's compose a list of parts that should be added.
    Strings parts;
    if (attach_part)
    {
        parts.push_back(partition);
    }
    else
    {
        LOG_DEBUG(log, "Looking for parts for partition " << partition << " in " << source_dir);
        ActiveDataPartSet active_parts;

        std::set<String> part_names;
        for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
        {
            String name = it.name();
            if (!ActiveDataPartSet::isPartDirectory(name))
                continue;
            if (!startsWith(name, partition))
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

    /// Synchronously check that added parts exist and are not spoiled at least on this replica. We will write checksums.txt if it does not exist.
    LOG_DEBUG(log, "Checking parts");
    for (const String & part : parts)
    {
        LOG_DEBUG(log, "Checking part " << part);
        data.loadPartAndFixMetadata(source_dir + part);
    }

    /// Select maximum free numbers to blocks to be added, less than RESERVED_BLOCK_NUMBERS.
    /// NOTE: The free numbers check does not synchronize. You can not perform multiple ATTACH/DETACH/DROP requests at the same time.
    Int64 min_used_number = RESERVED_BLOCK_NUMBERS;
    DayNum_t month = MergeTreeData::getMonthFromPartPrefix(partition);

    auto num_and_exists = data.getMinBlockNumberForMonth(month);
    if (num_and_exists.second && num_and_exists.first < min_used_number)
        min_used_number = num_and_exists.first;

    /// Add entries to the log.
    std::reverse(parts.begin(), parts.end());
    std::list<LogEntry> entries;
    zkutil::Ops ops;
    for (const String & part_name : parts)
    {
        ActiveDataPartSet::Part part;
        ActiveDataPartSet::parsePartName(part_name, part);
        part.left = part.right = --min_used_number;
        part.level = 0;        /// previous level has no sense after attach.
        String new_part_name = ActiveDataPartSet::getPartName(part.left_date, part.right_date, part.left, part.right, part.level);

        LOG_INFO(log, "Will attach " << part_name << " as " << new_part_name);

        entries.emplace_back();
        LogEntry & entry = entries.back();
        entry.type = LogEntry::ATTACH_PART;
        entry.source_replica = replica_name;
        entry.source_part_name = part_name;
        entry.new_part_name = new_part_name;
        entry.attach_unreplicated = unreplicated;
        entry.create_time = time(0);

        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            zookeeper_path + "/log/log-", entry.toString(), getZooKeeper()->getDefaultACL(), zkutil::CreateMode::PersistentSequential));
    }

    LOG_DEBUG(log, "Adding attaches to log");

    getZooKeeper()->multi(ops);

    /// If necessary, wait until the operation is performed on itself or on all replicas.
    if (settings.replication_alter_partitions_sync != 0)
    {
        size_t i = 0;
        for (LogEntry & entry : entries)
        {
            String log_znode_path = dynamic_cast<zkutil::Op::Create &>(*ops[i]).getPathCreated();
            entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

            if (settings.replication_alter_partitions_sync == 1)
                waitForReplicaToProcessLogEntry(replica_name, entry);
            else
                waitForAllReplicasToProcessLogEntry(entry);

            ++i;
        }
    }
}

bool StorageReplicatedMergeTree::checkTableCanBeDropped() const
{
    /// Consider only synchronized data
    const_cast<MergeTreeData &>(getData()).recalculateColumnSizes();
    context.checkTableCanBeDropped(database_name, table_name, getData().getTotalCompressedSize());
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
        if (zookeeper->tryGetChildren(zookeeper_path + "/replicas", replicas) == ZOK && replicas.empty())
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

    data.setPath(new_full_path, true);
    if (unreplicated_data)
        unreplicated_data->setPath(new_full_path + "unreplicated/", false);

    database_name = new_database_name;
    table_name = new_table_name;
    full_path = new_full_path;

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


AbandonableLockInZooKeeper StorageReplicatedMergeTree::allocateBlockNumber(const String & month_name)
{
    auto zookeeper = getZooKeeper();

    String month_path = zookeeper_path + "/block_numbers/" + month_name;
    if (!existsNodeCached(month_path))
    {
        /// Create a node in `block_numbers` for the month and skip over it N = RESERVED_BLOCK_NUMBERS of increment values.
        /// It is necessary that in the future, if needed, you can add data to the beginning.
        zkutil::Ops ops;
        auto acl = zookeeper->getDefaultACL();
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(month_path, "", acl, zkutil::CreateMode::Persistent));
        for (size_t i = 0; i < RESERVED_BLOCK_NUMBERS; ++i)
        {
            ops.emplace_back(std::make_unique<zkutil::Op::Create>(month_path + "/skip_increment", "", acl, zkutil::CreateMode::Persistent));
            ops.emplace_back(std::make_unique<zkutil::Op::Remove>(month_path + "/skip_increment", -1));
        }
        /// Ignore the errors - it could not work if someone else executed this line before us.
        zookeeper->tryMulti(ops);
    }

    return AbandonableLockInZooKeeper(
        zookeeper_path + "/block_numbers/" + month_name + "/block-",
        zookeeper_path + "/temp", *zookeeper);
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

    res.is_leader = is_leader_node;
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

    /// Load in reverse order to preserve consistency (successful update time must be after update start time).
    /// Probably doesn't matter because pullLogsToQueue() acts as a barrier.
    time_t successful_queue_update_time = last_successful_queue_update_attempt_time.load();
    time_t queue_update_time = last_queue_update_attempt_time.load();

    time_t current_time = time(nullptr);

    if (!queue_update_time)
    {
        /// We have not even tried to update queue yet (perhaps replica is readonly).
        /// As we have no info about the current state of replication log, return effectively infinite delay.
        return current_time;
    }
    else if (min_unprocessed_insert_time)
    {
        /// There are some unprocessed insert entries in queue.
        return (current_time > min_unprocessed_insert_time) ? (current_time - min_unprocessed_insert_time) : 0;
    }
    else if (queue_update_time > successful_queue_update_time)
    {
        /// Queue is empty, but there are some in-flight or failed queue update attempts
        /// (likely because of problems with connecting to ZooKeeper).
        /// Return the time passed since last attempt.
        return (current_time > queue_update_time) ? (current_time - queue_update_time) : 0;
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
              * If the replica for some reason has stalled `queueUpdatingThread`,
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


void StorageReplicatedMergeTree::fetchPartition(const Field & partition, const String & from_, const Settings & settings)
{
    String partition_str = MergeTreeData::getMonthName(partition);

    String from = from_;
    if (from.back() == '/')
        from.resize(from.size() - 1);

    LOG_INFO(log, "Will fetch partition " << partition_str << " from shard " << from_);

    /** Let's check that there is no such partition in the `detached` directory (where we will write the downloaded parts).
      * Unreliable (there is a race condition) - such a partition may appear a little later.
      */
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it{data.getFullPath() + "detached/"}; dir_it != dir_end; ++dir_it)
        if (startsWith(dir_it.name(), partition_str))
            throw Exception("Detached partition " + partition_str + " already exists.", ErrorCodes::PARTITION_ALREADY_EXISTS);

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
            throw Exception("Too much retries to fetch parts from " + best_replica_path, ErrorCodes::TOO_MUCH_RETRIES_TO_FETCH_PARTS);

        Strings parts = getZooKeeper()->getChildren(best_replica_path + "/parts");
        ActiveDataPartSet active_parts_set(parts);
        Strings parts_to_fetch;

        if (missing_parts.empty())
        {
            parts_to_fetch = active_parts_set.getParts();

            /// Leaving only the parts of the desired partition.
            Strings parts_to_fetch_partition;
            for (const String & part : parts_to_fetch)
                if (startsWith(part, partition_str))
                    parts_to_fetch_partition.push_back(part);

            parts_to_fetch = std::move(parts_to_fetch_partition);

            if (parts_to_fetch.empty())
                throw Exception("Partition " + partition_str + " on " + best_replica_path + " doesn't exist", ErrorCodes::PARTITION_DOESNT_EXIST);
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


void StorageReplicatedMergeTree::freezePartition(const Field & partition, const String & with_name, const Settings & settings)
{
    /// The prefix can be arbitrary. Not necessarily a month - you can specify only a year.
    String prefix = partition.getType() == Field::Types::UInt64
        ? toString(partition.get<UInt64>())
        : partition.safeGet<String>();

    data.freezePartition(prefix, with_name);
    if (unreplicated_data)
        unreplicated_data->freezePartition(prefix, with_name);
}


void StorageReplicatedMergeTree::reshardPartitions(ASTPtr query, const String & database_name,
    const Field & first_partition, const Field & last_partition,
    const WeightedZooKeeperPaths & weighted_zookeeper_paths,
    const ASTPtr & sharding_key_expr, bool do_copy, const Field & coordinator,
    const Settings & settings)
{
    auto & resharding_worker = context.getReshardingWorker();
    if (!resharding_worker.isStarted())
        throw Exception{"Resharding background thread is not running", ErrorCodes::RESHARDING_NO_WORKER};

    bool has_coordinator = !coordinator.isNull();
    std::string coordinator_id;
    UInt64 block_number = 0;

    /// List of local partitions that need to be resharded.
    ReshardingWorker::PartitionList partition_list;

    /// The aforementioned list comprises:
    /// - first, the list of partitions that are to be resharded on more than one
    /// shard. Given any such partition, a job runs on each shard under the supervision
    /// of a coordinator;
    /// - second, the list of partitions that are to be resharded only on this shard.
    /// The iterator below indicates the beginning of the list of these so-called
    /// uncoordinated partitions.
    ReshardingWorker::PartitionList::const_iterator uncoordinated_begin;

    std::string dumped_coordinator_state;

    auto handle_exception = [&](const std::string & msg = "")
    {
        try
        {
            /// Before jobs are submitted, errors and cancellations are both
            /// considered as errors.
            resharding_worker.setStatus(coordinator_id, ReshardingWorker::STATUS_ERROR, msg);
            dumped_coordinator_state = resharding_worker.dumpCoordinatorState(coordinator_id);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    };

    try
    {
        zkutil::RWLock deletion_lock;

        if (has_coordinator)
        {
            coordinator_id = coordinator.get<const String &>();
            deletion_lock = resharding_worker.createDeletionLock(coordinator_id);
        }

        zkutil::RWLock::Guard<zkutil::RWLock::Read, zkutil::RWLock::NonBlocking> guard{deletion_lock};
        if (!deletion_lock.ownsLock())
            throw Exception{"Coordinator has been deleted", ErrorCodes::RESHARDING_COORDINATOR_DELETED};

        if (has_coordinator)
            block_number = resharding_worker.subscribe(coordinator_id, queryToString(query));

        NameAndTypePair column_desc = ITableDeclaration::getColumn(sharding_key_expr->getColumnName());
        if (column_desc.type->isNullable())
            throw Exception{"Sharding key must not be nullable", ErrorCodes::RESHARDING_NULLABLE_SHARDING_KEY};

        for (const auto & weighted_path : weighted_zookeeper_paths)
        {
            UInt64 weight = weighted_path.second;
            if (weight == 0)
                throw Exception{"Shard has invalid weight", ErrorCodes::INVALID_SHARD_WEIGHT};
        }

        {
            std::vector<std::string> all_paths;
            all_paths.reserve(weighted_zookeeper_paths.size());
            for (const auto & weighted_path : weighted_zookeeper_paths)
                all_paths.push_back(weighted_path.first);
            std::sort(all_paths.begin(), all_paths.end());
            if (std::adjacent_find(all_paths.begin(), all_paths.end()) != all_paths.end())
                throw Exception{"Shard paths must be distinct", ErrorCodes::DUPLICATE_SHARD_PATHS};
        }

        DayNum_t first_partition_num = !first_partition.isNull() ? MergeTreeData::getMonthDayNum(first_partition) : DayNum_t();
        DayNum_t last_partition_num = !last_partition.isNull() ? MergeTreeData::getMonthDayNum(last_partition) : DayNum_t();

        if (first_partition_num && last_partition_num)
        {
            if (first_partition_num > last_partition_num)
                throw Exception{"Invalid interval of partitions", ErrorCodes::INVALID_PARTITIONS_INTERVAL};
        }

        if (!first_partition_num && last_partition_num)
            throw Exception{"Received invalid parameters for resharding", ErrorCodes::RESHARDING_INVALID_PARAMETERS};

        bool include_all = !first_partition_num;

        /// Make a list of local partitions that need to be resharded.
        std::set<std::string> unique_partition_list;
        const MergeTreeData::DataParts & data_parts = data.getDataParts();
        for (MergeTreeData::DataParts::iterator it = data_parts.cbegin(); it != data_parts.cend(); ++it)
        {
            const MergeTreeData::DataPartPtr & current_part = *it;
            DayNum_t month = current_part->month;
            if (include_all || ((month >= first_partition_num) && (month <= last_partition_num)))
                unique_partition_list.insert(MergeTreeData::getMonthName(month));
        }

        partition_list.assign(unique_partition_list.begin(), unique_partition_list.end());

        if (partition_list.empty())
        {
            if (!has_coordinator)
                throw Exception{"No existing partition found", ErrorCodes::PARTITION_DOESNT_EXIST};
        }
        else
        {
            /// Ensure that the local and replicated table structures match.
            enforceShardsConsistency(weighted_zookeeper_paths);

            /// Verify that there is enough free space for all tasks locally and on all replicas.
            auto replica_to_space_info = gatherReplicaSpaceInfo(weighted_zookeeper_paths);
            for (const auto & partition : partition_list)
            {
                size_t partition_size = data.getPartitionSize(partition);
                if (!checkSpaceForResharding(replica_to_space_info, partition_size))
                    throw Exception{"Insufficient space available for resharding operation "
                        "on partition " + partition, ErrorCodes::INSUFFICIENT_SPACE_FOR_RESHARDING};
            }
        }

        if (has_coordinator)
        {
            size_t old_node_count = resharding_worker.getNodeCount(coordinator_id);
            resharding_worker.addPartitions(coordinator_id, partition_list);
            resharding_worker.waitForCheckCompletion(coordinator_id);

            /// At this point, all the performers know exactly the number of partitions
            /// that are to be processed.

            auto count = resharding_worker.getPartitionCount(coordinator_id);
            if (count == 0)
                throw Exception{"No existing partition found", ErrorCodes::PARTITION_DOESNT_EXIST};

            if (partition_list.empty())
            {
                /// We have no partitions, so we opt out.
                resharding_worker.unsubscribe(coordinator_id);
            }

            resharding_worker.waitForOptOutCompletion(coordinator_id, old_node_count);

            /// At this point, all the performers that actually have some partitions
            /// are in a coherent state.

            if (partition_list.empty())
                return;

            if (resharding_worker.getNodeCount(coordinator_id) == 1)
            {
                /// Degenerate case: we are the only participating node.
                /// All our jobs are uncoordinated.
                deletion_lock.release();
                resharding_worker.deleteCoordinator(coordinator_id);
                uncoordinated_begin = partition_list.cbegin();
            }
            else
            {
                /// Split the list of partitions into a list of coordinated jobs
                /// and a list of uncoordinated jobs.
                uncoordinated_begin = resharding_worker.categorizePartitions(coordinator_id, partition_list);
            }

            if (uncoordinated_begin == partition_list.cbegin())
            {
                coordinator_id.clear();
                has_coordinator = false;
            }
        }
        else
        {
            /// All our jobs are uncoordinated.
            uncoordinated_begin = partition_list.cbegin();
        }

        /// First, submit coordinated background resharding jobs.
        for (auto it = partition_list.cbegin(); it != uncoordinated_begin; ++it)
        {
            ReshardingJob job;
            job.database_name = database_name;
            job.table_name = getTableName();
            job.partition = *it;
            job.paths = weighted_zookeeper_paths;
            job.sharding_key_expr = sharding_key_expr;
            job.coordinator_id = coordinator_id;
            job.block_number = block_number;
            job.do_copy = do_copy;

            resharding_worker.submitJob(job);
        }

        /// Then, submit uncoordinated background resharding jobs.
        for (auto it = uncoordinated_begin; it != partition_list.cend(); ++it)
        {
            ReshardingJob job;
            job.database_name = database_name;
            job.table_name = getTableName();
            job.partition = *it;
            job.paths = weighted_zookeeper_paths;
            job.sharding_key_expr = sharding_key_expr;
            job.do_copy = do_copy;

            resharding_worker.submitJob(job);
        }
    }
    catch (const Exception & ex)
    {
        if (has_coordinator)
        {
            if ((ex.code() == ErrorCodes::RESHARDING_NO_SUCH_COORDINATOR) ||
                (ex.code() == ErrorCodes::RESHARDING_NO_COORDINATOR_MEMBERSHIP) ||
                (ex.code() == ErrorCodes::RESHARDING_ALREADY_SUBSCRIBED) ||
                (ex.code() == ErrorCodes::RESHARDING_INVALID_QUERY))
            {
                /// Any of these errors occurs only when a user attempts to send
                /// manually a query ALTER TABLE ... RESHARD ... that specifies
                /// the parameter COORDINATE WITH, in spite of the fact that no user
                /// should ever use this parameter. Since taking into account such
                /// errors may botch an ongoing distributed resharding job, we
                /// intentionally ignore them.
            }
            else if ((ex.code() == ErrorCodes::RWLOCK_NO_SUCH_LOCK) ||
                (ex.code() == ErrorCodes::NO_SUCH_BARRIER) ||
                (ex.code() == ErrorCodes::RESHARDING_COORDINATOR_DELETED))
            {
                /// For any reason the coordinator has disappeared. So obviously
                /// we don't have any means to notify other nodes of an error.
            }
            else if (ex.code() == ErrorCodes::RESHARDING_COORDINATOR_DELETED)
            {
                /// nothing here
            }
            else
            {
                handle_exception(ex.message());
                LOG_ERROR(log, dumped_coordinator_state);
            }
        }

        throw;
    }
    catch (const std::exception & ex)
    {
        if (has_coordinator)
        {
            handle_exception(ex.what());
            LOG_ERROR(log, dumped_coordinator_state);
        }
        throw;
    }
    catch (...)
    {
        if (has_coordinator)
        {
            handle_exception();
            LOG_ERROR(log, dumped_coordinator_state);
        }
        throw;
    }
}

void StorageReplicatedMergeTree::enforceShardsConsistency(const WeightedZooKeeperPaths & weighted_zookeeper_paths)
{
    const auto & columns = getColumnsList();

    auto zookeeper = getZooKeeper();

    for (const auto & weighted_path : weighted_zookeeper_paths)
    {
        auto columns_str = zookeeper->get(weighted_path.first + "/columns");
        auto columns_desc = ColumnsDescription<true>::parse(columns_str);

        if (!std::equal(columns.begin(), columns.end(), columns_desc.columns.begin()))
            throw Exception{"Table is inconsistent accross shards", ErrorCodes::INCONSISTENT_TABLE_ACCROSS_SHARDS};
    }
}

StorageReplicatedMergeTree::ReplicaToSpaceInfo
StorageReplicatedMergeTree::gatherReplicaSpaceInfo(const WeightedZooKeeperPaths & weighted_zookeeper_paths)
{
    struct TaskInfo
    {
        TaskInfo(const std::string & replica_path_,
            const ReplicatedMergeTreeAddress & address_)
            : replica_path(replica_path_), address(address_)
        {
        }

        TaskInfo(const TaskInfo &) = delete;
        TaskInfo & operator=(const TaskInfo &) = delete;

        TaskInfo(TaskInfo &&) = default;
        TaskInfo & operator=(TaskInfo &&) = default;

        std::string replica_path;
        ReplicatedMergeTreeAddress address;
    };

    using TaskInfoList = std::vector<TaskInfo>;
    TaskInfoList task_info_list;

    ReplicaToSpaceInfo replica_to_space_info;

    /// Now we check for free space on the remote replicas.
    UInt64 total_weight = 0;
    for (const auto & weighted_path : weighted_zookeeper_paths)
    {
        UInt64 weight = weighted_path.second;
        total_weight += weight;
    }

    auto & local_space_info = replica_to_space_info[replica_path];
    local_space_info.factor = 1.1;
    local_space_info.available_size = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

    for (const auto & weighted_path : weighted_zookeeper_paths)
    {
        auto zookeeper = getZooKeeper();

        const auto & path = weighted_path.first;
        UInt64 weight = weighted_path.second;

        long double factor = (weight / static_cast<long double>(total_weight)) * 1.1;

        auto children = zookeeper->getChildren(path + "/replicas");
        for (const auto & child : children)
        {
            const std::string child_replica_path = path + "/replicas/" + child;
            if (child_replica_path != replica_path)
            {
                replica_to_space_info[child_replica_path].factor = factor;

                auto host = zookeeper->get(child_replica_path + "/host");
                ReplicatedMergeTreeAddress host_desc(host);

                task_info_list.emplace_back(child_replica_path, host_desc);
            }
        }
    }

    ThreadPool pool(task_info_list.size());

    using Tasks = std::vector<std::packaged_task<size_t()> >;
    Tasks tasks(task_info_list.size());

    try
    {
        for (size_t i = 0; i < task_info_list.size(); ++i)
        {
            const auto & entry = task_info_list[i];
            const auto & replica_path = entry.replica_path;
            const auto & address = entry.address;

            InterserverIOEndpointLocation location{replica_path, address.host, address.replication_port};

            tasks[i] = Tasks::value_type{std::bind(&RemoteDiskSpaceMonitor::Client::getFreeSpace,
                &disk_space_monitor_client, location)};
            pool.schedule([i, &tasks]{ tasks[i](); });
        }
    }
    catch (...)
    {
        pool.wait();
        throw;
    }

    pool.wait();

    for (size_t i = 0; i < task_info_list.size(); ++i)
    {
        size_t remote_available_size = tasks[i].get_future().get();
        const auto & remote_replica_path = task_info_list[i].replica_path;
        replica_to_space_info.at(remote_replica_path).available_size = remote_available_size;
    }

    return replica_to_space_info;
}

bool StorageReplicatedMergeTree::checkSpaceForResharding(const ReplicaToSpaceInfo & replica_to_space_info,
    size_t partition_size) const
{
    /// Safe multiplication.
    auto scale_size = [](size_t size, long double factor)
    {
        feclearexcept(FE_OVERFLOW);
        feclearexcept(FE_UNDERFLOW);

        long double result = static_cast<long double>(size) * factor;

        if ((fetestexcept(FE_OVERFLOW) != 0) || (fetestexcept(FE_UNDERFLOW) != 0))
            throw Exception{"StorageReplicatedMergeTree: floating point exception triggered", ErrorCodes::LOGICAL_ERROR};
        if (result > static_cast<long double>(std::numeric_limits<size_t>::max()))
            throw Exception{"StorageReplicatedMergeTree: integer overflow", ErrorCodes::LOGICAL_ERROR};

        return static_cast<size_t>(result);
    };

    for (const auto & entry : replica_to_space_info)
    {
        const auto & info = entry.second;
        size_t required_size = scale_size(partition_size, info.factor);
        if (info.available_size < required_size)
            return false;
    }

    return true;
}

}
