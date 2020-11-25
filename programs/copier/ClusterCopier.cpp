#include "ClusterCopier.h"

#include "Internals.h"

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/setThreadName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNFINISHED;
    extern const int BAD_ARGUMENTS;
}


void ClusterCopier::init()
{
    auto zookeeper = context.getZooKeeper();

    task_description_watch_callback = [this] (const Coordination::WatchResponse & response)
    {
        if (response.error != Coordination::Error::ZOK)
            return;
        UInt64 version = ++task_description_version;
        LOG_DEBUG(log, "Task description should be updated, local version {}", version);
    };

    task_description_path = task_zookeeper_path + "/description";
    task_cluster = std::make_unique<TaskCluster>(task_zookeeper_path, working_database_name);

    reloadTaskDescription();
    task_cluster_initial_config = task_cluster_current_config;

    task_cluster->loadTasks(*task_cluster_initial_config);
    context.setClustersConfig(task_cluster_initial_config, task_cluster->clusters_prefix);

    /// Set up shards and their priority
    task_cluster->random_engine.seed(task_cluster->random_device());
    for (auto & task_table : task_cluster->table_tasks)
    {
        task_table.cluster_pull = context.getCluster(task_table.cluster_pull_name);
        task_table.cluster_push = context.getCluster(task_table.cluster_push_name);
        task_table.initShards(task_cluster->random_engine);
    }

    LOG_DEBUG(log, "Will process {} table tasks", task_cluster->table_tasks.size());

    /// Do not initialize tables, will make deferred initialization in process()

    zookeeper->createAncestors(getWorkersPathVersion() + "/");
    zookeeper->createAncestors(getWorkersPath() + "/");
}

template <typename T>
decltype(auto) ClusterCopier::retry(T && func, UInt64 max_tries)
{
    std::exception_ptr exception;

    if (max_tries == 0)
        throw Exception("Cannot perform zero retries", ErrorCodes::LOGICAL_ERROR);

    for (UInt64 try_number = 1; try_number <= max_tries; ++try_number)
    {
        try
        {
            return func();
        }
        catch (...)
        {
            exception = std::current_exception();
            if (try_number < max_tries)
            {
                tryLogCurrentException(log, "Will retry");
                std::this_thread::sleep_for(default_sleep_time);
            }
        }
    }

    std::rethrow_exception(exception);
}


void ClusterCopier::discoverShardPartitions(const ConnectionTimeouts & timeouts, const TaskShardPtr & task_shard)
{
    TaskTable & task_table = task_shard->task_table;

    LOG_INFO(log, "Discover partitions of shard {}", task_shard->getDescription());

    auto get_partitions = [&] () { return getShardPartitions(timeouts, *task_shard); };
    auto existing_partitions_names = retry(get_partitions, 60);
    Strings filtered_partitions_names;
    Strings missing_partitions;

    /// Check that user specified correct partition names
    auto check_partition_format = [] (const DataTypePtr & type, const String & partition_text_quoted)
    {
        MutableColumnPtr column_dummy = type->createColumn();
        ReadBufferFromString rb(partition_text_quoted);

        try
        {
            type->deserializeAsTextQuoted(*column_dummy, rb, FormatSettings());
        }
        catch (Exception & e)
        {
            throw Exception("Partition " + partition_text_quoted + " has incorrect format. " + e.displayText(), ErrorCodes::BAD_ARGUMENTS);
        }
    };

    if (task_table.has_enabled_partitions)
    {
        /// Process partition in order specified by <enabled_partitions/>
        for (const String & partition_name : task_table.enabled_partitions)
        {
            /// Check that user specified correct partition names
            check_partition_format(task_shard->partition_key_column.type, partition_name);

            auto it = existing_partitions_names.find(partition_name);

            /// Do not process partition if it is not in enabled_partitions list
            if (it == existing_partitions_names.end())
            {
                missing_partitions.emplace_back(partition_name);
                continue;
            }

            filtered_partitions_names.emplace_back(*it);
        }

        for (const String & partition_name : existing_partitions_names)
        {
            if (!task_table.enabled_partitions_set.count(partition_name))
            {
                LOG_DEBUG(log, "Partition {} will not be processed, since it is not in enabled_partitions of {}", partition_name, task_table.table_id);
            }
        }
    }
    else
    {
        for (const String & partition_name : existing_partitions_names)
            filtered_partitions_names.emplace_back(partition_name);
    }

    for (const String & partition_name : filtered_partitions_names)
    {
        const size_t number_of_splits = task_table.number_of_splits;
        task_shard->partition_tasks.emplace(partition_name, ShardPartition(*task_shard, partition_name, number_of_splits));
        task_shard->checked_partitions.emplace(partition_name, true);

        auto shard_partition_it = task_shard->partition_tasks.find(partition_name);
        PartitionPieces & shard_partition_pieces = shard_partition_it->second.pieces;

        for (size_t piece_number = 0; piece_number < number_of_splits; ++piece_number)
        {
            bool res = checkPresentPartitionPiecesOnCurrentShard(timeouts, *task_shard, partition_name, piece_number);
            shard_partition_pieces.emplace_back(shard_partition_it->second, piece_number, res);
        }
    }

    if (!missing_partitions.empty())
    {
        WriteBufferFromOwnString ss;
        for (const String & missing_partition : missing_partitions)
            ss << " " << missing_partition;

        LOG_WARNING(log, "There are no {} partitions from enabled_partitions in shard {} :{}", missing_partitions.size(), task_shard->getDescription(), ss.str());
    }

    LOG_DEBUG(log, "Will copy {} partitions from shard {}", task_shard->partition_tasks.size(), task_shard->getDescription());
}

void ClusterCopier::discoverTablePartitions(const ConnectionTimeouts & timeouts, TaskTable & task_table, UInt64 num_threads)
{
    /// Fetch partitions list from a shard
    {
        ThreadPool thread_pool(num_threads ? num_threads : 2 * getNumberOfPhysicalCPUCores());

        for (const TaskShardPtr & task_shard : task_table.all_shards)
            thread_pool.scheduleOrThrowOnError([this, timeouts, task_shard]()
            {
                setThreadName("DiscoverPartns");
                discoverShardPartitions(timeouts, task_shard);
            });

        LOG_DEBUG(log, "Waiting for {} setup jobs", thread_pool.active());
        thread_pool.wait();
    }
}

void ClusterCopier::uploadTaskDescription(const std::string & task_path, const std::string & task_file, const bool force)
{
    auto local_task_description_path = task_path + "/description";

    String task_config_str;
    {
        ReadBufferFromFile in(task_file);
        readStringUntilEOF(task_config_str, in);
    }
    if (task_config_str.empty())
        return;

    auto zookeeper = context.getZooKeeper();

    zookeeper->createAncestors(local_task_description_path);
    auto code = zookeeper->tryCreate(local_task_description_path, task_config_str, zkutil::CreateMode::Persistent);
    if (code != Coordination::Error::ZOK && force)
        zookeeper->createOrUpdate(local_task_description_path, task_config_str, zkutil::CreateMode::Persistent);

    LOG_DEBUG(log, "Task description {} uploaded to {} with result {} ({})",
        ((code != Coordination::Error::ZOK && !force) ? "not " : ""), local_task_description_path, code, Coordination::errorMessage(code));
}

void ClusterCopier::reloadTaskDescription()
{
    auto zookeeper = context.getZooKeeper();
    task_description_watch_zookeeper = zookeeper;

    String task_config_str;
    Coordination::Stat stat{};
    Coordination::Error code;

    zookeeper->tryGetWatch(task_description_path, task_config_str, &stat, task_description_watch_callback, &code);
    if (code != Coordination::Error::ZOK)
        throw Exception("Can't get description node " + task_description_path, ErrorCodes::BAD_ARGUMENTS);

    LOG_DEBUG(log, "Loading description, zxid={}", task_description_current_stat.czxid);
    auto config = getConfigurationFromXMLString(task_config_str);

    /// Setup settings
    task_cluster->reloadSettings(*config);
    context.setSettings(task_cluster->settings_common);

    task_cluster_current_config = config;
    task_description_current_stat = stat;
}

void ClusterCopier::updateConfigIfNeeded()
{
    UInt64 version_to_update = task_description_version;
    bool is_outdated_version = task_description_current_version != version_to_update;
    bool is_expired_session  = !task_description_watch_zookeeper || task_description_watch_zookeeper->expired();

    if (!is_outdated_version && !is_expired_session)
        return;

    LOG_DEBUG(log, "Updating task description");
    reloadTaskDescription();

    task_description_current_version = version_to_update;
}

void ClusterCopier::process(const ConnectionTimeouts & timeouts)
{
    for (TaskTable & task_table : task_cluster->table_tasks)
    {
        LOG_INFO(log, "Process table task {} with {} shards, {} of them are local ones", task_table.table_id, task_table.all_shards.size(), task_table.local_shards.size());

        if (task_table.all_shards.empty())
            continue;

        /// Discover partitions of each shard and total set of partitions
        if (!task_table.has_enabled_partitions)
        {
            /// If there are no specified enabled_partitions, we must discover them manually
            discoverTablePartitions(timeouts, task_table);

            /// After partitions of each shard are initialized, initialize cluster partitions
            for (const TaskShardPtr & task_shard : task_table.all_shards)
            {
                for (const auto & partition_elem : task_shard->partition_tasks)
                {
                    const String & partition_name = partition_elem.first;
                    task_table.cluster_partitions.emplace(partition_name, ClusterPartition{});
                }
            }

            for (auto & partition_elem : task_table.cluster_partitions)
            {
                const String & partition_name = partition_elem.first;

                for (const TaskShardPtr & task_shard : task_table.all_shards)
                    task_shard->checked_partitions.emplace(partition_name);

                task_table.ordered_partition_names.emplace_back(partition_name);
            }
        }
        else
        {
            /// If enabled_partitions are specified, assume that each shard has all partitions
            /// We will refine partition set of each shard in future

            for (const String & partition_name : task_table.enabled_partitions)
            {
                task_table.cluster_partitions.emplace(partition_name, ClusterPartition{});
                task_table.ordered_partition_names.emplace_back(partition_name);
            }
        }

        task_table.watch.restart();

        /// Retry table processing
        bool table_is_done = false;
        for (UInt64 num_table_tries = 0; num_table_tries < max_table_tries; ++num_table_tries)
        {
            if (tryProcessTable(timeouts, task_table))
            {
                table_is_done = true;
                break;
            }
        }

        /// Delete helping tables in both cases (whole table is done or not)
        dropHelpingTables(task_table);

        if (!table_is_done)
        {
            throw Exception("Too many tries to process table " + task_table.table_id + ". Abort remaining execution",
                            ErrorCodes::UNFINISHED);
        }
    }
}

/// Protected section


/*
 * Creates task worker node and checks maximum number of workers not to exceed the limit.
 * To achieve this we have to check version of workers_version_path node and create current_worker_path
 * node atomically.
 * */

zkutil::EphemeralNodeHolder::Ptr ClusterCopier::createTaskWorkerNodeAndWaitIfNeed(
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & description,
    bool unprioritized)
{
    std::chrono::milliseconds current_sleep_time = default_sleep_time;
    static constexpr std::chrono::milliseconds max_sleep_time(30000); // 30 sec

    if (unprioritized)
        std::this_thread::sleep_for(current_sleep_time);

    String workers_version_path = getWorkersPathVersion();
    String workers_path         = getWorkersPath();
    String current_worker_path  = getCurrentWorkerNodePath();

    UInt64 num_bad_version_errors = 0;

    while (true)
    {
        updateConfigIfNeeded();

        Coordination::Stat stat;
        zookeeper->get(workers_version_path, &stat);
        auto version = stat.version;
        zookeeper->get(workers_path, &stat);

        if (static_cast<UInt64>(stat.numChildren) >= task_cluster->max_workers)
        {
            LOG_DEBUG(log, "Too many workers ({}, maximum {}). Postpone processing {}", stat.numChildren, task_cluster->max_workers, description);

            if (unprioritized)
                current_sleep_time = std::min(max_sleep_time, current_sleep_time + default_sleep_time);

            std::this_thread::sleep_for(current_sleep_time);
            num_bad_version_errors = 0;
        }
        else
        {
            Coordination::Requests ops;
            ops.emplace_back(zkutil::makeSetRequest(workers_version_path, description, version));
            ops.emplace_back(zkutil::makeCreateRequest(current_worker_path, description, zkutil::CreateMode::Ephemeral));
            Coordination::Responses responses;
            auto code = zookeeper->tryMulti(ops, responses);

            if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNODEEXISTS)
                return std::make_shared<zkutil::EphemeralNodeHolder>(current_worker_path, *zookeeper, false, false, description);

            if (code == Coordination::Error::ZBADVERSION)
            {
                ++num_bad_version_errors;

                /// Try to make fast retries
                if (num_bad_version_errors > 3)
                {
                    LOG_DEBUG(log, "A concurrent worker has just been added, will check free worker slots again");
                    std::chrono::milliseconds random_sleep_time(std::uniform_int_distribution<int>(1, 1000)(task_cluster->random_engine));
                    std::this_thread::sleep_for(random_sleep_time);
                    num_bad_version_errors = 0;
                }
            }
            else
                throw Coordination::Exception(code);
        }
    }
}


bool ClusterCopier::checkPartitionPieceIsClean(
        const zkutil::ZooKeeperPtr & zookeeper,
        const CleanStateClock & clean_state_clock,
        const String & task_status_path)
{
    LogicalClock task_start_clock;

    Coordination::Stat stat{};
    if (zookeeper->exists(task_status_path, &stat))
        task_start_clock = LogicalClock(stat.mzxid);

    return clean_state_clock.is_clean() && (!task_start_clock.hasHappened() || clean_state_clock.discovery_zxid <= task_start_clock);
}


bool ClusterCopier::checkAllPiecesInPartitionAreDone(const TaskTable & task_table, const String & partition_name, const TasksShard & shards_with_partition)
{
    bool answer = true;
    for (size_t piece_number = 0; piece_number < task_table.number_of_splits; ++piece_number)
    {
        bool piece_is_done = checkPartitionPieceIsDone(task_table, partition_name, piece_number, shards_with_partition);
        if (!piece_is_done)
            LOG_DEBUG(log, "Partition {} piece {} is not already done.", partition_name, piece_number);
        answer &= piece_is_done;
    }

    return answer;
}


/* The same as function above
 * Assume that we don't know on which shards do we have partition certain piece.
 * We'll check them all (I mean shards that contain the whole partition)
 * And shards that don't have certain piece MUST mark that piece is_done true.
 * */
bool ClusterCopier::checkPartitionPieceIsDone(const TaskTable & task_table, const String & partition_name,
                               size_t piece_number, const TasksShard & shards_with_partition)
{
    LOG_DEBUG(log, "Check that all shards processed partition {} piece {} successfully", partition_name, piece_number);

    auto zookeeper = context.getZooKeeper();

    /// Collect all shards that contain partition piece number piece_number.
    Strings piece_status_paths;
    for (const auto & shard : shards_with_partition)
    {
        ShardPartition & task_shard_partition = shard->partition_tasks.find(partition_name)->second;
        ShardPartitionPiece & shard_partition_piece = task_shard_partition.pieces[piece_number];
        piece_status_paths.emplace_back(shard_partition_piece.getShardStatusPath());
    }

    std::vector<int64_t> zxid1, zxid2;

    try
    {
        std::vector<zkutil::ZooKeeper::FutureGet> get_futures;
        for (const String & path : piece_status_paths)
            get_futures.emplace_back(zookeeper->asyncGet(path));

        // Check that state is Finished and remember zxid
        for (auto & future : get_futures)
        {
            auto res = future.get();

            TaskStateWithOwner status = TaskStateWithOwner::fromString(res.data);
            if (status.state != TaskState::Finished)
            {
                LOG_INFO(log, "The task {} is being rewritten by {}. Partition piece will be rechecked", res.data, status.owner);
                return false;
            }

            zxid1.push_back(res.stat.pzxid);
        }

        const String piece_is_dirty_flag_path = task_table.getCertainPartitionPieceIsDirtyPath(partition_name, piece_number);
        const String piece_is_dirty_cleaned_path = task_table.getCertainPartitionPieceIsCleanedPath(partition_name, piece_number);
        const String piece_task_status_path = task_table.getCertainPartitionPieceTaskStatusPath(partition_name, piece_number);

        CleanStateClock clean_state_clock (zookeeper, piece_is_dirty_flag_path, piece_is_dirty_cleaned_path);

        const bool is_clean = checkPartitionPieceIsClean(zookeeper, clean_state_clock, piece_task_status_path);


        if (!is_clean)
        {
            LOG_INFO(log, "Partition {} become dirty", partition_name);
            return false;
        }

        get_futures.clear();
        for (const String & path : piece_status_paths)
            get_futures.emplace_back(zookeeper->asyncGet(path));

        // Remember zxid of states again
        for (auto & future : get_futures)
        {
            auto res = future.get();
            zxid2.push_back(res.stat.pzxid);
        }
    }
    catch (const Coordination::Exception & e)
    {
        LOG_INFO(log, "A ZooKeeper error occurred while checking partition {} piece number {}. Will recheck the partition. Error: {}", partition_name, toString(piece_number), e.displayText());
        return false;
    }

    // If all task is finished and zxid is not changed then partition could not become dirty again
    for (UInt64 shard_num = 0; shard_num < piece_status_paths.size(); ++shard_num)
    {
        if (zxid1[shard_num] != zxid2[shard_num])
        {
            LOG_INFO(log, "The task {} is being modified now. Partition piece will be rechecked", piece_status_paths[shard_num]);
            return false;
        }
    }

    LOG_INFO(log, "Partition {} piece number {} is copied successfully", partition_name, toString(piece_number));
    return true;
}


TaskStatus ClusterCopier::tryMoveAllPiecesToDestinationTable(const TaskTable & task_table, const String & partition_name)
{
    bool inject_fault = false;
    if (move_fault_probability > 0)
    {
        double value = std::uniform_real_distribution<>(0, 1)(task_table.task_cluster.random_engine);
        inject_fault = value < move_fault_probability;
    }

    LOG_DEBUG(log, "Try to move {} to destination table", partition_name);

    auto zookeeper = context.getZooKeeper();

    const auto current_partition_attach_is_active = task_table.getPartitionAttachIsActivePath(partition_name);
    const auto current_partition_attach_is_done   = task_table.getPartitionAttachIsDonePath(partition_name);

    /// Create ephemeral node to mark that we are active and process the partition
    zookeeper->createAncestors(current_partition_attach_is_active);
    zkutil::EphemeralNodeHolderPtr partition_attach_node_holder;
    try
    {
        partition_attach_node_holder = zkutil::EphemeralNodeHolder::create(current_partition_attach_is_active, *zookeeper, host_id);
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_DEBUG(log, "Someone is already moving pieces {}", current_partition_attach_is_active);
            return TaskStatus::Active;
        }

        throw;
    }


    /// Exit if task has been already processed;
    /// create blocking node to signal cleaning up if it is abandoned
    {
        String status_data;
        if (zookeeper->tryGet(current_partition_attach_is_done, status_data))
        {
            TaskStateWithOwner status = TaskStateWithOwner::fromString(status_data);
            if (status.state == TaskState::Finished)
            {
                LOG_DEBUG(log, "All pieces for partition from this task {} has been successfully moved to destination table by {}", current_partition_attach_is_active, status.owner);
                return TaskStatus::Finished;
            }

            /// Task is abandoned, because previously we created ephemeral node, possibly in other copier's process.
            /// Initialize DROP PARTITION
            LOG_DEBUG(log, "Moving piece for partition {} has not been successfully finished by {}. Will try to move by myself.", current_partition_attach_is_active, status.owner);

            /// Remove is_done marker.
            zookeeper->remove(current_partition_attach_is_done);
        }
    }


    /// Try start processing, create node about it
    {
        String start_state = TaskStateWithOwner::getData(TaskState::Started, host_id);
        zookeeper->create(current_partition_attach_is_done, start_state, zkutil::CreateMode::Persistent);
    }

    /// Move partition to original destination table.
    for (size_t current_piece_number = 0; current_piece_number < task_table.number_of_splits; ++current_piece_number)
    {
        LOG_DEBUG(log, "Trying to move partition {} piece {} to original table", partition_name, toString(current_piece_number));

        ASTPtr query_alter_ast;
        String query_alter_ast_string;

        DatabaseAndTableName original_table = task_table.table_push;
        DatabaseAndTableName helping_table = DatabaseAndTableName(original_table.first,
                                                                  original_table.second + "_piece_" +
                                                                  toString(current_piece_number));

        Settings settings_push = task_cluster->settings_push;

        /// It is important, ALTER ATTACH PARTITION must be done synchronously
        /// And we will execute this ALTER query on each replica of a shard.
        /// It is correct, because this query is idempotent.
        settings_push.replication_alter_partitions_sync = 2;

        query_alter_ast_string += " ALTER TABLE " + getQuotedTable(original_table) +
                                  ((partition_name == "'all'") ? " ATTACH PARTITION ID " : " ATTACH PARTITION ") + partition_name +
                                  " FROM " + getQuotedTable(helping_table);

        LOG_DEBUG(log, "Executing ALTER query: {}", query_alter_ast_string);

        try
        {
            size_t num_nodes = executeQueryOnCluster(
                    task_table.cluster_push,
                    query_alter_ast_string,
                    settings_push,
                    PoolMode::GET_MANY,
                    ClusterExecutionMode::ON_EACH_NODE);

            LOG_INFO(log, "Number of nodes that executed ALTER query successfully : {}", toString(num_nodes));
        }
        catch (...)
        {
            LOG_DEBUG(log, "Error while moving partition {} piece {} to original table", partition_name, toString(current_piece_number));
            throw;
        }

        if (inject_fault)
            throw Exception("Copy fault injection is activated", ErrorCodes::UNFINISHED);

        try
        {
            String query_deduplicate_ast_string;
            if (!task_table.isReplicatedTable())
            {
                query_deduplicate_ast_string += " OPTIMIZE TABLE " + getQuotedTable(original_table) +
                                                ((partition_name == "'all'") ? " PARTITION ID " : " PARTITION ") + partition_name + " DEDUPLICATE;";

                LOG_DEBUG(log, "Executing OPTIMIZE DEDUPLICATE query: {}", query_alter_ast_string);

                UInt64 num_nodes = executeQueryOnCluster(
                        task_table.cluster_push,
                        query_deduplicate_ast_string,
                        task_cluster->settings_push,
                        PoolMode::GET_MANY);

                LOG_INFO(log, "Number of shard that executed OPTIMIZE DEDUPLICATE query successfully : {}", toString(num_nodes));
            }
        }
        catch (...)
        {
            LOG_DEBUG(log, "Error while executing OPTIMIZE DEDUPLICATE partition {}in the original table", partition_name);
            throw;
        }
    }

    /// Create node to signal that we finished moving
    {
        String state_finished = TaskStateWithOwner::getData(TaskState::Finished, host_id);
        zookeeper->set(current_partition_attach_is_done, state_finished, 0);
    }

    return TaskStatus::Finished;
}

/// Removes MATERIALIZED and ALIAS columns from create table query
ASTPtr ClusterCopier::removeAliasColumnsFromCreateQuery(const ASTPtr & query_ast)
{
    const ASTs & column_asts = query_ast->as<ASTCreateQuery &>().columns_list->columns->children;
    auto new_columns = std::make_shared<ASTExpressionList>();

    for (const ASTPtr & column_ast : column_asts)
    {
        const auto & column = column_ast->as<ASTColumnDeclaration &>();

        if (!column.default_specifier.empty())
        {
            ColumnDefaultKind kind = columnDefaultKindFromString(column.default_specifier);
            if (kind == ColumnDefaultKind::Materialized || kind == ColumnDefaultKind::Alias)
                continue;
        }

        new_columns->children.emplace_back(column_ast->clone());
    }

    ASTPtr new_query_ast = query_ast->clone();
    auto & new_query = new_query_ast->as<ASTCreateQuery &>();

    auto new_columns_list = std::make_shared<ASTColumns>();
    new_columns_list->set(new_columns_list->columns, new_columns);
    if (const auto * indices = query_ast->as<ASTCreateQuery>()->columns_list->indices)
        new_columns_list->set(new_columns_list->indices, indices->clone());

    new_query.replace(new_query.columns_list, new_columns_list);

    return new_query_ast;
}

/// Replaces ENGINE and table name in a create query
std::shared_ptr<ASTCreateQuery> rewriteCreateQueryStorage(const ASTPtr & create_query_ast,
                                                          const DatabaseAndTableName & new_table,
                                                          const ASTPtr & new_storage_ast)
{
    const auto & create = create_query_ast->as<ASTCreateQuery &>();
    auto res = std::make_shared<ASTCreateQuery>(create);

    if (create.storage == nullptr || new_storage_ast == nullptr)
        throw Exception("Storage is not specified", ErrorCodes::LOGICAL_ERROR);

    res->database = new_table.first;
    res->table = new_table.second;

    res->children.clear();
    res->set(res->columns_list, create.columns_list->clone());
    res->set(res->storage, new_storage_ast->clone());

    return res;
}


bool ClusterCopier::tryDropPartitionPiece(
        ShardPartition & task_partition,
        const size_t current_piece_number,
        const zkutil::ZooKeeperPtr & zookeeper,
        const CleanStateClock & clean_state_clock)
{
    if (is_safe_mode)
        throw Exception("DROP PARTITION is prohibited in safe mode", ErrorCodes::NOT_IMPLEMENTED);

    TaskTable & task_table = task_partition.task_shard.task_table;
    ShardPartitionPiece & partition_piece = task_partition.pieces[current_piece_number];

    const String current_shards_path                  = partition_piece.getPartitionPieceShardsPath();
    const String current_partition_active_workers_dir = partition_piece.getPartitionPieceActiveWorkersPath();
    const String is_dirty_flag_path                   = partition_piece.getPartitionPieceIsDirtyPath();
    const String dirty_cleaner_path                   = partition_piece.getPartitionPieceCleanerPath();
    const String is_dirty_cleaned_path                = partition_piece.getPartitionPieceIsCleanedPath();

    zkutil::EphemeralNodeHolder::Ptr cleaner_holder;
    try
    {
        cleaner_holder = zkutil::EphemeralNodeHolder::create(dirty_cleaner_path, *zookeeper, host_id);
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_DEBUG(log, "Partition {} piece {} is cleaning now by somebody, sleep", task_partition.name, toString(current_piece_number));
            std::this_thread::sleep_for(default_sleep_time);
            return false;
        }

        throw;
    }

    Coordination::Stat stat{};
    if (zookeeper->exists(current_partition_active_workers_dir, &stat))
    {
        if (stat.numChildren != 0)
        {
            LOG_DEBUG(log, "Partition {} contains {} active workers while trying to drop it. Going to sleep.", task_partition.name, stat.numChildren);
            std::this_thread::sleep_for(default_sleep_time);
            return false;
        }
        else
        {
            zookeeper->remove(current_partition_active_workers_dir);
        }
    }

    {
        zkutil::EphemeralNodeHolder::Ptr active_workers_lock;
        try
        {
            active_workers_lock = zkutil::EphemeralNodeHolder::create(current_partition_active_workers_dir, *zookeeper, host_id);
        }
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_DEBUG(log, "Partition {} is being filled now by somebody, sleep", task_partition.name);
                return false;
            }

            throw;
        }

        // Lock the dirty flag
        zookeeper->set(is_dirty_flag_path, host_id, clean_state_clock.discovery_version.value());
        zookeeper->tryRemove(partition_piece.getPartitionPieceCleanStartPath());
        CleanStateClock my_clock(zookeeper, is_dirty_flag_path, is_dirty_cleaned_path);

        /// Remove all status nodes
        {
            Strings children;
            if (zookeeper->tryGetChildren(current_shards_path, children) == Coordination::Error::ZOK)
                for (const auto & child : children)
                {
                    zookeeper->removeRecursive(current_shards_path + "/" + child);
                }
        }


        DatabaseAndTableName original_table = task_table.table_push;
        DatabaseAndTableName helping_table = DatabaseAndTableName(original_table.first, original_table.second + "_piece_" + toString(current_piece_number));

        String query = "ALTER TABLE " + getQuotedTable(helping_table);
        query += ((task_partition.name == "'all'") ? " DROP PARTITION ID " : " DROP PARTITION ")  + task_partition.name + "";

        /// TODO: use this statement after servers will be updated up to 1.1.54310
        // query += " DROP PARTITION ID '" + task_partition.name + "'";

        ClusterPtr & cluster_push = task_table.cluster_push;
        Settings settings_push = task_cluster->settings_push;

        /// It is important, DROP PARTITION must be done synchronously
        settings_push.replication_alter_partitions_sync = 2;

        LOG_DEBUG(log, "Execute distributed DROP PARTITION: {}", query);
        /// We have to drop partition_piece on each replica
        size_t num_shards = executeQueryOnCluster(
                cluster_push, query,
                settings_push,
                PoolMode::GET_MANY,
                ClusterExecutionMode::ON_EACH_NODE);

        LOG_INFO(log, "DROP PARTITION was successfully executed on {} nodes of a cluster.", num_shards);

        /// Update the locking node
        if (!my_clock.is_stale())
        {
            zookeeper->set(is_dirty_flag_path, host_id, my_clock.discovery_version.value());
            if (my_clock.clean_state_version)
                zookeeper->set(is_dirty_cleaned_path, host_id, my_clock.clean_state_version.value());
            else
                zookeeper->create(is_dirty_cleaned_path, host_id, zkutil::CreateMode::Persistent);
        }
        else
        {
            LOG_DEBUG(log, "Clean state is altered when dropping the partition, cowardly bailing");
            /// clean state is stale
            return false;
        }

        LOG_INFO(log, "Partition {} piece {} was dropped on cluster {}", task_partition.name, toString(current_piece_number), task_table.cluster_push_name);
        if (zookeeper->tryCreate(current_shards_path, host_id, zkutil::CreateMode::Persistent) == Coordination::Error::ZNODEEXISTS)
            zookeeper->set(current_shards_path, host_id);
    }

    LOG_INFO(log, "Partition {} piece {} is safe for work now.", task_partition.name, toString(current_piece_number));
    return true;
}

bool ClusterCopier::tryProcessTable(const ConnectionTimeouts & timeouts, TaskTable & task_table)
{
    /// An heuristic: if previous shard is already done, then check next one without sleeps due to max_workers constraint
    bool previous_shard_is_instantly_finished = false;

    /// Process each partition that is present in cluster
    for (const String & partition_name : task_table.ordered_partition_names)
    {
        if (!task_table.cluster_partitions.count(partition_name))
            throw Exception("There are no expected partition " + partition_name + ". It is a bug", ErrorCodes::LOGICAL_ERROR);

        ClusterPartition & cluster_partition = task_table.cluster_partitions[partition_name];

        Stopwatch watch;
        /// We will check all the shards of the table and check if they contain current partition.
        TasksShard expected_shards;
        UInt64 num_failed_shards = 0;

        ++cluster_partition.total_tries;

        LOG_DEBUG(log, "Processing partition {} for the whole cluster", partition_name);

        /// Process each source shard having current partition and copy current partition
        /// NOTE: shards are sorted by "distance" to current host
        bool has_shard_to_process = false;
        for (const TaskShardPtr & shard : task_table.all_shards)
        {
            /// Does shard have a node with current partition?
            if (shard->partition_tasks.count(partition_name) == 0)
            {
                /// If not, did we check existence of that partition previously?
                if (shard->checked_partitions.count(partition_name) == 0)
                {
                    auto check_shard_has_partition = [&] () { return checkShardHasPartition(timeouts, *shard, partition_name); };
                    bool has_partition = retry(check_shard_has_partition);

                    shard->checked_partitions.emplace(partition_name);

                    if (has_partition)
                    {
                        const size_t number_of_splits = task_table.number_of_splits;
                        shard->partition_tasks.emplace(partition_name, ShardPartition(*shard, partition_name, number_of_splits));
                        LOG_DEBUG(log, "Discovered partition {} in shard {}", partition_name, shard->getDescription());
                        /// To save references in the future.
                        auto shard_partition_it = shard->partition_tasks.find(partition_name);
                        PartitionPieces & shard_partition_pieces = shard_partition_it->second.pieces;

                        for (size_t piece_number = 0; piece_number < number_of_splits; ++piece_number)
                        {
                            auto res = checkPresentPartitionPiecesOnCurrentShard(timeouts, *shard, partition_name, piece_number);
                            shard_partition_pieces.emplace_back(shard_partition_it->second, piece_number, res);
                        }
                    }
                    else
                    {
                        LOG_DEBUG(log, "Found that shard {} does not contain current partition {}", shard->getDescription(), partition_name);
                        continue;
                    }
                }
                else
                {
                    /// We have already checked that partition, but did not discover it
                    previous_shard_is_instantly_finished = true;
                    continue;
                }
            }

            auto it_shard_partition = shard->partition_tasks.find(partition_name);
            /// Previously when we discovered that shard does not contain current partition, we skipped it.
            /// At this moment partition have to be present.
            if (it_shard_partition == shard->partition_tasks.end())
                throw Exception("There are no such partition in a shard. This is a bug.", ErrorCodes::LOGICAL_ERROR);
            auto & partition = it_shard_partition->second;

            expected_shards.emplace_back(shard);

            /// Do not sleep if there is a sequence of already processed shards to increase startup
            bool is_unprioritized_task = !previous_shard_is_instantly_finished && shard->priority.is_remote;
            TaskStatus task_status = TaskStatus::Error;
            bool was_error = false;
            has_shard_to_process = true;
            for (UInt64 try_num = 0; try_num < max_shard_partition_tries; ++try_num)
            {
                task_status = tryProcessPartitionTask(timeouts, partition, is_unprioritized_task);

                /// Exit if success
                if (task_status == TaskStatus::Finished)
                    break;

                was_error = true;

                /// Skip if the task is being processed by someone
                if (task_status == TaskStatus::Active)
                    break;

                /// Repeat on errors
                std::this_thread::sleep_for(default_sleep_time);
            }

            if (task_status == TaskStatus::Error)
                ++num_failed_shards;

            previous_shard_is_instantly_finished = !was_error;
        }

        cluster_partition.elapsed_time_seconds += watch.elapsedSeconds();

        /// Check that whole cluster partition is done
        /// Firstly check the number of failed partition tasks, then look into ZooKeeper and ensure that each partition is done
        bool partition_copying_is_done = num_failed_shards == 0;
        try
        {
            partition_copying_is_done =
                    !has_shard_to_process
                    || (partition_copying_is_done && checkAllPiecesInPartitionAreDone(task_table, partition_name, expected_shards));
        }
        catch (...)
        {
            tryLogCurrentException(log);
            partition_copying_is_done = false;
        }


        bool partition_moving_is_done = false;
        /// Try to move only if all pieces were copied.
        if (partition_copying_is_done)
        {
            for (UInt64 try_num = 0; try_num < max_shard_partition_piece_tries_for_alter; ++try_num)
            {
                try
                {
                    auto res = tryMoveAllPiecesToDestinationTable(task_table, partition_name);
                    /// Exit and mark current task is done.
                    if (res == TaskStatus::Finished)
                    {
                        partition_moving_is_done = true;
                        break;
                    }

                    /// Exit if this task is active.
                    if (res == TaskStatus::Active)
                        break;

                    /// Repeat on errors.
                    std::this_thread::sleep_for(default_sleep_time);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Some error occurred while moving pieces to destination table for partition " + partition_name);
                }
            }
        }

        if (partition_copying_is_done && partition_moving_is_done)
        {
            task_table.finished_cluster_partitions.emplace(partition_name);

            task_table.bytes_copied += cluster_partition.bytes_copied;
            task_table.rows_copied += cluster_partition.rows_copied;
            double elapsed = cluster_partition.elapsed_time_seconds;

            LOG_INFO(log, "It took {} seconds to copy partition {}: {} uncompressed bytes, {} rows and {} source blocks are copied",
                elapsed, partition_name,
                formatReadableSizeWithDecimalSuffix(cluster_partition.bytes_copied),
                formatReadableQuantity(cluster_partition.rows_copied),
                cluster_partition.blocks_copied);

            if (cluster_partition.rows_copied)
            {
                LOG_INFO(log, "Average partition speed: {} per second.", formatReadableSizeWithDecimalSuffix(cluster_partition.bytes_copied / elapsed));
            }

            if (task_table.rows_copied)
            {
                LOG_INFO(log, "Average table {} speed: {} per second.", task_table.table_id, formatReadableSizeWithDecimalSuffix(task_table.bytes_copied / elapsed));
            }
        }
    }

    UInt64 required_partitions = task_table.cluster_partitions.size();
    UInt64 finished_partitions = task_table.finished_cluster_partitions.size();
    bool table_is_done = finished_partitions >= required_partitions;

    if (!table_is_done)
    {
        LOG_INFO(log, "Table {} is not processed yet.Copied {} of {}, will retry", task_table.table_id, finished_partitions, required_partitions);
    }

    return table_is_done;
}

/// Job for copying partition from particular shard.
TaskStatus ClusterCopier::tryProcessPartitionTask(const ConnectionTimeouts & timeouts, ShardPartition & task_partition, bool is_unprioritized_task)
{
    TaskStatus res;

    try
    {
        res = iterateThroughAllPiecesInPartition(timeouts, task_partition, is_unprioritized_task);
    }
    catch (...)
    {
        tryLogCurrentException(log, "An error occurred while processing partition " + task_partition.name);
        res = TaskStatus::Error;
    }

    /// At the end of each task check if the config is updated
    try
    {
        updateConfigIfNeeded();
    }
    catch (...)
    {
        tryLogCurrentException(log, "An error occurred while updating the config");
    }

    return res;
}

TaskStatus ClusterCopier::iterateThroughAllPiecesInPartition(const ConnectionTimeouts & timeouts, ShardPartition & task_partition,
                                                       bool is_unprioritized_task)
{
    const size_t total_number_of_pieces = task_partition.task_shard.task_table.number_of_splits;

    TaskStatus res{TaskStatus::Finished};

    bool was_failed_pieces = false;
    bool was_active_pieces = false;

    for (size_t piece_number = 0; piece_number < total_number_of_pieces; piece_number++)
    {
        for (UInt64 try_num = 0; try_num < max_shard_partition_tries; ++try_num)
        {
            LOG_INFO(log, "Attempt number {} to process partition {} piece number {} on shard number {} with index {}.",
                try_num, task_partition.name, piece_number,
                task_partition.task_shard.numberInCluster(),
                task_partition.task_shard.indexInCluster());

            res = processPartitionPieceTaskImpl(timeouts, task_partition, piece_number, is_unprioritized_task);

            /// Exit if success
            if (res == TaskStatus::Finished)
                break;

            /// Skip if the task is being processed by someone
            if (res == TaskStatus::Active)
                break;

            /// Repeat on errors
            std::this_thread::sleep_for(default_sleep_time);
        }

        was_active_pieces = (res == TaskStatus::Active);
        was_failed_pieces = (res == TaskStatus::Error);
    }

    if (was_failed_pieces)
        return TaskStatus::Error;

    if (was_active_pieces)
        return TaskStatus::Active;

    return TaskStatus::Finished;
}


TaskStatus ClusterCopier::processPartitionPieceTaskImpl(
        const ConnectionTimeouts & timeouts, ShardPartition & task_partition,
        const size_t current_piece_number, bool is_unprioritized_task)
{
    TaskShard & task_shard = task_partition.task_shard;
    TaskTable & task_table = task_shard.task_table;
    ClusterPartition & cluster_partition  = task_table.getClusterPartition(task_partition.name);
    ShardPartitionPiece & partition_piece = task_partition.pieces[current_piece_number];

    const size_t number_of_splits = task_table.number_of_splits;
    const String primary_key_comma_separated = task_table.primary_key_comma_separated;

    /// We need to update table definitions for each partition, it could be changed after ALTER
    createShardInternalTables(timeouts, task_shard, true);

    auto split_table_for_current_piece = task_shard.list_of_split_tables_on_shard[current_piece_number];

    auto zookeeper = context.getZooKeeper();

    const String piece_is_dirty_flag_path          = partition_piece.getPartitionPieceIsDirtyPath();
    const String piece_is_dirty_cleaned_path       = partition_piece.getPartitionPieceIsCleanedPath();
    const String current_task_piece_is_active_path = partition_piece.getActiveWorkerPath();
    const String current_task_piece_status_path    = partition_piece.getShardStatusPath();

    /// Auxiliary functions:

    /// Creates is_dirty node to initialize DROP PARTITION
    auto create_is_dirty_node = [&] (const CleanStateClock & clock)
    {
        if (clock.is_stale())
            LOG_DEBUG(log, "Clean state clock is stale while setting dirty flag, cowardly bailing");
        else if (!clock.is_clean())
            LOG_DEBUG(log, "Thank you, Captain Obvious");
        else if (clock.discovery_version)
        {
            LOG_DEBUG(log, "Updating clean state clock");
            zookeeper->set(piece_is_dirty_flag_path, host_id, clock.discovery_version.value());
        }
        else
        {
            LOG_DEBUG(log, "Creating clean state clock");
            zookeeper->create(piece_is_dirty_flag_path, host_id, zkutil::CreateMode::Persistent);
        }
    };

    /// Returns SELECT query filtering current partition and applying user filter
    auto get_select_query = [&] (const DatabaseAndTableName & from_table, const String & fields, bool enable_splitting, String limit = "")
    {
        String query;
        query += "SELECT " + fields + " FROM " + getQuotedTable(from_table);

        if (enable_splitting && experimental_use_sample_offset)
            query += " SAMPLE 1/" + toString(number_of_splits) + " OFFSET " + toString(current_piece_number) + "/" + toString(number_of_splits);

        /// TODO: Bad, it is better to rewrite with ASTLiteral(partition_key_field)
        query += " WHERE (" + queryToString(task_table.engine_push_partition_key_ast) + " = (" + task_partition.name + " AS partition_key))";

        if (enable_splitting && !experimental_use_sample_offset)
            query += " AND ( cityHash64(" + primary_key_comma_separated + ") %" + toString(number_of_splits) + " = " + toString(current_piece_number) + " )";

        if (!task_table.where_condition_str.empty())
            query += " AND (" + task_table.where_condition_str + ")";

        if (!limit.empty())
            query += " LIMIT " + limit;

        ParserQuery p_query(query.data() + query.size());

        const auto & settings = context.getSettingsRef();
        return parseQuery(p_query, query, settings.max_query_size, settings.max_parser_depth);
    };

    /// Load balancing
    auto worker_node_holder = createTaskWorkerNodeAndWaitIfNeed(zookeeper, current_task_piece_status_path, is_unprioritized_task);

    LOG_DEBUG(log, "Processing {}", current_task_piece_status_path);

    const String piece_status_path = partition_piece.getPartitionPieceShardsPath();

    CleanStateClock clean_state_clock(zookeeper, piece_is_dirty_flag_path, piece_is_dirty_cleaned_path);

    const bool is_clean = checkPartitionPieceIsClean(zookeeper, clean_state_clock, piece_status_path);

    /// Do not start if partition piece is dirty, try to clean it
    if (is_clean)
    {
        LOG_DEBUG(log, "Partition {} piece {} appears to be clean", task_partition.name, current_piece_number);
        zookeeper->createAncestors(current_task_piece_status_path);
    }
    else
    {
        LOG_DEBUG(log, "Partition {} piece {} is dirty, try to drop it", task_partition.name, current_piece_number);

        try
        {
            tryDropPartitionPiece(task_partition, current_piece_number, zookeeper, clean_state_clock);
        }
        catch (...)
        {
            tryLogCurrentException(log, "An error occurred when clean partition");
        }

        return TaskStatus::Error;
    }

    /// Create ephemeral node to mark that we are active and process the partition
    zookeeper->createAncestors(current_task_piece_is_active_path);
    zkutil::EphemeralNodeHolderPtr partition_task_node_holder;
    try
    {
        partition_task_node_holder = zkutil::EphemeralNodeHolder::create(current_task_piece_is_active_path, *zookeeper, host_id);
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_DEBUG(log, "Someone is already processing {}", current_task_piece_is_active_path);
            return TaskStatus::Active;
        }

        throw;
    }

    /// Exit if task has been already processed;
    /// create blocking node to signal cleaning up if it is abandoned
    {
        String status_data;
        if (zookeeper->tryGet(current_task_piece_status_path, status_data))
        {
            TaskStateWithOwner status = TaskStateWithOwner::fromString(status_data);
            if (status.state == TaskState::Finished)
            {
                LOG_DEBUG(log, "Task {} has been successfully executed by {}", current_task_piece_status_path, status.owner);
                return TaskStatus::Finished;
            }

            /// Task is abandoned, because previously we created ephemeral node, possibly in other copier's process.
            /// Initialize DROP PARTITION
            LOG_DEBUG(log, "Task {} has not been successfully finished by {}. Partition will be dropped and refilled.", current_task_piece_status_path, status.owner);

            create_is_dirty_node(clean_state_clock);
            return TaskStatus::Error;
        }
    }


    /// Exit if current piece is absent on this shard. Also mark it as finished, because we will check
    /// whether each shard have processed each partitition (and its pieces).
    if (partition_piece.is_absent_piece)
    {
        String state_finished = TaskStateWithOwner::getData(TaskState::Finished, host_id);
        auto res = zookeeper->tryCreate(current_task_piece_status_path, state_finished, zkutil::CreateMode::Persistent);
        if (res == Coordination::Error::ZNODEEXISTS)
            LOG_DEBUG(log, "Partition {} piece {} is absent on current replica of a shard. But other replicas have already marked it as done.", task_partition.name, current_piece_number);
        if (res == Coordination::Error::ZOK)
            LOG_DEBUG(log, "Partition {} piece {} is absent on current replica of a shard. Will mark it as done. Other replicas will do the same.", task_partition.name, current_piece_number);
        return TaskStatus::Finished;
    }

    /// Check that destination partition is empty if we are first worker
    /// NOTE: this check is incorrect if pull and push tables have different partition key!
    String clean_start_status;
    if (!zookeeper->tryGet(partition_piece.getPartitionPieceCleanStartPath(), clean_start_status) || clean_start_status != "ok")
    {
        zookeeper->createIfNotExists(partition_piece.getPartitionPieceCleanStartPath(), "");
        auto checker = zkutil::EphemeralNodeHolder::create(partition_piece.getPartitionPieceCleanStartPath() + "/checker",
                                                           *zookeeper, host_id);
        // Maybe we are the first worker

        ASTPtr query_select_ast = get_select_query(split_table_for_current_piece, "count()", /*enable_splitting*/ true);
        UInt64 count;
        {
            Context local_context = context;
            // Use pull (i.e. readonly) settings, but fetch data from destination servers
            local_context.setSettings(task_cluster->settings_pull);
            local_context.setSetting("skip_unavailable_shards", true);

            Block block = getBlockWithAllStreamData(InterpreterFactory::get(query_select_ast, local_context)->execute().getInputStream());
            count = (block) ? block.safeGetByPosition(0).column->getUInt(0) : 0;
        }

        if (count != 0)
        {
            LOG_INFO(log, "Partition {} piece {}is not empty. In contains {} rows.", task_partition.name, current_piece_number, count);
            Coordination::Stat stat_shards{};
            zookeeper->get(partition_piece.getPartitionPieceShardsPath(), &stat_shards);

            /// NOTE: partition is still fresh if dirt discovery happens before cleaning
            if (stat_shards.numChildren == 0)
            {
                LOG_WARNING(log, "There are no workers for partition {} piece {}, but destination table contains {} rows. Partition will be dropped and refilled.", task_partition.name, toString(current_piece_number), count);

                create_is_dirty_node(clean_state_clock);
                return TaskStatus::Error;
            }
        }
        zookeeper->set(partition_piece.getPartitionPieceCleanStartPath(), "ok");
    }
    /// At this point, we need to sync that the destination table is clean
    /// before any actual work

    /// Try start processing, create node about it
    {
        String start_state = TaskStateWithOwner::getData(TaskState::Started, host_id);
        CleanStateClock new_clean_state_clock(zookeeper, piece_is_dirty_flag_path, piece_is_dirty_cleaned_path);
        if (clean_state_clock != new_clean_state_clock)
        {
            LOG_INFO(log, "Partition {} piece {} clean state changed, cowardly bailing", task_partition.name, toString(current_piece_number));
            return TaskStatus::Error;
        }
        else if (!new_clean_state_clock.is_clean())
        {
            LOG_INFO(log, "Partition {} piece {} is dirty and will be dropped and refilled", task_partition.name, toString(current_piece_number));
            create_is_dirty_node(new_clean_state_clock);
            return TaskStatus::Error;
        }
        zookeeper->create(current_task_piece_status_path, start_state, zkutil::CreateMode::Persistent);
    }

    /// Try create table (if not exists) on each shard
    {
        /// Define push table for current partition piece
        auto database_and_table_for_current_piece= std::pair<String, String>(
                task_table.table_push.first,
                task_table.table_push.second + "_piece_" + toString(current_piece_number));

        auto new_engine_push_ast = task_table.engine_push_ast;
        if (task_table.isReplicatedTable())
        {
            new_engine_push_ast = task_table.rewriteReplicatedCreateQueryToPlain();
        }

        auto create_query_push_ast = rewriteCreateQueryStorage(
                task_shard.current_pull_table_create_query,
                database_and_table_for_current_piece, new_engine_push_ast);

        create_query_push_ast->as<ASTCreateQuery &>().if_not_exists = true;
        String query = queryToString(create_query_push_ast);

        LOG_DEBUG(log, "Create destination tables. Query: {}", query);
        UInt64 shards = executeQueryOnCluster(task_table.cluster_push, query, task_cluster->settings_push, PoolMode::GET_MANY);
        LOG_DEBUG(log, "Destination tables {} have been created on {} shards of {}",
            getQuotedTable(task_table.table_push), shards, task_table.cluster_push->getShardCount());
    }

    /// Do the copying
    {
        bool inject_fault = false;
        if (copy_fault_probability > 0)
        {
            double value = std::uniform_real_distribution<>(0, 1)(task_table.task_cluster.random_engine);
            inject_fault = value < copy_fault_probability;
        }

        // Select all fields
        ASTPtr query_select_ast = get_select_query(task_shard.table_read_shard, "*", /*enable_splitting*/ true, inject_fault ? "1" : "");

        LOG_DEBUG(log, "Executing SELECT query and pull from {} : {}", task_shard.getDescription(), queryToString(query_select_ast));

        ASTPtr query_insert_ast;
        {
            String query;
            query += "INSERT INTO " + getQuotedTable(split_table_for_current_piece) + " VALUES ";

            ParserQuery p_query(query.data() + query.size());
            const auto & settings = context.getSettingsRef();
            query_insert_ast = parseQuery(p_query, query, settings.max_query_size, settings.max_parser_depth);

            LOG_DEBUG(log, "Executing INSERT query: {}", query);
        }

        try
        {
            std::unique_ptr<Context> context_select = std::make_unique<Context>(context);
            context_select->setSettings(task_cluster->settings_pull);

            std::unique_ptr<Context> context_insert = std::make_unique<Context>(context);
            context_insert->setSettings(task_cluster->settings_push);

            /// Custom INSERT SELECT implementation
            BlockInputStreamPtr input;
            BlockOutputStreamPtr output;
            {
                BlockIO io_select = InterpreterFactory::get(query_select_ast, *context_select)->execute();
                BlockIO io_insert = InterpreterFactory::get(query_insert_ast, *context_insert)->execute();

                input = io_select.getInputStream();
                output = io_insert.out;
            }

            /// Fail-fast optimization to abort copying when the current clean state expires
            std::future<Coordination::ExistsResponse> future_is_dirty_checker;

            Stopwatch watch(CLOCK_MONOTONIC_COARSE);
            constexpr UInt64 check_period_milliseconds = 500;

            /// Will asynchronously check that ZooKeeper connection and is_dirty flag appearing while copying data
            auto cancel_check = [&] ()
            {
                if (zookeeper->expired())
                    throw Exception("ZooKeeper session is expired, cancel INSERT SELECT", ErrorCodes::UNFINISHED);

                if (!future_is_dirty_checker.valid())
                    future_is_dirty_checker = zookeeper->asyncExists(piece_is_dirty_flag_path);

                /// check_period_milliseconds should less than average insert time of single block
                /// Otherwise, the insertion will slow a little bit
                if (watch.elapsedMilliseconds() >= check_period_milliseconds)
                {
                    Coordination::ExistsResponse status = future_is_dirty_checker.get();

                    if (status.error != Coordination::Error::ZNONODE)
                    {
                        LogicalClock dirt_discovery_epoch (status.stat.mzxid);
                        if (dirt_discovery_epoch == clean_state_clock.discovery_zxid)
                            return false;
                        throw Exception("Partition is dirty, cancel INSERT SELECT", ErrorCodes::UNFINISHED);
                    }
                }

                return false;
            };

            /// Update statistics
            /// It is quite rough: bytes_copied don't take into account DROP PARTITION.
            auto update_stats = [&cluster_partition] (const Block & block)
            {
                cluster_partition.bytes_copied += block.bytes();
                cluster_partition.rows_copied += block.rows();
                cluster_partition.blocks_copied += 1;
            };

            /// Main work is here
            copyData(*input, *output, cancel_check, update_stats);

            // Just in case
            if (future_is_dirty_checker.valid())
                future_is_dirty_checker.get();

            if (inject_fault)
                throw Exception("Copy fault injection is activated", ErrorCodes::UNFINISHED);
        }
        catch (...)
        {
            tryLogCurrentException(log, "An error occurred during copying, partition will be marked as dirty");
            create_is_dirty_node(clean_state_clock);
            return TaskStatus::Error;
        }
    }

    LOG_INFO(log, "Partition {} piece {} copied. But not moved to original destination table.", task_partition.name, toString(current_piece_number));


    /// Try create original table (if not exists) on each shard
    try
    {
        auto create_query_push_ast = rewriteCreateQueryStorage(task_shard.current_pull_table_create_query,
                                                               task_table.table_push, task_table.engine_push_ast);
        auto & create = create_query_push_ast->as<ASTCreateQuery &>();
        create.if_not_exists = true;
        InterpreterCreateQuery::prepareOnClusterQuery(create, context, task_table.cluster_push_name);
        String query = queryToString(create_query_push_ast);

        LOG_DEBUG(log, "Create destination tables. Query: {}", query);
        UInt64 shards = executeQueryOnCluster(task_table.cluster_push, query, task_cluster->settings_push, PoolMode::GET_MANY);
        LOG_DEBUG(log, "Destination tables {} have been created on {} shards of {}", getQuotedTable(task_table.table_push), shards, task_table.cluster_push->getShardCount());
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error while creating original table. Maybe we are not first.");
    }

    /// Finalize the processing, change state of current partition task (and also check is_dirty flag)
    {
        String state_finished = TaskStateWithOwner::getData(TaskState::Finished, host_id);
        CleanStateClock new_clean_state_clock (zookeeper, piece_is_dirty_flag_path, piece_is_dirty_cleaned_path);
        if (clean_state_clock != new_clean_state_clock)
        {
            LOG_INFO(log, "Partition {} piece {} clean state changed, cowardly bailing", task_partition.name, toString(current_piece_number));
            return TaskStatus::Error;
        }
        else if (!new_clean_state_clock.is_clean())
        {
            LOG_INFO(log, "Partition {} piece {} became dirty and will be dropped and refilled", task_partition.name, toString(current_piece_number));
            create_is_dirty_node(new_clean_state_clock);
            return TaskStatus::Error;
        }
        zookeeper->set(current_task_piece_status_path, state_finished, 0);
    }

    return TaskStatus::Finished;
}

void ClusterCopier::dropAndCreateLocalTable(const ASTPtr & create_ast)
{
    const auto & create = create_ast->as<ASTCreateQuery &>();
    dropLocalTableIfExists({create.database, create.table});

    InterpreterCreateQuery interpreter(create_ast, context);
    interpreter.execute();
}

void ClusterCopier::dropLocalTableIfExists(const DatabaseAndTableName & table_name) const
{
    auto drop_ast = std::make_shared<ASTDropQuery>();
    drop_ast->if_exists = true;
    drop_ast->database = table_name.first;
    drop_ast->table = table_name.second;

    InterpreterDropQuery interpreter(drop_ast, context);
    interpreter.execute();
}


void ClusterCopier::dropHelpingTables(const TaskTable & task_table)
{
    LOG_DEBUG(log, "Removing helping tables");
    for (size_t current_piece_number = 0; current_piece_number < task_table.number_of_splits; ++current_piece_number)
    {
        DatabaseAndTableName original_table = task_table.table_push;
        DatabaseAndTableName helping_table = DatabaseAndTableName(original_table.first, original_table.second + "_piece_" + toString(current_piece_number));

        String query = "DROP TABLE IF EXISTS " + getQuotedTable(helping_table);

        const ClusterPtr & cluster_push = task_table.cluster_push;
        Settings settings_push = task_cluster->settings_push;

        LOG_DEBUG(log, "Execute distributed DROP TABLE: {}", query);
        /// We have to drop partition_piece on each replica
        UInt64 num_nodes = executeQueryOnCluster(
                cluster_push, query,
                settings_push,
                PoolMode::GET_MANY,
                ClusterExecutionMode::ON_EACH_NODE);

        LOG_DEBUG(log, "DROP TABLE query was successfully executed on {} nodes.", toString(num_nodes));
    }
}


void ClusterCopier::dropParticularPartitionPieceFromAllHelpingTables(const TaskTable & task_table, const String & partition_name)
{
    LOG_DEBUG(log, "Try drop partition partition from all helping tables.");
    for (size_t current_piece_number = 0; current_piece_number < task_table.number_of_splits; ++current_piece_number)
    {
        DatabaseAndTableName original_table = task_table.table_push;
        DatabaseAndTableName helping_table = DatabaseAndTableName(original_table.first, original_table.second + "_piece_" + toString(current_piece_number));

        String query = "ALTER TABLE " + getQuotedTable(helping_table) + ((partition_name == "'all'") ? " DROP PARTITION ID " : " DROP PARTITION ") + partition_name;

        const ClusterPtr & cluster_push = task_table.cluster_push;
        Settings settings_push = task_cluster->settings_push;

        LOG_DEBUG(log, "Execute distributed DROP PARTITION: {}", query);
        /// We have to drop partition_piece on each replica
        UInt64 num_nodes = executeQueryOnCluster(
                cluster_push, query,
                settings_push,
                PoolMode::GET_MANY,
                ClusterExecutionMode::ON_EACH_NODE);

        LOG_DEBUG(log, "DROP PARTITION query was successfully executed on {} nodes.", toString(num_nodes));
    }
    LOG_DEBUG(log, "All helping tables dropped partition {}", partition_name);
}

String ClusterCopier::getRemoteCreateTable(const DatabaseAndTableName & table, Connection & connection, const Settings * settings)
{
    String query = "SHOW CREATE TABLE " + getQuotedTable(table);
    Block block = getBlockWithAllStreamData(std::make_shared<RemoteBlockInputStream>(
            connection, query, InterpreterShowCreateQuery::getSampleBlock(), context, settings));

    return typeid_cast<const ColumnString &>(*block.safeGetByPosition(0).column).getDataAt(0).toString();
}

ASTPtr ClusterCopier::getCreateTableForPullShard(const ConnectionTimeouts & timeouts, TaskShard & task_shard)
{
    /// Fetch and parse (possibly) new definition
    auto connection_entry = task_shard.info.pool->get(timeouts, &task_cluster->settings_pull, true);
    String create_query_pull_str = getRemoteCreateTable(
            task_shard.task_table.table_pull,
            *connection_entry,
            &task_cluster->settings_pull);

    ParserCreateQuery parser_create_query;
    const auto & settings = context.getSettingsRef();
    return parseQuery(parser_create_query, create_query_pull_str, settings.max_query_size, settings.max_parser_depth);
}

/// If it is implicitly asked to create split Distributed table for certain piece on current shard, we will do it.
void ClusterCopier::createShardInternalTables(const ConnectionTimeouts & timeouts,
        TaskShard & task_shard, bool create_split)
{
    TaskTable & task_table = task_shard.task_table;

    /// We need to update table definitions for each part, it could be changed after ALTER
    task_shard.current_pull_table_create_query = getCreateTableForPullShard(timeouts, task_shard);

    /// Create local Distributed tables:
    ///  a table fetching data from current shard and a table inserting data to the whole destination cluster
    String read_shard_prefix = ".read_shard_" + toString(task_shard.indexInCluster()) + ".";
    String split_shard_prefix = ".split.";
    task_shard.table_read_shard = DatabaseAndTableName(working_database_name, read_shard_prefix + task_table.table_id);
    task_shard.main_table_split_shard = DatabaseAndTableName(working_database_name, split_shard_prefix + task_table.table_id);

    for (const auto & piece_number : ext::range(0, task_table.number_of_splits))
    {
        task_shard.list_of_split_tables_on_shard[piece_number] =
                DatabaseAndTableName(working_database_name, split_shard_prefix + task_table.table_id + "_piece_" + toString(piece_number));
    }

    /// Create special cluster with single shard
    String shard_read_cluster_name = read_shard_prefix + task_table.cluster_pull_name;
    ClusterPtr cluster_pull_current_shard = task_table.cluster_pull->getClusterWithSingleShard(task_shard.indexInCluster());
    context.setCluster(shard_read_cluster_name, cluster_pull_current_shard);

    auto storage_shard_ast = createASTStorageDistributed(shard_read_cluster_name, task_table.table_pull.first, task_table.table_pull.second);

    auto create_query_ast = removeAliasColumnsFromCreateQuery(task_shard.current_pull_table_create_query);

    auto create_table_pull_ast = rewriteCreateQueryStorage(create_query_ast, task_shard.table_read_shard, storage_shard_ast);
    dropAndCreateLocalTable(create_table_pull_ast);

    if (create_split)
    {
        auto create_table_split_piece_ast = rewriteCreateQueryStorage(
                create_query_ast,
                task_shard.main_table_split_shard,
                task_table.main_engine_split_ast);

        dropAndCreateLocalTable(create_table_split_piece_ast);

        /// Create auxiliary split tables for each piece
        for (const auto & piece_number : ext::range(0, task_table.number_of_splits))
        {
            const auto & storage_piece_split_ast = task_table.auxiliary_engine_split_asts[piece_number];

            create_table_split_piece_ast = rewriteCreateQueryStorage(
                    create_query_ast,
                    task_shard.list_of_split_tables_on_shard[piece_number],
                    storage_piece_split_ast);

            dropAndCreateLocalTable(create_table_split_piece_ast);
        }
    }

}


std::set<String> ClusterCopier::getShardPartitions(const ConnectionTimeouts & timeouts, TaskShard & task_shard)
{
    std::set<String> res;

    createShardInternalTables(timeouts, task_shard, false);

    TaskTable & task_table = task_shard.task_table;

    const String & partition_name = queryToString(task_table.engine_push_partition_key_ast);

    if (partition_name == "'all'")
    {
        res.emplace("'all'");
        return res;
    }

    String query;
    {
        WriteBufferFromOwnString wb;
        wb << "SELECT DISTINCT " << partition_name << " AS partition FROM"
           << " " << getQuotedTable(task_shard.table_read_shard) << " ORDER BY partition DESC";
        query = wb.str();
    }

    ParserQuery parser_query(query.data() + query.size());
    const auto & settings = context.getSettingsRef();
    ASTPtr query_ast = parseQuery(parser_query, query, settings.max_query_size, settings.max_parser_depth);

    LOG_DEBUG(log, "Computing destination partition set, executing query: {}", query);

    Context local_context = context;
    local_context.setSettings(task_cluster->settings_pull);
    Block block = getBlockWithAllStreamData(InterpreterFactory::get(query_ast, local_context)->execute().getInputStream());

    if (block)
    {
        ColumnWithTypeAndName & column = block.getByPosition(0);
        task_shard.partition_key_column = column;

        for (size_t i = 0; i < column.column->size(); ++i)
        {
            WriteBufferFromOwnString wb;
            column.type->serializeAsTextQuoted(*column.column, i, wb, FormatSettings());
            res.emplace(wb.str());
        }
    }

    LOG_DEBUG(log, "There are {} destination partitions in shard {}", res.size(), task_shard.getDescription());

    return res;
}

bool ClusterCopier::checkShardHasPartition(const ConnectionTimeouts & timeouts,
        TaskShard & task_shard, const String & partition_quoted_name)
{
    createShardInternalTables(timeouts, task_shard, false);

    TaskTable & task_table = task_shard.task_table;

    std::string query = "SELECT 1 FROM " + getQuotedTable(task_shard.table_read_shard)
                        + " WHERE (" + queryToString(task_table.engine_push_partition_key_ast) +
                        " = (" + partition_quoted_name + " AS partition_key))";

    if (!task_table.where_condition_str.empty())
        query += " AND (" + task_table.where_condition_str + ")";

    query += " LIMIT 1";

    LOG_DEBUG(log, "Checking shard {} for partition {} existence, executing query: {}", task_shard.getDescription(), partition_quoted_name, query);

    ParserQuery parser_query(query.data() + query.size());
const auto & settings = context.getSettingsRef();
    ASTPtr query_ast = parseQuery(parser_query, query, settings.max_query_size, settings.max_parser_depth);

    Context local_context = context;
    local_context.setSettings(task_cluster->settings_pull);
    return InterpreterFactory::get(query_ast, local_context)->execute().getInputStream()->read().rows() != 0;
}

bool ClusterCopier::checkPresentPartitionPiecesOnCurrentShard(const ConnectionTimeouts & timeouts,
                           TaskShard & task_shard, const String & partition_quoted_name, size_t current_piece_number)
{
    createShardInternalTables(timeouts, task_shard, false);

    TaskTable & task_table = task_shard.task_table;
    const size_t number_of_splits = task_table.number_of_splits;
    const String & primary_key_comma_separated = task_table.primary_key_comma_separated;

    UNUSED(primary_key_comma_separated);

    std::string query = "SELECT 1 FROM " + getQuotedTable(task_shard.table_read_shard);

    if (experimental_use_sample_offset)
        query += " SAMPLE 1/" + toString(number_of_splits) + " OFFSET " + toString(current_piece_number) + "/" + toString(number_of_splits);

    query += " WHERE (" + queryToString(task_table.engine_push_partition_key_ast)
                        + " = (" + partition_quoted_name + " AS partition_key))";

    if (!experimental_use_sample_offset)
        query += " AND (cityHash64(" + primary_key_comma_separated + ") % "
                 + std::to_string(number_of_splits) + " = " + std::to_string(current_piece_number) + " )";

    if (!task_table.where_condition_str.empty())
        query += " AND (" + task_table.where_condition_str + ")";

    query += " LIMIT 1";

    LOG_DEBUG(log, "Checking shard {} for partition {} piece {} existence, executing query: {}", task_shard.getDescription(), partition_quoted_name, std::to_string(current_piece_number), query);

    ParserQuery parser_query(query.data() + query.size());
    const auto & settings = context.getSettingsRef();
    ASTPtr query_ast = parseQuery(parser_query, query, settings.max_query_size, settings.max_parser_depth);

    Context local_context = context;
    local_context.setSettings(task_cluster->settings_pull);
    auto result = InterpreterFactory::get(query_ast, local_context)->execute().getInputStream()->read().rows();
    if (result != 0)
        LOG_DEBUG(log, "Partition {} piece number {} is PRESENT on shard {}", partition_quoted_name, std::to_string(current_piece_number), task_shard.getDescription());
    else
        LOG_DEBUG(log, "Partition {} piece number {} is ABSENT on shard {}", partition_quoted_name, std::to_string(current_piece_number), task_shard.getDescription());
    return result != 0;
}

/** Executes simple query (without output streams, for example DDL queries) on each shard of the cluster
  * Returns number of shards for which at least one replica executed query successfully
  */
UInt64 ClusterCopier::executeQueryOnCluster(
        const ClusterPtr & cluster,
        const String & query,
        const Settings & current_settings,
        PoolMode pool_mode,
        ClusterExecutionMode execution_mode,
        UInt64 max_successful_executions_per_shard) const
{
    auto num_shards = cluster->getShardsInfo().size();
    std::vector<UInt64> per_shard_num_successful_replicas(num_shards, 0);

    ParserQuery p_query(query.data() + query.size());
    ASTPtr query_ast = parseQuery(p_query, query, current_settings.max_query_size, current_settings.max_parser_depth);

    /// We will have to execute query on each replica of a shard.
    if (execution_mode == ClusterExecutionMode::ON_EACH_NODE)
        max_successful_executions_per_shard = 0;

    std::atomic<size_t> origin_replicas_number = 0;

    /// We need to execute query on one replica at least
    auto do_for_shard = [&] (UInt64 shard_index, Settings shard_settings)
    {
        setThreadName("QueryForShard");

        const Cluster::ShardInfo & shard = cluster->getShardsInfo().at(shard_index);
        UInt64 & num_successful_executions = per_shard_num_successful_replicas.at(shard_index);
        num_successful_executions = 0;

        auto increment_and_check_exit = [&] () -> bool
        {
            ++num_successful_executions;
            return max_successful_executions_per_shard && num_successful_executions >= max_successful_executions_per_shard;
        };

        UInt64 num_replicas = cluster->getShardsAddresses().at(shard_index).size();

        origin_replicas_number += num_replicas;
        UInt64 num_local_replicas = shard.getLocalNodeCount();
        UInt64 num_remote_replicas = num_replicas - num_local_replicas;

        /// In that case we don't have local replicas, but do it just in case
        for (UInt64 i = 0; i < num_local_replicas; ++i)
        {
            auto interpreter = InterpreterFactory::get(query_ast, context);
            interpreter->execute();

            if (increment_and_check_exit())
                return;
        }

        /// Will try to make as many as possible queries
        if (shard.hasRemoteConnections())
        {
            shard_settings.max_parallel_replicas = num_remote_replicas ? num_remote_replicas : 1;

            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(shard_settings).getSaturated(shard_settings.max_execution_time);
            auto connections = shard.pool->getMany(timeouts, &shard_settings, pool_mode);

            for (auto & connection : connections)
            {
                if (connection.isNull())
                    continue;

                try
                {
                    /// CREATE TABLE and DROP PARTITION queries return empty block
                    RemoteBlockInputStream stream{*connection, query, Block{}, context, &shard_settings};
                    NullBlockOutputStream output{Block{}};
                    copyData(stream, output);

                    if (increment_and_check_exit())
                        return;
                }
                catch (const Exception &)
                {
                    LOG_INFO(log, getCurrentExceptionMessage(false, true));
                }
            }
        }
    };

    {
        ThreadPool thread_pool(std::min<UInt64>(num_shards, getNumberOfPhysicalCPUCores()));

        for (UInt64 shard_index = 0; shard_index < num_shards; ++shard_index)
            thread_pool.scheduleOrThrowOnError([=, shard_settings = current_settings] { do_for_shard(shard_index, std::move(shard_settings)); });

        thread_pool.wait();
    }

    UInt64 successful_nodes = 0;
    for (UInt64 num_replicas : per_shard_num_successful_replicas)
    {
        if (execution_mode == ClusterExecutionMode::ON_EACH_NODE)
            successful_nodes += num_replicas;
        else
            /// Count only successful shards
            successful_nodes += (num_replicas > 0);
    }

    if (execution_mode == ClusterExecutionMode::ON_EACH_NODE && successful_nodes != origin_replicas_number)
    {
        LOG_INFO(log, "There was an error while executing ALTER on each node. Query was executed on {} nodes. But had to be executed on {}", toString(successful_nodes), toString(origin_replicas_number.load()));
    }

    return successful_nodes;
}

}
