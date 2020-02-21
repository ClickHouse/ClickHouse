#include "ClusterCopier.h"

#include "Internals.h"

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>

namespace DB

{

void ClusterCopier::init()
{
    auto zookeeper = context.getZooKeeper();

    task_description_watch_callback = [this] (const Coordination::WatchResponse & response)
    {
        if (response.error != Coordination::ZOK)
            return;
        UInt64 version = ++task_descprtion_version;
        LOG_DEBUG(log, "Task description should be updated, local version " << version);
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

    LOG_DEBUG(log, "Will process " << task_cluster->table_tasks.size() << " table tasks");

    /// Do not initialize tables, will make deferred initialization in process()

    zookeeper->createAncestors(getWorkersPathVersion() + "/");
    zookeeper->createAncestors(getWorkersPath() + "/");
}

template <typename T>
decltype(auto) ClusterCopier::retry(T && func, UInt64 max_tries)
{
    std::exception_ptr exception;

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

    LOG_INFO(log, "Discover partitions of shard " << task_shard->getDescription());

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
                LOG_DEBUG(log, "Partition " << partition_name << " will not be processed, since it is not in "
                                            << "enabled_partitions of " << task_table.table_id);
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
        task_shard->partition_tasks.emplace(partition_name, ShardPartition(*task_shard, partition_name));
        task_shard->checked_partitions.emplace(partition_name, true);
    }

    if (!missing_partitions.empty())
    {
        std::stringstream ss;
        for (const String & missing_partition : missing_partitions)
            ss << " " << missing_partition;

        LOG_WARNING(log, "There are no " << missing_partitions.size() << " partitions from enabled_partitions in shard "
                         << task_shard->getDescription() << " :" << ss.str());
    }

    LOG_DEBUG(log, "Will copy " << task_shard->partition_tasks.size() << " partitions from shard " << task_shard->getDescription());
}

void ClusterCopier::discoverTablePartitions(const ConnectionTimeouts & timeouts, TaskTable & task_table, UInt64 num_threads)
{
    /// Fetch partitions list from a shard
    {
        ThreadPool thread_pool(num_threads ? num_threads : 2 * getNumberOfPhysicalCPUCores());

        for (const TaskShardPtr & task_shard : task_table.all_shards)
            thread_pool.scheduleOrThrowOnError([this, timeouts, task_shard]() { discoverShardPartitions(timeouts, task_shard); });

        LOG_DEBUG(log, "Waiting for " << thread_pool.active() << " setup jobs");
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
    if (code && force)
        zookeeper->createOrUpdate(local_task_description_path, task_config_str, zkutil::CreateMode::Persistent);

    LOG_DEBUG(log, "Task description " << ((code && !force) ? "not " : "") << "uploaded to " << local_task_description_path << " with result " << code << " ("<< zookeeper->error2string(code) << ")");
}

void ClusterCopier::reloadTaskDescription()
{
    auto zookeeper = context.getZooKeeper();
    task_description_watch_zookeeper = zookeeper;

    String task_config_str;
    Coordination::Stat stat;
    int code;

    zookeeper->tryGetWatch(task_description_path, task_config_str, &stat, task_description_watch_callback, &code);
    if (code)
        throw Exception("Can't get description node " + task_description_path, ErrorCodes::BAD_ARGUMENTS);

    LOG_DEBUG(log, "Loading description, zxid=" << task_descprtion_current_stat.czxid);
    auto config = getConfigurationFromXMLString(task_config_str);

    /// Setup settings
    task_cluster->reloadSettings(*config);
    context.getSettingsRef() = task_cluster->settings_common;

    task_cluster_current_config = config;
    task_descprtion_current_stat = stat;
}

void ClusterCopier::updateConfigIfNeeded()
{
    UInt64 version_to_update = task_descprtion_version;
    bool is_outdated_version = task_descprtion_current_version != version_to_update;
    bool is_expired_session = !task_description_watch_zookeeper || task_description_watch_zookeeper->expired();

    if (!is_outdated_version && !is_expired_session)
        return;

    LOG_DEBUG(log, "Updating task description");
    reloadTaskDescription();

    task_descprtion_current_version = version_to_update;
}

void ClusterCopier::process(const ConnectionTimeouts & timeouts)
{
    for (TaskTable & task_table : task_cluster->table_tasks)
    {
        LOG_INFO(log, "Process table task " << task_table.table_id << " with "
                      << task_table.all_shards.size() << " shards, " << task_table.local_shards.size() << " of them are local ones");

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

        if (!table_is_done)
        {
            throw Exception("Too many tries to process table " + task_table.table_id + ". Abort remaining execution",
                            ErrorCodes::UNFINISHED);
        }
    }
}

/// Protected section

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
    String workers_path = getWorkersPath();
    String current_worker_path = getCurrentWorkerNodePath();

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
            LOG_DEBUG(log, "Too many workers (" << stat.numChildren << ", maximum " << task_cluster->max_workers << ")"
                << ". Postpone processing " << description);

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

            if (code == Coordination::ZOK || code == Coordination::ZNODEEXISTS)
                return std::make_shared<zkutil::EphemeralNodeHolder>(current_worker_path, *zookeeper, false, false, description);

            if (code == Coordination::ZBADVERSION)
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

/** Checks that the whole partition of a table was copied. We should do it carefully due to dirty lock.
 * State of some task could change during the processing.
 * We have to ensure that all shards have the finished state and there is no dirty flag.
 * Moreover, we have to check status twice and check zxid, because state can change during the checking.
 */
bool ClusterCopier::checkPartitionIsDone(const TaskTable & task_table, const String & partition_name, const TasksShard & shards_with_partition)
{
    LOG_DEBUG(log, "Check that all shards processed partition " << partition_name << " successfully");

    auto zookeeper = context.getZooKeeper();

    Strings status_paths;
    for (auto & shard : shards_with_partition)
    {
        ShardPartition & task_shard_partition = shard->partition_tasks.find(partition_name)->second;
        status_paths.emplace_back(task_shard_partition.getShardStatusPath());
    }

    std::vector<int64_t> zxid1, zxid2;

    try
    {
        std::vector<zkutil::ZooKeeper::FutureGet> get_futures;
        for (const String & path : status_paths)
            get_futures.emplace_back(zookeeper->asyncGet(path));

        // Check that state is Finished and remember zxid
        for (auto & future : get_futures)
        {
            auto res = future.get();

            TaskStateWithOwner status = TaskStateWithOwner::fromString(res.data);
            if (status.state != TaskState::Finished)
            {
                LOG_INFO(log, "The task " << res.data << " is being rewritten by " << status.owner << ". Partition will be rechecked");
                return false;
            }

            zxid1.push_back(res.stat.pzxid);
        }

        // Check that partition is not dirty
        {
            CleanStateClock clean_state_clock (
                                               zookeeper,
                                               task_table.getPartitionIsDirtyPath(partition_name),
                                               task_table.getPartitionIsCleanedPath(partition_name)
                                               );
            Coordination::Stat stat;
            LogicalClock task_start_clock;
            if (zookeeper->exists(task_table.getPartitionTaskStatusPath(partition_name), &stat))
                task_start_clock = LogicalClock(stat.mzxid);
            zookeeper->get(task_table.getPartitionTaskStatusPath(partition_name), &stat);
            if (!clean_state_clock.is_clean() || task_start_clock <= clean_state_clock.discovery_zxid)
            {
                LOG_INFO(log, "Partition " << partition_name << " become dirty");
                return false;
            }
        }

        get_futures.clear();
        for (const String & path : status_paths)
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
        LOG_INFO(log, "A ZooKeeper error occurred while checking partition " << partition_name
                      << ". Will recheck the partition. Error: " << e.displayText());
        return false;
    }

    // If all task is finished and zxid is not changed then partition could not become dirty again
    for (UInt64 shard_num = 0; shard_num < status_paths.size(); ++shard_num)
    {
        if (zxid1[shard_num] != zxid2[shard_num])
        {
            LOG_INFO(log, "The task " << status_paths[shard_num] << " is being modified now. Partition will be rechecked");
            return false;
        }
    }

    LOG_INFO(log, "Partition " << partition_name << " is copied successfully");
    return true;
}

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
    if (auto indices = query_ast->as<ASTCreateQuery>()->columns_list->indices)
        new_columns_list->set(new_columns_list->indices, indices->clone());

    new_query.replace(new_query.columns_list, new_columns_list);

    return new_query_ast;
}

std::shared_ptr<ASTCreateQuery> ClusterCopier::rewriteCreateQueryStorage(const ASTPtr & create_query_ast, const DatabaseAndTableName & new_table, const ASTPtr & new_storage_ast)
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


bool ClusterCopier::tryDropPartition(ShardPartition & task_partition, const zkutil::ZooKeeperPtr & zookeeper, const CleanStateClock & clean_state_clock)
{
    if (is_safe_mode)
        throw Exception("DROP PARTITION is prohibited in safe mode", ErrorCodes::NOT_IMPLEMENTED);

    TaskTable & task_table = task_partition.task_shard.task_table;

    const String current_shards_path = task_partition.getPartitionShardsPath();
    const String current_partition_active_workers_dir = task_partition.getPartitionActiveWorkersPath();
    const String is_dirty_flag_path = task_partition.getCommonPartitionIsDirtyPath();
    const String dirt_cleaner_path = is_dirty_flag_path + "/cleaner";
    const String is_dirt_cleaned_path = task_partition.getCommonPartitionIsCleanedPath();

    zkutil::EphemeralNodeHolder::Ptr cleaner_holder;
    try
    {
        cleaner_holder = zkutil::EphemeralNodeHolder::create(dirt_cleaner_path, *zookeeper, host_id);
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::ZNODEEXISTS)
        {
            LOG_DEBUG(log, "Partition " << task_partition.name << " is cleaning now by somebody, sleep");
            std::this_thread::sleep_for(default_sleep_time);
            return false;
        }

        throw;
    }

    Coordination::Stat stat;
    if (zookeeper->exists(current_partition_active_workers_dir, &stat))
    {
        if (stat.numChildren != 0)
        {
            LOG_DEBUG(log, "Partition " << task_partition.name << " contains " << stat.numChildren << " active workers while trying to drop it. Going to sleep.");
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
            if (e.code == Coordination::ZNODEEXISTS)
            {
                LOG_DEBUG(log, "Partition " << task_partition.name << " is being filled now by somebody, sleep");
                return false;
            }

            throw;
        }

        // Lock the dirty flag
        zookeeper->set(is_dirty_flag_path, host_id, clean_state_clock.discovery_version.value());
        zookeeper->tryRemove(task_partition.getPartitionCleanStartPath());
        CleanStateClock my_clock(zookeeper, is_dirty_flag_path, is_dirt_cleaned_path);

        /// Remove all status nodes
        {
            Strings children;
            if (zookeeper->tryGetChildren(current_shards_path, children) == Coordination::ZOK)
                for (const auto & child : children)
                {
                    zookeeper->removeRecursive(current_shards_path + "/" + child);
                }
        }

        String query = "ALTER TABLE " + getQuotedTable(task_table.table_push);
        query += " DROP PARTITION " + task_partition.name + "";

        /// TODO: use this statement after servers will be updated up to 1.1.54310
        // query += " DROP PARTITION ID '" + task_partition.name + "'";

        ClusterPtr & cluster_push = task_table.cluster_push;
        Settings settings_push = task_cluster->settings_push;

        /// It is important, DROP PARTITION must be done synchronously
        settings_push.replication_alter_partitions_sync = 2;

        LOG_DEBUG(log, "Execute distributed DROP PARTITION: " << query);
        /// Limit number of max executing replicas to 1
        UInt64 num_shards = executeQueryOnCluster(cluster_push, query, nullptr, &settings_push, PoolMode::GET_ONE, 1);

        if (num_shards < cluster_push->getShardCount())
        {
            LOG_INFO(log, "DROP PARTITION wasn't successfully executed on " << cluster_push->getShardCount() - num_shards << " shards");
            return false;
        }

        /// Update the locking node
        if (!my_clock.is_stale())
        {
            zookeeper->set(is_dirty_flag_path, host_id, my_clock.discovery_version.value());
            if (my_clock.clean_state_version)
                zookeeper->set(is_dirt_cleaned_path, host_id, my_clock.clean_state_version.value());
            else
                zookeeper->create(is_dirt_cleaned_path, host_id, zkutil::CreateMode::Persistent);
        }
        else
        {
            LOG_DEBUG(log, "Clean state is altered when dropping the partition, cowardly bailing");
            /// clean state is stale
            return false;
        }

        LOG_INFO(log, "Partition " << task_partition.name << " was dropped on cluster " << task_table.cluster_push_name);
        if (zookeeper->tryCreate(current_shards_path, host_id, zkutil::CreateMode::Persistent) == Coordination::ZNODEEXISTS)
            zookeeper->set(current_shards_path, host_id);
    }

    LOG_INFO(log, "Partition " << task_partition.name << " is safe for work now.");
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
        TasksShard expected_shards;
        UInt64 num_failed_shards = 0;

        ++cluster_partition.total_tries;

        LOG_DEBUG(log, "Processing partition " << partition_name << " for the whole cluster");

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
                        shard->partition_tasks.emplace(partition_name, ShardPartition(*shard, partition_name));
                        LOG_DEBUG(log, "Discovered partition " << partition_name << " in shard " << shard->getDescription());
                    }
                    else
                    {
                        LOG_DEBUG(log, "Found that shard " << shard->getDescription() << " does not contain current partition " << partition_name);
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
            if (it_shard_partition == shard->partition_tasks.end())
                 throw Exception("There are no such partition in a shard. This is a bug.", ErrorCodes::LOGICAL_ERROR);
            auto & partition = it_shard_partition->second;

            expected_shards.emplace_back(shard);

            /// Do not sleep if there is a sequence of already processed shards to increase startup
            bool is_unprioritized_task = !previous_shard_is_instantly_finished && shard->priority.is_remote;
            PartitionTaskStatus task_status = PartitionTaskStatus::Error;
            bool was_error = false;
            has_shard_to_process = true;
            for (UInt64 try_num = 0; try_num < max_shard_partition_tries; ++try_num)
            {
                task_status = tryProcessPartitionTask(timeouts, partition, is_unprioritized_task);

                /// Exit if success
                if (task_status == PartitionTaskStatus::Finished)
                    break;

                was_error = true;

                /// Skip if the task is being processed by someone
                if (task_status == PartitionTaskStatus::Active)
                    break;

                /// Repeat on errors
                std::this_thread::sleep_for(default_sleep_time);
            }

            if (task_status == PartitionTaskStatus::Error)
                ++num_failed_shards;

            previous_shard_is_instantly_finished = !was_error;
        }

        cluster_partition.elapsed_time_seconds += watch.elapsedSeconds();

        /// Check that whole cluster partition is done
        /// Firstly check the number of failed partition tasks, then look into ZooKeeper and ensure that each partition is done
        bool partition_is_done = num_failed_shards == 0;
        try
        {
            partition_is_done =
                !has_shard_to_process
                || (partition_is_done && checkPartitionIsDone(task_table, partition_name, expected_shards));
        }
        catch (...)
        {
            tryLogCurrentException(log);
            partition_is_done = false;
        }

        if (partition_is_done)
        {
            task_table.finished_cluster_partitions.emplace(partition_name);

            task_table.bytes_copied += cluster_partition.bytes_copied;
            task_table.rows_copied += cluster_partition.rows_copied;
            double elapsed = cluster_partition.elapsed_time_seconds;

            LOG_INFO(log, "It took " << std::fixed << std::setprecision(2) << elapsed << " seconds to copy partition " << partition_name
                     << ": " << formatReadableSizeWithDecimalSuffix(cluster_partition.bytes_copied) << " uncompressed bytes"
                     << ", " << formatReadableQuantity(cluster_partition.rows_copied) << " rows"
                     << " and " << cluster_partition.blocks_copied << " source blocks are copied");

            if (cluster_partition.rows_copied)
            {
                LOG_INFO(log, "Average partition speed: "
                    << formatReadableSizeWithDecimalSuffix(cluster_partition.bytes_copied / elapsed) << " per second.");
            }

            if (task_table.rows_copied)
            {
                LOG_INFO(log, "Average table " << task_table.table_id << " speed: "
                    << formatReadableSizeWithDecimalSuffix(task_table.bytes_copied / elapsed) << " per second.");
            }
        }
    }

    UInt64 required_partitions = task_table.cluster_partitions.size();
    UInt64 finished_partitions = task_table.finished_cluster_partitions.size();
    bool table_is_done = finished_partitions >= required_partitions;

    if (!table_is_done)
    {
        LOG_INFO(log, "Table " + task_table.table_id + " is not processed yet."
            << "Copied " << finished_partitions << " of " << required_partitions << ", will retry");
    }

    return table_is_done;
}


PartitionTaskStatus ClusterCopier::tryProcessPartitionTask(const ConnectionTimeouts & timeouts, ShardPartition & task_partition, bool is_unprioritized_task)
{
    PartitionTaskStatus res;

    try
    {
        res = processPartitionTaskImpl(timeouts, task_partition, is_unprioritized_task);
    }
    catch (...)
    {
        tryLogCurrentException(log, "An error occurred while processing partition " + task_partition.name);
        res = PartitionTaskStatus::Error;
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

PartitionTaskStatus ClusterCopier::processPartitionTaskImpl(const ConnectionTimeouts & timeouts, ShardPartition & task_partition, bool is_unprioritized_task)
{
    TaskShard & task_shard = task_partition.task_shard;
    TaskTable & task_table = task_shard.task_table;
    ClusterPartition & cluster_partition = task_table.getClusterPartition(task_partition.name);

    /// We need to update table definitions for each partition, it could be changed after ALTER
    createShardInternalTables(timeouts, task_shard);

    auto zookeeper = context.getZooKeeper();

    const String is_dirty_flag_path = task_partition.getCommonPartitionIsDirtyPath();
    const String is_dirt_cleaned_path = task_partition.getCommonPartitionIsCleanedPath();
    const String current_task_is_active_path = task_partition.getActiveWorkerPath();
    const String current_task_status_path = task_partition.getShardStatusPath();

    /// Auxiliary functions:

    /// Creates is_dirty node to initialize DROP PARTITION
    auto create_is_dirty_node = [&, this] (const CleanStateClock & clock)
    {
        if (clock.is_stale())
            LOG_DEBUG(log, "Clean state clock is stale while setting dirty flag, cowardly bailing");
        else if (!clock.is_clean())
            LOG_DEBUG(log, "Thank you, Captain Obvious");
        else if (clock.discovery_version)
        {
            LOG_DEBUG(log, "Updating clean state clock");
            zookeeper->set(is_dirty_flag_path, host_id, clock.discovery_version.value());
        }
        else
        {
            LOG_DEBUG(log, "Creating clean state clock");
            zookeeper->create(is_dirty_flag_path, host_id, zkutil::CreateMode::Persistent);
        }
    };

    /// Returns SELECT query filtering current partition and applying user filter
    auto get_select_query = [&] (const DatabaseAndTableName & from_table, const String & fields, String limit = "")
    {
        String query;
        query += "SELECT " + fields + " FROM " + getQuotedTable(from_table);
        /// TODO: Bad, it is better to rewrite with ASTLiteral(partition_key_field)
        query += " WHERE (" + queryToString(task_table.engine_push_partition_key_ast) + " = (" + task_partition.name + " AS partition_key))";
        if (!task_table.where_condition_str.empty())
            query += " AND (" + task_table.where_condition_str + ")";
        if (!limit.empty())
            query += " LIMIT " + limit;

        ParserQuery p_query(query.data() + query.size());
        return parseQuery(p_query, query, 0);
    };

    /// Load balancing
    auto worker_node_holder = createTaskWorkerNodeAndWaitIfNeed(zookeeper, current_task_status_path, is_unprioritized_task);

    LOG_DEBUG(log, "Processing " << current_task_status_path);

    CleanStateClock clean_state_clock (zookeeper, is_dirty_flag_path, is_dirt_cleaned_path);

    LogicalClock task_start_clock;
    {
        Coordination::Stat stat;
        if (zookeeper->exists(task_partition.getPartitionShardsPath(), &stat))
            task_start_clock = LogicalClock(stat.mzxid);
    }

    /// Do not start if partition is dirty, try to clean it
    if (clean_state_clock.is_clean()
        && (!task_start_clock.hasHappened() || clean_state_clock.discovery_zxid <= task_start_clock))
    {
        LOG_DEBUG(log, "Partition " << task_partition.name << " appears to be clean");
        zookeeper->createAncestors(current_task_status_path);
    }
    else
    {
        LOG_DEBUG(log, "Partition " << task_partition.name << " is dirty, try to drop it");

        try
        {
            tryDropPartition(task_partition, zookeeper, clean_state_clock);
        }
        catch (...)
        {
            tryLogCurrentException(log, "An error occurred when clean partition");
        }

        return PartitionTaskStatus::Error;
    }

    /// Create ephemeral node to mark that we are active and process the partition
    zookeeper->createAncestors(current_task_is_active_path);
    zkutil::EphemeralNodeHolderPtr partition_task_node_holder;
    try
    {
        partition_task_node_holder = zkutil::EphemeralNodeHolder::create(current_task_is_active_path, *zookeeper, host_id);
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::ZNODEEXISTS)
        {
            LOG_DEBUG(log, "Someone is already processing " << current_task_is_active_path);
            return PartitionTaskStatus::Active;
        }

        throw;
    }

    /// Exit if task has been already processed;
    /// create blocking node to signal cleaning up if it is abandoned
    {
        String status_data;
        if (zookeeper->tryGet(current_task_status_path, status_data))
        {
            TaskStateWithOwner status = TaskStateWithOwner::fromString(status_data);
            if (status.state == TaskState::Finished)
            {
                LOG_DEBUG(log, "Task " << current_task_status_path << " has been successfully executed by " << status.owner);
                return PartitionTaskStatus::Finished;
            }

            // Task is abandoned, initialize DROP PARTITION
            LOG_DEBUG(log, "Task " << current_task_status_path << " has not been successfully finished by " << status.owner << ". Partition will be dropped and refilled.");

            create_is_dirty_node(clean_state_clock);
            return PartitionTaskStatus::Error;
        }
    }

    /// Check that destination partition is empty if we are first worker
    /// NOTE: this check is incorrect if pull and push tables have different partition key!
    String clean_start_status;
    if (!zookeeper->tryGet(task_partition.getPartitionCleanStartPath(), clean_start_status) || clean_start_status != "ok")
    {
        zookeeper->createIfNotExists(task_partition.getPartitionCleanStartPath(), "");
        auto checker = zkutil::EphemeralNodeHolder::create(task_partition.getPartitionCleanStartPath() + "/checker", *zookeeper, host_id);
        // Maybe we are the first worker
        ASTPtr query_select_ast = get_select_query(task_shard.table_split_shard, "count()");
        UInt64 count;
        {
            Context local_context = context;
            // Use pull (i.e. readonly) settings, but fetch data from destination servers
            local_context.getSettingsRef() = task_cluster->settings_pull;
            local_context.getSettingsRef().skip_unavailable_shards = true;

            Block block = getBlockWithAllStreamData(InterpreterFactory::get(query_select_ast, local_context)->execute().in);
            count = (block) ? block.safeGetByPosition(0).column->getUInt(0) : 0;
        }

        if (count != 0)
        {
            Coordination::Stat stat_shards;
            zookeeper->get(task_partition.getPartitionShardsPath(), &stat_shards);

            /// NOTE: partition is still fresh if dirt discovery happens before cleaning
            if (stat_shards.numChildren == 0)
            {
                LOG_WARNING(log, "There are no workers for partition " << task_partition.name
                                 << ", but destination table contains " << count << " rows"
                                 << ". Partition will be dropped and refilled.");

                create_is_dirty_node(clean_state_clock);
                return PartitionTaskStatus::Error;
            }
        }
        zookeeper->set(task_partition.getPartitionCleanStartPath(), "ok");
    }
    /// At this point, we need to sync that the destination table is clean
    /// before any actual work

    /// Try start processing, create node about it
    {
        String start_state = TaskStateWithOwner::getData(TaskState::Started, host_id);
        CleanStateClock new_clean_state_clock (zookeeper, is_dirty_flag_path, is_dirt_cleaned_path);
        if (clean_state_clock != new_clean_state_clock)
        {
            LOG_INFO(log, "Partition " << task_partition.name << " clean state changed, cowardly bailing");
            return PartitionTaskStatus::Error;
        }
        else if (!new_clean_state_clock.is_clean())
        {
            LOG_INFO(log, "Partition " << task_partition.name << " is dirty and will be dropped and refilled");
            create_is_dirty_node(new_clean_state_clock);
            return PartitionTaskStatus::Error;
        }
        zookeeper->create(current_task_status_path, start_state, zkutil::CreateMode::Persistent);
    }

    /// Try create table (if not exists) on each shard
    {
        auto create_query_push_ast = rewriteCreateQueryStorage(task_shard.current_pull_table_create_query, task_table.table_push, task_table.engine_push_ast);
        create_query_push_ast->as<ASTCreateQuery &>().if_not_exists = true;
        String query = queryToString(create_query_push_ast);

        LOG_DEBUG(log, "Create destination tables. Query: " << query);
        UInt64 shards = executeQueryOnCluster(task_table.cluster_push, query, create_query_push_ast, &task_cluster->settings_push,
                                PoolMode::GET_MANY);
        LOG_DEBUG(log, "Destination tables " << getQuotedTable(task_table.table_push) << " have been created on " << shards
                                             << " shards of " << task_table.cluster_push->getShardCount());
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
        ASTPtr query_select_ast = get_select_query(task_shard.table_read_shard, "*", inject_fault ? "1" : "");

        LOG_DEBUG(log, "Executing SELECT query and pull from " << task_shard.getDescription()
                       << " : " << queryToString(query_select_ast));

        ASTPtr query_insert_ast;
        {
            String query;
            query += "INSERT INTO " + getQuotedTable(task_shard.table_split_shard) + " VALUES ";

            ParserQuery p_query(query.data() + query.size());
            query_insert_ast = parseQuery(p_query, query, 0);

            LOG_DEBUG(log, "Executing INSERT query: " << query);
        }

        try
        {
            /// Custom INSERT SELECT implementation
            Context context_select = context;
            context_select.getSettingsRef() = task_cluster->settings_pull;

            Context context_insert = context;
            context_insert.getSettingsRef() = task_cluster->settings_push;

            BlockInputStreamPtr input;
            BlockOutputStreamPtr output;
            {
                BlockIO io_select = InterpreterFactory::get(query_select_ast, context_select)->execute();
                BlockIO io_insert = InterpreterFactory::get(query_insert_ast, context_insert)->execute();

                input = io_select.in;
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
                    future_is_dirty_checker = zookeeper->asyncExists(is_dirty_flag_path);

                /// check_period_milliseconds should less than average insert time of single block
                /// Otherwise, the insertion will slow a little bit
                if (watch.elapsedMilliseconds() >= check_period_milliseconds)
                {
                    Coordination::ExistsResponse status = future_is_dirty_checker.get();

                    if (status.error != Coordination::ZNONODE)
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
            return PartitionTaskStatus::Error;
        }
    }

    /// Finalize the processing, change state of current partition task (and also check is_dirty flag)
    {
        String state_finished = TaskStateWithOwner::getData(TaskState::Finished, host_id);
        CleanStateClock new_clean_state_clock (zookeeper, is_dirty_flag_path, is_dirt_cleaned_path);
        if (clean_state_clock != new_clean_state_clock)
        {
            LOG_INFO(log, "Partition " << task_partition.name << " clean state changed, cowardly bailing");
            return PartitionTaskStatus::Error;
        }
        else if (!new_clean_state_clock.is_clean())
        {
            LOG_INFO(log, "Partition " << task_partition.name << " became dirty and will be dropped and refilled");
            create_is_dirty_node(new_clean_state_clock);
            return PartitionTaskStatus::Error;
        }
        zookeeper->set(current_task_status_path, state_finished, 0);
    }

    LOG_INFO(log, "Partition " << task_partition.name << " copied");
    return PartitionTaskStatus::Finished;
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
    auto connection_entry = task_shard.info.pool->get(timeouts, &task_cluster->settings_pull);
    String create_query_pull_str = getRemoteCreateTable(
        task_shard.task_table.table_pull,
        *connection_entry,
        &task_cluster->settings_pull);

    ParserCreateQuery parser_create_query;
    return parseQuery(parser_create_query, create_query_pull_str, 0);
}

void ClusterCopier::createShardInternalTables(const ConnectionTimeouts & timeouts, TaskShard & task_shard, bool create_split)
{
    TaskTable & task_table = task_shard.task_table;

    /// We need to update table definitions for each part, it could be changed after ALTER
    task_shard.current_pull_table_create_query = getCreateTableForPullShard(timeouts, task_shard);

    /// Create local Distributed tables:
    ///  a table fetching data from current shard and a table inserting data to the whole destination cluster
    String read_shard_prefix = ".read_shard_" + toString(task_shard.indexInCluster()) + ".";
    String split_shard_prefix = ".split.";
    task_shard.table_read_shard = DatabaseAndTableName(working_database_name, read_shard_prefix + task_table.table_id);
    task_shard.table_split_shard = DatabaseAndTableName(working_database_name, split_shard_prefix + task_table.table_id);

    /// Create special cluster with single shard
    String shard_read_cluster_name = read_shard_prefix + task_table.cluster_pull_name;
    ClusterPtr cluster_pull_current_shard = task_table.cluster_pull->getClusterWithSingleShard(task_shard.indexInCluster());
    context.setCluster(shard_read_cluster_name, cluster_pull_current_shard);

    auto storage_shard_ast = createASTStorageDistributed(shard_read_cluster_name, task_table.table_pull.first, task_table.table_pull.second);
    const auto & storage_split_ast = task_table.engine_split_ast;

    auto create_query_ast = removeAliasColumnsFromCreateQuery(task_shard.current_pull_table_create_query);
    auto create_table_pull_ast = rewriteCreateQueryStorage(create_query_ast, task_shard.table_read_shard, storage_shard_ast);
    auto create_table_split_ast = rewriteCreateQueryStorage(create_query_ast, task_shard.table_split_shard, storage_split_ast);

    dropAndCreateLocalTable(create_table_pull_ast);

    if (create_split)
        dropAndCreateLocalTable(create_table_split_ast);
}


std::set<String> ClusterCopier::getShardPartitions(const ConnectionTimeouts & timeouts, TaskShard & task_shard)
{
    createShardInternalTables(timeouts, task_shard, false);

    TaskTable & task_table = task_shard.task_table;

    String query;
    {
        WriteBufferFromOwnString wb;
        wb << "SELECT DISTINCT " << queryToString(task_table.engine_push_partition_key_ast) << " AS partition FROM"
           << " " << getQuotedTable(task_shard.table_read_shard) << " ORDER BY partition DESC";
        query = wb.str();
    }

    ParserQuery parser_query(query.data() + query.size());
    ASTPtr query_ast = parseQuery(parser_query, query, 0);

    LOG_DEBUG(log, "Computing destination partition set, executing query: " << query);

    Context local_context = context;
    local_context.setSettings(task_cluster->settings_pull);
    Block block = getBlockWithAllStreamData(InterpreterFactory::get(query_ast, local_context)->execute().in);

    std::set<String> res;
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

    LOG_DEBUG(log, "There are " << res.size() << " destination partitions in shard " << task_shard.getDescription());

    return res;
}

bool ClusterCopier::checkShardHasPartition(const ConnectionTimeouts & timeouts, TaskShard & task_shard, const String & partition_quoted_name)
{
    createShardInternalTables(timeouts, task_shard, false);

    TaskTable & task_table = task_shard.task_table;

    std::string query = "SELECT 1 FROM " + getQuotedTable(task_shard.table_read_shard)
        + " WHERE (" + queryToString(task_table.engine_push_partition_key_ast) + " = (" + partition_quoted_name + " AS partition_key))";

    if (!task_table.where_condition_str.empty())
        query += " AND (" + task_table.where_condition_str + ")";

    query += " LIMIT 1";

    LOG_DEBUG(log, "Checking shard " << task_shard.getDescription() << " for partition "
                   << partition_quoted_name << " existence, executing query: " << query);

    ParserQuery parser_query(query.data() + query.size());
    ASTPtr query_ast = parseQuery(parser_query, query, 0);

    Context local_context = context;
    local_context.setSettings(task_cluster->settings_pull);
    return InterpreterFactory::get(query_ast, local_context)->execute().in->read().rows() != 0;
}


UInt64 ClusterCopier::executeQueryOnCluster(
    const ClusterPtr & cluster,
    const String & query,
    const ASTPtr & query_ast_,
    const Settings * settings,
    PoolMode pool_mode,
    UInt64 max_successful_executions_per_shard) const
{
    auto num_shards = cluster->getShardsInfo().size();
    std::vector<UInt64> per_shard_num_successful_replicas(num_shards, 0);

    ASTPtr query_ast;
    if (query_ast_ == nullptr)
    {
        ParserQuery p_query(query.data() + query.size());
        query_ast = parseQuery(p_query, query, 0);
    }
    else
        query_ast = query_ast_;


    /// We need to execute query on one replica at least
    auto do_for_shard = [&] (UInt64 shard_index)
    {
        const Cluster::ShardInfo & shard = cluster->getShardsInfo().at(shard_index);
        UInt64 & num_successful_executions = per_shard_num_successful_replicas.at(shard_index);
        num_successful_executions = 0;

        auto increment_and_check_exit = [&] ()
        {
            ++num_successful_executions;
            return max_successful_executions_per_shard && num_successful_executions >= max_successful_executions_per_shard;
        };

        UInt64 num_replicas = cluster->getShardsAddresses().at(shard_index).size();
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
            Settings current_settings = settings ? *settings : task_cluster->settings_common;
            current_settings.max_parallel_replicas = num_remote_replicas ? num_remote_replicas : 1;

            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings.max_execution_time);
            auto connections = shard.pool->getMany(timeouts, &current_settings, pool_mode);

            for (auto & connection : connections)
            {
                if (connection.isNull())
                    continue;

                try
                {
                    /// CREATE TABLE and DROP PARTITION queries return empty block
                    RemoteBlockInputStream stream{*connection, query, Block{}, context, &current_settings};
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
            thread_pool.scheduleOrThrowOnError([=] { do_for_shard(shard_index); });

        thread_pool.wait();
    }

    UInt64 successful_shards = 0;
    for (UInt64 num_replicas : per_shard_num_successful_replicas)
        successful_shards += (num_replicas > 0);

    return successful_shards;
}

}
