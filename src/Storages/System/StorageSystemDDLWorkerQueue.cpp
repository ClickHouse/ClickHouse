#include <Storages/System/StorageSystemDDLWorkerQueue.h>
#include <Interpreters/DDLTask.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <Interpreters/Context.h>
#include <Interpreters/ZooKeeperLog.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>


namespace fs = std::filesystem;


namespace DB
{

enum class Status
{
    INACTIVE,
    ACTIVE,
    FINISHED,
    REMOVING,
    UNKNOWN,
};

using GetResponseFuture = std::future<Coordination::GetResponse>;
using ListResponseFuture = std::future<Coordination::ListResponse>;
using GetResponseFutures = std::vector<GetResponseFuture>;
using ListResponseFutures = std::vector<ListResponseFuture>;

static std::vector<std::pair<String, Int8>> getStatusEnumsAndValues()
{
    return std::vector<std::pair<String, Int8>>{
        {"Inactive",    static_cast<Int8>(Status::INACTIVE)},
        {"Active",      static_cast<Int8>(Status::ACTIVE)},
        {"Finished",    static_cast<Int8>(Status::FINISHED)},
        {"Removing",    static_cast<Int8>(Status::REMOVING)},
        {"Unknown",     static_cast<Int8>(Status::UNKNOWN)},
    };
}

NamesAndTypesList StorageSystemDDLWorkerQueue::getNamesAndTypes()
{
    return {
        {"entry",               std::make_shared<DataTypeString>()},
        {"entry_version",       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
        {"initiator_host",      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"initiator_port",      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt16>())},
        {"cluster",             std::make_shared<DataTypeString>()},
        {"query",               std::make_shared<DataTypeString>()},
        {"settings",            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
        {"query_create_time",   std::make_shared<DataTypeDateTime>()},

        {"host",                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"port",                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt16>())},
        {"status",              std::make_shared<DataTypeNullable>(std::make_shared<DataTypeEnum8>(getStatusEnumsAndValues()))},
        {"exception_code",      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt16>())},
        {"exception_text",      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"query_finish_time",   std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())},
        {"query_duration_ms",   std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
    };
}

static String clusterNameFromDDLQuery(ContextPtr context, const DDLTask & task)
{
    const char * begin = task.entry.query.data();
    const char * end = begin + task.entry.query.size();
    const auto & settings = context->getSettingsRef();

    String description = fmt::format("from {}", task.entry_path);
    ParserQuery parser_query(end, settings.allow_settings_after_format_in_insert);
    ASTPtr query = parseQuery(parser_query, begin, end, description,
                              settings.max_query_size,
                              settings.max_parser_depth);

    String cluster_name;
    if (const auto * query_on_cluster = dynamic_cast<const ASTQueryWithOnCluster *>(query.get()))
        cluster_name = query_on_cluster->cluster;

    return cluster_name;
}

static void fillCommonColumns(MutableColumns & res_columns, size_t & col, const DDLTask & task, const String & cluster_name, UInt64 query_create_time_ms)
{
    /// entry
    res_columns[col++]->insert(task.entry_name);

    /// entry_version
    res_columns[col++]->insert(task.entry.version);

    if (task.entry.initiator.empty())
    {
        /// initiator_host
        res_columns[col++]->insert(Field{});
        /// initiator_port
        res_columns[col++]->insert(Field{});
    }
    else
    {
        HostID initiator = HostID::fromString(task.entry.initiator);
        /// initiator_host
        res_columns[col++]->insert(initiator.host_name);
        /// initiator_port
        res_columns[col++]->insert(initiator.port);
    }

    /// cluster
    res_columns[col++]->insert(cluster_name);

    /// query
    res_columns[col++]->insert(task.entry.query);

    Map settings_map;
    if (task.entry.settings)
    {
        for (const auto & change : *task.entry.settings)
        {
            Tuple pair;
            pair.push_back(change.name);
            pair.push_back(toString(change.value));
            settings_map.push_back(std::move(pair));
        }
    }

    /// settings
    res_columns[col++]->insert(settings_map);

    res_columns[col++]->insert(static_cast<UInt64>(query_create_time_ms / 1000));
}

static void repeatValuesInCommonColumns(MutableColumns & res_columns, size_t num_filled_columns)
{
    if (res_columns[num_filled_columns - 1]->size() == res_columns[num_filled_columns]->size() + 1)
    {
        /// Common columns are already filled
        return;
    }

    /// Copy values from previous row
    assert(res_columns[num_filled_columns - 1]->size() == res_columns[num_filled_columns]->size());
    for (size_t filled_col = 0; filled_col < num_filled_columns; ++filled_col)
        res_columns[filled_col]->insert((*res_columns[filled_col])[res_columns[filled_col]->size() - 1]);
}

static void fillHostnameColumns(MutableColumns & res_columns, size_t & col, const HostID & host_id)
{
    /// NOTE host_id.host_name can be a domain name or an IP address
    /// We could try to resolve domain name or reverse resolve an address and add two separate columns,
    /// but seems like it's not really needed, so we show host_id.host_name as is.

    /// host
    res_columns[col++]->insert(host_id.host_name);

    /// port
    res_columns[col++]->insert(host_id.port);
}

static void fillStatusColumnsWithNulls(MutableColumns & res_columns, size_t & col, Status status)
{
    /// status
    res_columns[col++]->insert(static_cast<Int8>(status));
    /// exception_code
    res_columns[col++]->insert(Field{});
    /// exception_text
    res_columns[col++]->insert(Field{});
    /// query_finish_time
    res_columns[col++]->insert(Field{});
    /// query_duration_ms
    res_columns[col++]->insert(Field{});
}

static void fillStatusColumns(MutableColumns & res_columns, size_t & col,
                                GetResponseFuture & finished_data_future,
                                UInt64 query_create_time_ms)
{
    auto maybe_finished_status = finished_data_future.get();
    if (maybe_finished_status.error == Coordination::Error::ZNONODE)
        return fillStatusColumnsWithNulls(res_columns, col, Status::REMOVING);

    /// asyncTryGet should throw on other error codes
    assert(maybe_finished_status.error == Coordination::Error::ZOK);

    /// status
    res_columns[col++]->insert(static_cast<Int8>(Status::FINISHED));

    auto execution_status = ExecutionStatus::fromText(maybe_finished_status.data);
    /// exception_code
    res_columns[col++]->insert(execution_status.code);
    /// exception_text
    res_columns[col++]->insert(execution_status.message);

    UInt64 query_finish_time_ms = maybe_finished_status.stat.ctime;
    /// query_finish_time
    res_columns[col++]->insert(static_cast<UInt64>(query_finish_time_ms / 1000));
    /// query_duration_ms
    res_columns[col++]->insert(static_cast<UInt64>(query_finish_time_ms - query_create_time_ms));
}


void StorageSystemDDLWorkerQueue::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    zkutil::ZooKeeperPtr zookeeper = context->getZooKeeper();
    fs::path ddl_zookeeper_path = context->getConfigRef().getString("distributed_ddl.path", "/clickhouse/task_queue/ddl/");

    Strings ddl_task_paths = zookeeper->getChildren(ddl_zookeeper_path);

    GetResponseFutures ddl_task_futures;
    ListResponseFutures active_nodes_futures;
    ListResponseFutures finished_nodes_futures;

    for (const auto & task_path : ddl_task_paths)
    {
        ddl_task_futures.push_back(zookeeper->asyncTryGet(ddl_zookeeper_path / task_path));
        /// List status dirs. Active host may become finished, so we list active first.
        active_nodes_futures.push_back(zookeeper->asyncTryGetChildrenNoThrow(ddl_zookeeper_path / task_path / "active"));
        finished_nodes_futures.push_back(zookeeper->asyncTryGetChildrenNoThrow(ddl_zookeeper_path / task_path / "finished"));
    }

    for (size_t i = 0; i < ddl_task_paths.size(); ++i)
    {
        auto maybe_task = ddl_task_futures[i].get();
        if (maybe_task.error != Coordination::Error::ZOK)
        {
            /// Task is removed
            assert(maybe_task.error == Coordination::Error::ZNONODE);
            continue;
        }

        DDLTask task{ddl_task_paths[i], ddl_zookeeper_path / ddl_task_paths[i]};
        try
        {
            task.entry.parse(maybe_task.data);
        }
        catch (Exception & e)
        {
            e.addMessage("On parsing DDL entry {}: {}", task.entry_path, maybe_task.data);
            throw;
        }

        String cluster_name = clusterNameFromDDLQuery(context, task);
        UInt64 query_create_time_ms = maybe_task.stat.ctime;

        size_t col = 0;
        fillCommonColumns(res_columns, col, task, cluster_name, query_create_time_ms);

        /// At first we process finished nodes, to avoid duplication if some host was active
        /// and suddenly become finished during status dirs listing.
        /// Then we process active (but not finished) hosts.
        /// And then we process the rest hosts from task.entry.hosts list.
        /// NOTE: It's not guaranteed that task.entry.hosts contains all host ids from status dirs.
        std::unordered_set<String> processed_hosts;

        /// Race condition with DDLWorker::cleanupQueue(...) is possible.
        /// We may get incorrect list of finished nodes if task is currently removing.
        /// To avoid showing INACTIVE status for hosts that have actually executed query,
        /// we will detect if someone is removing task and show special REMOVING status.
        /// Also we should distinguish it from another case when status dirs are not created yet (extremely rare case).
        bool is_removing_task = false;

        auto maybe_finished_hosts = finished_nodes_futures[i].get();
        if (maybe_finished_hosts.error == Coordination::Error::ZOK)
        {
            GetResponseFutures finished_status_futures;
            for (const auto & host_id_str : maybe_finished_hosts.names)
                finished_status_futures.push_back(zookeeper->asyncTryGet(fs::path(task.entry_path) / "finished" / host_id_str));

            for (size_t host_idx = 0; host_idx < maybe_finished_hosts.names.size(); ++host_idx)
            {
                const auto & host_id_str = maybe_finished_hosts.names[host_idx];
                HostID host_id = HostID::fromString(host_id_str);
                repeatValuesInCommonColumns(res_columns, col);
                size_t rest_col = col;
                fillHostnameColumns(res_columns, rest_col, host_id);
                fillStatusColumns(res_columns, rest_col, finished_status_futures[host_idx], query_create_time_ms);
                processed_hosts.insert(host_id_str);
            }
        }
        else if (maybe_finished_hosts.error == Coordination::Error::ZNONODE)
        {
            /// Rare case: Either status dirs are not created yet or already removed.
            /// We can distinguish it by checking if task node exists, because "query-xxx" and "query-xxx/finished"
            /// are removed in single multi-request
            is_removing_task = !zookeeper->exists(task.entry_path);
        }
        else
        {
            throw Coordination::Exception(maybe_finished_hosts.error, fs::path(task.entry_path) / "finished");
        }

        /// Process active nodes
        auto maybe_active_hosts = active_nodes_futures[i].get();
        if (maybe_active_hosts.error == Coordination::Error::ZOK)
        {
            for (const auto & host_id_str : maybe_active_hosts.names)
            {
                if (processed_hosts.contains(host_id_str))
                    continue;

                HostID host_id = HostID::fromString(host_id_str);
                repeatValuesInCommonColumns(res_columns, col);
                size_t rest_col = col;
                fillHostnameColumns(res_columns, rest_col, host_id);
                fillStatusColumnsWithNulls(res_columns, rest_col, Status::ACTIVE);
                processed_hosts.insert(host_id_str);
            }
        }
        else if (maybe_active_hosts.error == Coordination::Error::ZNONODE)
        {
            /// Rare case: Either status dirs are not created yet or task is currently removing.
            /// When removing a task, at first we remove "query-xxx/active" (not recursively),
            /// then recursively remove everything except "query-xxx/finished"
            /// and then remove "query-xxx" and "query-xxx/finished".
            is_removing_task = is_removing_task ||
                (zookeeper->exists(fs::path(task.entry_path) / "finished") && !zookeeper->exists(fs::path(task.entry_path) / "active")) ||
                !zookeeper->exists(task.entry_path);
        }
        else
        {
            throw Coordination::Exception(maybe_active_hosts.error, fs::path(task.entry_path) / "active");
        }

        /// Process the rest hosts
        for (const auto & host_id : task.entry.hosts)
        {
            if (processed_hosts.contains(host_id.toString()))
                continue;

            Status status = is_removing_task ? Status::REMOVING : Status::INACTIVE;
            repeatValuesInCommonColumns(res_columns, col);
            size_t rest_col = col;
            fillHostnameColumns(res_columns, rest_col, host_id);
            fillStatusColumnsWithNulls(res_columns, rest_col, status);
            processed_hosts.insert(host_id.toString());
        }

        if (processed_hosts.empty())
        {
            /// We don't know any hosts, just fill the rest columns with nulls.
            /// host
            res_columns[col++]->insert(Field{});
            /// port
            res_columns[col++]->insert(Field{});
            fillStatusColumnsWithNulls(res_columns, col, Status::UNKNOWN);
        }
    }
}

}
