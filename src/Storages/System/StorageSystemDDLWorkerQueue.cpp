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
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Core/Settings.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>


namespace fs = std::filesystem;


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_settings_after_format_in_insert;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

enum class Status : uint8_t
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

ColumnsDescription StorageSystemDDLWorkerQueue::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"entry",               std::make_shared<DataTypeString>(), "Query id."},
        {"entry_version",       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "Version of the entry."},
        {"initiator_host",      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Host that initiated the DDL operation."},
        {"initiator_port",      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt16>()), "Port used by the initiator."},
        {"cluster",             std::make_shared<DataTypeString>(), "Cluster name, empty if not determined."},
        {"query",               std::make_shared<DataTypeString>(), "Query executed."},
        {"settings",            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "Settings used in the DDL operation."},
        {"query_create_time",   std::make_shared<DataTypeDateTime>(), "Query created time."},

        {"host",                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Hostname."},
        {"port",                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt16>()), "Host Port."},
        {"status",              std::make_shared<DataTypeNullable>(std::make_shared<DataTypeEnum8>(getStatusEnumsAndValues())), "Status of the query."},
        {"exception_code",      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt16>()), "Exception code."},
        {"exception_text",      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Exception message."},
        {"query_finish_time",   std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Query finish time."},
        {"query_duration_ms",   std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Duration of query execution (in milliseconds)."},
    };
}

static String clusterNameFromDDLQuery(ContextPtr context, const DDLTask & task)
{
    const char * begin = task.entry.query.data();
    const char * end = begin + task.entry.query.size();
    const auto & settings = context->getSettingsRef();

    String description = fmt::format("from {}", task.entry_path);
    ParserQuery parser_query(end, settings[Setting::allow_settings_after_format_in_insert]);
    ASTPtr query;

    try
    {
        query = parseQuery(
            parser_query, begin, end, description, settings[Setting::max_query_size], settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
    }
    catch (const Exception & e)
    {
        LOG_INFO(getLogger("StorageSystemDDLWorkerQueue"), "Failed to determine cluster");
        if (e.code() == ErrorCodes::SYNTAX_ERROR)
        {
            /// ignore parse error and present available information
            return "";
        }
        throw;
    }

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
            pair.push_back(fieldToString(change.value));
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
    chassert(res_columns[num_filled_columns - 1]->size() == res_columns[num_filled_columns]->size());
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
                                Coordination::GetResponse & finished_data,
                                UInt64 query_create_time_ms)
{
    if (finished_data.error == Coordination::Error::ZNONODE)
    {
        fillStatusColumnsWithNulls(res_columns, col, Status::REMOVING);
        return;
    }

    /// asyncTryGet should throw on other error codes
    chassert(finished_data.error == Coordination::Error::ZOK);

    /// status
    res_columns[col++]->insert(static_cast<Int8>(Status::FINISHED));

    auto execution_status = ExecutionStatus::fromText(finished_data.data);
    /// exception_code
    res_columns[col++]->insert(execution_status.code);
    /// exception_text
    res_columns[col++]->insert(execution_status.message);

    UInt64 query_finish_time_ms = finished_data.stat.ctime;
    /// query_finish_time
    res_columns[col++]->insert(query_finish_time_ms / 1000);
    /// query_duration_ms
    res_columns[col++]->insert(query_finish_time_ms - query_create_time_ms);
}


void StorageSystemDDLWorkerQueue::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto component_guard = Coordination::setCurrentComponent("StorageSystemDDLWorkerQueue::fillData");
    auto & ddl_worker = context->getDDLWorker();
    fs::path ddl_zookeeper_path = ddl_worker.getQueueDir();
    zkutil::ZooKeeperPtr zookeeper = ddl_worker.getZooKeeperFromContext();
    Strings ddl_task_paths = zookeeper->getChildren(ddl_zookeeper_path);


    std::vector<std::string> ddl_task_full_paths;
    ddl_task_full_paths.reserve(ddl_task_paths.size());
    std::vector<std::string> ddl_task_status_paths;
    ddl_task_status_paths.reserve(ddl_task_paths.size() * 2); // for active and finished

    for (const auto & task_path : ddl_task_paths)
    {
        ddl_task_full_paths.push_back(ddl_zookeeper_path / task_path);
        /// List status dirs. Active host may become finished, so we list active first.
        ddl_task_status_paths.push_back(ddl_zookeeper_path / task_path / "active");
        ddl_task_status_paths.push_back(ddl_zookeeper_path / task_path / "finished");
    }

    auto ddl_tasks_info = zookeeper->tryGet(ddl_task_full_paths);
    auto ddl_task_statuses = zookeeper->tryGetChildren(ddl_task_status_paths);

    for (size_t i = 0; i < ddl_task_paths.size(); ++i)
    {
        auto & task_info = ddl_tasks_info[i];
        if (task_info.error != Coordination::Error::ZOK)
        {
            /// Task is removed
            chassert(task_info.error == Coordination::Error::ZNONODE);
            continue;
        }

        DDLTask task{ddl_task_paths[i], ddl_zookeeper_path / ddl_task_paths[i]};
        try
        {
            task.entry.parse(task_info.data);
        }
        catch (Exception & e)
        {
            e.addMessage("On parsing DDL entry {}: {}", task.entry_path, task_info.data);
            throw;
        }

        String cluster_name = clusterNameFromDDLQuery(context, task);
        UInt64 query_create_time_ms = task_info.stat.ctime;

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

        auto & finished_hosts = ddl_task_statuses[i * 2 + 1];
        if (finished_hosts.error == Coordination::Error::ZOK)
        {
            std::vector<std::string> finished_status_paths;
            finished_status_paths.reserve(finished_hosts.names.size());
            for (const auto & host_id_str : finished_hosts.names)
                finished_status_paths.push_back(fs::path(task.entry_path) / "finished" / host_id_str);

            auto finished_statuses = zookeeper->tryGet(finished_status_paths);
            for (size_t host_idx = 0; host_idx < finished_hosts.names.size(); ++host_idx)
            {
                const auto & host_id_str = finished_hosts.names[host_idx];
                HostID host_id = HostID::fromString(host_id_str);
                repeatValuesInCommonColumns(res_columns, col);
                size_t rest_col = col;
                fillHostnameColumns(res_columns, rest_col, host_id);
                fillStatusColumns(res_columns, rest_col, finished_statuses[host_idx], query_create_time_ms);
                processed_hosts.insert(host_id_str);
            }
        }
        else if (finished_hosts.error == Coordination::Error::ZNONODE)
        {
            /// Rare case: Either status dirs are not created yet or already removed.
            /// We can distinguish it by checking if task node exists, because "query-xxx" and "query-xxx/finished"
            /// are removed in single multi-request
            is_removing_task = !zookeeper->exists(task.entry_path);
        }
        else
        {
            throw Coordination::Exception::fromPath(finished_hosts.error, fs::path(task.entry_path) / "finished");
        }

        /// Process active nodes
        auto & active_hosts = ddl_task_statuses[i * 2];
        if (active_hosts.error == Coordination::Error::ZOK)
        {
            for (const auto & host_id_str : active_hosts.names)
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
        else if (active_hosts.error == Coordination::Error::ZNONODE)
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
            throw Coordination::Exception::fromPath(active_hosts.error, fs::path(task.entry_path) / "active");
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
