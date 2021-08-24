#include <algorithm>
#include <filesystem>

#include "StorageSystemDDLWorkerQueue.h"

#include <Columns/ColumnArray.h>
#include <Interpreters/DDLTask.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>


namespace fs = std::filesystem;

enum Status
{
    ACTIVE,
    FINISHED,
    UNKNOWN,
    ERRORED
};

namespace DB
{
std::vector<std::pair<String, Int8>> getStatusEnumsAndValues()
{
    return std::vector<std::pair<String, Int8>>{
        {"Active", static_cast<Int8>(Status::ACTIVE)},
        {"Finished", static_cast<Int8>(Status::FINISHED)},
        {"Unknown", static_cast<Int8>(Status::UNKNOWN)},
        {"Errored", static_cast<Int8>(Status::ERRORED)},
    };
}


std::vector<std::pair<String, Int8>> getZooKeeperErrorEnumsAndValues()
{
    return std::vector<std::pair<String, Int8>>{
        {"ZOK", static_cast<Int8>(Coordination::Error::ZOK)},
        {"ZSYSTEMERROR", static_cast<Int8>(Coordination::Error::ZSYSTEMERROR)},
        {"ZRUNTIMEINCONSISTENCY", static_cast<Int8>(Coordination::Error::ZRUNTIMEINCONSISTENCY)},
        {"ZDATAINCONSISTENCY", static_cast<Int8>(Coordination::Error::ZDATAINCONSISTENCY)},
        {"ZCONNECTIONLOSS", static_cast<Int8>(Coordination::Error::ZCONNECTIONLOSS)},
        {"ZMARSHALLINGERROR", static_cast<Int8>(Coordination::Error::ZMARSHALLINGERROR)},
        {"ZUNIMPLEMENTED", static_cast<Int8>(Coordination::Error::ZUNIMPLEMENTED)},
        {"ZOPERATIONTIMEOUT", static_cast<Int8>(Coordination::Error::ZOPERATIONTIMEOUT)},
        {"ZBADARGUMENTS", static_cast<Int8>(Coordination::Error::ZBADARGUMENTS)},
        {"ZINVALIDSTATE", static_cast<Int8>(Coordination::Error::ZINVALIDSTATE)},
        {"ZAPIERROR", static_cast<Int8>(Coordination::Error::ZAPIERROR)},
        {"ZNONODE", static_cast<Int8>(Coordination::Error::ZNONODE)},
        {"ZNOAUTH", static_cast<Int8>(Coordination::Error::ZNOAUTH)},
        {"ZBADVERSION", static_cast<Int8>(Coordination::Error::ZBADVERSION)},
        {"ZNOCHILDRENFOREPHEMERALS", static_cast<Int8>(Coordination::Error::ZNOCHILDRENFOREPHEMERALS)},
        {"ZNODEEXISTS", static_cast<Int8>(Coordination::Error::ZNODEEXISTS)},
        {"ZNOTEMPTY", static_cast<Int8>(Coordination::Error::ZNOTEMPTY)},
        {"ZSESSIONEXPIRED", static_cast<Int8>(Coordination::Error::ZSESSIONEXPIRED)},
        {"ZINVALIDCALLBACK", static_cast<Int8>(Coordination::Error::ZINVALIDCALLBACK)},
        {"ZINVALIDACL", static_cast<Int8>(Coordination::Error::ZINVALIDACL)},
        {"ZAUTHFAILED", static_cast<Int8>(Coordination::Error::ZAUTHFAILED)},
        {"ZCLOSING", static_cast<Int8>(Coordination::Error::ZCLOSING)},
        {"ZNOTHING", static_cast<Int8>(Coordination::Error::ZNOTHING)},
        {"ZSESSIONMOVED", static_cast<Int8>(Coordination::Error::ZSESSIONMOVED)},
    };
}


NamesAndTypesList StorageSystemDDLWorkerQueue::getNamesAndTypes()
{
    return {
        {"entry", std::make_shared<DataTypeString>()},
        {"host_name", std::make_shared<DataTypeString>()},
        {"host_address", std::make_shared<DataTypeString>()},
        {"port", std::make_shared<DataTypeUInt16>()},
        {"status", std::make_shared<DataTypeEnum8>(getStatusEnumsAndValues())},
        {"cluster", std::make_shared<DataTypeString>()},
        {"query", std::make_shared<DataTypeString>()},
        {"initiator", std::make_shared<DataTypeString>()},
        {"query_start_time", std::make_shared<DataTypeDateTime>()},
        {"query_finish_time", std::make_shared<DataTypeDateTime>()},
        {"query_duration_ms", std::make_shared<DataTypeUInt64>()},
        {"exception_code", std::make_shared<DataTypeEnum8>(getZooKeeperErrorEnumsAndValues())},
    };
}

static String clusterNameFromDDLQuery(ContextPtr context, const DDLLogEntry & entry)
{
    const char * begin = entry.query.data();
    const char * end = begin + entry.query.size();
    ASTPtr query;
    ASTQueryWithOnCluster * query_on_cluster;
    String cluster_name;
    ParserQuery parser_query(end);
    String description;
    query = parseQuery(parser_query, begin, end, description, 0, context->getSettingsRef().max_parser_depth);
    if (query && (query_on_cluster = dynamic_cast<ASTQueryWithOnCluster *>(query.get())))
        cluster_name = query_on_cluster->cluster;
    return cluster_name;
}

void StorageSystemDDLWorkerQueue::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    zkutil::ZooKeeperPtr zookeeper = context->getZooKeeper();
    Coordination::Error zk_exception_code = Coordination::Error::ZOK;
    String ddl_zookeeper_path = config.getString("distributed_ddl.path", "/clickhouse/task_queue/ddl/");
    String ddl_query_path;

    // this is equivalent to query zookeeper at the `ddl_zookeeper_path`
    /* [zk: localhost:2181(CONNECTED) 51] ls /clickhouse/task_queue/ddl
       [query-0000000000, query-0000000001, query-0000000002, query-0000000003, query-0000000004]
    */

    zkutil::Strings queries;

    Coordination::Error code = zookeeper->tryGetChildren(ddl_zookeeper_path, queries);
    // if there is an error here, just register the code.
    // the queries will be empty and so there will be nothing to fill the table
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
        zk_exception_code = code;

    const auto clusters = context->getClusters();
    for (const auto & name_and_cluster : clusters->getContainer())
    {
        const ClusterPtr & cluster = name_and_cluster.second;
        const auto & shards_info = cluster->getShardsInfo();
        const auto & addresses_with_failover = cluster->getShardsAddresses();
        for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
        {
            const auto & shard_addresses = addresses_with_failover[shard_index];
            const auto & shard_info = shards_info[shard_index];
            const auto pool_status = shard_info.pool->getStatus();
            for (size_t replica_index = 0; replica_index < shard_addresses.size(); ++replica_index)
            {
                /*  Dir contents of every query will be similar to
                 [zk: localhost:2181(CONNECTED) 53] ls /clickhouse/task_queue/ddl/query-0000000004
                [active, finished]
                */
                std::vector<std::future<Coordination::GetResponse>> futures;
                futures.reserve(queries.size());
                for (const String & q : queries)
                {
                    futures.push_back(zookeeper->asyncTryGet(fs::path(ddl_zookeeper_path) / q));
                }
                for (size_t query_id = 0; query_id < queries.size(); query_id++)
                {
                    Int64 query_finish_time = 0;
                    size_t i = 0;
                    res_columns[i++]->insert(queries[query_id]); // entry
                    const auto & address = shard_addresses[replica_index];
                    res_columns[i++]->insert(address.host_name);
                    auto resolved = address.getResolvedAddress();
                    res_columns[i++]->insert(resolved ? resolved->host().toString() : String()); // host_address
                    res_columns[i++]->insert(address.port);
                    ddl_query_path = fs::path(ddl_zookeeper_path) / queries[query_id];

                    zkutil::Strings active_nodes;
                    zkutil::Strings finished_nodes;

                    code = zookeeper->tryGetChildren(fs::path(ddl_query_path) / "active", active_nodes);
                    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
                        zk_exception_code = code;

                    code = zookeeper->tryGetChildren(fs::path(ddl_query_path) / "finished", finished_nodes);
                    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
                        zk_exception_code = code;

                    /* status:
                 * active: If the hostname:port entry is present under active path.
                 * finished: If the hostname:port entry is present under the finished path.
                 * errored: If the hostname:port entry is present under the finished path but the error count is not 0.
                 * unknown: If the above cases don't hold true, then status is unknown.
                 */
                    if (std::find(active_nodes.begin(), active_nodes.end(), address.toString()) != active_nodes.end())
                    {
                        res_columns[i++]->insert(static_cast<Int8>(Status::ACTIVE));
                    }
                    else if (std::find(finished_nodes.begin(), finished_nodes.end(), address.toString()) != finished_nodes.end())
                    {
                        if (pool_status[replica_index].error_count != 0)
                        {
                            res_columns[i++]->insert(static_cast<Int8>(Status::ERRORED));
                        }
                        else
                        {
                            res_columns[i++]->insert(static_cast<Int8>(Status::FINISHED));
                        }
                        // regardless of the status finished or errored, the node host_name:port entry was found under the /finished path
                        // & should be able to get the contents of the znode at /finished path.
                        auto res_fn = zookeeper->asyncTryGet(fs::path(ddl_query_path) / "finished");
                        auto stat_fn = res_fn.get().stat;
                        query_finish_time = stat_fn.mtime;
                    }
                    else
                    {
                        res_columns[i++]->insert(static_cast<Int8>(Status::UNKNOWN));
                    }

                    Coordination::GetResponse res;
                    res = futures[query_id].get();

                    auto query_start_time = res.stat.mtime;

                    DDLLogEntry entry;
                    entry.parse(res.data);
                    String cluster_name = clusterNameFromDDLQuery(context, entry);

                    res_columns[i++]->insert(cluster_name);
                    res_columns[i++]->insert(entry.query);
                    res_columns[i++]->insert(entry.initiator);
                    res_columns[i++]->insert(UInt64(query_start_time / 1000));
                    res_columns[i++]->insert(UInt64(query_finish_time / 1000));
                    res_columns[i++]->insert(UInt64(query_finish_time - query_start_time));
                    res_columns[i++]->insert(static_cast<Int8>(zk_exception_code));
                }
            }
        }
    }
}
}
