#pragma once
#include <optional>
#include <unordered_map>
#include <Common/config.h>
#if USE_HIVE
#include <memory>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <IO/Marshallable.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/Hive/HiveFile.h>
#include <Storages/SelectQueryInfo.h>
namespace DB
{

class HiveQueryTaskPackage : public Marshallable
{
public:
    HiveQueryTaskPackage() = default;
    // Support different files assign strategies.
    String policy_name;
    /**
     * @brief Different files assign strategies may have their data structure, all are serialzied into data.
     *
     */
    String data;
    void marshal(MarshallablePack & p) const override { p << policy_name << data; }
    void unmarshal(MarshallableUnPack & p) override { p >> policy_name >> data; }
};

/**
 * Distributed query by hive cluster is executed in two main steps
 * 1. The initiator node get all related hdfs files from hive metastore server, and assign hdfs files to different clickhouse cluster nodes.
 *    This is is done by IHiveQueryTaskIterateCallback. And then setup remoted query calls to all nodes
 * 2. Every nodes receive the query from initiato node, and call `getReadTaskCallback()` to get assigned hdfs files from the initiator node.
 *    Make a storage `StorageHive` and pass the collected files into it.
 *    This is is done in IHiveQueryTaskFilesCollector.
 *
 * Different files assign strategies could be implemented, and control which to use by `setting hive_cluster_task_iterate_policy`
 */
/**
 * @brief used to build task_iter callback in StorageHiveCluster for distributing files into different nodes
 */
class IHiveQueryTaskIterateCallback
{
public:
    struct Arguments
    {
        String cluster_name;
        std::shared_ptr<HiveSettings> storage_settings;
        ColumnsDescription columns;
        ContextPtr context;
        SelectQueryInfo * query_info;
        String hive_metastore_url;
        String hive_database;
        String hive_table;
        ASTPtr partition_by_ast;
        unsigned num_streams;
        Arguments & operator=(const Arguments &) = default;
    };
    virtual ~IHiveQueryTaskIterateCallback() = default;

    virtual void setupArgs(const Arguments &) = 0;
    /**
     * @brief build a callback for specified node
     */
    virtual std::shared_ptr<TaskIterator> buildCallback(const Cluster::Address &) = 0;
    virtual String getName() = 0;
};
using HiveQueryTaskIterateCallbackPtr = std::shared_ptr<IHiveQueryTaskIterateCallback>;
using HiveQueryTaskIterateCallbackBuilder = std::function<HiveQueryTaskIterateCallbackPtr()>;


/**
 * @brief Every work node in StorageHiveCluster uses this to collect its own hdfs files
 */
class IHiveQueryTaskFilesCollector : public WithContext
{
public:
    virtual ~IHiveQueryTaskFilesCollector() = default;
    struct Arguments
    {
        ContextPtr context;
        SelectQueryInfo * query_info;
        String hive_metastore_url;
        String hive_database;
        String hive_table;
        std::shared_ptr<HiveSettings> storage_settings;
        ColumnsDescription columns;
        unsigned num_streams;
        ASTPtr partition_by_ast;
        Arguments & operator=(const Arguments & args) = default;
    };
    virtual void setupCallbackData(const String & data_) = 0;
    virtual void setupArgs(const Arguments &) = 0;
    virtual HiveFiles collectHiveFiles() = 0;
    virtual String getName() = 0;
};
using HiveQueryTaskFilesCollectorPtr = std::shared_ptr<IHiveQueryTaskFilesCollector>;
using HiveQueryTaskFilesCollectorBuilder = std::function<HiveQueryTaskFilesCollectorPtr()>;

struct DistributedHiveQueryTaskBuilder
{
    HiveQueryTaskIterateCallbackBuilder task_iterator_callback;
    HiveQueryTaskFilesCollectorBuilder files_collector;
};
using DistributedHiveQueryTaskBuilderPtr = std::unique_ptr<DistributedHiveQueryTaskBuilder>;

}

#endif
