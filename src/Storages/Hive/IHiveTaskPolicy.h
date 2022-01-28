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

class HiveTaskPackage : public Marshallable
{
public:
    HiveTaskPackage() = default;
    String policy_name;
    String data;
    void marshal(MarshallablePack & p) const override { p << policy_name << data; }
    void unmarshal(MarshallableUnPack & p) override { p >> policy_name >> data; }
};

/**
 * @brief used to build task_iter callback in StorageHiveCluster for distributing files into different nodes
 */
class IHiveTaskIterateCallback
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
    virtual ~IHiveTaskIterateCallback() = default;

    virtual void setupArgs(const Arguments &) = 0;
    /**
     * @brief build a callback for specified node
     */
    virtual std::shared_ptr<TaskIterator> buildCallback(const Cluster::Address &) = 0;
    virtual String getName() = 0;
};

/**
 * @brief Every work node in StorageHiveCluster uses this to collect its own hdfs files
 */
class IHiveTaskFilesCollector : public WithContext
{
public:
    virtual ~IHiveTaskFilesCollector() = default;
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
using HiveTaskFilesCollectorBuilder = std::function<std::shared_ptr<IHiveTaskFilesCollector>()>;

} // namespace DB

#endif
