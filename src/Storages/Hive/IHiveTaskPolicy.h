#pragma once
#include <Common/config.h>
#if USE_HIVE
#include <memory>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/Hive/HiveFile.h>
#include <Storages/SelectQueryInfo.h>
namespace DB
{
/**
 * @brief used to build task_iter callback in StorageHiveCluster for distributing files into different nodes
 * 
 */
class IHiveTaskIterateCallback
{
public:
    struct Arguments
    {
        ContextPtr context;
        SelectQueryInfo * query_info;
        String hive_metastore_url;
        String hive_database;
        String hive_table;
    };
    virtual ~IHiveTaskIterateCallback() = default;

    virtual void init(const Arguments &) = 0;
    /**
     * @brief build a callback for specified node
     * 
     */
    virtual std::shared_ptr<TaskIterator> buildCallback(const Cluster::Address &) = 0;
};

/**
 * @brief Every work node in StorageHiveCluster uses this to collect its own hdfs files 
 * 
 */
class IHiveTaskFilesCollector : public WithContext
{
public:
    virtual ~IHiveTaskFilesCollector() = default;
    struct Arguments
    {
        //Poco::JSON::Object::Ptr callback_config;
        ContextPtr context;
        SelectQueryInfo * query_info;
        String hive_metastore_url;
        String hive_database;
        String hive_table;
        std::shared_ptr<HiveSettings> storage_settings;
        ColumnsDescription columns;
        unsigned num_streams;
        ASTPtr partition_by_ast;
        Arguments & operator = (const Arguments & args) = default;
    };
    virtual void init_query_env(const Arguments &) = 0;
    virtual HiveFiles collectHiveFiles() = 0; 
};
} // namespace DB

#endif
