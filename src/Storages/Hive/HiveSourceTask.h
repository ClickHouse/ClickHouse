#pragma once
#include <Common/config.h>

#if USE_HIVE
#include <optional>
#include <unordered_map>
#include <memory>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/Hive/HiveFile.h>
#include <Storages/SelectQueryInfo.h>
#include <Poco/JSON/Parser.h>
namespace DB
{
enum class HivePruneLevel
{
    None, /// Do not prune
    Partition,
    File,
    Split,
    Max = Split,
};

String pruneLevelToString(HivePruneLevel level);

class IHiveSourceFilesCollectCallback
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
    };

    virtual ~IHiveSourceFilesCollectCallback() = default;
    virtual void initialize(const Arguments &) = 0;
    virtual std::shared_ptr<TaskIterator> buildCollectCallback(const Cluster::Address &) = 0;
    virtual String getName() = 0;
};

using HiveSourceFilesCollectCallbackPtr = std::shared_ptr<IHiveSourceFilesCollectCallback>;
using HiveSourceFilesCollectCallbackBuilder = std::function<HiveSourceFilesCollectCallbackPtr()>;

/// An interface class for implementing different collect strategies
class IHiveSourceFilesCollector
{
public:
    virtual ~IHiveSourceFilesCollector() = default;
    struct Arguments
    {
        ContextPtr context;
        const SelectQueryInfo * query_info;
        String hive_metastore_url;
        String hive_database;
        String hive_table;
        std::shared_ptr<HiveSettings> storage_settings;
        ColumnsDescription columns;
        UInt64 num_streams;
        ASTPtr partition_by_ast;
        String callback_data;
    };
    virtual void initialize(const Arguments &) = 0;
    virtual HiveFiles collect(HivePruneLevel prune_level) = 0;
    virtual String getName() = 0;
};
using HiveSourceFilesCollectorPtr = std::shared_ptr<IHiveSourceFilesCollector>;
using HiveSourceFilesCollectorBuilder = std::function<HiveSourceFilesCollectorPtr()>;

class HiveSourceCollectCallbackFactory : private boost::noncopyable
{
public:
    static HiveSourceCollectCallbackFactory & instance();

    std::optional<HiveSourceFilesCollectCallbackPtr> getCallback(const String name)
    {
        auto it = builders.find(name);
        if (it == builders.end())
            return {};
        return it->second();
    }

    void registerBuilder(const String & name, HiveSourceFilesCollectCallbackBuilder builder);
protected:
    HiveSourceCollectCallbackFactory() = default;

    static void registerBuilders(HiveSourceCollectCallbackFactory & factory);
private:
    std::unordered_map<String, HiveSourceFilesCollectCallbackBuilder> builders;
};

class HiveSourceCollectorFactory : private boost::noncopyable
{
public:
    static HiveSourceCollectorFactory & instance();

    std::optional<HiveSourceFilesCollectorPtr> getCollector(const String name)
    {
        auto it = builders.find(name);
        if (it == builders.end())
            return {};
        return it->second();
    }

    void registerBuilder(const String & name, HiveSourceFilesCollectorBuilder builder);
protected:
    HiveSourceCollectorFactory() = default;

    static void registerBuilders(HiveSourceCollectorFactory & factory);
private:
    std::unordered_map<String, HiveSourceFilesCollectorBuilder> builders;
};
}
#endif
