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
/**
 * @brief An abstract strategy for collecting hive files. Make StorageHive more extensible and can't support hive cluster query later
 */
class IHiveQueryTaskFilesCollector : public WithContext
{
public:

    enum class PruneLevel
    {
        None, /// Do not prune
        Partition,
        File,
        Split,
        Max = Split,
    };

    static String pruneLevelToString(PruneLevel level) { return String(magic_enum::enum_name(level)); }

    virtual ~IHiveQueryTaskFilesCollector() = default;
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
        Arguments & operator=(const Arguments & args) = default;
    };
    virtual void setupCallbackData(const String & data_) = 0;
    virtual void setupArgs(const Arguments &) = 0;
    virtual HiveFiles collect(PruneLevel prune_level) = 0;
    virtual String getName() = 0;
};
using HiveQueryTaskFilesCollectorPtr = std::shared_ptr<IHiveQueryTaskFilesCollector>;
using HiveQueryTaskFilesCollectorBuilder = std::function<HiveQueryTaskFilesCollectorPtr()>;
}
#endif
