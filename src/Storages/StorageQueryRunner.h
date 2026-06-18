#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage.h>

namespace DB
{

class QueryRunnerDispatcher;
enum class QueryRunnerMode : uint8_t;

class StorageQueryRunner final : public IStorage, WithContext
{
public:
    StorageQueryRunner(
        const StorageID & table_id_,
        ColumnsDescription columns_,
        ConstraintsDescription constraints_,
        const String & comment,
        const ASTPtr & sql_security_,
        const String & cluster_name_,
        UInt64 shard_num_,
        QueryRunnerMode mode_,
        UInt64 num_threads_,
        UInt64 max_queue_size_,
        ContextPtr context_);

    ~StorageQueryRunner() override;

    std::string getName() const override { return "QueryRunner"; }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    void startup() override;
    void shutdown(bool is_drop) override;
    void drop() override;

    bool supportsParallelInsert() const override { return true; }

private:
    QueryRunnerMode mode;
    std::unique_ptr<QueryRunnerDispatcher> dispatcher;
    LoggerPtr log;
};

}
