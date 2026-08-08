#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage.h>

namespace DB
{

class QueryRunnerDispatcher;
enum class QueryRunnerMode : uint8_t;
struct QueryRunnerSettings;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

class StorageQueryRunner final : public IStorage, WithContext
{
public:
    StorageQueryRunner(
        const StorageID & table_id_,
        ColumnsDescription columns_,
        ConstraintsDescription constraints_,
        const String & comment,
        const ASTPtr & sql_security_,
        const QueryRunnerSettings & settings,
        ContextPtr context_);

    ~StorageQueryRunner() override;

    std::string getName() const override { return "QueryRunner"; }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    void startup() override;
    void shutdown(bool is_drop) override;
    void drop() override;

    void waitForQueriesToFinish(const QueryStatusPtr & query_status);

    bool supportsParallelInsert() const override { return true; }

private:
    QueryRunnerMode mode;
    std::unique_ptr<QueryRunnerDispatcher> dispatcher;
    LoggerPtr log;
};

}
