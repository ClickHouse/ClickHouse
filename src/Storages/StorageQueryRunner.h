#pragma once

#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage.h>

namespace DB
{

class ASTStorage;
class QueryRunnerDispatcher;
enum class QueryRunnerMode : uint8_t;
struct QueryRunnerSettings;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

/// Validates the `cluster`/`shard` settings of a `QueryRunner` engine and performs the
/// `READ`+`WRITE ON REMOTE` check a clustered one requires, under `local_context`. Called both while
/// the storage is constructed and from the `ON CLUSTER` initiator preflight.
void validateQueryRunnerTarget(ASTStorage & storage_def, ContextPtr local_context, LoadingStrictnessLevel mode);

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
