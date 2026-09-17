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

    /// Reports the five settings this engine acts on, from what it was given - see the definition.
    SettingDescriptions getTableSettings(ContextPtr query_context) const override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    void shutdown(bool is_drop) override;
    void drop() override;

    void waitForQueriesToFinish(const QueryStatusPtr & query_status);

    bool supportsParallelInsert() const override { return true; }

private:
    QueryRunnerMode mode;
    /// The engine consumes its settings at construction - `mode` here, the rest inside the dispatcher - so the
    /// enumeration is taken while they are still at hand, and reported as it was then.
    SettingDescriptions settings_descriptions;
    std::unique_ptr<QueryRunnerDispatcher> dispatcher;
    LoggerPtr log;
};

}
