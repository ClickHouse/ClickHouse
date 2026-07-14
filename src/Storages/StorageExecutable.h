#pragma once

#include <Storages/IStorage.h>
#include <Processors/Sources/ShellCommandSource.h>


namespace DB
{
struct ExecutableSettings;

/**
 * This class represents table engine for external executable files.
 * Executable storage that will start process for read.
 * ExecutablePool storage maintain pool of processes and take process from pool for read.
 */
class StorageExecutable final : public IStorage
{
public:
    StorageExecutable(
        const StorageID & table_id,
        const String & format,
        const ExecutableSettings & settings,
        const std::vector<ASTPtr> & input_queries,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints,
        const String & comment);

    ~StorageExecutable() override;

    String getName() const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t threads) override;

private:
    std::unique_ptr<ExecutableSettings> settings;
    std::vector<ASTPtr> input_queries;
    LoggerPtr log;
    std::unique_ptr<ShellCommandSourceCoordinator> coordinator;
};

}
