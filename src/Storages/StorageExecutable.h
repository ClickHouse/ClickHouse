#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Processors/Sources/ShellCommandSource.h>


namespace DB
{
struct ExecutableSettings;

/**
 * This class represents table engine for external executable files.
 * Executable storage that will start process for read.
 * ExecutablePool storage maintain pool of processes and take process from pool for read.
 */
class StorageExecutable final : public StorageWithCommonVirtualColumns
{
public:
    StorageExecutable(
        const StorageID & table_id,
        const String & format,
        const ExecutableSettings & settings,
        const VectorWithMemoryTracking<ASTPtr> & input_queries,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints,
        const String & comment,
        bool query_is_table_function_argument_ = false);

    ~StorageExecutable() override;

    String getName() const override;

    bool supportsTruncate() const override { return false; }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
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
    VectorWithMemoryTracking<ASTPtr> input_queries;
    /// The input queries are a part of the calling query (the `executable` table function), not a stored definition.
    const bool query_is_table_function_argument;
    LoggerPtr log;
    std::unique_ptr<ShellCommandSourceCoordinator> coordinator;
};

}
