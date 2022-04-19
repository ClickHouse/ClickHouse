#pragma once

#include <base/logger_useful.h>
#include <base/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <Processors/Sources/ShellCommandSource.h>
#include <Storages/ExecutableSettings.h>


namespace DB
{

/**
 * This class represents table engine for external executable files.
 * Executable storage that will start process for read.
 * ExecutablePool storage maintain pool of processes and take process from pool for read.
 */
class StorageExecutable final : public shared_ptr_helper<StorageExecutable>, public IStorage
{
    friend struct shared_ptr_helper<StorageExecutable>;

public:

    String getName() const override
    {
        if (settings.is_executable_pool)
            return "ExecutablePool";
        else
            return "Executable";
    }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned threads) override;

protected:

    StorageExecutable(
        const StorageID & table_id,
        const String & format,
        const ExecutableSettings & settings,
        const std::vector<ASTPtr> & input_queries,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints);

private:
    ExecutableSettings settings;
    std::vector<ASTPtr> input_queries;
    Poco::Logger * log;
    std::unique_ptr<ShellCommandSourceCoordinator> coordinator;
};

}
