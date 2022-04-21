#pragma once

#include <base/logger_useful.h>
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
class StorageExecutable final : public IStorage
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageExecutable> create(TArgs &&... args)
    {
        return std::make_shared<StorageExecutable>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageExecutable(CreatePasskey, TArgs &&... args) : StorageExecutable{std::forward<TArgs>(args)...}
    {
    }

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
