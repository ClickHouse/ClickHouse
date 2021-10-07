#pragma once

#include <common/logger_useful.h>
#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <DataStreams/ShellCommandSource.h>
#include <Storages/ExecutablePoolSettings.h>


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
        if (process_pool)
            return "ExecutablePool";
        else
            return "Executable";
    }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned threads) override;

protected:

    StorageExecutable(
        const StorageID & table_id,
        const String & script_name_,
        const std::vector<String> & arguments_,
        const String & format_,
        const std::vector<ASTPtr> & input_queries_,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints);

    StorageExecutable(
        const StorageID & table_id,
        const String & script_name_,
        const std::vector<String> & arguments_,
        const String & format_,
        const std::vector<ASTPtr> & input_queries_,
        const ExecutablePoolSettings & pool_settings_,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints);

private:
    String script_name;
    std::vector<String> arguments;
    String format;
    std::vector<ASTPtr> input_queries;
    ExecutablePoolSettings pool_settings;
    std::shared_ptr<ProcessPool> process_pool;
    Poco::Logger * log;
};

}
