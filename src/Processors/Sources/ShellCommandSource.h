#pragma once

#include <memory>

#include <base/BorrowedObjectPool.h>

#include <Common/ShellCommandSettings.h>
#include <Common/ShellCommand.h>
#include <Common/ThreadPool.h>

#include <IO/ReadHelpers.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/ISource.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>


namespace DB
{

class ShellCommandHolder;
using ShellCommandHolderPtr = std::unique_ptr<ShellCommandHolder>;

using ProcessPool = BorrowedObjectPool<ShellCommandHolderPtr>;

struct ShellCommandSourceConfiguration
{
    /// Read fixed number of rows from command output
    bool read_fixed_number_of_rows = false;
    /// Valid only if read_fixed_number_of_rows = true
    bool read_number_of_rows_from_process_output = false;
    /// Valid only if read_fixed_number_of_rows = true
    size_t number_of_rows_to_read = 0;
    /// Max block size
    size_t max_block_size = DEFAULT_BLOCK_SIZE;
};

class ShellCommandSourceCoordinator
{
public:

    struct Configuration
    {

        /// Script output format
        std::string format;

        /// Command termination timeout in seconds
        size_t command_termination_timeout_seconds = 10;

        /// Timeout for reading data from command stdout
        size_t command_read_timeout_milliseconds = 10000;

        /// Timeout for writing data to command stdin
        size_t command_write_timeout_milliseconds = 10000;

        /// Reaction when external command outputs data to its stderr.
        ExternalCommandStderrReaction stderr_reaction = ExternalCommandStderrReaction::NONE;

        /// Will throw if the command exited with
        /// non-zero status code.
        /// NOTE: If executable pool is used, we cannot check exit code,
        /// which makes this configuration no effect.
        size_t check_exit_code = false;

        /// Pool size valid only if executable_pool = true
        size_t pool_size = 16;

        /// Max command execution time in seconds. Valid only if executable_pool = true
        size_t max_command_execution_time_seconds = 10;

        /// Should pool of processes be created.
        bool is_executable_pool = false;

        /// Send number_of_rows\n before sending chunk to process.
        bool send_chunk_header = false;

        /// Execute script direct or with /bin/bash.
        bool execute_direct = true;

    };

    explicit ShellCommandSourceCoordinator(const Configuration & configuration_);

    const Configuration & getConfiguration() const
    {
        return configuration;
    }

    Pipe createPipe(
        const std::string & command,
        const std::vector<std::string> & arguments,
        std::vector<Pipe> && input_pipes,
        Block sample_block,
        ContextPtr context,
        const ShellCommandSourceConfiguration & source_configuration = {});

    Pipe createPipe(
        const std::string & command,
        std::vector<Pipe> && input_pipes,
        Block sample_block,
        ContextPtr context,
        const ShellCommandSourceConfiguration & source_configuration = {})
    {
        return createPipe(command, {}, std::move(input_pipes), std::move(sample_block), std::move(context), source_configuration);
    }

    Pipe createPipe(
        const std::string & command,
        const std::vector<std::string> & arguments,
        Block sample_block,
        ContextPtr context)
    {
        return createPipe(command, arguments, {}, std::move(sample_block), std::move(context), {});
    }

    Pipe createPipe(
        const std::string & command,
        Block sample_block,
        ContextPtr context)
    {
        return createPipe(command, {}, {}, std::move(sample_block), std::move(context), {});
    }

private:

    Configuration configuration;

    std::shared_ptr<ProcessPool> process_pool = nullptr;
};

}
