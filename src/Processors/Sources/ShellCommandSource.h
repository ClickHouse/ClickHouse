#pragma once

#include <array>
#include <memory>
#include <string_view>

#include <base/BorrowedObjectPool.h>

#include <Common/ShellCommand.h>
#include <Common/ShellCommandSettings.h>
#include <Common/ThreadPool.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Processors/ISimpleTransform.h>
#include <Processors/ISource.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{

class ShellCommandHolder;
using ShellCommandHolderPtr = std::unique_ptr<ShellCommandHolder>;

/// The configuration options that only the shared-memory transport understands. `use_shared_memory`
/// comes first: it is the one that turns the transport on, and the rest only qualify it.
inline constexpr std::array<std::string_view, 4> SHARED_MEMORY_CONFIGURATION_KEYS
{
    "use_shared_memory",
    "shared_memory_size",
    "shared_memory_max_size",
    "shared_memory_pipeline",
};

/// Throws if any of the options above appears under `config_prefix`. For a surface that does not
/// implement the shared-memory transport at all: without this, a command that was explicitly
/// configured for shared memory would quietly run over the pipes instead, which is the one outcome
/// whoever wrote those lines did not intend. `surface` names what is being configured and goes into
/// the message. Rejects on the key being present rather than on its value - a `0` says just as
/// clearly that its author believed this transport applied here.
void checkSharedMemoryIsNotConfigured(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const std::string & surface);

using ProcessPool = BorrowedObjectPool<ShellCommandHolderPtr>;

class UDFProcessSubtreeSampler;

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
    /// Optional accumulator for executable_pool UDF resource accounting.
    /// Only set by the executable_pool UDF factory; other consumers leave it null.
    std::shared_ptr<UDFProcessSubtreeSampler> sampler;
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

        /// Requested capacity for command stdin/stdout pipes. Zero keeps the OS default.
        size_t command_pipe_capacity = 0;

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

        /// True when this coordinator runs an executable or executable pool UDF.
        bool is_user_defined_function = false;

        /// Exchange data through a shared-memory file instead of the stdin/stdout pipes.
        /// The pipes then carry only control commands (see ShellCommandSource.cpp).
        bool use_shared_memory = false;

        /// Initial size in bytes of the shared-memory region. Valid only if use_shared_memory = true.
        size_t shared_memory_size = 0;

        /// Upper bound in bytes to which the region may grow on demand. When it equals
        /// shared_memory_size the region never grows. Valid only if use_shared_memory = true.
        size_t shared_memory_max_size = 0;

        /// Overlap serialization of the next chunk with the child's processing of the current one
        /// using two regions and a background thread. Doubles the region memory. Valid only if
        /// use_shared_memory = true.
        bool shared_memory_pipeline = false;


    };

    explicit ShellCommandSourceCoordinator(const Configuration & configuration_);

    const Configuration & getConfiguration() const
    {
        return configuration;
    }

    Pipe createPipe(
        const std::string & command,
        const VectorWithMemoryTracking<std::string> & arguments,
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

    Pipe
    createPipe(const std::string & command, const VectorWithMemoryTracking<std::string> & arguments, Block sample_block, ContextPtr context)
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
