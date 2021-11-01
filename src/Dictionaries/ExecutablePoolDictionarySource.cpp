#include "ExecutablePoolDictionarySource.h"

#include <base/logger_useful.h>
#include <Common/LocalDateTime.h>
#include <Common/ShellCommand.h>

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/ShellCommandSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Formats/formatBlock.h>

#include <Interpreters/Context.h>

#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DICTIONARY_ACCESS_DENIED;
    extern const int UNSUPPORTED_METHOD;
    extern const int TIMEOUT_EXCEEDED;
}

ExecutablePoolDictionarySource::ExecutablePoolDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    Block & sample_block_,
    std::shared_ptr<ShellCommandCoordinator> coordinator_,
    ContextPtr context_)
    : dict_struct(dict_struct_)
    , configuration(configuration_)
    , sample_block(sample_block_)
    , coordinator(std::move(coordinator_))
    , context(context_)
    , log(&Poco::Logger::get("ExecutablePoolDictionarySource"))
{
    /// Remove keys from sample_block for implicit_key dictionary because
    /// these columns will not be returned from source
    /// Implicit key means that the source script will return only values,
    /// and the correspondence to the requested keys is determined implicitly - by the order of rows in the result.
    if (configuration.implicit_key)
    {
        auto keys_names = dict_struct.getKeysNames();

        for (auto & key_name : keys_names)
        {
            size_t key_column_position_in_block = sample_block.getPositionByName(key_name);
            sample_block.erase(key_column_position_in_block);
        }
    }
}

ExecutablePoolDictionarySource::ExecutablePoolDictionarySource(const ExecutablePoolDictionarySource & other)
    : dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , sample_block(other.sample_block)
    , coordinator(other.coordinator)
    , context(Context::createCopy(other.context))
    , log(&Poco::Logger::get("ExecutablePoolDictionarySource"))
{
}

Pipe ExecutablePoolDictionarySource::loadAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadAll method");
}

Pipe ExecutablePoolDictionarySource::loadUpdatedAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadUpdatedAll method");
}

Pipe ExecutablePoolDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    auto block = blockForIds(dict_struct, ids);
    return getStreamForBlock(block);
}

Pipe ExecutablePoolDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    return getStreamForBlock(block);
}

Pipe ExecutablePoolDictionarySource::getStreamForBlock(const Block & block)
{
    auto source = std::make_shared<SourceFromSingleChunk>(block);
    auto shell_input_pipe = Pipe(std::move(source));

    ShellCommandSourceConfiguration command_configuration;
    command_configuration.read_fixed_number_of_rows = true;
    command_configuration.number_of_rows_to_read = block.rows();

    Pipes shell_input_pipes;
    shell_input_pipes.emplace_back(std::move(shell_input_pipe));

    return coordinator->createPipe(configuration.command, std::move(shell_input_pipes), std::move(sample_block), context, command_configuration);
}

bool ExecutablePoolDictionarySource::isModified() const
{
    return true;
}

bool ExecutablePoolDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool ExecutablePoolDictionarySource::hasUpdateField() const
{
    return false;
}

DictionarySourcePtr ExecutablePoolDictionarySource::clone() const
{
    return std::make_shared<ExecutablePoolDictionarySource>(*this);
}

std::string ExecutablePoolDictionarySource::toString() const
{
    size_t pool_size = coordinator->getConfiguration().pool_size;
    return "ExecutablePool size: " + std::to_string(pool_size) + " command: " + configuration.command;
}

void registerDictionarySourceExecutablePool(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr global_context,
                                 const std::string & /* default_database */,
                                 bool created_from_ddl) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `executable_pool` does not support attribute expressions");

        /// Executable dictionaries may execute arbitrary commands.
        /// It's OK for dictionaries created by administrator from xml-file, but
        /// maybe dangerous for dictionaries created from DDL-queries.
        if (created_from_ddl && global_context->getApplicationType() != Context::ApplicationType::LOCAL)
            throw Exception(ErrorCodes::DICTIONARY_ACCESS_DENIED, "Dictionaries with executable pool dictionary source are not allowed to be created from DDL query");

        ContextMutablePtr context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        String settings_config_prefix = config_prefix + ".executable_pool";

        size_t max_command_execution_time = config.getUInt64(settings_config_prefix + ".max_command_execution_time", 10);

        size_t max_execution_time_seconds = static_cast<size_t>(context->getSettings().max_execution_time.totalSeconds());
        if (max_execution_time_seconds != 0 && max_command_execution_time > max_execution_time_seconds)
            max_command_execution_time = max_execution_time_seconds;

        ExecutablePoolDictionarySource::Configuration configuration
        {
            .command = config.getString(settings_config_prefix + ".command"),
            .implicit_key = config.getBool(settings_config_prefix + ".implicit_key", false),
        };

        ShellCommandCoordinator::Configuration shell_command_coordinator_configration
        {
            .format = config.getString(settings_config_prefix + ".format"),
            .pool_size = config.getUInt64(settings_config_prefix + ".size"),
            .command_termination_timeout = config.getUInt64(settings_config_prefix + ".command_termination_timeout", 10),
            .max_command_execution_time = max_command_execution_time,
            .is_executable_pool = true,
            .send_chunk_header = config.getBool(settings_config_prefix + ".send_chunk_header", false),
            .execute_direct = false
        };

        std::shared_ptr<ShellCommandCoordinator> coordinator = std::make_shared<ShellCommandCoordinator>(shell_command_coordinator_configration);

        return std::make_unique<ExecutablePoolDictionarySource>(dict_struct, configuration, sample_block, std::move(coordinator), context);
    };

    factory.registerSource("executable_pool", create_table_source);
}

}
