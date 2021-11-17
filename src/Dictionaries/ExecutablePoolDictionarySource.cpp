#include "ExecutablePoolDictionarySource.h"

#include <base/logger_useful.h>
#include <base/LocalDateTime.h>
#include <Common/ShellCommand.h>

#include <Formats/formatBlock.h>

#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

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
    ContextPtr context_)
    : dict_struct(dict_struct_)
    , configuration(configuration_)
    , sample_block(sample_block_)
    , context(context_)
    /// If pool size == 0 then there is no size restrictions. Poco max size of semaphore is integer type.
    , process_pool(std::make_shared<ProcessPool>(configuration.pool_size == 0 ? std::numeric_limits<int>::max() : configuration.pool_size))
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
    , context(Context::createCopy(other.context))
    , process_pool(std::make_shared<ProcessPool>(configuration.pool_size))
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
    std::unique_ptr<ShellCommand> process;
    bool result = process_pool->tryBorrowObject(process, [this]()
    {
        ShellCommand::Config config(configuration.command);
        config.terminate_in_destructor_strategy = ShellCommand::DestructorStrategy{ true /*terminate_in_destructor*/, configuration.command_termination_timeout };
        auto shell_command = ShellCommand::execute(config);
        return shell_command;
    }, configuration.max_command_execution_time * 10000);

    if (!result)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
            "Could not get process from pool, max command execution timeout exceeded {} seconds",
            configuration.max_command_execution_time);

    size_t rows_to_read = block.rows();
    auto * process_in = &process->in;
    ShellCommandSource::SendDataTask task = [process_in, block, this]() mutable
    {
        auto & out = *process_in;

        if (configuration.send_chunk_header)
        {
            writeText(block.rows(), out);
            writeChar('\n', out);
        }

        auto output_format = context->getOutputFormat(configuration.format, out, block.cloneEmpty());
        formatBlock(output_format, block);
    };
    std::vector<ShellCommandSource::SendDataTask> tasks = {std::move(task)};

    ShellCommandSourceConfiguration command_configuration;
    command_configuration.read_fixed_number_of_rows = true;
    command_configuration.number_of_rows_to_read = rows_to_read;
    Pipe pipe(std::make_unique<ShellCommandSource>(context, configuration.format, sample_block, std::move(process), std::move(tasks), command_configuration, process_pool));

    if (configuration.implicit_key)
        pipe.addTransform(std::make_shared<TransformWithAdditionalColumns>(block, pipe.getHeader()));

    return pipe;
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
    return std::make_unique<ExecutablePoolDictionarySource>(*this);
}

std::string ExecutablePoolDictionarySource::toString() const
{
    return "ExecutablePool size: " + std::to_string(configuration.pool_size) + " command: " + configuration.command;
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
            .format = config.getString(settings_config_prefix + ".format"),
            .pool_size = config.getUInt64(settings_config_prefix + ".size"),
            .command_termination_timeout = config.getUInt64(settings_config_prefix + ".command_termination_timeout", 10),
            .max_command_execution_time = max_command_execution_time,
            .implicit_key = config.getBool(settings_config_prefix + ".implicit_key", false),
            .send_chunk_header = config.getBool(settings_config_prefix + ".send_chunk_header", false)
        };

        return std::make_unique<ExecutablePoolDictionarySource>(dict_struct, configuration, sample_block, context);
    };

    factory.registerSource("executable_pool", create_table_source);
}

}
