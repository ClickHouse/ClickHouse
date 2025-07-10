#include <Dictionaries/ExecutableDictionarySource.h>

#include <filesystem>

#include <boost/algorithm/string/split.hpp>

#include <Common/logger_useful.h>
#include <Common/LocalDateTime.h>
#include <Common/filesystemHelpers.h>

#include <Processors/Sources/ShellCommandSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Formats/formatBlock.h>

#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>

#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DICTIONARY_ACCESS_DENIED;
    extern const int UNSUPPORTED_METHOD;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

    void updateCommandIfNeeded(String & command, bool execute_direct, ContextPtr context)
    {
        if (!execute_direct)
            return;

        auto global_context = context->getGlobalContext();
        auto user_scripts_path = global_context->getUserScriptsPath();
        auto script_path = user_scripts_path + '/' + command;

        if (!fileOrSymlinkPathStartsWith(script_path, user_scripts_path))
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Executable file {} must be inside user scripts folder {}",
                command,
                user_scripts_path);

        if (!FS::exists(script_path))
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Executable file {} does not exist inside user scripts folder {}",
                command,
                user_scripts_path);

        if (!FS::canExecute(script_path))
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Executable file {} is not executable inside user scripts folder {}",
                command,
                user_scripts_path);

        command = std::move(script_path);
    }

}

ExecutableDictionarySource::ExecutableDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    Block & sample_block_,
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator_,
    ContextPtr context_)
    : log(getLogger("ExecutableDictionarySource"))
    , dict_struct(dict_struct_)
    , configuration(configuration_)
    , sample_block(sample_block_)
    , coordinator(std::move(coordinator_))
    , context(context_)
{
    /// Remove keys from sample_block for implicit_key dictionary because
    /// these columns will not be returned from source
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

ExecutableDictionarySource::ExecutableDictionarySource(const ExecutableDictionarySource & other)
    : log(getLogger("ExecutableDictionarySource"))
    , update_time(other.update_time)
    , dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , sample_block(other.sample_block)
    , coordinator(other.coordinator)
    , context(Context::createCopy(other.context))
{
}

QueryPipeline ExecutableDictionarySource::loadAll()
{
    if (configuration.implicit_key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutableDictionarySource with implicit_key does not support loadAll method");

    LOG_TRACE(log, "loadAll {}", toString());

    const auto & coordinator_configuration = coordinator->getConfiguration();
    auto command = configuration.command;
    updateCommandIfNeeded(command, coordinator_configuration.execute_direct, context);

    return QueryPipeline(coordinator->createPipe(command, configuration.command_arguments, {}, sample_block, context));
}

QueryPipeline ExecutableDictionarySource::loadUpdatedAll()
{
    if (configuration.implicit_key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutableDictionarySource with implicit_key does not support loadUpdatedAll method");

    time_t new_update_time = time(nullptr);

    const auto & coordinator_configuration = coordinator->getConfiguration();
    auto command = configuration.command;
    updateCommandIfNeeded(command, coordinator_configuration.execute_direct, context);

    auto command_arguments = configuration.command_arguments;

    if (update_time)
    {
        auto update_difference = DB::toString(LocalDateTime(update_time - configuration.update_lag));

        if (coordinator_configuration.execute_direct)
        {
            command_arguments.emplace_back(configuration.update_field);
            command_arguments.emplace_back(std::move(update_difference));
        }
        else
        {
            command += ' ' + configuration.update_field + ' ' + update_difference;
        }
    }

    update_time = new_update_time;

    LOG_TRACE(log, "loadUpdatedAll {}", command);

    return QueryPipeline(coordinator->createPipe(command, command_arguments, {}, sample_block, context));
}

QueryPipeline ExecutableDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    auto block = blockForIds(dict_struct, ids);
    return getStreamForBlock(block);
}

QueryPipeline ExecutableDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    return getStreamForBlock(block);
}

QueryPipeline ExecutableDictionarySource::getStreamForBlock(const Block & block)
{
    const auto & coordinator_configuration = coordinator->getConfiguration();
    String command = configuration.command;
    updateCommandIfNeeded(command, coordinator_configuration.execute_direct, context);

    auto source = std::make_shared<SourceFromSingleChunk>(block);
    auto shell_input_pipe = Pipe(std::move(source));

    Pipes shell_input_pipes;
    shell_input_pipes.emplace_back(std::move(shell_input_pipe));

    auto pipe = coordinator->createPipe(command, configuration.command_arguments, std::move(shell_input_pipes), sample_block, context);

    if (configuration.implicit_key)
        pipe.addTransform(std::make_shared<TransformWithAdditionalColumns>(block, pipe.getHeader()));

    return QueryPipeline(std::move(pipe));
}

bool ExecutableDictionarySource::isModified() const
{
    return true;
}

bool ExecutableDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool ExecutableDictionarySource::hasUpdateField() const
{
    return !configuration.update_field.empty();
}

DictionarySourcePtr ExecutableDictionarySource::clone() const
{
    return std::make_shared<ExecutableDictionarySource>(*this);
}

std::string ExecutableDictionarySource::toString() const
{
    return "Executable: " + configuration.command;
}

void registerDictionarySourceExecutable(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const String & /*name*/,
                                 const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr global_context,
                                 const std::string & /* default_database */,
                                 bool created_from_ddl) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Dictionary source of type `executable` does not support attribute expressions");

        /// Executable dictionaries may execute arbitrary commands.
        /// It's OK for dictionaries created by administrator from xml-file, but
        /// maybe dangerous for dictionaries created from DDL-queries.
        if (created_from_ddl && global_context->getApplicationType() != Context::ApplicationType::LOCAL)
            throw Exception(ErrorCodes::DICTIONARY_ACCESS_DENIED,
                            "Dictionaries with executable dictionary source are not allowed "
                            "to be created from DDL query");

        auto context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        std::string settings_config_prefix = config_prefix + ".executable";

        bool execute_direct = config.getBool(settings_config_prefix + ".execute_direct", false);
        std::string command_value = config.getString(settings_config_prefix + ".command");
        std::vector<String> command_arguments;

        if (execute_direct)
        {
            boost::split(command_arguments, command_value, [](char c) { return c == ' '; });

            command_value = std::move(command_arguments[0]);
            command_arguments.erase(command_arguments.begin());
        }

        ExecutableDictionarySource::Configuration configuration
        {
            .command = std::move(command_value),
            .command_arguments = std::move(command_arguments),
            .update_field = config.getString(settings_config_prefix + ".update_field", ""),
            .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1),
            .implicit_key = config.getBool(settings_config_prefix + ".implicit_key", false),
        };

        ShellCommandSourceCoordinator::Configuration shell_command_coordinator_configration
        {
            .format = config.getString(settings_config_prefix + ".format"),
            .command_termination_timeout_seconds = config.getUInt64(settings_config_prefix + ".command_termination_timeout", 10),
            .command_read_timeout_milliseconds = config.getUInt64(settings_config_prefix + ".command_read_timeout", 10000),
            .command_write_timeout_milliseconds = config.getUInt64(settings_config_prefix + ".command_write_timeout", 10000),
            .stderr_reaction = parseExternalCommandStderrReaction(config.getString(settings_config_prefix + ".stderr_reaction", "log_last")),
            .check_exit_code = config.getBool(settings_config_prefix + ".check_exit_code", true),
            .is_executable_pool = false,
            .send_chunk_header = config.getBool(settings_config_prefix + ".send_chunk_header", false),
            .execute_direct = config.getBool(settings_config_prefix + ".execute_direct", false)
        };

        auto coordinator = std::make_shared<ShellCommandSourceCoordinator>(shell_command_coordinator_configration);
        return std::make_unique<ExecutableDictionarySource>(dict_struct, configuration, sample_block, std::move(coordinator), context);
    };

    factory.registerSource("executable", create_table_source);
}

}
