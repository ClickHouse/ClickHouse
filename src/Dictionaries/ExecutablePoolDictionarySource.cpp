#include <Dictionaries/ExecutablePoolDictionarySource.h>

#include <filesystem>

#include <boost/algorithm/string/split.hpp>

#include <Common/logger_useful.h>
#include <Common/LocalDateTime.h>
#include <Common/filesystemHelpers.h>

#include <Core/Settings.h>

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
namespace Setting
{
    extern const SettingsSeconds max_execution_time;

    /// Cloud only
    extern const SettingsBool cloud_mode;
}

namespace ErrorCodes
{
    extern const int DICTIONARY_ACCESS_DENIED;
    extern const int UNSUPPORTED_METHOD;
    extern const int SUPPORT_IS_DISABLED;
}

ExecutablePoolDictionarySource::ExecutablePoolDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    Block & sample_block_,
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator_,
    ContextPtr context_)
    : dict_struct(dict_struct_)
    , configuration(configuration_)
    , sample_block(sample_block_)
    , coordinator(std::move(coordinator_))
    , context(context_)
    , log(getLogger("ExecutablePoolDictionarySource"))
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
    , log(getLogger("ExecutablePoolDictionarySource"))
{
}

BlockIO ExecutablePoolDictionarySource::loadAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadAll method");
}

BlockIO ExecutablePoolDictionarySource::loadUpdatedAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadUpdatedAll method");
}

BlockIO ExecutablePoolDictionarySource::loadIds(const VectorWithMemoryTracking<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    auto block = blockForIds(dict_struct, ids);
    BlockIO io;
    io.pipeline = getStreamForBlock(block);
    return io;
}

BlockIO ExecutablePoolDictionarySource::loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    BlockIO io;
    io.pipeline = getStreamForBlock(block);
    return io;
}

QueryPipeline ExecutablePoolDictionarySource::getStreamForBlock(const Block & block)
{
    String command = configuration.command;
    const auto & coordinator_configuration = coordinator->getConfiguration();

    if (coordinator_configuration.execute_direct)
    {
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

    auto header = std::make_shared<const Block>(block);
    auto source = std::make_shared<SourceFromSingleChunk>(header);
    auto shell_input_pipe = Pipe(std::move(source));

    ShellCommandSourceConfiguration command_configuration;
    command_configuration.read_fixed_number_of_rows = true;
    command_configuration.number_of_rows_to_read = block.rows();

    Pipes shell_input_pipes;
    shell_input_pipes.emplace_back(std::move(shell_input_pipe));

    auto pipe = coordinator->createPipe(
        command,
        configuration.command_arguments,
        std::move(shell_input_pipes),
        sample_block,
        context,
        command_configuration);

    if (configuration.implicit_key)
        pipe.addTransform(std::make_shared<TransformWithAdditionalColumns>(header, pipe.getSharedHeader()));

    return QueryPipeline(std::move(pipe));
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

void registerDictionarySourceExecutablePool(DictionarySourceFactory & factory);
void registerDictionarySourceExecutablePool(DictionarySourceFactory & factory)
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
        if (global_context->getSettingsRef()[Setting::cloud_mode])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Dictionary source of type `executable pool` is disabled");

        if (dict_struct.has_expressions)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Dictionary source of type `executable_pool` does not support attribute expressions");

        /// Executable dictionaries may execute arbitrary commands.
        /// It's OK for dictionaries created by administrator from xml-file, but
        /// maybe dangerous for dictionaries created from DDL-queries.
        if (created_from_ddl && global_context->getApplicationType() != Context::ApplicationType::LOCAL)
            throw Exception(ErrorCodes::DICTIONARY_ACCESS_DENIED,
                            "Dictionaries with executable pool dictionary source are not allowed "
                            "to be created from DDL query");

        ContextMutablePtr context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        String settings_config_prefix = config_prefix + ".executable_pool";

        size_t max_command_execution_time = config.getUInt64(settings_config_prefix + ".max_command_execution_time", 10);

        size_t max_execution_time_seconds = static_cast<size_t>(context->getSettingsRef()[Setting::max_execution_time].totalSeconds());
        if (max_execution_time_seconds != 0 && max_command_execution_time > max_execution_time_seconds)
            max_command_execution_time = max_execution_time_seconds;

        bool execute_direct = config.getBool(settings_config_prefix + ".execute_direct", false);
        std::string command_value = config.getString(settings_config_prefix + ".command");
        VectorWithMemoryTracking<String> command_arguments;

        if (execute_direct)
        {
            boost::split(command_arguments, command_value, [](char c) { return c == ' '; });

            command_value = std::move(command_arguments[0]);
            command_arguments.erase(command_arguments.begin());
        }

        ExecutablePoolDictionarySource::Configuration configuration
        {
            .command = std::move(command_value),
            .command_arguments = std::move(command_arguments),
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
            .pool_size = config.getUInt64(settings_config_prefix + ".pool_size", 16),
            .max_command_execution_time_seconds = max_command_execution_time,
            .is_executable_pool = true,
            .send_chunk_header = config.getBool(settings_config_prefix + ".send_chunk_header", false),
            .execute_direct = execute_direct
        };

        auto coordinator = std::make_shared<ShellCommandSourceCoordinator>(shell_command_coordinator_configration);
        return std::make_unique<ExecutablePoolDictionarySource>(dict_struct, configuration, sample_block, std::move(coordinator), context);
    };

    factory.registerSource("executable_pool", create_table_source, Documentation{
        .description = R"DOCS_MD(
# Executable Pool dictionary source

Executable pool allows loading data from a pool of processes.
This source does not work with dictionary layouts that need to load all data from source.

Executable pool works if the dictionary [is stored](/reference/statements/create/dictionary/layouts/overview#storing-dictionaries-in-memory) using one of the following layouts:
- `cache`
- `complex_key_cache`
- `ssd_cache`
- `complex_key_ssd_cache`
- `direct`
- `complex_key_direct`

Executable pool will spawn a pool of processes with the specified command and keep them running until they exit. The program should read data from STDIN while it is available and output the result to STDOUT. It can wait for the next block of data on STDIN. ClickHouse will not close STDIN after processing a block of data, but will pipe another chunk of data when needed. The executable script should be ready for this way of data processing — it should poll STDIN and flush data to STDOUT early.

Example of settings:

<Tabs>
<Tab title="DDL">

```sql
SOURCE(EXECUTABLE_POOL(
    command 'while read key; do printf "$key\tData for key $key\n"; done'
    format 'TabSeparated'
    pool_size 10
    max_command_execution_time 10
    implicit_key false
))
```

</Tab>
<Tab title="Configuration file">

```xml
<source>
    <executable_pool>
        <command><command>while read key; do printf "$key\tData for key $key\n"; done</command</command>
        <format>TabSeparated</format>
        <pool_size>10</pool_size>
        <max_command_execution_time>10<max_command_execution_time>
        <implicit_key>false</implicit_key>
    </executable_pool>
</source>
```

</Tab>
</Tabs>

Setting fields:

| Setting | Description |
|---------|-------------|
| `command` | The absolute path to the executable file, or the file name (if the program directory is written to `PATH`). |
| `format` | The file format. All the formats described in [Formats](/reference/formats/index) are supported. |
| `pool_size` | Size of pool. If 0 is specified as `pool_size` then there is no pool size restrictions. Default value is `16`. |
| `command_termination_timeout` | Executable script should contain main read-write loop. After dictionary is destroyed, pipe is closed, and executable file will have `command_termination_timeout` seconds to shutdown before ClickHouse will send SIGTERM signal to child process. Specified in seconds. Default value is `10`. Optional. |
| `max_command_execution_time` | Maximum executable script command execution time for processing block of data. Specified in seconds. Default value is `10`. Optional. |
| `command_read_timeout` | Timeout for reading data from command stdout in milliseconds. Default value `10000`. Optional. |
| `command_write_timeout` | Timeout for writing data to command stdin in milliseconds. Default value `10000`. Optional. |
| `implicit_key` | The executable source file can return only values, and the correspondence to the requested keys is determined implicitly by the order of rows in the result. Default value is `false`. Optional. |
| `execute_direct` | If `execute_direct` = `1`, then `command` will be searched inside user_scripts folder specified by [user_scripts_path](/reference/settings/server-settings/settings/user#user_scripts_path). Additional script arguments can be specified using whitespace separator. Example: `script_name arg1 arg2`. If `execute_direct` = `0`, `command` is passed as argument for `bin/sh -c`. Default value is `1`. Optional. |
| `send_chunk_header` | Controls whether to send row count before sending a chunk of data to process. Default value is `false`. Optional. |

That dictionary source can be configured only via XML configuration. Creating dictionaries with executable source via DDL is disabled, otherwise, the DB user would be able to execute arbitrary binary on ClickHouse node.
)DOCS_MD",
        .syntax = "SOURCE(EXECUTABLE_POOL(command 'script.sh' format 'TabSeparated' pool_size 4))",
        .related = {"executable"}});
}

}
