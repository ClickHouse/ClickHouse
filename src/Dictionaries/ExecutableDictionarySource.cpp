#include "ExecutableDictionarySource.h"

#include <base/logger_useful.h>
#include <base/LocalDateTime.h>
#include <Common/ShellCommand.h>

#include <Processors/Sources/ShellCommandSource.h>
#include <Formats/formatBlock.h>

#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/registerDictionaries.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DICTIONARY_ACCESS_DENIED;
    extern const int UNSUPPORTED_METHOD;
}

ExecutableDictionarySource::ExecutableDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    Block & sample_block_,
    ContextPtr context_)
    : log(&Poco::Logger::get("ExecutableDictionarySource"))
    , dict_struct(dict_struct_)
    , configuration(configuration_)
    , sample_block{sample_block_}
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
    : log(&Poco::Logger::get("ExecutableDictionarySource"))
    , update_time(other.update_time)
    , dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , sample_block(other.sample_block)
    , context(Context::createCopy(other.context))
{
}

Pipe ExecutableDictionarySource::loadAll()
{
    if (configuration.implicit_key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutableDictionarySource with implicit_key does not support loadAll method");

    LOG_TRACE(log, "loadAll {}", toString());

    ShellCommand::Config config(configuration.command);
    auto process = ShellCommand::execute(config);

    Pipe pipe(std::make_unique<ShellCommandSource>(context, configuration.format, sample_block, std::move(process)));
    return pipe;
}

Pipe ExecutableDictionarySource::loadUpdatedAll()
{
    if (configuration.implicit_key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutableDictionarySource with implicit_key does not support loadUpdatedAll method");

    time_t new_update_time = time(nullptr);
    SCOPE_EXIT(update_time = new_update_time);

    std::string command_with_update_field = configuration.command;
    if (update_time)
        command_with_update_field += " " + configuration.update_field + " " + DB::toString(LocalDateTime(update_time - configuration.update_lag));

    LOG_TRACE(log, "loadUpdatedAll {}", command_with_update_field);
    ShellCommand::Config config(command_with_update_field);
    auto process = ShellCommand::execute(config);
    Pipe pipe(std::make_unique<ShellCommandSource>(context, configuration.format, sample_block, std::move(process)));
    return pipe;
}

Pipe ExecutableDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    auto block = blockForIds(dict_struct, ids);
    return getStreamForBlock(block);
}

Pipe ExecutableDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    return getStreamForBlock(block);
}

Pipe ExecutableDictionarySource::getStreamForBlock(const Block & block)
{
    ShellCommand::Config config(configuration.command);
    auto process = ShellCommand::execute(config);
    auto * process_in = &process->in;

    ShellCommandSource::SendDataTask task = {[process_in, block, this]()
    {
        auto & out = *process_in;

        if (configuration.send_chunk_header)
        {
            writeText(block.rows(), out);
            writeChar('\n', out);
        }

        auto output_format = context->getOutputFormat(configuration.format, out, block.cloneEmpty());
        formatBlock(output_format, block);
        out.close();
    }};
    std::vector<ShellCommandSource::SendDataTask> tasks = {std::move(task)};

    Pipe pipe(std::make_unique<ShellCommandSource>(context, configuration.format, sample_block, std::move(process), std::move(tasks)));

    if (configuration.implicit_key)
        pipe.addTransform(std::make_shared<TransformWithAdditionalColumns>(block, pipe.getHeader()));

    return pipe;
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
    return std::make_unique<ExecutableDictionarySource>(*this);
}

std::string ExecutableDictionarySource::toString() const
{
    return "Executable: " + configuration.command;
}

void registerDictionarySourceExecutable(DictionarySourceFactory & factory)
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `executable` does not support attribute expressions");

        /// Executable dictionaries may execute arbitrary commands.
        /// It's OK for dictionaries created by administrator from xml-file, but
        /// maybe dangerous for dictionaries created from DDL-queries.
        if (created_from_ddl && global_context->getApplicationType() != Context::ApplicationType::LOCAL)
            throw Exception(ErrorCodes::DICTIONARY_ACCESS_DENIED, "Dictionaries with executable dictionary source are not allowed to be created from DDL query");

        auto context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        std::string settings_config_prefix = config_prefix + ".executable";

        ExecutableDictionarySource::Configuration configuration
        {
            .command = config.getString(settings_config_prefix + ".command"),
            .format = config.getString(settings_config_prefix + ".format"),
            .update_field = config.getString(settings_config_prefix + ".update_field", ""),
            .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1),
            .implicit_key = config.getBool(settings_config_prefix + ".implicit_key", false),
            .send_chunk_header = config.getBool(settings_config_prefix + ".send_chunk_header", false)
        };

        return std::make_unique<ExecutableDictionarySource>(dict_struct, configuration, sample_block, context);
    };

    factory.registerSource("executable", create_table_source);
}

}
