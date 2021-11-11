#include <Storages/StorageExecutable.h>

#include <filesystem>

#include <Common/ShellCommand.h>
#include <Common/filesystemHelpers.h>

#include <Core/Block.h>

#include <IO/ReadHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/StorageFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TIMEOUT_EXCEEDED;
}

StorageExecutable::StorageExecutable(
    const StorageID & table_id_,
    const String & script_name_,
    const std::vector<String> & arguments_,
    const String & format_,
    const std::vector<ASTPtr> & input_queries_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints)
    : IStorage(table_id_)
    , script_name(script_name_)
    , arguments(arguments_)
    , format(format_)
    , input_queries(input_queries_)
    , log(&Poco::Logger::get("StorageExecutable"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setConstraints(constraints);
    setInMemoryMetadata(storage_metadata);
}

StorageExecutable::StorageExecutable(
    const StorageID & table_id_,
    const String & script_name_,
    const std::vector<String> & arguments_,
    const String & format_,
    const std::vector<ASTPtr> & input_queries_,
    const ExecutableSettings & settings_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints)
    : IStorage(table_id_)
    , script_name(script_name_)
    , arguments(arguments_)
    , format(format_)
    , input_queries(input_queries_)
    , settings(settings_)
    /// If pool size == 0 then there is no size restrictions. Poco max size of semaphore is integer type.
    , process_pool(std::make_shared<ProcessPool>(settings.pool_size == 0 ? std::numeric_limits<int>::max() : settings.pool_size))
    , log(&Poco::Logger::get("StorageExecutablePool"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setConstraints(constraints);
    setInMemoryMetadata(storage_metadata);
}

class SendingChunkHeaderTransform final : public ISimpleTransform
{
public:
    SendingChunkHeaderTransform(const Block & header, WriteBuffer & buffer_)
        : ISimpleTransform(header, header, false)
        , buffer(buffer_)
    {
    }

    String getName() const override { return "SendingChunkHeaderTransform"; }

protected:

    void transform(Chunk & chunk) override
    {
        writeText(chunk.getNumRows(), buffer);
        writeChar('\n', buffer);
    }

private:
    WriteBuffer & buffer;
};

Pipe StorageExecutable::read(
    const Names & /*column_names*/,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*threads*/)
{
    auto user_scripts_path = context->getUserScriptsPath();
    auto script_path = user_scripts_path + '/' + script_name;

    if (!pathStartsWith(script_path, user_scripts_path))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Executable file {} must be inside user scripts folder {}",
            script_name,
            user_scripts_path);

    if (!std::filesystem::exists(std::filesystem::path(script_path)))
         throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Executable file {} does not exist inside user scripts folder {}",
            script_name,
            user_scripts_path);

    std::vector<QueryPipelineBuilder> inputs;
    inputs.reserve(input_queries.size());

    for (auto & input_query : input_queries)
    {
        InterpreterSelectWithUnionQuery interpreter(input_query, context, {});
        inputs.emplace_back(interpreter.buildQueryPipeline());
    }

    ShellCommand::Config config(script_path);
    config.arguments = arguments;
    for (size_t i = 1; i < inputs.size(); ++i)
        config.write_fds.emplace_back(i + 2);

    std::unique_ptr<ShellCommand> process;

    bool is_executable_pool = (process_pool != nullptr);
    if (is_executable_pool)
    {
        bool result = process_pool->tryBorrowObject(process, [&config, this]()
        {
            config.terminate_in_destructor_strategy = ShellCommand::DestructorStrategy{ true /*terminate_in_destructor*/, settings.command_termination_timeout };
            auto shell_command = ShellCommand::executeDirect(config);
            return shell_command;
        }, settings.max_command_execution_time * 10000);

        if (!result)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                "Could not get process from pool, max command execution timeout exceeded {} seconds",
                settings.max_command_execution_time);
    }
    else
    {
        process = ShellCommand::executeDirect(config);
    }

    std::vector<ShellCommandSource::SendDataTask> tasks;
    tasks.reserve(inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        WriteBufferFromFile * write_buffer = nullptr;

        if (i == 0)
        {
            write_buffer = &process->in;
        }
        else
        {
            auto descriptor = i + 2;
            auto it = process->write_fds.find(descriptor);
            if (it == process->write_fds.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Process does not contain descriptor to write {}", descriptor);

            write_buffer = &it->second;
        }

        inputs[i].resize(1);
        if (settings.send_chunk_header)
        {
            auto transform = std::make_shared<SendingChunkHeaderTransform>(inputs[i].getHeader(), *write_buffer);
            inputs[i].addTransform(std::move(transform));
        }

        auto pipeline = std::make_shared<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(inputs[i])));

        auto out = context->getOutputFormat(format, *write_buffer, materializeBlock(pipeline->getHeader()));
        out->setAutoFlush();
        pipeline->complete(std::move(out));

        ShellCommandSource::SendDataTask task = [pipeline, write_buffer, is_executable_pool]()
        {
            CompletedPipelineExecutor executor(*pipeline);
            executor.execute();

            if (!is_executable_pool)
                write_buffer->close();
        };

        tasks.emplace_back(std::move(task));
    }

    auto sample_block = metadata_snapshot->getSampleBlock();

    ShellCommandSourceConfiguration configuration;
    configuration.max_block_size = max_block_size;

    if (is_executable_pool)
    {
        configuration.read_fixed_number_of_rows = true;
        configuration.read_number_of_rows_from_process_output = true;
    }

    Pipe pipe(std::make_unique<ShellCommandSource>(context, format, std::move(sample_block), std::move(process), std::move(tasks), configuration, process_pool));
    return pipe;
}

void registerStorageExecutable(StorageFactory & factory)
{
    auto register_storage = [](const StorageFactory::Arguments & args, bool is_executable_pool) -> StoragePtr
    {
        auto local_context = args.getLocalContext();

        if (args.engine_args.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "StorageExecutable requires minimum 2 arguments: script_name, format, [input_query...]");

        for (size_t i = 0; i < 2; ++i)
            args.engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[i], local_context);

        auto scipt_name_with_arguments_value = args.engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        std::vector<String> script_name_with_arguments;
        boost::split(script_name_with_arguments, scipt_name_with_arguments_value, [](char c) { return c == ' '; });

        auto script_name = script_name_with_arguments[0];
        script_name_with_arguments.erase(script_name_with_arguments.begin());
        auto format = args.engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        std::vector<ASTPtr> input_queries;
        for (size_t i = 2; i < args.engine_args.size(); ++i)
        {
            ASTPtr query = args.engine_args[i]->children.at(0);
            if (!query->as<ASTSelectWithUnionQuery>())
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD, "StorageExecutable argument is invalid input query {}",
                    query->formatForErrorMessage());

            input_queries.emplace_back(std::move(query));
        }

        const auto & columns = args.columns;
        const auto & constraints = args.constraints;

        if (is_executable_pool)
        {
            size_t max_command_execution_time = 10;

            size_t max_execution_time_seconds = static_cast<size_t>(args.getContext()->getSettings().max_execution_time.totalSeconds());
            if (max_execution_time_seconds != 0 && max_command_execution_time > max_execution_time_seconds)
                max_command_execution_time = max_execution_time_seconds;

            ExecutableSettings pool_settings;
            pool_settings.max_command_execution_time = max_command_execution_time;
            if (args.storage_def->settings)
                pool_settings.loadFromQuery(*args.storage_def);

            return StorageExecutable::create(args.table_id, script_name, script_name_with_arguments, format, input_queries, pool_settings, columns, constraints);
        }
        else
        {
            return StorageExecutable::create(args.table_id, script_name, script_name_with_arguments, format, input_queries, columns, constraints);
        }
    };

    factory.registerStorage("Executable", [&](const StorageFactory::Arguments & args)
    {
        return register_storage(args, false /*is_executable_pool*/);
    });

    factory.registerStorage("ExecutablePool", [&](const StorageFactory::Arguments & args)
    {
        return register_storage(args, true /*is_executable_pool*/);
    });
}

};

