#include <Storages/StorageExecutable.h>

#include <filesystem>
#include <unistd.h>

#include <boost/algorithm/string/split.hpp>

#include <Common/filesystemHelpers.h>

#include <Core/Block.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    void transformToSingleBlockSources(Pipes & inputs)
    {
        size_t inputs_size = inputs.size();
        for (size_t i = 0; i < inputs_size; ++i)
        {
            auto && input = inputs[i];
            QueryPipeline input_pipeline(std::move(input));
            PullingPipelineExecutor input_pipeline_executor(input_pipeline);

            auto header = input_pipeline_executor.getHeader();
            auto result_block = header.cloneEmpty();

            size_t result_block_columns = result_block.columns();

            Block result;
            while (input_pipeline_executor.pull(result))
            {
                for (size_t result_block_index = 0; result_block_index < result_block_columns; ++result_block_index)
                {
                    auto & block_column = result.safeGetByPosition(result_block_index);
                    auto & result_block_column = result_block.safeGetByPosition(result_block_index);

                    result_block_column.column->assumeMutable()->insertRangeFrom(*block_column.column, 0, block_column.column->size());
                }
            }

            auto source = std::make_shared<SourceFromSingleChunk>(std::move(result_block));
            inputs[i] = Pipe(std::move(source));
        }
    }
}

StorageExecutable::StorageExecutable(
    const StorageID & table_id_,
    const String & format,
    const ExecutableSettings & settings_,
    const std::vector<ASTPtr> & input_queries_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints)
    : IStorage(table_id_)
    , settings(settings_)
    , input_queries(input_queries_)
    , log(settings.is_executable_pool ? &Poco::Logger::get("StorageExecutablePool") : &Poco::Logger::get("StorageExecutable"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setConstraints(constraints);
    setInMemoryMetadata(storage_metadata);

    ShellCommandSourceCoordinator::Configuration configuration
    {
        .format = format,
        .command_termination_timeout_seconds = settings.command_termination_timeout,
        .command_read_timeout_milliseconds = settings.command_read_timeout,
        .command_write_timeout_milliseconds = settings.command_write_timeout,

        .pool_size = settings.pool_size,
        .max_command_execution_time_seconds = settings.max_command_execution_time,

        .is_executable_pool = settings.is_executable_pool,
        .send_chunk_header = settings.send_chunk_header,
        .execute_direct = true
    };

    coordinator = std::make_unique<ShellCommandSourceCoordinator>(std::move(configuration));
}

void StorageExecutable::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*threads*/)
{
    auto & script_name = settings.script_name;

    auto user_scripts_path = context->getUserScriptsPath();
    auto script_path = user_scripts_path + '/' + script_name;

    if (!fileOrSymlinkPathStartsWith(script_path, user_scripts_path))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Executable file {} must be inside user scripts folder {}",
            script_name,
            user_scripts_path);

    if (!FS::exists(script_path))
         throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Executable file {} does not exist inside user scripts folder {}",
            script_name,
            user_scripts_path);

    if (!FS::canExecute(script_path))
         throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Executable file {} is not executable inside user scripts folder {}",
            script_name,
            user_scripts_path);

    Pipes inputs;
    QueryPlanResourceHolder resources;
    inputs.reserve(input_queries.size());

    for (auto & input_query : input_queries)
    {
        InterpreterSelectWithUnionQuery interpreter(input_query, context, {});
        auto builder = interpreter.buildQueryPipeline();
        inputs.emplace_back(QueryPipelineBuilder::getPipe(std::move(builder), resources));
    }

    /// For executable pool we read data from input streams and convert it to single blocks streams.
    if (settings.is_executable_pool)
        transformToSingleBlockSources(inputs);

    auto sample_block = storage_snapshot->metadata->getSampleBlock();

    ShellCommandSourceConfiguration configuration;
    configuration.max_block_size = max_block_size;

    if (settings.is_executable_pool)
    {
        configuration.read_fixed_number_of_rows = true;
        configuration.read_number_of_rows_from_process_output = true;
    }

    auto pipe = coordinator->createPipe(script_path, settings.script_arguments, std::move(inputs), std::move(sample_block), context, configuration);
    IStorage::readFromPipe(query_plan, std::move(pipe), column_names, storage_snapshot, query_info, context, getName());
    query_plan.addResources(std::move(resources));
}

void registerStorageExecutable(StorageFactory & factory)
{
    auto register_storage = [](const StorageFactory::Arguments & args, bool is_executable_pool) -> StoragePtr
    {
        auto local_context = args.getLocalContext();

        if (args.engine_args.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "StorageExecutable requires minimum 2 arguments: script_name, format, [input_query...]");

        auto arg_it = args.engine_args.begin();
        for (size_t i = 0; i < 2; ++i, ++arg_it)
            *arg_it = evaluateConstantExpressionOrIdentifierAsLiteral(*arg_it, local_context);

        auto script_name_with_arguments_value = checkAndGetLiteralArgument<String>(args.engine_args.front(), "script_name_with_arguments_value");

        std::vector<String> script_name_with_arguments;
        boost::split(script_name_with_arguments, script_name_with_arguments_value, [](char c) { return c == ' '; });

        auto script_name = script_name_with_arguments[0];
        script_name_with_arguments.erase(script_name_with_arguments.begin());
        auto format = checkAndGetLiteralArgument<String>(*++args.engine_args.begin(), "format");

        std::vector<ASTPtr> input_queries;
        for (; arg_it != args.engine_args.end(); ++arg_it)
        {
            ASTPtr query = (*arg_it)->children.front();
            if (!query->as<ASTSelectWithUnionQuery>())
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD, "StorageExecutable argument is invalid input query {}",
                    query->formatForErrorMessage());

            input_queries.emplace_back(std::move(query));
        }

        const auto & columns = args.columns;
        const auto & constraints = args.constraints;

        ExecutableSettings settings;
        settings.script_name = script_name;
        settings.script_arguments = script_name_with_arguments;
        settings.is_executable_pool = is_executable_pool;

        if (is_executable_pool)
        {
            size_t max_command_execution_time = 10;

            size_t max_execution_time_seconds = static_cast<size_t>(args.getContext()->getSettings().max_execution_time.totalSeconds());
            if (max_execution_time_seconds != 0 && max_command_execution_time > max_execution_time_seconds)
                max_command_execution_time = max_execution_time_seconds;

            settings.max_command_execution_time = max_command_execution_time;
        }

        if (args.storage_def->settings)
            settings.loadFromQuery(*args.storage_def);

        auto global_context = args.getContext()->getGlobalContext();
        return std::make_shared<StorageExecutable>(args.table_id, format, settings, input_queries, columns, constraints);
    };

    StorageFactory::StorageFeatures storage_features;
    storage_features.supports_settings = true;

    factory.registerStorage("Executable", [&](const StorageFactory::Arguments & args)
    {
        return register_storage(args, false /*is_executable_pool*/);
    }, storage_features);

    factory.registerStorage("ExecutablePool", [&](const StorageFactory::Arguments & args)
    {
        return register_storage(args, true /*is_executable_pool*/);
    }, storage_features);
}

}

