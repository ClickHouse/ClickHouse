#include <Storages/StorageExecutable.h>

#include <filesystem>

#include <Common/ShellCommand.h>
#include <Core/Block.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/Pipe.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/StorageFactory.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/ShellCommandSource.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
    if (!std::filesystem::exists(std::filesystem::path(script_path)))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Executable file {} does not exists inside {}",
            script_name,
            user_scripts_path);

    std::vector<BlockInputStreamPtr> inputs;
    inputs.reserve(input_queries.size());

    for (auto & input_query : input_queries)
    {
        InterpreterSelectWithUnionQuery interpreter(input_query, context, {});
        auto input = interpreter.execute().getInputStream();
        inputs.emplace_back(std::move(input));
    }

    ShellCommand::Config config(script_path);
    config.arguments = arguments;
    for (size_t i = 1; i < inputs.size(); ++i)
        config.write_fds.emplace_back(i + 2);

    auto process = ShellCommand::executeDirect(config);

    std::vector<ShellCommandSource::SendDataTask> tasks;
    tasks.reserve(inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        BlockInputStreamPtr input_stream = inputs[i];
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

        ShellCommandSource::SendDataTask task = [input_stream, write_buffer, context, this]()
        {
            auto output_stream = context->getOutputStream(format, *write_buffer, input_stream->getHeader().cloneEmpty());
            input_stream->readPrefix();
            output_stream->writePrefix();

            while (auto block = input_stream->read())
                output_stream->write(block);

            input_stream->readSuffix();
            output_stream->writeSuffix();

            output_stream->flush();
            write_buffer->close();
        };

        tasks.emplace_back(std::move(task));
    }

    auto sample_block = metadata_snapshot->getSampleBlock();
    Pipe pipe(std::make_unique<ShellCommandSource>(context, format, sample_block, std::move(process), log, std::move(tasks), max_block_size));
    return pipe;
}

void registerStorageExecutable(StorageFactory & factory)
{
    factory.registerStorage("Executable", [](const StorageFactory::Arguments & args)
    {
        auto local_context = args.getLocalContext();

        if (args.engine_args.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "StorageExecutable requires minimum 2 arguments: script_name, format, [input_query...]");

        for (size_t i = 0; i < 2; ++i)
            args.engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[i], local_context);

        auto scipt_name_with_arguments_value = args.engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        std::vector<String> script_name_with_arguments;
        boost::split(script_name_with_arguments, scipt_name_with_arguments_value, [](char c){ return c == ' '; });

        auto script_name = script_name_with_arguments[0];
        script_name_with_arguments.erase(script_name_with_arguments.begin());
        auto format = args.engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        std::vector<ASTPtr> input_queries;
        for (size_t i = 2; i < args.engine_args.size(); ++i)
        {
            ASTPtr query = args.engine_args[i]->children.at(0);
            if (!query->as<ASTSelectWithUnionQuery>())
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "StorageExecutable argument is invalid input query {}",
                    query->formatForErrorMessage());

            input_queries.emplace_back(std::move(query));
        }

        const auto & columns = args.columns;
        const auto & constraints = args.constraints;

        return StorageExecutable::create(args.table_id, script_name, script_name_with_arguments, format, input_queries, columns, constraints);
    });
}

};

