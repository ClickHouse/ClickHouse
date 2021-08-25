#include <Storages/StorageExecutable.h>

#include <filesystem>

#include <Common/ShellCommand.h>
#include <Core/Block.h>
#include <IO/ReadHelpers.h>
#include <Processors/Pipe.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFactory.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/ShellCommandSource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

StorageExecutable::StorageExecutable(
    const StorageID & table_id_,
    const String & script_name_,
    const String & format_,
    const std::vector<BlockInputStreamPtr> & inputs_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints)
    : IStorage(table_id_)
    , script_name(script_name_)
    , format(format_)
    , inputs(inputs_)
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

    auto sample_block = metadata_snapshot->getSampleBlock();

    ShellCommand::Config config(script_path);

    for (size_t i = 0; i < inputs.size() - 1; ++i)
        config.write_descriptors.emplace_back(i + 3);

    auto process = ShellCommand::execute(config);

    Pipe result;
    if (inputs.empty())
    {
        Pipe pipe(FormatFactory::instance().getInput(format, process->out, std::move(sample_block), context, max_block_size));
        pipe.addTransform(std::make_shared<ShellCommandOwningTransform>(pipe.getHeader(), log, std::move(process)));

        result = std::move(pipe);
    }
    else
    {
        Pipe pipe(std::make_unique<ShellCommandSourceWithBackgroundThread>(context, format, std::move(sample_block), std::move(process), log,
            [context, config, this](ShellCommand & command) mutable
            {
                std::vector<std::pair<BlockInputStreamPtr, BlockOutputStreamPtr>> input_output_streams;

                size_t inputs_size = inputs.size();
                input_output_streams.reserve(inputs_size);

                auto & out = command.in;
                auto & stdin_input_stream = inputs[0];
                auto stdin_output_stream = context->getOutputStream(format, out, stdin_input_stream->getHeader().cloneEmpty());
                input_output_streams.emplace_back(stdin_input_stream, stdin_output_stream);

                for (size_t i = 0; i < config.write_descriptors.size(); ++i)
                {
                    auto write_descriptor = config.write_descriptors[i];
                    auto it = command.write_descriptors.find(write_descriptor);
                    if (it == command.write_descriptors.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Process does not contain descriptor to write {}", write_descriptor);

                    auto input_stream = inputs[i];
                    auto output_stream = context->getOutputStream(format, it->second, input_stream->getHeader().cloneEmpty());
                    input_output_streams.emplace_back(input_stream, output_stream);
                }

                for (auto & [input_stream, output_stream] : input_output_streams)
                {
                    input_stream->readPrefix();
                    output_stream->writePrefix();

                    while (auto block = input_stream->read())
                        output_stream->write(block);

                    input_stream->readSuffix();
                    output_stream->writeSuffix();

                    output_stream->flush();
                    out.close();
                }
            }));

        result = std::move(pipe);
    }

    return result;
}
};

