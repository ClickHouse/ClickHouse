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
    extern const int LOGICAL_ERROR;
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

    for (size_t i = 1; i < inputs.size(); ++i)
        config.write_fds.emplace_back(i + 2);

    auto process = ShellCommand::execute(config);

    std::vector<ShellCommandSource::SendDataTask> tasks;
    tasks.reserve(inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        BlockInputStreamPtr input_stream = inputs[i];
        WriteBufferFromFile * write_buffer;

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

    Pipe pipe(std::make_unique<ShellCommandSource>(context, format, sample_block, std::move(process), log, std::move(tasks), max_block_size));
    return pipe;
}

};

