#include <Storages/StorageExecutable.h>
#include <Processors/Pipe.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFactory.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <DataStreams/ShellCommandSource.h>
#include <Core/Block.h>
#include <Common/ShellCommand.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_IDENTIFIER;
}

StorageExecutable::StorageExecutable(
    const StorageID & table_id_,
    const String & file_path_,
    const String & format_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints)
    : IStorage(table_id_)
    , file_path(file_path_)
    , format(format_)
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
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    auto process = ShellCommand::execute(file_path);
    auto sample_block = metadata_snapshot->getSampleBlock();
    Pipe pipe(FormatFactory::instance().getInput(format, process->out, std::move(sample_block), std::move(context_), max_block_size));
    pipe.addTransform(std::make_shared<ShellCommandOwningTransform>(pipe.getHeader(), log, std::move(process)));
    return pipe;
}
};

