#include <Processors/Sources/LazyReadFromFileSource.h>
#include <Storages/StorageFile.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LazyReadFromFileSource::LazyReadFromFileSource(
    SharedHeader header,
    std::shared_ptr<StorageFile> storage_,
    ReadFromFormatInfo info_,
    ContextPtr context_,
    size_t max_block_size_,
    FileLazyMaterializingRowsPtr lazy_materializing_rows_)
    : IProcessor({}, {std::move(header)})
    , storage(std::move(storage_))
    , info(std::move(info_))
    , context(std::move(context_))
    , max_block_size(max_block_size_)
    , lazy_materializing_rows(std::move(lazy_materializing_rows_))
{
}

IProcessor::Status LazyReadFromFileSource::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (lazy_materializing_rows)
        return Status::UpdatePipeline;

    if (inputs.empty())
    {
        /// No rows survived the LIMIT, nothing to read.
        output.finish();
        return Status::Finished;
    }

    auto & input = inputs.front();
    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    output.push(input.pull());
    return Status::PortFull;
}

IProcessor::PipelineUpdate LazyReadFromFileSource::updatePipeline()
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazyReadFromFileSource: no lazy materializing rows");

    auto rows = std::move(lazy_materializing_rows);
    lazy_materializing_rows.reset();

    if (rows->rows_in_files.empty())
        return {};

    auto source = StorageFile::createLazyRowsSource(
        storage, info, context, max_block_size, std::move(rows->rows_in_files));

    auto & source_output = source->getOutputs().front();
    inputs.emplace_back(source_output.getHeader(), this);
    connect(source_output, inputs.back());
    inputs.back().setNeeded();

    return PipelineUpdate{.to_add = {source}, .to_remove = {}};
}

}
