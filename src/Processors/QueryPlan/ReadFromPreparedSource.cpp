#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 query_plan_max_step_description_length;
}

ReadFromPreparedSource::ReadFromPreparedSource(Pipe pipe_)
    : ISourceStep(pipe_.getSharedHeader())
    , pipe(std::move(pipe_))
{
}

// TODO(mfilitov): maybe calculate it in ctor?
bool ReadFromPreparedSource::isEmpty() const
{
    const auto & processors = pipe.getProcessors();
    for (const auto & processor : processors)
    {
        if (!typeid_cast<const NullSource *>(processor.get()))
            return false;
    }

    return true;
}

void ReadFromPreparedSource::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

ReadFromStorageStep::ReadFromStorageStep(
    Pipe pipe_,
    StoragePtr storage_,
    ContextPtr context_,
    const SelectQueryInfo & query_info_)
    : ReadFromPreparedSource(std::move(pipe_))
    , storage(std::move(storage_))
    , context(std::move(context_))
    , query_info(query_info_)
{
    auto description = storage->getName();
    setStepDescription(description, context->getSettingsRef()[Setting::query_plan_max_step_description_length]);

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(query_info.storage_limits);
}

}
