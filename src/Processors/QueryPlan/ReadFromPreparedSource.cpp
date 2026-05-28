#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>


#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/System/StorageSystemOne.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 query_plan_max_step_description_length;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadFromPreparedSource::ReadFromPreparedSource(Pipe pipe_)
    : ISourceStep(pipe_.getSharedHeader())
    , pipe(std::move(pipe_))
{
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

void ReadFromStorageStep::serialize(Serialization & ctx) const
{
    if (storage->as<StorageSystemOne>() == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ReadFromStorageStep serailization is implemented only for StorageSystemOne, got: {}", storage->getName());

    writeStringBinary(storage->getName(), ctx.out);
}

std::unique_ptr<IQueryPlanStep> ReadFromStorageStep::deserialize(Deserialization & ctx)
{
    String storage_name;
    readStringBinary(storage_name, ctx.in);
    if (storage_name != "SystemOne")
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ReadFromStorageStep deserialization is implemented only for StorageSystemOne, got: {}", storage_name);

    /// "Fake" system.one represented by a chunk with single row
    auto column = DataTypeUInt8().createColumnConst(1, 0u)->convertToFullColumnIfConst();
    Chunk chunk({ std::move(column) }, 1);

    auto source = std::make_shared<SourceFromSingleChunk>(ctx.output_header, std::move(chunk));
    source->addTotalRowsApprox(1);

    return std::make_unique<ReadFromPreparedSource>(Pipe(source));
}

void registerReadFromStorageStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadFromStorage", ReadFromStorageStep::deserialize);
}

}
