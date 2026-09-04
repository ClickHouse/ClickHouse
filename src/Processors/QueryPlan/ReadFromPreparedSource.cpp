#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>


#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Processors/QueryPlan/StepManifest.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 query_plan_max_step_description_length;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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

namespace
{

constexpr auto READ_FROM_STORAGE_MANIFEST = StepManifest<ReadFromStorageStep, ReadFromStorageWire>("ReadFromStorage")
    .nameIntroducedIn(1)
    .baseFormat(field("storage_name", WireFieldClass::Logical, &ReadFromStorageWire::storage_name));

}

ReadFromStorageWire ReadFromStorageStep::toWire() const
{
    /// Not a logical error: a caller (e.g. the distributed-plan serializability check) may probe an
    /// unsupported plan, and a logical error would abort debug/fuzzer builds instead of being handled.
    if (storage->getName() != "SystemOne")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ReadFromStorageStep serialization is implemented only for StorageSystemOne, got: {}", storage->getName());
    return ReadFromStorageWire{storage->getName()};
}

QueryPlanStepPtr ReadFromStorageStep::fromWire(ReadFromStorageWire wire, Deserialization & ctx)
{
    if (wire.storage_name != "SystemOne")
        throw Exception(ErrorCodes::INCORRECT_DATA, "ReadFromStorageStep deserialization is implemented only for StorageSystemOne, got: {}", wire.storage_name);

    /// "Fake" system.one represented by a chunk with single row
    auto column = DataTypeUInt8().createColumnConst(1, 0u)->convertToFullColumnIfConst();
    Chunk chunk({ std::move(column) }, 1);
    auto source = std::make_shared<SourceFromSingleChunk>(ctx.output_header, std::move(chunk));
    source->addTotalRowsApprox(1);
    return std::make_unique<ReadFromPreparedSource>(Pipe(source));
}

void ReadFromStorageStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(READ_FROM_STORAGE_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr ReadFromStorageStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(READ_FROM_STORAGE_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void ReadFromStorageStep::serializeLegacy(Serialization & ctx) const
{
    /// Not a logical error: a caller (e.g. the distributed-plan serializability check) may probe an
    /// unsupported plan, and a logical error would abort debug/fuzzer builds instead of being handled.
    if (storage->getName() != "SystemOne")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ReadFromStorageStep serialization is implemented only for StorageSystemOne, got: {}", storage->getName());

    writeStringBinary(storage->getName(), ctx.out);
}

bool ReadFromStorageStep::isSerializable() const
{
    return storage && storage->getName() == "SystemOne";
}

QueryPlanStepPtr ReadFromStorageStep::deserializeLegacy(Deserialization & ctx)
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

void registerReadFromStorageStep(QueryPlanStepRegistry & registry);
void registerReadFromStorageStep(QueryPlanStepRegistry & registry)
{
    registerManifest<READ_FROM_STORAGE_MANIFEST>(registry, ReadFromStorageStep::deserialize);
}

}
