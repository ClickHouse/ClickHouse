#include <Storages/System/StorageSystemOne.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Core/ProtocolDefines.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}


StorageSystemOne::StorageSystemOne(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    /// This column doesn't have a comment, because otherwise it will be added to all tables created via:
    /// CREATE TABLE test (dummy UInt8) ENGINE = Distributed(`default`, `system.one`)
    storage_metadata.setColumns(ColumnsDescription({{"dummy", std::make_shared<DataTypeUInt8>()}}));
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemOne::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}


void StorageSystemOne::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    query_plan.addStep(std::make_unique<ReadFromSystemOneStep>(column_names, storage_snapshot));
}


ReadFromSystemOneStep::ReadFromSystemOneStep(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot_
)
    : ISourceStep(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names_)))
{
}


ReadFromSystemOneStep::ReadFromSystemOneStep(SharedHeader header_)
    : ISourceStep(std::move(header_))
{
}


void ReadFromSystemOneStep::serialize(Serialization & ctx) const
{
    /// The step name is only registered since this version; an older peer would not know it and
    /// would fail on the stream, so fail closed rather than write bytes it cannot parse.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SYSTEM_SOURCE_STEPS)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Serializing a ReadFromSystemOne step requires query plan serialization version >= {}; "
            "all nodes must run the same version", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SYSTEM_SOURCE_STEPS);
}


QueryPlanStepPtr ReadFromSystemOneStep::deserialize(Deserialization & ctx)
{
    /// Mirrors the guard in `serialize`: a peer below this version cannot have written this step.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SYSTEM_SOURCE_STEPS)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Deserializing a ReadFromSystemOne step requires query plan serialization version >= {}; "
            "all nodes must run the same version", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SYSTEM_SOURCE_STEPS);

    return std::make_unique<ReadFromSystemOneStep>(ctx.output_header);
}


void ReadFromSystemOneStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto column = DataTypeUInt8().createColumnConst(1, 0u)->convertToFullColumnIfConst();
    Chunk chunk({ std::move(column) }, 1);

    auto source = std::make_shared<SourceFromSingleChunk>(getOutputHeader(), std::move(chunk));
    source->addTotalRowsApprox(1);

    pipeline.init(Pipe(source));
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemOne) }

namespace DB
{

void registerReadFromSystemOneStep(QueryPlanStepRegistry & registry);
void registerReadFromSystemOneStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadFromSystemOne", &ReadFromSystemOneStep::deserialize);
}

}
