#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>


namespace DB
{

class ReadFromSystemNumbersStep final : public SourceStepWithFilter
{
public:
    ReadFromSystemNumbersStep(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        StoragePtr storage_,
        size_t max_block_size_,
        size_t num_streams_);

    String getName() const override { return "ReadFromSystemNumbers"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanStepPtr clone() const override;

    /// A replica rebuilds the read from the table's parameters. The source filter and the pushed-down
    /// LIMIT are not serialized: the replica sets them again when it optimizes the received plan.
    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

private:
    Pipe makePipe();

    const Names column_names;
    StoragePtr storage;
    ExpressionActionsPtr key_expression;
    size_t max_block_size;
    size_t num_streams;
};

}
