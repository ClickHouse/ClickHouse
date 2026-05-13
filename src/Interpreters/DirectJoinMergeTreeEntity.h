#pragma once

#include <Common/PODArray_fwd.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Processors/Chunk.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageSnapshot.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

namespace DB
{

class IStorage;

class DirectJoinMergeTreeEntity : public IKeyValueEntity
{
public:
    DirectJoinMergeTreeEntity(QueryPlan && lookup_plan_, ActionsDAG filter_dag_, ContextPtr context_);

    Names getPrimaryKey() const override;

    Chunk getByKeys(
        const ColumnsWithTypeAndName & keys,
        const Names & required_columns,
        PaddedPODArray<UInt8> & out_null_map,
        IColumn::Offsets & out_offsets) const override;

    Block getSampleBlock(const Names & required_columns) const override;

private:
    Chunk executePlan(QueryPlan & plan) const;

    QueryPlan lookup_plan;
    ActionsDAG filter_dag;
    ContextPtr context;
    QueryPlanOptimizationSettings plan_optimization_settings;
    BuildQueryPipelineSettings pipeline_build_settings;

    LoggerPtr log;
};

}
