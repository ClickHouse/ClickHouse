#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SaveSubqueryResultToBufferStep.h>

namespace DB
{

class ReadFromCommonBufferStep : public ISourceStep
{
public:
    ReadFromCommonBufferStep(
        const SharedHeader & header_,
        ChunkBufferPtr chunk_buffer_,
        size_t max_streams_);

    String getName() const override { return "ReadFromCommonBuffer"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
private:
    ChunkBufferPtr chunk_buffer;
    size_t max_streams;
};

class CommonSubplanReferenceStep : public ISourceStep
{
public:
    explicit CommonSubplanReferenceStep(
        const SharedHeader & header_,
        QueryPlan::Node * subplan_root_,
        ColumnIdentifiers columns_to_use_)
        : ISourceStep(header_)
        , subplan_root(subplan_root_)
        , columns_to_use(std::move(columns_to_use_))
    {}

    String getName() const override { return "CommonSubplanReference"; }

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CommonSubplanReference cannot be used to build pipeline");
    }

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<CommonSubplanReferenceStep>(getOutputHeader(), subplan_root, columns_to_use);
    }

    QueryPlan::Node * getSubplanReferenceRoot() const { return subplan_root; }

    const ColumnIdentifiers & getColumnsToUse() const { return columns_to_use; }

    ColumnIdentifiers extractColumnsToUse() { return std::move(columns_to_use); }

private:
    QueryPlan::Node * subplan_root = nullptr;

    ColumnIdentifiers columns_to_use;
};

}
