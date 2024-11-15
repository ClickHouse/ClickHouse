#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/Joins.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

struct DynamiclyFilteredPartsRanges;
using DynamiclyFilteredPartsRangesPtr = std::shared_ptr<DynamiclyFilteredPartsRanges>;

class ColumnSet;

/// This structure is used to filter left table by the right one after HashJoin is filled.
struct DynamicJoinFilters
{
    struct Clause
    {
        ColumnSet * set;
        Names keys;
    };

    std::vector<Clause> clauses;
    DynamiclyFilteredPartsRangesPtr parts;
    ActionsDAG actions;
    ContextPtr context;
    StorageMetadataPtr metadata;

    void filterDynamicPartsByFilledJoin(const IJoin & join);
};

using DynamicJoinFiltersPtr = std::shared_ptr<DynamicJoinFilters>;

/// Join two data streams.
class JoinStep : public IQueryPlanStep
{
public:
    JoinStep(
        const Header & left_header_,
        const Header & right_header_,
        JoinPtr join_,
        size_t max_block_size_,
        size_t max_streams_,
        bool keep_left_read_in_order_);

    String getName() const override { return "Join"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const JoinPtr & getJoin() const { return join; }
    void setJoin(JoinPtr join_) { join = std::move(join_); }
    bool allowPushDownToRight() const;

    void setDynamicFilter(DynamicJoinFiltersPtr dynamic_filter_);

private:
    void updateOutputHeader() override;

    JoinPtr join;
    size_t max_block_size;
    size_t max_streams;
    bool keep_left_read_in_order;

    DynamicJoinFiltersPtr dynamic_filter;
};

/// Special step for the case when Join is already filled.
/// For StorageJoin and Dictionary.
class FilledJoinStep : public ITransformingStep
{
public:
    FilledJoinStep(const Header & input_header_, JoinPtr join_, size_t max_block_size_);

    String getName() const override { return "FilledJoin"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const JoinPtr & getJoin() const { return join; }

private:
    void updateOutputHeader() override;

    JoinPtr join;
    size_t max_block_size;
};

}
