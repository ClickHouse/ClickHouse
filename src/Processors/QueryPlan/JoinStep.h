#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/Joins.h>

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

/// Join two data streams.
class JoinStep : public IQueryPlanStep
{
public:
    JoinStep(
        const SharedHeader & left_header_,
        const SharedHeader & right_header_,
        JoinPtr join_,
        size_t max_block_size_,
        size_t min_block_size_rows_,
        size_t min_block_size_bytes_,
        size_t max_streams_,
        NameSet required_output_,
        bool keep_left_read_in_order_,
        bool use_new_analyzer_,
        bool use_join_disjunctions_push_down_ = false);

    String getName() const override { return "Join"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const JoinPtr & getJoin() const { return join; }
    void setJoin(JoinPtr join_, bool swap_streams_ = false);
    bool allowPushDownToRight() const;

    /// Swap automatically if not set, otherwise always or never, depending on the value
    std::optional<bool> swap_join_tables = false;

    struct PrimaryKeyNamesPair
    {
        std::string lhs_name;
        std::string rhs_name;
    };

    using PrimaryKeySharding = std::vector<PrimaryKeyNamesPair>;

    /// Set names of PK columns for optimized for JOIN sharder by PK ranges.
    /// Names are required for EXPLAIN only.
    void enableJoinByLayers(PrimaryKeySharding sharding) { primary_key_sharding = std::move(sharding); }
    void keepLeftPipelineInOrder() { keep_left_read_in_order = true; }

    bool isOptimized() const { return optimized; }
    void setOptimized() { optimized = true; }

private:
    bool optimized = false;
    void updateOutputHeader() override;

    /// Header that expected to be returned from IJoin
    SharedHeader join_algorithm_header;

    JoinPtr join;
    size_t max_block_size;
    size_t min_block_size_rows;
    size_t min_block_size_bytes;
    size_t max_streams;

    const NameSet required_output;
    std::set<size_t> columns_to_remove;
    bool keep_left_read_in_order;
    bool use_new_analyzer = false;
    bool use_join_disjunctions_push_down;
    bool disjunctions_optimization_applied = false;    /// Flag that indicates that disjunction optimization was already applied
    /// to prevent infinite optimization loop

public:
    /// Check if disjunction optimization was already applied to this JoinStep
    bool isDisjunctionsOptimizationApplied() const { return disjunctions_optimization_applied; }

    /// Mark that disjunction optimization has been applied to this JoinStep
    void setDisjunctionsOptimizationApplied(bool value) { disjunctions_optimization_applied = value; }

    bool swap_streams = false;
    bool useJoinDisjunctionsPushDown() const { return use_join_disjunctions_push_down; }
    PrimaryKeySharding primary_key_sharding;
};

/// Special step for the case when Join is already filled.
/// For StorageJoin and Dictionary.
class FilledJoinStep : public ITransformingStep
{
public:
    FilledJoinStep(const SharedHeader & input_header_, JoinPtr join_, size_t max_block_size_);

    String getName() const override { return "FilledJoin"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const JoinPtr & getJoin() const { return join; }

    bool isDisjunctionsOptimizationApplied() const { return disjunctions_optimization_applied; }
    void setDisjunctionsOptimizationApplied(bool v) { disjunctions_optimization_applied = v; }

private:
    void updateOutputHeader() override;

    JoinPtr join;
    size_t max_block_size;
    bool disjunctions_optimization_applied = false;
};

}
