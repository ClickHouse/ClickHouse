#pragma once

#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>

namespace DB
{

class StorageJoin;
class IKeyValueEntity;
struct JoinAlgorithmParams;

struct PreparedJoinStorage
{
    std::unordered_map<String, String> column_mapping;

    /// At most one of these fields is set
    std::shared_ptr<StorageJoin> storage_join;
    std::shared_ptr<const IKeyValueEntity> storage_key_value;

    operator bool() const { return storage_join || storage_key_value; } /// NOLINT

    template <typename Visitor>
    void visit(Visitor && visitor)
    {
        if (storage_join)
            visitor(storage_join);
        else if (storage_key_value)
            visitor(storage_key_value);
    }
};


/** JoinStepLogical is a logical step for JOIN operation.
  * Doesn't contain any specific join algorithm or other execution details.
  * It's place holder for join operation with it's description that can be serialized.
  * Transformed to actual join step during plan optimization.
  */
class JoinStepLogical final : public IQueryPlanStep
{
public:
    JoinStepLogical(
        SharedHeader left_header_,
        SharedHeader right_header_,
        JoinOperator join_operator_,
        JoinExpressionActions join_expression_actions_,
        const NameSet & required_output_columns_,
        const std::unordered_map<String, const ActionsDAG::Node *> & changed_types,
        bool use_nulls_,
        JoinSettings join_settings_,
        SortingStep::Settings sorting_settings_);

    JoinStepLogical(
        const SharedHeader & left_header_,
        const SharedHeader & right_header_,
        JoinOperator join_operator_,
        JoinExpressionActions join_expression_actions_,
        std::vector<const ActionsDAG::Node *> actions_after_join_,
        JoinSettings join_settings_,
        SortingStep::Settings sorting_settings_);

    ~JoinStepLogical() override;

    String getName() const override { return "JoinLogical"; }
    String getSerializationName() const override { return "Join"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const SortingStep::Settings & getSortingSettings() const { return sorting_settings; }
    const JoinSettings & getJoinSettings() const { return join_settings; }
    JoinSettings & getJoinSettings() { return join_settings; }
    const JoinOperator & getJoinOperator() const { return join_operator; }
    JoinOperator & getJoinOperator() { return join_operator; }

    const ActionsDAG & getActionsDAG() const { return *expression_actions.getActionsDAG(); }

    std::pair<JoinExpressionActions, JoinOperator> detachExpressions()
    {
        return {std::move(expression_actions), std::move(join_operator)};
    }

    const JoinSettings & getSettings() const { return join_settings; }

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override
    {
        return expression_actions.getActionsDAG()->hasCorrelatedColumns();
    }

    void addConditions(ActionsDAG actions_dag);
    std::optional<ActionsDAG::ActionsForFilterPushDown> getFilterActions(JoinTableSide side, const SharedHeader & stream_header);

    struct ActionsDAGWithKeys
    {
        ActionsDAG actions_dag;
        ActionsDAG::NodeRawConstPtrs keys;
    };

    std::optional<std::pair<ActionsDAGWithKeys, ActionsDAGWithKeys>>
    preCalculateKeys(const SharedHeader & left_header, const SharedHeader & right_header);

    static void buildPhysicalJoin(
        QueryPlan::Node & node,
        const QueryPlanOptimizationSettings & optimization_settings,
        QueryPlan::Nodes & nodes);

    std::unordered_set<JoinTableSide> typeChangingSides() const;

    bool isOptimized() const { return optimized; }
    std::optional<UInt64> getResultRowsEstimation() const { return result_rows_estimation; }
    void setOptimized(std::optional<UInt64> estimated_rows_ = {}, std::optional<UInt64> left_rows_ = {}, std::optional<UInt64> right_rows_ = {})
    {
        optimized = true;
        result_rows_estimation = estimated_rows_;
        left_rows_estimation = left_rows_;
        right_rows_estimation = right_rows_;
    }

    void setInputLabels(String left_table_label_, String right_table_label_)
    {
        left_table_label = std::move(left_table_label_);
        right_table_label = std::move(right_table_label_);
    }

    std::pair<std::reference_wrapper<const String>, std::reference_wrapper<const String>> getInputLabels() const
    {
        return {std::cref(left_table_label), std::cref(right_table_label)};
    }

    String getReadableRelationName() const;

    ActionsDAG::NodeRawConstPtrs getActionsAfterJoin() const { return actions_after_join; }

    std::string_view getDummyStats() const { return dummy_stats; }
    void setDummyStats(String dummy_stats_) { dummy_stats = std::move(dummy_stats_); }

    bool isDisjunctionsOptimizationApplied() const { return disjunctions_optimization_applied; }
    void setDisjunctionsOptimizationApplied(bool v) { disjunctions_optimization_applied = v; }

    UInt64 getRightHashTableCacheKey() const { return right_hash_table_cache_key; }
    void setRightHashTableCacheKey(UInt64 right_hash_table_cache_key_) { right_hash_table_cache_key = right_hash_table_cache_key_; }

protected:
    void updateOutputHeader() override;

    std::vector<std::pair<String, String>> describeJoinProperties() const;

    JoinExpressionActions expression_actions;
    JoinOperator join_operator;

    /// This is the nodes which used to split expressions calculated before and after join
    /// Nodes from this list are used as inputs for ActionsDAG executed after join operation
    /// It can be input or node with toNullable function applied to input
    std::vector<const ActionsDAG::Node *> actions_after_join = {};

    JoinSettings join_settings;
    SortingStep::Settings sorting_settings;

    /// Runtime info, do not serialize

    bool optimized = false;
    std::optional<UInt64> result_rows_estimation = {};
    std::optional<UInt64> left_rows_estimation = {};
    std::optional<UInt64> right_rows_estimation = {};
    UInt64 right_hash_table_cache_key = 0;

    String left_table_label;
    String right_table_label;

    /// Dummy stats retrieved from hints, used for debugging
    String dummy_stats;


    std::unique_ptr<JoinAlgorithmParams> join_algorithm_params;
    VolumePtr tmp_volume;
    TemporaryDataOnDiskScopePtr tmp_data;

private:
    bool disjunctions_optimization_applied = false;
};


class JoinStepLogicalLookup final : public ISourceStep
{
public:
    JoinStepLogicalLookup(QueryPlan child_plan_, PreparedJoinStorage prepared_join_storage_, bool use_nulls_);

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override;
    String getName() const override { return "JoinStepLogicalLookup"; }

    PreparedJoinStorage & getPreparedJoinStorage() { return prepared_join_storage; }

    bool useNulls() const { return use_nulls; }

    void optimize(const QueryPlanOptimizationSettings & optimization_settings);
private:
    PreparedJoinStorage prepared_join_storage;
    QueryPlan child_plan;

    bool use_nulls = false;
    bool optimized = false;
};

std::string_view joinTypePretty(JoinKind join_kind, JoinStrictness strictness);


}
