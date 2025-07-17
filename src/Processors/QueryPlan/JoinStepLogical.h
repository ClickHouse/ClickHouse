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
    const JoinOperator & getJoinOperator() const { return join_operator; }
    JoinOperator & getJoinOperator() { return join_operator; }

    JoinPtr convertToPhysical(
        JoinActionRef & post_filter,
        bool is_explain_logical,
        UInt64 max_threads,
        UInt64 max_entries_for_hash_table_stats,
        String initial_query_id,
        std::chrono::milliseconds lock_acquire_timeout,
        const ExpressionActionsSettings & actions_settings,
        std::optional<UInt64> rhs_size_estimation);

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

    static void buildPhysicalJoin(
        QueryPlan::Node & node,
        const QueryPlanOptimizationSettings & optimization_settings,
        QueryPlan::Nodes & nodes);

    bool changesColumnsType() const { return false; }

    bool isOptimized() const { return optimized; }
    size_t getResultRowsEstimation() const { return result_rows_estimation; }
    void setOptimized(size_t estimated_rows_ = 0)
    {
        optimized = true;
        result_rows_estimation = estimated_rows_;
    }

    void setInputLabels(String left_table_label_, String right_table_label_)
    {
        left_table_label = std::move(left_table_label_);
        right_table_label = std::move(right_table_label_);
    }

    std::pair<std::string_view, std::string_view> getInputLabels() const
    {
        std::string_view left_label = left_table_label;
        std::string_view right_label = right_table_label;
        return {left_label, right_label};
    }

    String getReadableRelationName() const;

    ActionsDAG::NodeRawConstPtrs getActionsAfterJoin() const { return actions_after_join; }

protected:
    void updateOutputHeader() override;

    std::vector<std::pair<String, String>> describeJoinProperties() const;

    JoinExpressionActions expression_actions;
    JoinOperator join_operator;

    /// This is the nodes which used to split expressions calculated before and after join
    /// Nodes from this list are used as inputs for ActionsDAG executed after join operation
    /// It can be input or node with toNullable function applied to input
    std::vector<const ActionsDAG::Node *> actions_after_join = {};

    String left_table_label;
    String right_table_label;

    JoinSettings join_settings;
    SortingStep::Settings sorting_settings;

    bool optimized = false;
    size_t result_rows_estimation = 0;

    std::unique_ptr<JoinAlgorithmParams> join_algorithm_params;
    VolumePtr tmp_volume;
    TemporaryDataOnDiskScopePtr tmp_data;
};


class JoinStepLogicalLookup final : public ISourceStep
{
public:
    JoinStepLogicalLookup(QueryPlan child_plan_, PreparedJoinStorage prepared_join_storage_);

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override;
    String getName() const override { return "JoinStepLogicalLookup"; }

    PreparedJoinStorage & getPreparedJoinStorage() { return prepared_join_storage; }

    bool useNulls() const { return use_nulls; }
    void setUseNulls(bool use_nulls_ = true) { use_nulls = use_nulls_; }

    void optimize(const QueryPlanOptimizationSettings & optimization_settings);
private:
    PreparedJoinStorage prepared_join_storage;

    bool use_nulls = false;
    bool optimized = false;
    QueryPlan child_plan;
};

std::string_view joinTypePretty(JoinKind join_kind, JoinStrictness strictness);


}
