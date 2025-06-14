#pragma once

#include <optional>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/joinCost.h>
#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>

namespace DB
{

class StorageJoin;
class IKeyValueEntity;

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
        const Block & left_header_,
        const Block & right_header_,
        JoinOperator join_operator_,
        JoinExpressionActions join_expression_actions_,
        const NameSet & required_output_columns_,
        const std::unordered_map<String, const ActionsDAG::Node *> & changed_types,
        bool use_nulls_,
        JoinSettings join_settings_,
        SortingStep::Settings sorting_settings_);

    JoinStepLogical(
        const Block & left_header_,
        const Block & right_header_,
        JoinOperator join_operator_,
        JoinExpressionActions join_expression_actions_,
        std::vector<const ActionsDAG::Node *> actions_after_join_,
        JoinSettings join_settings_,
        SortingStep::Settings sorting_settings_);

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

    const JoinSettings & getSettings() const { return join_settings; }
    bool useNulls() const { return use_nulls; }

    struct HashTableKeyHashes
    {
        UInt64 key_hash_left;
        UInt64 key_hash_right;
    };

    void setHashTableCacheKeys(UInt64 left_key_hash, UInt64 right_key_hash)
    {
        hash_table_key_hashes.emplace(left_key_hash, right_key_hash);
    }

    const std::optional<HashTableKeyHashes> & getHashTableKeyHashes() const { return hash_table_key_hashes; }

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override
    {
        return expression_actions.getActionsDAG()->hasCorrelatedColumns();
    }

    void addConditions(ActionsDAG::NodeRawConstPtrs conditions);
    std::optional<ActionsDAG> getFilterActions(JoinTableSide side, String & filter_column_name);

    static void buildPhysicalJoin(
        QueryPlan::Node & node,
        std::vector<RelationStats> relation_stats,
        const QueryPlanOptimizationSettings & optimization_settings,
        QueryPlan::Nodes & nodes);

protected:
    void updateOutputHeader() override;

    std::vector<std::pair<String, String>> describeJoinProperties() const;

    std::optional<HashTableKeyHashes> hash_table_key_hashes;

    JoinExpressionActions expression_actions;
    JoinOperator join_operator;

    /// This is the nodes which used to split expressions calculated before and after join
    /// Nodes from this list are used as inputs for ActionsDAG executed after join operation
    /// It can be input or node with toNullable function applied to input
    std::vector<const ActionsDAG::Node *> actions_after_join = {};

    bool use_nulls;

    JoinSettings join_settings;
    SortingStep::Settings sorting_settings;

    VolumePtr tmp_volume;
    TemporaryDataOnDiskScopePtr tmp_data;

    /// Add some information from convertToPhysical to description in explain output.
    std::vector<std::pair<String, String>> runtime_info_description;
};


class JoinStepLogicalLookup final : public ISourceStep
{
public:
    JoinStepLogicalLookup(Block header, PreparedJoinStorage prepared_join_storage_);

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override;
    String getName() const override { return "JoinStepLogicalLookup"; }

    PreparedJoinStorage & getPreparedJoinStorage() { return prepared_join_storage; }

private:
    PreparedJoinStorage prepared_join_storage;
};

}
