#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/JoinInfo.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/SipHash.h>

namespace DB
{

class StorageJoin;
class IKeyValueEntity;

struct PreparedJoinStorage
{
    std::unordered_map<String, String> column_mapping;

    /// None or one of these fields is set
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
        JoinInfo join_info_,
        JoinExpressionActions join_expression_actions_,
        Names required_output_columns_,
        ContextPtr query_context_);

    String getName() const override { return "JoinLogical"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool hasPreparedJoinStorage() const;
    void setPreparedJoinStorage(PreparedJoinStorage storage);
    void setHashTableCacheKey(IQueryTreeNode::HashState hash_table_key_hash_);
    const SortingStep::Settings & getSortingSettings() const { return sorting_settings; }
    const JoinSettings & getJoinSettings() const { return join_settings; }
    const JoinInfo & getJoinInfo() const { return join_info; }
    JoinInfo & getJoinInfo() { return join_info; }

    std::optional<ActionsDAG> getFilterActions(JoinTableSide side, String & filter_column_name);

    void setSwapInputs() { swap_inputs = true; }
    bool areInputsSwapped() const { return swap_inputs; }

    JoinPtr convertToPhysical(
        JoinActionRef & post_filter,
        bool is_explain_logical,
        UInt64 max_threads,
        UInt64 max_entries_for_hash_table_stats,
        String initial_query_id,
        std::chrono::milliseconds lock_acquire_timeout);

    JoinExpressionActions & getExpressionActions() { return expression_actions; }

    ContextPtr getContext() const { return query_context; }
    const JoinSettings & getSettings() const { return join_settings; }
    bool useNulls() const { return use_nulls; }

protected:
    void updateOutputHeader() override;

    std::vector<std::pair<String, String>> describeJoinActions() const;

    JoinExpressionActions expression_actions;
    JoinInfo join_info;

    Names required_output_columns;

    bool use_nulls;

    JoinSettings join_settings;
    SortingStep::Settings sorting_settings;
    ExpressionActionsSettings expression_actions_settings;

    bool swap_inputs = false;

    PreparedJoinStorage prepared_join_storage;
    IQueryTreeNode::HashState hash_table_key_hash;

    VolumePtr tmp_volume;
    TemporaryDataOnDiskScopePtr tmp_data;

    ContextPtr query_context;

    /// Add some information from convertToPhysical to description in explain output.
    std::vector<std::pair<String, String>> runtime_info_description;
};

}
