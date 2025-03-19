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
    explicit JoinStepLogical(Header left_header, ContextPtr query_context_);

    String getName() const override { return "JoinLogical"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool canFlatten(const ContextPtr & context_) const;
    bool hasPreparedJoinStorage() const;
    void setPreparedJoinStorage(PreparedJoinStorage storage);
    void setHashTableCacheKey(IQueryTreeNode::HashState hash_table_key_hash_);
    const SortingStep::Settings & getSortingSettings() const { return sorting_settings; }
    const JoinSettings & getJoinSettings() const { return join_settings; }

    JoinOperator & addInput(JoinOperator join_operator, const Header & header);

    std::optional<ActionsDAG> getFilterActions(JoinTableSide side, String & filter_column_name);

    void setSwapInputs() { swap_inputs = true; }
    bool areInputsSwapped() const { return swap_inputs; }


    struct PhysicalJoinNode
    {
        ActionsDAGPtr actions;
        JoinActionRef filter;
        JoinPtr join_strategy;

        BaseRelsSet left_child;
        BaseRelsSet right_child;
    };

    struct PhysicalJoinTree
    {
        std::unordered_map<BaseRelsSet, PhysicalJoinNode> nodes;
    };

    PhysicalJoinTree convertToPhysical(bool is_explain_logical)

    const JoinSettings & getSettings() const { return join_settings; }

    std::vector<Names> & getUsingColumnsMapping() { return using_columns_mapping; }

    size_t getNumberOfTables() const;
    BaseRelsSet getNullExtendedTables() const;
    Headers getCurrentHeaders() const;
    void addRequiredOutput(const NameSet & columns);

    JoinOperator & getJoinOperator(size_t index = 0) { return join_operators.at(index); }
    const JoinOperator & getJoinOperator(size_t index = 0) const { return join_operators.at(index); }

protected:
    void updateOutputHeader() override;

    template <typename ResultType>
    void describeJoinActionsImpl(ResultType & result) const;

    std::vector<JoinOperator> join_operators;
    std::vector<IQueryTreeNode::HashState> hash_table_key_hashes;

    bool swap_inputs = false;
    NameSet required_output_columns;

    PreparedJoinStorage prepared_join_storage;

    JoinSettings join_settings;
    SortingStep::Settings sorting_settings;
    ExpressionActionsSettings expression_actions_settings;

    VolumePtr tmp_volume;
    TemporaryDataOnDiskScopePtr tmp_data;

    /// Add some information from convertToPhysical to description in explain output.
    std::vector<std::pair<String, String>> runtime_info_description;

    std::vector<Names> using_columns_mapping;

    ContextPtr query_context;
};

}
