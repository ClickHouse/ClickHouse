#pragma once

#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/Optimizations/joinCost.h>
#include <Common/SafePtr.h>

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

struct QueryPlanOptimizationSettings;

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
        bool use_nulls_,
        JoinSettings join_settings_,
        SortingStep::Settings sorting_settings_);

    String getName() const override { return "JoinLogical"; }
    String getSerializationName() const override { return "Join"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool canFlatten() const;
    bool hasPreparedJoinStorage() const;
    void setPreparedJoinStorage(PreparedJoinStorage storage);

    const NameSet & getRequiredOutputColumns() const { return required_output_columns; }

    JoinOperator & addInput(JoinOperator join_operator, const Header & header);

    std::optional<ActionsDAG> getFilterActions(JoinTableSide side, String & filter_column_name);

    QueryPlan::Node * optimizeToPhysicalPlan(
        std::vector<QueryPlan::Node *> input_steps,
        QueryPlan::Nodes & query_plan_nodes,
        const QueryPlanOptimizationSettings & optimization_settings);

    bool useNulls() const { return use_nulls; }

    void setHashTableCacheKey(UInt64 hash_table_key_hash_, size_t idx);

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    std::vector<Names> & getUsingColumnsMapping() { return using_columns_mapping; }

    size_t getNumberOfTables() const;
    BaseRelsSet getNullExtendedTables() const;
    Headers getCurrentHeaders() const;
    void addRequiredOutput(const NameSet & columns);

    JoinOperator & getJoinOperator(size_t index = 0) { return join_operators.at(index); }
    const JoinOperator & getJoinOperator(size_t index = 0) const { return join_operators.at(index); }

    const std::vector<JoinOperator> & getJoinOperators() const { return join_operators; }

    const JoinSettings & getJoinSettings() const { return join_settings; }

    ActionsDAG & getExpressionActions()
    {
        if (!expression_actions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression actions are not initialized");
        return *expression_actions;
    }

    ActionsDAG cloneExpressionActions(size_t input_num)
    {
        if (!expression_actions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression actions are not initialized");
        UNUSED(input_num);
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cloning of expression actions is not implemented");
    }

    void setRelationStats(RelationStats new_stats, size_t index);
    const std::vector<RelationStats> & getRelationStats() const { return relation_stats; }

    void setRelationLabel(std::string_view label, size_t index);
protected:
    void updateOutputHeader() override;

    template <typename ResultType>
    void describeJoinActionsImpl(ResultType & result) const;

    DPJoinEntryPtr optimized_plan = nullptr;
    std::vector<JoinOperator> join_operators;
    ActionsDAGPtr expression_actions;

    std::vector<RelationStats> relation_stats;

    std::vector<UInt64> hash_table_key_hashes;

    bool use_nulls;
    NameSet required_output_columns;

    PreparedJoinStorage prepared_join_storage;

    JoinSettings join_settings;
    SortingStep::Settings sorting_settings;

    VolumePtr tmp_volume;
    TemporaryDataOnDiskScopePtr tmp_data;

    std::vector<Names> using_columns_mapping;

    LoggerPtr log = getLogger("JoinStepLogical");
};

}
