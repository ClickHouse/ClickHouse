#pragma once

#include <Interpreters/JoinInfo.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/SortingStep.h>

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
    const SortingStep::Settings & getSortingSettings() const { return sorting_settings; }
    const JoinSettings & getJoinSettings() const { return join_settings; }

    const Names & getRequiredOutputColumns() const { return required_output_columns; }

    JoinOperator & addInput(JoinOperator join_operator, const Header & header);

    std::optional<ActionsDAG> getFilterActions(JoinTableSide side, String & filter_column_name);

    void setSwapInputs() { swap_inputs = true; }
    bool areInputsSwapped() const { return swap_inputs; }

    struct PhysicalJoinNode
    {
        ActionsDAGPtr actions;
        JoinActionRef filter{nullptr};

        JoinPtr join_strategy;

        BaseRelsSet left_child;
        BaseRelsSet right_child;
    };

    struct PhysicalJoinTree
    {
        BaseRelsSet root;
        std::unordered_map<BaseRelsSet, PhysicalJoinNode> nodes;
    };

    PhysicalJoinTree convertToPhysical(
        bool is_explain_logical,
        UInt64 max_threads,
        UInt64 max_entries_for_hash_table_stats,
        String initial_query_id,
        std::chrono::milliseconds lock_acquire_timeout,
        const ExpressionActionsSettings & actions_settings);


    const JoinSettings & getSettings() const { return join_settings; }
    bool useNulls() const { return use_nulls; }

<<<<<<< HEAD
    void appendRequiredOutputsToActions(JoinActionRef & post_filter);

    void setHashTableCacheKeys(UInt64 left_key_hash, UInt64 right_key_hash)
    {
        hash_table_key_hash_left = left_key_hash;
        hash_table_key_hash_right = right_key_hash;
    }
=======

    void setHashTableCacheKey(UInt64 hash_table_key_hash_, size_t idx);
>>>>>>> 06fc8532418 (wip join flatten)

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

protected:
    void updateOutputHeader() override;

    template <typename ResultType>
    void describeJoinActionsImpl(ResultType & result) const;

    std::vector<JoinOperator> join_operators;

    std::vector<UInt64> hash_table_key_hashes;

    std::optional<UInt64> hash_table_key_hash_left;
    std::optional<UInt64> hash_table_key_hash_right;

    bool use_nulls;
    bool swap_inputs = false;
    NameSet required_output_columns;

    PreparedJoinStorage prepared_join_storage;

    JoinSettings join_settings;
    SortingStep::Settings sorting_settings;

    VolumePtr tmp_volume;
    TemporaryDataOnDiskScopePtr tmp_data;

    /// Add some information from convertToPhysical to description in explain output.
    std::vector<std::pair<String, String>> runtime_info_description;

    std::vector<Names> using_columns_mapping;
};

}
