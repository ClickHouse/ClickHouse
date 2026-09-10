#pragma once

#include <memory>
#include <Core/Defines.h>
#include <optional>
#include <string_view>
#include <utility>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>

namespace DB
{

class StorageJoin;
class IKeyValueEntity;
struct JoinAlgorithmParams;
struct StorageID;

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

struct JoinWire;

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

    std::vector<JoinActionRef> getInputActions() const;
    std::vector<JoinActionRef> getOutputActions() const;

    std::pair<JoinExpressionActions, JoinOperator> detachExpressions()
    {
        return {std::move(expression_actions), std::move(join_operator)};
    }

    const JoinSettings & getSettings() const { return join_settings; }

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `JoinStepLogical.cpp` declares.
    JoinWire toWire() const;
    static QueryPlanStepPtr fromWire(JoinWire wire, Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override
    {
        return expression_actions.getActionsDAG()->hasCorrelatedColumns();
    }

    void addConditions(ActionsDAG actions_dag);

    /// Extract the part of the JOIN ON expression that can be evaluated on `side` alone, to be applied
    /// as a filter on that input.
    std::optional<ActionsDAG::ActionsForFilterPushDown> getFilterActions(
        JoinTableSide side, const SharedHeader & left_header, const SharedHeader & right_header);

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
    bool hasImpreciseEstimate() const { return imprecise_estimate; }
    const std::unordered_map<String, ColumnStats> & getResultColumnStats() const { return result_column_stats; }
    std::optional<UInt64> getInputRowsEstimation(JoinTableSide side) const;

    void setOptimized(
        std::optional<UInt64> estimated_rows_ = {},
        std::unordered_map<String, ColumnStats> column_stats_ = {},
        bool imprecise_estimate_ = false)
    {
        optimized = true;
        result_rows_estimation = estimated_rows_;
        result_column_stats = std::move(column_stats_);
        imprecise_estimate = imprecise_estimate_;
    }

    void setInputLabels(String left_table_label_, String right_table_label_)
    {
        left_relation = RelationEstimateInfo{.name = std::move(left_table_label_)};
        right_relation = RelationEstimateInfo{.name = std::move(right_table_label_)};
    }

    void setInputRelations(RelationEstimateInfo left_relation_, RelationEstimateInfo right_relation_)
    {
        left_relation = std::move(left_relation_);
        right_relation = std::move(right_relation_);
    }

    std::pair<std::reference_wrapper<const String>, std::reference_wrapper<const String>> getInputLabels() const
    {
        return {std::cref(left_relation.name), std::cref(right_relation.name)};
    }

    String getReadableRelationName() const;

    ActionsDAG::NodeRawConstPtrs getActionsAfterJoin() const { return actions_after_join; }

    std::string_view getTableStatsHint() const { return table_stats_hint; }
    void setTableStatsHint(String table_stats_hint_) { table_stats_hint = std::move(table_stats_hint_); }

    bool canRemoveUnusedColumns() const override;
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) override;
    bool canRemoveColumnsFromOutput() const override;

    bool isDisjunctionsOptimizationApplied() const { return disjunctions_optimization_applied; }
    void setDisjunctionsOptimizationApplied(bool v) { disjunctions_optimization_applied = v; }

    /// Swap left and right sides
    void swapInputs();

    UInt64 getRightHashTableCacheKey() const { return right_hash_table_cache_key; }
    void setRightHashTableCacheKey(UInt64 right_hash_table_cache_key_) { right_hash_table_cache_key = right_hash_table_cache_key_; }

    UInt64 getJoinOutputCacheKey() const { return join_output_cache_key; }
    void setJoinOutputCacheKey(UInt64 join_output_cache_key_) { join_output_cache_key = join_output_cache_key_; }

    const NameSet & notNullFiltersDerivedColumns(JoinTableSide side) const
    {
        return side == JoinTableSide::Left ? not_null_filters_derived_left : not_null_filters_derived_right;
    }

    void addNotNullFiltersDerivedColumns(JoinTableSide side, const NameSet & columns)
    {
        auto & derived = side == JoinTableSide::Left ? not_null_filters_derived_left : not_null_filters_derived_right;
        derived.insert(columns.begin(), columns.end());
    }

protected:
    SharedHeader calculateOutputHeader(const NameSet & required_output_columns_set) const;
    void updateOutputHeader() override;

    bool isDummyColumnOfThisStep(const ActionsDAG::Node * node) const;

    std::vector<std::pair<String, String>> describeJoinProperties() const;

    JoinExpressionActions expression_actions;
    JoinOperator join_operator;

    /// These are the nodes which are used to split expressions calculated before and after join
    /// Nodes from this list are used as inputs for ActionsDAG executed after join operation
    /// It can be input or node with toNullable function applied to input
    ActionsDAG::NodeRawConstPtrs actions_after_join = {};

    JoinSettings join_settings;
    SortingStep::Settings sorting_settings;

    /// Runtime info, do not serialize

    bool optimized = false;
    std::optional<UInt64> result_rows_estimation = {};
    std::unordered_map<String, ColumnStats> result_column_stats = {};

    /// True when the row count estimation used by join reordering was derived from the primary index
    /// rather than column statistics (because `use_statistics` is enabled but statistics are missing).
    bool imprecise_estimate = false;
    UInt64 right_hash_table_cache_key = 0;
    UInt64 join_output_cache_key = 0;

    RelationEstimateInfo left_relation;
    RelationEstimateInfo right_relation;

    /// Table statistics hint passed via query parameter, consumed by the Cascades optimizer.
    String table_stats_hint;


    std::unique_ptr<JoinAlgorithmParams> join_algorithm_params;
    VolumePtr tmp_volume;
    TemporaryDataOnDiskScopePtr tmp_data;

    /// Columns of each input for which an IS NOT NULL filter was already derived.
    NameSet not_null_filters_derived_left;
    NameSet not_null_filters_derived_right;

private:
    /// Streams below the framed format.
    void serializeSettingsLegacy(QueryPlanSerializationSettings & settings) const;
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);

    bool disjunctions_optimization_applied = false;
};


class JoinStepLogicalLookup final : public ISourceStep
{
public:
    JoinStepLogicalLookup(QueryPlan child_plan_, PreparedJoinStorage prepared_join_storage_, bool use_nulls_);

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override;
    String getName() const override { return "JoinStepLogicalLookup"; }

    QueryPlanRawPtrs getChildPlans() override;

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

/// Whether the IEJoin algorithm is preferred for this join: `ie_join` is listed first in
/// `join_algorithm` and the ON expression has two inequality conditions the operator can take.
/// For optimization passes that would otherwise claim the join for a hash-family algorithm
/// (e.g. runtime filters). The condition eligibility is the same one the conversion to the
/// physical step applies, so `true` means IEJoin takes the join unless the right side is a
/// prepared `Join` storage (which those passes exclude on their own).
bool isIEJoinPreferred(const JoinOperator & join_operator, const JoinSettings & join_settings);


/// The join expression DAG with the two things that refer into it by node id: the join operator
/// and the after-join node list. They go on the wire as one unit. Reading also builds the
/// expression actions around the DAG, because the operator's references bind to them.
struct JoinExpressionsWire
{
    std::shared_ptr<const ActionsDAG> actions_dag = std::make_shared<const ActionsDAG>();
    JoinOperator join_operator{};
    ActionsDAG::NodeRawConstPtrs actions_after_join;
    std::optional<JoinExpressionActions> expression_actions;
};

/// What `JoinStepLogical` puts on the wire in the framed format. The members from `join_algorithm`
/// on travel through the settings channel; they are the join settings and the sorting settings of a
/// merge join.
struct JoinWire
{
    JoinExpressionsWire join;

    std::vector<JoinAlgorithm> join_algorithm = {JoinAlgorithm::DIRECT, JoinAlgorithm::PARALLEL_HASH, JoinAlgorithm::HASH};
    UInt64 max_block_size = DEFAULT_BLOCK_SIZE;
    UInt64 max_rows_in_join = 0;
    UInt64 max_bytes_in_join = 0;
    UInt64 default_max_bytes_in_join = 1000000000;
    UInt64 max_joined_block_size_rows = DEFAULT_BLOCK_SIZE;
    UInt64 max_joined_block_size_bytes = 4 * 1024 * 1024;
    UInt64 min_joined_block_size_rows = DEFAULT_BLOCK_SIZE;
    UInt64 min_joined_block_size_bytes = 524288;
    bool joined_block_split_single_row = false;
    bool parallel_non_joined_rows_processing = true;
    OverflowMode join_overflow_mode = OverflowMode::THROW;
    bool join_any_take_last_row = false;
    UInt64 cross_join_min_rows_to_compress = 10000000;
    UInt64 cross_join_min_bytes_to_compress = 1024ULL * 1024 * 1024;
    UInt64 partial_merge_join_left_table_buffer_bytes = 0;
    UInt64 partial_merge_join_rows_in_right_blocks = 65536;
    UInt64 join_on_disk_max_files_to_merge = 64;
    UInt64 grace_hash_join_initial_buckets = 1;
    UInt64 grace_hash_join_max_buckets = 1024;
    UInt64 max_bytes_before_external_join = 0;
    Float64 max_bytes_ratio_before_external_join = 0;
    UInt64 max_rows_in_set_to_optimize_join = 0;
    String temporary_files_codec = "LZ4";
    UInt64 temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    bool collect_hash_table_stats_during_joins = true;
    UInt64 max_size_to_preallocate_for_joins = 1000000000000;
    UInt64 parallel_hash_join_threshold = 100000;
    UInt64 join_output_by_rowlist_perkey_rows_threshold = 5;
    bool allow_experimental_join_right_table_sorting = false;
    UInt64 join_to_sort_minimum_perkey_rows = 40;
    UInt64 join_to_sort_maximum_table_rows = 10000;
    bool allow_dynamic_type_in_join_keys = false;
    bool use_join_disjunctions_push_down = false;
    bool enable_lazy_columns_replication = false;
    bool enable_software_prefetch_in_join = true;
    bool use_hash_table_stats_for_join_reordering = false;
    bool enable_hash_join_row_store = true;
    Float64 min_rows_ratio_for_hash_join_row_store = 5.0;
    bool enable_join_fixed_hash_table_conversion = true;
    bool join_runtime_filter_from_fixed_hash_table = true;
    UInt64 max_rows_to_sort = 0;
    UInt64 max_bytes_to_sort = 0;
    OverflowMode sort_overflow_mode = OverflowMode::THROW;
    UInt64 max_bytes_before_remerge_sort = 1000000000;
    Float32 remerge_sort_lowered_memory_bytes_ratio = 2.0f;
    UInt64 max_bytes_before_external_sort = 0;
    Float64 max_bytes_ratio_before_external_sort = 0.5;
    UInt64 min_free_disk_space_for_temporary_data = 0;
    UInt64 prefer_external_sort_block_bytes = DEFAULT_BLOCK_SIZE * 256;
};

}
