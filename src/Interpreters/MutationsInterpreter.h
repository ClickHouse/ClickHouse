#pragma once

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MutationCommands.h>


namespace DB
{

class Context;
class QueryPlan;

class QueryPipelineBuilder;
using QueryPipelineBuilderPtr = std::unique_ptr<QueryPipelineBuilder>;

/// Return false if the data isn't going to be changed by mutations.
bool isStorageTouchedByMutations(
    MergeTreeData::DataPartPtr source_part,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MutationCommand> & commands,
    ContextPtr context
);

ASTPtr getPartitionAndPredicateExpressionForMutationCommand(
    const MutationCommand & command,
    const StoragePtr & storage,
    ContextPtr context
);

MutationCommand createCommandToApplyDeletedMask(const MutationCommand & command);

/// Create an input stream that will read data from storage and apply mutation commands (UPDATEs, DELETEs, MATERIALIZEs)
/// to this data.
class MutationsInterpreter
{
private:
    struct Stage;

public:
    struct Settings
    {
        explicit Settings(bool can_execute_) : can_execute(can_execute_) {}

        /// If false only analyze mutation expressions.
        bool can_execute = false;
        /// Whether all columns should be returned, not just updated
        bool return_all_columns = false;
        /// Whether we should return mutated or all existing rows
        bool return_mutated_rows = false;
        /// Where we should filter deleted rows by lightweight DELETE.
        bool apply_deleted_mask = true;
        /// Where we should recalculate skip indexes, TTL expressions, etc. that depend on updated columns.
        bool recalculate_dependencies_of_updated_columns = true;
    };

    /// Storage to mutate, array of mutations commands and context. If you really want to execute mutation
    /// use can_execute = true, in other cases (validation, amount of commands) it can be false
    MutationsInterpreter(
        StoragePtr storage_,
        StorageMetadataPtr metadata_snapshot_,
        MutationCommands commands_,
        ContextPtr context_,
        Settings settings_);

    /// Special case for *MergeTree
    MutationsInterpreter(
        MergeTreeData & storage_,
        MergeTreeData::DataPartPtr source_part_,
        AlterConversionsPtr alter_conversions_,
        StorageMetadataPtr metadata_snapshot_,
        MutationCommands commands_,
        Names available_columns_,
        ContextPtr context_,
        Settings settings_);

    void validate();
    size_t evaluateCommandsSize();

    /// The resulting stream will return blocks containing only changed columns and columns, that we need to recalculate indices.
    QueryPipelineBuilder execute();

    /// Only changed columns.
    Block getUpdatedHeader() const;

    const ColumnDependencies & getColumnDependencies() const;

    /// Latest mutation stage affects all columns in storage
    bool isAffectingAllColumns() const;

    NameSet grabMaterializedIndices() { return std::move(materialized_indices); }

    NameSet grabMaterializedStatistics() { return std::move(materialized_statistics); }

    NameSet grabMaterializedProjections() { return std::move(materialized_projections); }

    struct MutationKind
    {
        enum MutationKindEnum
        {
            MUTATE_UNKNOWN,
            MUTATE_INDEX_STATISTICS_PROJECTION,
            MUTATE_OTHER,
        } mutation_kind = MUTATE_UNKNOWN;

        void set(const MutationKindEnum & kind);
    };

    MutationKind::MutationKindEnum getMutationKind() const { return mutation_kind.mutation_kind; }

    /// Internal class which represents a data part for MergeTree
    /// or just storage for other storages.
    /// The main idea is to create a dedicated reading from MergeTree part.
    /// Additionally we propagate some storage properties.
    struct Source
    {
        StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & snapshot_, const ContextPtr & context_) const;
        StoragePtr getStorage() const;
        const MergeTreeData * getMergeTreeData() const;

        bool supportsLightweightDelete() const;
        bool hasLightweightDeleteMask() const;
        bool materializeTTLRecalculateOnly() const;
        bool hasSecondaryIndex(const String & name) const;
        bool hasProjection(const String & name) const;
        bool hasBrokenProjection(const String & name) const;
        bool isCompactPart() const;

        void read(
            Stage & first_stage,
            QueryPlan & plan,
            const StorageMetadataPtr & snapshot_,
            const ContextPtr & context_,
            bool apply_deleted_mask_,
            bool can_execute_) const;

        explicit Source(StoragePtr storage_);
        Source(MergeTreeData & storage_, MergeTreeData::DataPartPtr source_part_, AlterConversionsPtr alter_conversions_);

    private:
        StoragePtr storage;

        /// Special case for *MergeTree.
        MergeTreeData * data = nullptr;
        MergeTreeData::DataPartPtr part;
        AlterConversionsPtr alter_conversions;
    };

private:
    MutationsInterpreter(
        Source source_,
        StorageMetadataPtr metadata_snapshot_,
        MutationCommands commands_,
        Names available_columns_,
        ContextPtr context_,
        Settings settings_);

    void prepare(bool dry_run);

    void initQueryPlan(Stage & first_stage, QueryPlan & query_plan);
    void prepareMutationStages(std::vector<Stage> &prepared_stages, bool dry_run);
    QueryPipelineBuilder addStreamsForLaterStages(const std::vector<Stage> & prepared_stages, QueryPlan & plan) const;

    std::optional<SortDescription> getStorageSortDescriptionIfPossible(const Block & header) const;

    ASTPtr getPartitionAndPredicateExpressionForMutationCommand(const MutationCommand & command) const;

    Source source;
    StorageMetadataPtr metadata_snapshot;
    MutationCommands commands;
    Names available_columns;
    ContextPtr context;
    Settings settings;
    SelectQueryOptions select_limits;

    LoggerPtr logger;

    /// A sequence of mutation commands is executed as a sequence of stages. Each stage consists of several
    /// filters, followed by updating values of some columns. Commands can reuse expressions calculated by the
    /// previous commands in the same stage, but at the end of each stage intermediate columns are thrown away
    /// (they may contain wrong values because the column values have been updated).
    ///
    /// If an UPDATE command changes some columns that some MATERIALIZED columns depend on, a stage to
    /// recalculate these columns is added.
    ///
    /// Each stage has output_columns that contain columns that are changed at the end of that stage
    /// plus columns needed for the next mutations.
    ///
    /// First stage is special: it can contain only filters and is executed using InterpreterSelectQuery
    /// to take advantage of table indexes (if there are any). It's necessary because all mutations have
    /// `WHERE clause` part.

    struct Stage
    {
        explicit Stage(ContextPtr context_) : expressions_chain(context_) {}

        ASTs filters;
        std::unordered_map<String, ASTPtr> column_to_updated;

        /// Contains columns that are changed by this stage, columns changed by
        /// the previous stages and also columns needed by the next stages.
        NameSet output_columns;

        std::unique_ptr<ExpressionAnalyzer> analyzer;

        /// A chain of actions needed to execute this stage.
        /// First steps calculate filter columns for DELETEs (in the same order as in `filter_column_names`),
        /// then there is (possibly) an UPDATE step, and finally a projection step.
        ExpressionActionsChain expressions_chain;
        Names filter_column_names;

        /// Check that stage affects all storage columns
        bool isAffectingAllColumns(const Names & storage_columns) const;
    };

    std::unique_ptr<Block> updated_header;
    std::vector<Stage> stages;
    bool is_prepared = false; /// Has the sequence of stages been prepared.
    bool deleted_mask_updated = false;

    NameSet materialized_indices;
    NameSet materialized_projections;
    NameSet materialized_statistics;

    MutationKind mutation_kind; /// Do we meet any index or projection mutation.

    /// Columns, that we need to read for calculation of skip indices, projections or TTL expressions.
    ColumnDependencies dependencies;
};

}
