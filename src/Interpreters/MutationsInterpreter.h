#pragma once

#include <DataStreams/IBlockInputStream.h>
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

class QueryPipeline;
using QueryPipelinePtr = std::unique_ptr<QueryPipeline>;

/// Return false if the data isn't going to be changed by mutations.
bool isStorageTouchedByMutations(
    const StoragePtr & storage,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MutationCommand> & commands,
    Context context_copy
);

ASTPtr getPartitionAndPredicateExpressionForMutationCommand(
    const MutationCommand & command,
    const StoragePtr & storage,
    const Context & context
);

/// Create an input stream that will read data from storage and apply mutation commands (UPDATEs, DELETEs, MATERIALIZEs)
/// to this data.
class MutationsInterpreter
{
public:
    /// Storage to mutate, array of mutations commands and context. If you really want to execute mutation
    /// use can_execute = true, in other cases (validation, amount of commands) it can be false
    MutationsInterpreter(
        StoragePtr storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        MutationCommands commands_,
        const Context & context_,
        bool can_execute_);

    void validate();

    size_t evaluateCommandsSize();

    /// The resulting stream will return blocks containing only changed columns and columns, that we need to recalculate indices.
    BlockInputStreamPtr execute();

    /// Only changed columns.
    const Block & getUpdatedHeader() const;

    /// Latest mutation stage affects all columns in storage
    bool isAffectingAllColumns() const;

private:
    ASTPtr prepare(bool dry_run);

    struct Stage;

    ASTPtr prepareInterpreterSelectQuery(std::vector<Stage> &prepared_stages, bool dry_run);
    QueryPipelinePtr addStreamsForLaterStages(const std::vector<Stage> & prepared_stages, QueryPlan & plan) const;

    std::optional<SortDescription> getStorageSortDescriptionIfPossible(const Block & header) const;

    ASTPtr getPartitionAndPredicateExpressionForMutationCommand(const MutationCommand & command) const;

    StoragePtr storage;
    StorageMetadataPtr metadata_snapshot;
    MutationCommands commands;
    Context context;
    bool can_execute;

    ASTPtr mutation_ast;

    /// We have to store interpreter because it use own copy of context
    /// and some streams from execute method may use it.
    std::unique_ptr<InterpreterSelectQuery> select_interpreter;

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
        Stage(const Context & context_) : expressions_chain(context_) {}

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
};

}
