#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MutationCommands.h>


namespace DB
{

class Context;

/// Return false if the data isn't going to be changed by mutations.
bool isStorageTouchedByMutations(StoragePtr storage, const std::vector<MutationCommand> & commands, Context context_copy);

/// Create an input stream that will read data from storage and apply mutation commands (UPDATEs, DELETEs, MATERIALIZEs)
/// to this data.
class MutationsInterpreter
{
public:
    /// Storage to mutate, array of mutations commands and context. If you really want to execute mutation
    /// use can_execute = true, in other cases (validation, amount of commands) it can be false
    MutationsInterpreter(StoragePtr storage_, std::vector<MutationCommand> commands_, const Context & context_, bool can_execute_);

    void validate(TableStructureReadLockHolder & table_lock_holder);

    size_t evaluateCommandsSize();

    /// The resulting stream will return blocks containing only changed columns and columns, that we need to recalculate indices.
    BlockInputStreamPtr execute(TableStructureReadLockHolder & table_lock_holder);

    /// Only changed columns.
    const Block & getUpdatedHeader() const;

private:
    ASTPtr prepare(bool dry_run);

    struct Stage;

    ASTPtr prepareInterpreterSelectQuery(std::vector<Stage> &prepared_stages, bool dry_run);
    BlockInputStreamPtr addStreamsForLaterStages(const std::vector<Stage> & prepared_stages, BlockInputStreamPtr in) const;

    StoragePtr storage;
    std::vector<MutationCommand> commands;
    const Context & context;
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
    /// First stage is special: it can contain only DELETEs and is executed using InterpreterSelectQuery
    /// to take advantage of table indexes (if there are any).

    struct Stage
    {
        Stage(const Context & context_) : expressions_chain(context_) {}

        ASTs filters;
        std::unordered_map<String, ASTPtr> column_to_updated;

        /// Contains columns that are changed by this stage,
        /// columns changed by the previous stages and also columns needed by the next stages.
        NameSet output_columns;

        std::unique_ptr<ExpressionAnalyzer> analyzer;

        /// A chain of actions needed to execute this stage.
        /// First steps calculate filter columns for DELETEs (in the same order as in `filter_column_names`),
        /// then there is (possibly) an UPDATE stage, and finally a projection stage.
        ExpressionActionsChain expressions_chain;
        Names filter_column_names;
    };

    std::unique_ptr<Block> updated_header;
    std::vector<Stage> stages;
    bool is_prepared = false; /// Has the sequence of stages been prepared.
};

}
