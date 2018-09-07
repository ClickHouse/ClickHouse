#pragma once

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/IStorage.h>
#include <Storages/MutationCommands.h>


namespace DB
{

class Context;

/// Create an input stream that will read data from storage and apply mutation commands (UPDATEs, DELETEs)
/// to this data.
class MutationsInterpreter
{
public:
    MutationsInterpreter(StoragePtr storage_, std::vector<MutationCommand> commands_, const Context & context_)
        : storage(std::move(storage_))
        , commands(std::move(commands_))
        , context(context_)
    {
    }

    void validate();

    /// Return false if the data isn't going to be changed by mutations.
    bool isStorageTouchedByMutations() const;

    /// The resulting stream will return blocks containing changed columns only.
    BlockInputStreamPtr execute();

private:
    void prepare(bool dry_run);

    BlockInputStreamPtr addStreamsForLaterStages(BlockInputStreamPtr in) const;

private:
    StoragePtr storage;
    std::vector<MutationCommand> commands;
    const Context & context;

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
        Stage(const Context & context) : expressions_chain(context) {}

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

    std::unique_ptr<InterpreterSelectQuery> interpreter_select;
    std::vector<Stage> stages;
    bool is_prepared = false; /// Has the sequence of stages been prepared.
};

}
