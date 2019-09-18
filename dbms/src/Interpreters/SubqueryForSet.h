#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

class Join;
using JoinPtr = std::shared_ptr<Join>;

class InterpreterSelectWithUnionQuery;


/// Information on what to do when executing a subquery in the [GLOBAL] IN/JOIN section.
struct SubqueryForSet
{
    /// The source is obtained using the InterpreterSelectQuery subquery.
    BlockInputStreamPtr source;

    /// If set, build it from result.
    SetPtr set;
    JoinPtr join;
    /// Apply this actions to joined block.
    ExpressionActionsPtr joined_block_actions;

    /// If set, put the result into the table.
    /// This is a temporary table for transferring to remote servers for distributed query processing.
    StoragePtr table;

    void makeSource(std::shared_ptr<InterpreterSelectWithUnionQuery> & interpreter,
                    NamesWithAliases && joined_block_aliases_);

    Block renamedSampleBlock() const { return sample_block; }
    void renameColumns(Block & block);

private:
    NamesWithAliases joined_block_aliases; /// Rename column from joined block from this list.
    Block sample_block; /// source->getHeader() + column renames
};

/// ID of subquery -> what to do with it.
using SubqueriesForSets = std::unordered_map<String, SubqueryForSet>;

}
