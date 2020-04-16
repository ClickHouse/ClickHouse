#pragma once

#include <Core/Block.h>
#include <Storages/IStorage_fwd.h>
#include <Parsers/IAST.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/PreparedSets.h>


namespace DB
{

class InterpreterSelectWithUnionQuery;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

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
    Block sample_block; /// source->getHeader() + column renames

    /// If set, put the result into the table.
    /// This is a temporary table for transferring to remote servers for distributed query processing.
    StoragePtr table;

    void makeSource(std::shared_ptr<InterpreterSelectWithUnionQuery> & interpreter,
                    NamesWithAliases && joined_block_aliases_);

    void setJoinActions(ExpressionActionsPtr actions);

    bool insertJoinedBlock(Block & block);
    void setTotals();

private:
    NamesWithAliases joined_block_aliases; /// Rename column from joined block from this list.

    /// Rename source right table column names into qualified column names if they conflicts with left table ones.
    void renameColumns(Block & block);
};

/// ID of subquery -> what to do with it.
using SubqueriesForSets = std::unordered_map<String, SubqueryForSet>;

}
