#pragma once

#include <Parsers/StringRange.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

class Context;
class ASTFunction;


class Set;
using SetPtr = std::shared_ptr<Set>;
/// Will compare sets by their position in query string. It's possible because IAST::clone() doesn't chane IAST::range.
/// It should be taken into account when we want to change AST part which contains sets.
using PreparedSets = std::unordered_map<StringRange, SetPtr, StringRangePointersHash, StringRangePointersEqualTo>;

class Join;
using JoinPtr = std::shared_ptr<Join>;

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
    /// Rename column from joined block from this list.
    NamesWithAliases joined_block_aliases;

    /// If set, put the result into the table.
    /// This is a temporary table for transferring to remote servers for distributed query processing.
    StoragePtr table;
};

/// ID of subquery -> what to do with it.
using SubqueriesForSets = std::unordered_map<String, SubqueryForSet>;


/// The case of an explicit enumeration of values.
void makeExplicitSet(const ASTFunction * node, const Block & sample_block, bool create_ordered_set,
                     const Context & context, const SizeLimits & limits, PreparedSets & prepared_sets);


/** For ActionsVisitor
  * A stack of ExpressionActions corresponding to nested lambda expressions.
  * The new action should be added to the highest possible level.
  * For example, in the expression "select arrayMap(x -> x + column1 * column2, array1)"
  *  calculation of the product must be done outside the lambda expression (it does not depend on x),
  *  and the calculation of the sum is inside (depends on x).
  */
struct ScopeStack
{
    struct Level
    {
        ExpressionActionsPtr actions;
        NameSet new_columns;
    };

    using Levels = std::vector<Level>;

    Levels stack;

    const Context & context;

    ScopeStack(const ExpressionActionsPtr & actions, const Context & context_);

    void pushLevel(const NamesAndTypesList & input_columns);

    size_t getColumnLevel(const std::string & name);

    void addAction(const ExpressionAction & action);

    ExpressionActionsPtr popLevel();

    const Block & getSampleBlock() const;
};


/// Collect ExpressionAction from AST. Returns PreparedSets and SubqueriesForSets too.
/// After AST is visited source ExpressionActions should be updated with popActionsLevel() method.
class ActionsVisitor
{
public:
    ActionsVisitor(const Context & context_, SizeLimits set_size_limit_, size_t subquery_depth_,
                   const NamesAndTypesList & source_columns_, const ExpressionActionsPtr & actions,
                   PreparedSets & prepared_sets_, SubqueriesForSets & subqueries_for_sets_,
                   bool no_subqueries_, bool only_consts_, bool no_storage_or_local_, std::ostream * ostr_ = nullptr);

    void visit(const ASTPtr & ast);

    ExpressionActionsPtr popActionsLevel() { return actions_stack.popLevel(); }

private:
    const Context & context;
    SizeLimits set_size_limit;
    size_t subquery_depth;
    const NamesAndTypesList & source_columns;
    PreparedSets & prepared_sets;
    SubqueriesForSets & subqueries_for_sets;
    const bool no_subqueries;
    const bool only_consts;
    const bool no_storage_or_local;
    mutable size_t visit_depth;
    std::ostream * ostr;
    ScopeStack actions_stack;

    void makeSet(const ASTFunction * node, const Block & sample_block);
};

}
