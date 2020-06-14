#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/SubqueryForSet.h>
#include <Interpreters/InDepthNodeVisitor.h>


namespace DB
{

class Context;
class ASTFunction;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

 /// The case of an explicit enumeration of values.
SetPtr makeExplicitSet(
    const ASTFunction * node, const Block & sample_block, bool create_ordered_set,
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

class ASTIdentifier;
class ASTFunction;
class ASTLiteral;

/// Collect ExpressionAction from AST. Returns PreparedSets and SubqueriesForSets too.
class ActionsMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<ActionsMatcher, true>;

    struct Data
    {
        const Context & context;
        SizeLimits set_size_limit;
        size_t subquery_depth;
        const NamesAndTypesList & source_columns;
        PreparedSets & prepared_sets;
        SubqueriesForSets & subqueries_for_sets;
        bool no_subqueries;
        bool no_makeset;
        bool only_consts;
        bool no_storage_or_local;
        size_t visit_depth;
        ScopeStack actions_stack;

        /*
         * Remember the last unique column suffix to avoid quadratic behavior
         * when we add lots of column with same prefix. One counter for all
         * prefixes is good enough.
         */
        int next_unique_suffix;

        Data(const Context & context_, SizeLimits set_size_limit_, size_t subquery_depth_,
                const NamesAndTypesList & source_columns_, const ExpressionActionsPtr & actions,
                PreparedSets & prepared_sets_, SubqueriesForSets & subqueries_for_sets_,
                bool no_subqueries_, bool no_makeset_, bool only_consts_, bool no_storage_or_local_)
        :   context(context_),
            set_size_limit(set_size_limit_),
            subquery_depth(subquery_depth_),
            source_columns(source_columns_),
            prepared_sets(prepared_sets_),
            subqueries_for_sets(subqueries_for_sets_),
            no_subqueries(no_subqueries_),
            no_makeset(no_makeset_),
            only_consts(only_consts_),
            no_storage_or_local(no_storage_or_local_),
            visit_depth(0),
            actions_stack(actions, context),
            next_unique_suffix(actions_stack.getSampleBlock().columns() + 1)
        {}

        void updateActions(ExpressionActionsPtr & actions)
        {
            actions = actions_stack.popLevel();
        }

        void addAction(const ExpressionAction & action)
        {
            actions_stack.addAction(action);
        }

        const Block & getSampleBlock() const
        {
            return actions_stack.getSampleBlock();
        }

        /// Does result of the calculation already exists in the block.
        bool hasColumn(const String & columnName) const
        {
            return actions_stack.getSampleBlock().has(columnName);
        }

        /*
         * Generate a column name that is not present in the sample block, using
         * the given prefix and an optional numeric suffix.
         */
        String getUniqueName(const String & prefix)
        {
            const auto & block = getSampleBlock();
            auto result = prefix;

            // First, try the name without any suffix, because it is currently
            // used both as a display name and a column id.
            while (block.has(result))
            {
                result = prefix + "_" + toString(next_unique_suffix);
                ++next_unique_suffix;
            }

            return result;
        }
    };

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);

private:

    static void visit(const ASTIdentifier & identifier, const ASTPtr & ast, Data & data);
    static void visit(const ASTFunction & node, const ASTPtr & ast, Data & data);
    static void visit(const ASTLiteral & literal, const ASTPtr & ast, Data & data);

    static SetPtr makeSet(const ASTFunction & node, Data & data, bool no_subqueries);
};

using ActionsVisitor = ActionsMatcher::Visitor;

}
