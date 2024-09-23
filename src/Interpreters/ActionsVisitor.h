#pragma once

#include <deque>
#include <string_view>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/PreparedSets.h>
#include <Parsers/IAST.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

class ASTExpressionList;
class ASTFunction;

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

/// The case of an explicit enumeration of values.
FutureSetPtr makeExplicitSet(
    const ASTFunction * node, const ActionsDAG & actions, ContextPtr context, PreparedSets & prepared_sets);

/** For ActionsVisitor
  * A stack of ActionsDAG corresponding to nested lambda expressions.
  * The new action should be added to the highest possible level.
  * For example, in the expression "select arrayMap(x -> x + column1 * column2, array1)"
  *  calculation of the product must be done outside the lambda expression (it does not depend on x),
  *  and the calculation of the sum is inside (depends on x).
  */
struct ScopeStack : WithContext
{
    class Index;
    using IndexPtr = std::unique_ptr<Index>;

    struct Level
    {
        ActionsDAG actions_dag;
        IndexPtr index;
        NameSet inputs;

        ~Level();
        Level();
        Level(Level &&) noexcept;
    };

    using Levels = std::deque<Level>;

    Levels stack;

    ScopeStack(ActionsDAG actions_dag, ContextPtr context_);

    void pushLevel(const NamesAndTypesList & input_columns);

    size_t getColumnLevel(const std::string & name);

    void addColumn(ColumnWithTypeAndName column);
    void addAlias(const std::string & name, std::string alias);
    void addArrayJoin(const std::string & source_name, std::string result_name);
    void addFunction(const FunctionOverloadResolverPtr & function, const Names & argument_names, std::string result_name);

    ActionsDAG popLevel();

    const ActionsDAG & getLastActions() const;
    const Index & getLastActionsIndex() const;
    std::string dumpNames() const;
};

class ASTIdentifier;
class ASTFunction;
class ASTLiteral;

enum class GroupByKind : uint8_t
{
    NONE,
    ORDINARY,
    ROLLUP,
    CUBE,
    GROUPING_SETS,
};

/*
 * This class stores information about aggregation keys used in GROUP BY clause.
 * It's used for providing information about aggregation to GROUPING function
 * implementation.
*/
struct AggregationKeysInfo
{
    AggregationKeysInfo(
        std::reference_wrapper<const NamesAndTypesList> aggregation_keys_,
        std::reference_wrapper<const ColumnNumbersList> grouping_set_keys_,
        GroupByKind group_by_kind_)
        : aggregation_keys(aggregation_keys_)
        , grouping_set_keys(grouping_set_keys_)
        , group_by_kind(group_by_kind_)
    {}

    AggregationKeysInfo(const AggregationKeysInfo &) = default;
    AggregationKeysInfo(AggregationKeysInfo &&) = default;

    // Names and types of all used keys
    const NamesAndTypesList & aggregation_keys;
    // Indexes of aggregation keys used in each grouping set (only for GROUP BY GROUPING SETS)
    const ColumnNumbersList & grouping_set_keys;

    GroupByKind group_by_kind;
};

/// Collect ExpressionAction from AST. Returns PreparedSets
class ActionsMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<ActionsMatcher, true>;

    struct Data : public WithContext
    {
        SizeLimits set_size_limit;
        size_t subquery_depth;
        const NamesAndTypesList & source_columns;
        PreparedSetsPtr prepared_sets;
        bool no_subqueries;
        bool no_makeset;
        bool only_consts;
        size_t visit_depth;
        ScopeStack actions_stack;
        AggregationKeysInfo aggregation_keys_info;
        bool build_expression_with_window_functions;
        bool is_create_parameterized_view;

        /*
         * Remember the last unique column suffix to avoid quadratic behavior
         * when we add lots of column with same prefix. One counter for all
         * prefixes is good enough.
         */
        size_t next_unique_suffix;

        Data(
            ContextPtr context_,
            SizeLimits set_size_limit_,
            size_t subquery_depth_,
            std::reference_wrapper<const NamesAndTypesList> source_columns_,
            ActionsDAG actions_dag,
            PreparedSetsPtr prepared_sets_,
            bool no_subqueries_,
            bool no_makeset_,
            bool only_consts_,
            AggregationKeysInfo aggregation_keys_info_,
            bool build_expression_with_window_functions_ = false,
            bool is_create_parameterized_view_ = false);

        /// Does result of the calculation already exists in the block.
        bool hasColumn(const String & column_name) const;
        std::vector<std::string_view> getAllColumnNames() const;

        void addColumn(ColumnWithTypeAndName column)
        {
            actions_stack.addColumn(std::move(column));
        }

        void addAlias(const std::string & name, std::string alias)
        {
            actions_stack.addAlias(name, std::move(alias));
        }

        void addArrayJoin(const std::string & source_name, std::string result_name)
        {
            actions_stack.addArrayJoin(source_name, std::move(result_name));
        }

        void addFunction(const FunctionOverloadResolverPtr & function,
                         const Names & argument_names,
                         std::string result_name)
        {
            actions_stack.addFunction(function, argument_names, std::move(result_name));
        }

        ActionsDAG getActions()
        {
            return actions_stack.popLevel();
        }

        /*
         * Generate a column name that is not present in the sample block, using
         * the given prefix and an optional numeric suffix.
         */
        String getUniqueName(const String & prefix)
        {
            auto result = prefix;

            // First, try the name without any suffix, because it is currently
            // used both as a display name and a column id.
            while (hasColumn(result))
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
    static void visit(ASTExpressionList & expression_list, const ASTPtr & ast, Data & data);

    static FutureSetPtr makeSet(const ASTFunction & node, Data & data, bool no_subqueries);
    static ASTs doUntuple(const ASTFunction * function, ActionsMatcher::Data & data);
    static std::optional<NameAndTypePair> getNameAndTypeFromAST(const ASTPtr & ast, Data & data);
};

using ActionsVisitor = ActionsMatcher::Visitor;

}
