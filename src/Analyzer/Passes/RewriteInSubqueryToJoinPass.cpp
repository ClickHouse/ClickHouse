#include <Analyzer/Passes/RewriteInSubqueryToJoinPass.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>

#include <Core/Joins.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getLeastSupertype.h>

#include <Functions/logical.h>

#include <Interpreters/TableJoin.h>

#include <Storages/IStorage.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_in_subquery_to_join;
    extern const SettingsBool rewrite_in_to_join;
    extern const SettingsUInt64 max_rows_in_set;
    extern const SettingsUInt64 max_bytes_in_set;
    extern const SettingsJoinAlgorithm join_algorithm;
}

namespace
{

/// NULL never matches in a join, and top-level NULL keys behave identically on the IN path
/// (with `transform_null_in = 0` they are dropped from the set and evaluate to `negative`).
/// But a NULL *inside* a composite key is a regular part of the serialized set key and can match,
/// while join equality on such values returns NULL and never matches. Variant/Dynamic/Object can
/// hold NULLs the same way, so they are rejected too.
bool typeMayHoldNullsInside(const DataTypePtr & type)
{
    if (isNullableOrLowCardinalityNullable(type))
        return true;

    WhichDataType which(type);
    if (which.isVariant() || which.isDynamic() || which.isObject())
        return true;

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return typeMayHoldNullsInside(array_type->getNestedType());

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        for (const auto & element_type : tuple_type->getElements())
            if (typeMayHoldNullsInside(element_type))
                return true;
        return false;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        return typeMayHoldNullsInside(map_type->getNestedType());

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return typeMayHoldNullsInside(low_cardinality_type->getDictionaryType());

    return false;
}

bool findInTableExpression(const QueryTreeNodePtr & source, const QueryTreeNodePtr & table_expression)
{
    if (!source)
        return false;

    if (source->isEqual(*table_expression))
        return true;

    if (const auto * join_node = table_expression->as<JoinNode>())
    {
        return findInTableExpression(source, join_node->getLeftTableExpressionNode())
            || findInTableExpression(source, join_node->getRightTableExpressionNode());
    }

    if (const auto * cross_join_node = table_expression->as<CrossJoinNode>())
    {
        for (const auto & expression : cross_join_node->getTableExpressions())
            if (findInTableExpression(source, expression))
                return true;
        return false;
    }

    if (const auto * array_join_node = table_expression->as<ArrayJoinNode>())
        return findInTableExpression(source, array_join_node->getTableExpressionNode());

    return false;
}

/// Unlike `extractAllTableReferences`, also collects table functions: `remote(...)` and similar
/// must be caught by the remote guard.
bool joinTreesContainRemoteTable(const QueryTreeNodePtr & node)
{
    switch (node->getNodeType())
    {
        case QueryTreeNodeType::TABLE:
            return node->as<TableNode>()->getStorage()->isRemote();
        case QueryTreeNodeType::TABLE_FUNCTION:
            return node->as<TableFunctionNode>()->getStorage()->isRemote();
        case QueryTreeNodeType::QUERY:
        {
            const auto & join_tree = node->as<QueryNode>()->getJoinTreeNode();
            return join_tree && joinTreesContainRemoteTable(join_tree);
        }
        case QueryTreeNodeType::UNION:
        {
            for (const auto & union_query : node->as<UnionNode>()->getQueries().getNodes())
                if (joinTreesContainRemoteTable(union_query))
                    return true;
            return false;
        }
        case QueryTreeNodeType::ARRAY_JOIN:
            return joinTreesContainRemoteTable(node->as<ArrayJoinNode>()->getTableExpressionNode());
        case QueryTreeNodeType::CROSS_JOIN:
        {
            for (const auto & expression : node->as<CrossJoinNode>()->getTableExpressions())
                if (joinTreesContainRemoteTable(expression))
                    return true;
            return false;
        }
        case QueryTreeNodeType::JOIN:
        {
            const auto & join_node = node->as<JoinNode &>();
            return joinTreesContainRemoteTable(join_node.getLeftTableExpressionNode())
                || joinTreesContainRemoteTable(join_node.getRightTableExpressionNode());
        }
        default:
            return false;
    }
}

void extractConjuncts(const QueryTreeNodePtr & node, QueryTreeNodes & conjuncts)
{
    if (const auto * function_node = node->as<FunctionNode>();
        function_node && function_node->getFunctionName() == "and")
    {
        for (const auto & argument : function_node->getArguments().getNodes())
            extractConjuncts(argument, conjuncts);
        return;
    }

    conjuncts.push_back(node);
}

class RewriteInSubqueryToJoinVisitor : public InDepthQueryTreeVisitorWithContext<RewriteInSubqueryToJoinVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteInSubqueryToJoinVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        const auto & settings = getSettings();
        if (!settings[Setting::optimize_rewrite_in_subquery_to_join])
            return;

        /// The resolve-time `rewrite_in_to_join` rewrite has already turned IN into EXISTS by now,
        /// so there is nothing to match; be explicit about the precedence anyway.
        if (settings[Setting::rewrite_in_to_join])
            return;

        /// Explicit set limits are part of IN behavior: with `set_overflow_mode = 'break'` IN is
        /// defined to evaluate against a truncated set, and with 'throw' the query is defined to
        /// fail on a too-large subquery. A join honors neither, so keep the set path.
        if (settings[Setting::max_rows_in_set] != 0 || settings[Setting::max_bytes_in_set] != 0)
            return;

        /// LEFT SEMI/ANTI JOIN is not implemented by full_sorting_merge, and ANTI is not implemented
        /// by partial_merge; do not turn a working query into NOT_IMPLEMENTED.
        const auto & join_algorithms = settings[Setting::join_algorithm];
        if (!TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::HASH)
            && !TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::PARALLEL_HASH)
            && !TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::GRACE_HASH)
            && !TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::AUTO))
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node || !query_node->hasWhere() || query_node->isCorrelated())
            return;

        if (!query_node->getJoinTreeNode())
            return;

        QueryTreeNodes conjuncts;
        extractConjuncts(query_node->getWhere(), conjuncts);

        QueryTreeNodes remaining_conjuncts;
        bool any_rewritten = false;

        for (auto & conjunct : conjuncts)
        {
            if (tryRewrite(*query_node, conjunct))
                any_rewritten = true;
            else
                remaining_conjuncts.push_back(conjunct);
        }

        if (any_rewritten)
        {
            query_node->getWhere() = makeConjunction(remaining_conjuncts);
            performed_rewrite = true;
        }
    }

    bool performedRewrite() const { return performed_rewrite; }

private:
    bool tryRewrite(QueryNode & query_node, const QueryTreeNodePtr & conjunct)
    {
        auto * in_function = conjunct->as<FunctionNode>();
        if (!in_function)
            return false;

        /// Matching only `in`/`notIn` excludes `globalIn`/`globalNotIn` (external-table broadcast in
        /// distributed queries), `nullIn`/`notNullIn` (what `transform_null_in = 1` produces during
        /// resolution; their NULL semantics differ from a join) and the `*IgnoreSet` variants.
        const auto & function_name = in_function->getFunctionName();
        const bool is_not_in = function_name == "notIn";
        if (!is_not_in && function_name != "in")
            return false;

        const auto & in_arguments = in_function->getArguments().getNodes();
        if (in_arguments.size() != 2)
            return false;

        const auto & left_argument = in_arguments[0];
        const auto & right_argument = in_arguments[1];

        if (left_argument->as<ConstantNode>())
            return false;

        auto * subquery_node = right_argument->as<QueryNode>();
        if (!subquery_node)
            return false;

        /// A CTE body may be shared between multiple uses; renaming its projection in place is unsafe.
        if (subquery_node->isCTE() || subquery_node->isCorrelated() || containsCorrelatedSubquery(right_argument))
            return false;

        const auto & subquery_columns = subquery_node->getProjectionColumns();

        QueryTreeNodes left_keys;
        auto * left_tuple_function = left_argument->as<FunctionNode>();
        if (left_tuple_function && left_tuple_function->getFunctionName() == "tuple"
            && subquery_columns.size() > 1
            && left_tuple_function->getArguments().getNodes().size() == subquery_columns.size())
            left_keys = left_tuple_function->getArguments().getNodes();
        else if (subquery_columns.size() == 1)
            left_keys = {left_argument};
        else
            return false;

        if (!leftExpressionIsSafe(left_argument, query_node.getJoinTreeNode(), /*check_index_columns*/ !is_not_in))
            return false;

        for (size_t i = 0; i < left_keys.size(); ++i)
        {
            auto left_type = removeLowCardinality(left_keys[i]->getResultType());
            auto right_type = removeLowCardinality(subquery_columns[i].type);

            /// `notIn` evaluates to NULL for a NULL left key (the row is dropped by the filter),
            /// while ANTI JOIN keeps unmatched NULL-keyed rows. `in` and SEMI JOIN agree: both drop.
            if (is_not_in && left_type->isNullable())
                return false;

            /// An array on the right of IN would have been flattened during resolution when the left
            /// side is scalar; the remaining array-vs-array case is left to the set path.
            if (isArray(removeNullable(right_type)))
                return false;

            if (typeMayHoldNullsInside(removeNullable(left_type)) || typeMayHoldNullsInside(removeNullable(right_type)))
                return false;

            /// Join key type unification widens both sides losslessly, matching the semantics of the
            /// accurate-or-null cast the set path applies to the left keys (`Set::execute`). Without a
            /// common supertype the join would throw where IN works.
            if (!tryGetLeastSupertype(DataTypes{left_type, right_type}))
                return false;
        }

        /// GLOBAL IN is excluded by name above, but a plain IN over remote tables is subject to
        /// `distributed_product_mode` rules that a join does not replicate. Shard-local secondary
        /// queries still benefit: the pass runs on the shard again.
        if (joinTreesContainRemoteTable(right_argument) || joinTreesContainRemoteTable(query_node.getJoinTreeNode()))
            return false;

        rewriteToJoin(query_node, right_argument, *subquery_node, left_keys, is_not_in);
        return true;
    }

    void rewriteToJoin(
        QueryNode & query_node,
        const QueryTreeNodePtr & subquery,
        QueryNode & subquery_node,
        const QueryTreeNodes & left_keys,
        bool is_not_in)
    {
        ++rewrite_index;

        /// Rename the subquery projection to unique names to avoid collisions with columns of the
        /// outer scope (same technique as the `rewrite_in_to_join` EXISTS rewrite).
        auto subquery_columns = subquery_node.getProjectionColumns();
        Names unique_names;
        unique_names.reserve(subquery_columns.size());
        for (size_t i = 0; i < subquery_columns.size(); ++i)
            unique_names.push_back(fmt::format("__in_join_subquery_column_{}_{}", rewrite_index, i + 1));

        subquery_node.clearProjectionColumns();
        subquery_node.setProjectionAliasesToOverride(unique_names);
        subquery_node.resolveProjectionColumns(subquery_columns);

        QueryTreeNodes equalities;
        equalities.reserve(left_keys.size());
        for (size_t i = 0; i < left_keys.size(); ++i)
        {
            auto right_column_node = std::make_shared<ColumnNode>(
                NameAndTypePair{unique_names[i], subquery_columns[i].type},
                std::static_pointer_cast<ITableExpressionNode>(subquery));

            auto equals_node = std::make_shared<FunctionNode>("equals");
            equals_node->markAsOperator();
            equals_node->getArguments().getNodes() = {left_keys[i], std::move(right_column_node)};
            resolveOrdinaryFunctionNodeByName(*equals_node, "equals", getContext());
            equalities.push_back(std::move(equals_node));
        }

        auto join_node = std::make_shared<JoinNode>(
            query_node.getJoinTreeNode(),
            subquery,
            makeConjunction(equalities),
            JoinLocality::Unspecified,
            is_not_in ? JoinStrictness::Anti : JoinStrictness::Semi,
            JoinKind::Left,
            /*is_using_join_expression_*/ false);

        query_node.getJoinTreeNode() = std::move(join_node);
    }

    /// The left expression must be computable from the current join tree alone and evaluate
    /// identically when moved into a JOIN ON section. With `check_index_columns`, additionally
    /// require that none of its columns can drive index analysis on its table: `x IN (subquery)`
    /// over a primary-key/partition-key/skip-index column prunes parts and granules via
    /// `KeyCondition::tryPrepareSetIndex`, which a join (without runtime-filter index analysis)
    /// cannot replicate. Negative predicates do not prune, so `notIn` skips that check.
    bool leftExpressionIsSafe(const QueryTreeNodePtr & node, const QueryTreeNodePtr & join_tree, bool check_index_columns)
    {
        if (const auto * column_node = node->as<ColumnNode>())
        {
            auto source = column_node->getColumnSourceOrNull();
            if (!findInTableExpression(source, join_tree))
                return false;
            if (check_index_columns && columnMayDriveIndex(*column_node, source))
                return false;
            return true;
        }

        if (const auto * function_node = node->as<FunctionNode>())
        {
            if (function_node->getFunctionName() == "arrayJoin")
                return false;
            if (const auto & function = function_node->getFunction(); function && !function->isDeterministicInScopeOfQuery())
                return false;
            for (const auto & argument : function_node->getArguments().getNodes())
                if (!leftExpressionIsSafe(argument, join_tree, check_index_columns))
                    return false;
            return true;
        }

        if (node->as<ConstantNode>())
            return true;

        /// Lambdas, nested subqueries and anything else unexpected.
        return false;
    }

    static bool columnMayDriveIndex(const ColumnNode & column_node, const QueryTreeNodePtr & source)
    {
        const auto * table_node = source ? source->as<TableNode>() : nullptr;
        if (!table_node)
            return false;

        const auto & snapshot = table_node->getStorageSnapshot();
        if (!snapshot || !snapshot->metadata)
            return false;

        const auto & metadata = *snapshot->metadata;
        const auto & column_name = column_node.getColumnName();

        auto contains = [&](const Names & names)
        {
            return std::find(names.begin(), names.end(), column_name) != names.end();
        };

        if (metadata.hasPrimaryKey() && contains(metadata.getColumnsRequiredForPrimaryKey()))
            return true;
        if (metadata.hasPartitionKey() && contains(metadata.getColumnsRequiredForPartitionKey()))
            return true;
        for (const auto & index : metadata.getSecondaryIndices())
            if (contains(index.column_names))
                return true;

        return false;
    }

    static QueryTreeNodePtr makeConjunction(const QueryTreeNodes & nodes)
    {
        if (nodes.empty())
            return nullptr;

        if (nodes.size() == 1)
            return nodes.front();

        auto function_node = std::make_shared<FunctionNode>("and");
        function_node->markAsOperator();
        for (const auto & node : nodes)
            function_node->getArguments().getNodes().push_back(node);

        const auto & function = createInternalFunctionAndOverloadResolver();
        function_node->resolveAsFunction(function->build(function_node->getArgumentColumns()));
        return function_node;
    }

    size_t rewrite_index = 0;
    bool performed_rewrite = false;
};

}

void RewriteInSubqueryToJoinPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteInSubqueryToJoinVisitor visitor(context);
    visitor.visit(query_tree_node);

    /// Unique `__tableN` aliases were assigned at the end of query analysis, with each IN subquery
    /// numbered in its own scope. Moving a subquery into the join tree merges the scopes and can
    /// produce duplicate aliases (and thus duplicate planner column identifiers), so renumber.
    if (visitor.performedRewrite())
        createUniqueAliasesIfNecessary(query_tree_node, context);
}

}
