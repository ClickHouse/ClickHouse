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
#include <Analyzer/Utils.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>

#include <Core/Joins.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/hasNullable.h>

#include <Interpreters/MergeJoin.h>

#include <Storages/IStorage.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_in_subquery_to_join;
    extern const SettingsUInt64 max_rows_in_set;
    extern const SettingsUInt64 max_bytes_in_set;
    extern const SettingsJoinAlgorithm join_algorithm;
}

namespace
{

bool findInTableExpression(const QueryTreeNodePtr & source, const QueryTreeNodePtr & table_expression)
{
    if (!source)
        return false;

    if (source == table_expression || source->isEqual(*table_expression))
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

/// `extractAllTableReferences` skips table functions, but `remote(...)` and similar must be
/// caught here, so use `extractTableExpressions` and inspect both node kinds.
bool containsRemoteTable(const QueryTreeNodePtr & node)
{
    auto table_expressions = extractTableExpressions(
        std::static_pointer_cast<ITableExpressionNode>(node), /*add_array_join*/ false, /*recursive*/ true);

    for (const auto & table_expression : table_expressions)
    {
        if (const auto * table_node = table_expression->as<TableNode>())
        {
            if (table_node->getStorage()->isRemote())
                return true;
        }
        else if (const auto * table_function_node = table_expression->as<TableFunctionNode>())
        {
            if (table_function_node->getStorage()->isRemote())
                return true;
        }
    }

    return false;
}

/// Whether some enabled join algorithm can execute LEFT JOIN with the given strictness;
/// rewriting must not turn a working IN into NOT_IMPLEMENTED. DIRECT is excluded because a
/// subquery right side never qualifies for a direct join.
bool joinAlgorithmsCanExecuteLeftJoin(const std::vector<JoinAlgorithm> & join_algorithms, JoinStrictness strictness)
{
    for (const auto & algorithm : join_algorithms)
    {
        switch (algorithm)
        {
            case JoinAlgorithm::DEFAULT:
            case JoinAlgorithm::AUTO:
            case JoinAlgorithm::HASH:
            case JoinAlgorithm::PARALLEL_HASH:
            case JoinAlgorithm::GRACE_HASH:
            case JoinAlgorithm::PREFER_PARTIAL_MERGE:
                return true;
            case JoinAlgorithm::PARTIAL_MERGE:
                if (MergeJoin::isSupported(JoinKind::Left, strictness))
                    return true;
                break;
            default:
                break;
        }
    }

    return false;
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
        auto * query_node = node->as<QueryNode>();
        if (!query_node || !query_node->hasWhere() || !query_node->getJoinTreeNode() || query_node->isCorrelated())
            return;

        const auto * where_function = query_node->getWhere()->as<FunctionNode>();
        if (!where_function)
            return;

        const auto & where_function_name = where_function->getFunctionName();
        if (where_function_name != "and" && where_function_name != "in" && where_function_name != "notIn")
            return;

        const auto & settings = getSettings();
        if (!settings[Setting::optimize_rewrite_in_subquery_to_join])
            return;

        /// Explicit set limits are part of IN behavior ('break' truncates the set, 'throw' fails
        /// on a too-large subquery); a join honors neither.
        if (settings[Setting::max_rows_in_set] != 0 || settings[Setting::max_bytes_in_set] != 0)
            return;

        QueryTreeNodes conjuncts;
        extractConjuncts(query_node->getWhere(), conjuncts);

        QueryTreeNodes remaining_conjuncts;
        std::optional<bool> join_tree_has_remote_table;

        for (auto & conjunct : conjuncts)
        {
            if (!tryRewrite(*query_node, conjunct, join_tree_has_remote_table))
                remaining_conjuncts.push_back(conjunct);
        }

        if (remaining_conjuncts.size() != conjuncts.size())
            query_node->getWhere() = makeConjunction(remaining_conjuncts);
    }

    bool performedRewrite() const { return rewrite_index != 0; }

private:
    bool tryRewrite(QueryNode & query_node, const QueryTreeNodePtr & conjunct, std::optional<bool> & join_tree_has_remote_table)
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
        if (subquery_node->isCTE())
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

        const std::vector<JoinAlgorithm> join_algorithms = getSettings()[Setting::join_algorithm];
        if (!joinAlgorithmsCanExecuteLeftJoin(join_algorithms, is_not_in ? JoinStrictness::Anti : JoinStrictness::Semi))
            return false;

        for (size_t i = 0; i < left_keys.size(); ++i)
        {
            auto left_type = removeLowCardinality(left_keys[i]->getResultType());
            auto right_type = removeLowCardinality(subquery_columns[i].type);

            /// `notIn` evaluates to NULL for a NULL left key (the row is dropped by the filter),
            /// while ANTI JOIN keeps unmatched NULL-keyed rows. `in` and SEMI JOIN agree: both drop.
            if (is_not_in && left_type->isNullable())
                return false;

            /// Arrays on the right of IN have flattening semantics that join equality does not.
            if (isArray(removeNullable(right_type)))
                return false;

            /// A NULL inside a composite key is a regular part of the serialized set key and can
            /// match, while join equality on such values returns NULL and never matches (top-level
            /// NULL keys behave the same on both paths).
            if (hasTypeThatCanContainNulls(removeNullable(left_type)) || hasTypeThatCanContainNulls(removeNullable(right_type)))
                return false;

            /// With a common supertype both sides widen losslessly, matching the accurate-or-null
            /// cast of the set path; without one the join would throw where IN works.
            if (!tryGetLeastSupertype(DataTypes{left_type, right_type}))
                return false;
        }

        if (containsCorrelatedSubquery(right_argument))
            return false;

        if (!leftExpressionIsSafe(left_argument, query_node.getJoinTreeNode(), /*check_index_columns*/ !is_not_in))
            return false;

        /// A plain IN over remote tables is subject to `distributed_product_mode` rules that a join
        /// does not replicate. Shard-local secondary queries still benefit: the pass re-runs there.
        if (containsRemoteTable(right_argument))
            return false;

        if (!join_tree_has_remote_table.has_value())
            join_tree_has_remote_table = containsRemoteTable(query_node.getJoinTreeNode());
        if (*join_tree_has_remote_table)
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

        /// Rename the subquery projection to avoid collisions with outer-scope column names.
        auto subquery_columns = subquery_node.getProjectionColumns();
        Names unique_names;
        unique_names.reserve(subquery_columns.size());
        for (size_t i = 0; i < subquery_columns.size(); ++i)
            unique_names.push_back(fmt::format("__in_join_subquery_column_{}_{}", rewrite_index, i + 1));

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

        subquery_node.clearProjectionColumns();
        subquery_node.setProjectionAliasesToOverride(std::move(unique_names));
        subquery_node.resolveProjectionColumns(std::move(subquery_columns));

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

    /// The left expression must be computable from the current join tree alone. With
    /// `check_index_columns`, additionally require that none of its columns can drive
    /// primary-key/partition/skip-index analysis, which the set path enables and a join loses.
    /// Negative predicates do not prune, so `notIn` skips that check.
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

    size_t rewrite_index = 0;
};

}

void RewriteInSubqueryToJoinPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteInSubqueryToJoinVisitor visitor(context);
    visitor.visit(query_tree_node);

    /// `__tableN` aliases number each IN subquery in its own scope; moving one into the join tree
    /// can produce duplicate aliases (and duplicate planner column identifiers), so renumber.
    if (visitor.performedRewrite())
        createUniqueAliasesIfNecessary(query_tree_node, context);
}

}
