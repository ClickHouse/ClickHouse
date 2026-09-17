#include <Analyzer/IQueryTreeNode.h>
#include <Planner/CollectSets.h>

#include <Storages/StorageSet.h>
#include <Storages/MergeTree/MergeTreeData.h>
#if CLICKHOUSE_CLOUD
#include <Storages/StorageSharedSetJoin.h>
#endif

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Access/Common/AccessFlags.h>
#include <Core/Settings.h>
#include <Interpreters/misc.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>

#include <optional>
#include <unordered_set>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_lookup_index;
    extern const SettingsBool transform_null_in;
    extern const SettingsBool validate_enum_literals_in_operators;
    extern const SettingsUInt64 max_rows_in_set;
    extern const SettingsUInt64 max_bytes_in_set;
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct LookupSetFromStorage
{
    SetPtr set;
    StorageID storage_id;
};

bool hasUnsupportedLookupModifiers(const std::optional<TableExpressionModifiers> & table_expression_modifiers)
{
    return table_expression_modifiers
        && (table_expression_modifiers->hasFinal()
            || table_expression_modifiers->hasSampleSizeRatio()
            || table_expression_modifiers->hasSampleOffsetRatio()
            || table_expression_modifiers->hasStream());
}

std::optional<LookupSetFromStorage> tryGetLookupSetFromTableExpression(const QueryTreeNodePtr & table_expression, PlannerContext & planner_context)
{
    const auto & query_context = planner_context.getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    if (!settings[Setting::allow_experimental_lookup_index])
        return std::nullopt;

    /// The lookup `Set` is built once and cached/shared across queries with empty `SizeLimits`,
    /// so it cannot honor per-query `IN`-set limits (`max_rows_in_set` / `max_bytes_in_set` /
    /// `set_overflow_mode`). Fall back to the regular subquery/set path when set limits are
    /// active so those settings keep their normal semantics.
    if (settings[Setting::max_rows_in_set] != 0 || settings[Setting::max_bytes_in_set] != 0)
        return std::nullopt;

    auto * table_node = table_expression->as<TableNode>();
    if (table_node)
    {
        if (hasUnsupportedLookupModifiers(table_node->getTableExpressionModifiers()))
            return std::nullopt;

        auto storage = std::dynamic_pointer_cast<MergeTreeData>(table_node->getStorage());
        if (!storage)
            return std::nullopt;

        /// A `SELECT_FILTER` row policy on the right table would normally be applied
        /// during regular subquery execution; the lookup fast path bypasses it, so
        /// fall back to the regular subquery/set path to preserve visibility.
        if (getEffectiveRowPolicyFilter(storage, query_context))
            return std::nullopt;

        /// `additional_table_filters` are applied by the regular plan; the lookup fast path
        /// reads the storage with an empty `SelectQueryInfo` and would ignore them.
        if (hasAdditionalTableFilterForStorage(storage, table_expression->getOriginalAlias(), query_context))
            return std::nullopt;

        auto columns_to_select = table_node->getStorageSnapshot()->getColumns(GetColumnsOptions(GetColumnsOptions::Ordinary));
        Names key_names;
        key_names.reserve(columns_to_select.size());
        for (const auto & column : columns_to_select)
            key_names.push_back(column.name);

        /// The regular `IN table` path builds a plan for the right table that performs a `SELECT`
        /// access check on the columns it reads (`checkAccessRights`). The lookup fast path builds
        /// the `Set` directly from storage and never plans that read, so it must enforce the same
        /// check here on the lookup key columns. Do it *before* `tryGetLookupSet`, which reads the
        /// table and may populate the shared lookup cache: otherwise a user without `SELECT` on the
        /// right table could probe its keys through `IN table` or force a read/cache build of a
        /// table they cannot access.
        auto storage_id = table_node->getStorageID();
        if (storage_id.hasDatabase())
            query_context->checkAccess(AccessType::SELECT, storage_id, key_names);

        auto set = storage->tryGetLookupSet(key_names, table_node->getStorageSnapshot(), planner_context.getQueryContext());
        if (!set)
            return std::nullopt;

        return LookupSetFromStorage{std::move(set), std::move(storage_id)};
    }

    auto * query_node = table_expression->as<QueryNode>();
    if (!query_node
        || query_node->isDistinct()
        || query_node->hasSettingsChanges()
        || query_node->hasPrewhere()
        || query_node->hasWhere()
        || query_node->hasGroupBy()
        || query_node->hasHaving()
        || query_node->hasWindow()
        || query_node->hasQualify()
        || query_node->hasOrderBy()
        || query_node->hasLimitBy()
        || query_node->hasLimit()
        || query_node->hasOffset()
        || query_node->isGroupByWithTotals())
    {
        return std::nullopt;
    }

    const auto & inner_table_expression = query_node->getJoinTreeNode();
    auto * inner_table_node = inner_table_expression->as<TableNode>();
    if (!inner_table_node)
        return std::nullopt;

    if (hasUnsupportedLookupModifiers(inner_table_node->getTableExpressionModifiers()))
        return std::nullopt;

    auto storage = std::dynamic_pointer_cast<MergeTreeData>(inner_table_node->getStorage());
    if (!storage)
        return std::nullopt;

    if (getEffectiveRowPolicyFilter(storage, query_context))
        return std::nullopt;

    if (hasAdditionalTableFilterForStorage(storage, inner_table_expression->getOriginalAlias(), query_context))
        return std::nullopt;

    TableExpressionData::ColumnIdentifierToColumnName column_mapping;
    if (const auto * table_expression_data = planner_context.getTableExpressionDataOrNull(inner_table_expression))
    {
        column_mapping = table_expression_data->getColumnIdentifierToColumnName();
    }
    else
    {
        for (const auto & column : inner_table_node->getStorageSnapshot()->metadata->getColumns().getOrdinary())
            column_mapping.emplace(column.name, column.name);
    }

    Names key_names;
    key_names.reserve(query_node->getProjection().getNodes().size());
    for (const auto & projection_node : query_node->getProjection().getNodes())
    {
        const auto * column_node = projection_node->as<ColumnNode>();
        if (!column_node || column_node->hasExpression())
            return std::nullopt;

        auto column_source = column_node->getColumnSourceOrNull();
        if (!column_source || column_source.get() != inner_table_expression.get())
            return std::nullopt;

        auto mapping_it = column_mapping.find(column_node->getColumnName());
        if (mapping_it == column_mapping.end())
            return std::nullopt;

        key_names.push_back(mapping_it->second);
    }

    if (key_names.empty())
        return std::nullopt;

    /// Same `SELECT` access check as for the direct `IN table` case above: the trivial
    /// `IN (SELECT key FROM table)` subquery would be access-checked by the regular plan, but the
    /// lookup fast path replaces it with a ready set built from storage, so check access on the
    /// lookup key columns here too — *before* `tryGetLookupSet` reads the table and may populate
    /// the shared lookup cache, so an unauthorized user cannot force that read/cache build.
    auto storage_id = inner_table_node->getStorageID();
    if (storage_id.hasDatabase())
        query_context->checkAccess(AccessType::SELECT, storage_id, key_names);

    auto set = storage->tryGetLookupSet(key_names, inner_table_node->getStorageSnapshot(), planner_context.getQueryContext());
    if (!set)
        return std::nullopt;

    return LookupSetFromStorage{std::move(set), std::move(storage_id)};
}

class CollectSetsVisitor : public InDepthQueryTreeVisitorWithContext<CollectSetsVisitor>
{
public:
    CollectSetsVisitor(PlannerContext & planner_context_, std::vector<QueryTreeNodePtr> & pending_source_expressions_)
        : InDepthQueryTreeVisitorWithContext(planner_context_.getQueryContext())
        , planner_context(planner_context_)
        , pending_source_expressions(pending_source_expressions_)
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (const auto * constant_node = node->as<ConstantNode>())
            /// Collect sets from source expression as well.
            /// Most likely we will not build them, but those sets could be requested during analysis.
            /// A source expression is not a query tree child, and a chain of them can be arbitrarily
            /// long, so it is queued for a separate visit rather than descended into from here.
            if (constant_node->hasSourceExpression())
                pending_source_expressions.push_back(constant_node->getSourceExpression());

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isNameOfInFunction(function_node->getFunctionName()))
            return;

        if (function_node->getArguments().getNodes().size() < 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function '{}' is expected to have at least 2 arguments, got {}",
                function_node->getFunctionName(),
                function_node->getArguments().getNodes().size());

        auto in_first_argument = function_node->getArguments().getNodes().at(0);
        auto in_second_argument = function_node->getArguments().getNodes().at(1);
        auto in_second_argument_node_type = in_second_argument->getNodeType();

        const auto & settings = planner_context.getQueryContext()->getSettingsRef();
        auto & sets = planner_context.getPreparedSets();

        /// Tables and table functions are replaced with subquery at Analysis stage, except special Set table.
        auto * second_argument_table = in_second_argument->as<TableNode>();
        StorageSet * storage_set = second_argument_table != nullptr ? dynamic_cast<StorageSet *>(second_argument_table->getStorage().get()) : nullptr;

        if (storage_set)
        {
            /// Handle storage_set as ready set.
            auto set_key = in_second_argument->getTreeHash({.ignore_cte = true});
            if (sets.findStorage(set_key))
                return;
            auto ast = in_second_argument->toAST();
            sets.addFromStorage(set_key, std::move(ast), storage_set->getSet(), second_argument_table->getStorageID());
        }
        else if (const auto * constant_node = in_second_argument->as<ConstantNode>())
        {
            auto set = getSetElementsForConstantValue(
                in_first_argument->getResultType(), constant_node->getColumn(), constant_node->getResultType(),
                GetSetElementParams{
                    .transform_null_in = settings[Setting::transform_null_in],
                    .forbid_unknown_enum_values = settings[Setting::validate_enum_literals_in_operators],
                });

            if (set.empty())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Function '{}' second argument evaluated to Block with no columns",
                    function_node->getFunctionName());

            DataTypes set_element_types;
            set_element_types.reserve(set.size());
            /// Get the `set_element_types` from `set` instead of `in_first_argument` because
            /// inside `getSetElementsForConstantValue`, we already do necessary transformation including
            /// getting `dictionaryType` from `DataTypeLowCardinality`. Therefore, we can skip some steps here if
            /// we directly use `set` to get the `set_element_types`.
            for (const auto & elem : set)
                set_element_types.push_back(elem.type);

            set_element_types = Set::getElementTypes(std::move(set_element_types), settings[Setting::transform_null_in]);
            auto set_key = in_second_argument->getTreeHash({.ignore_cte = true});

            if (sets.findTuple(set_key, set_element_types))
                return;

            auto ast = in_second_argument->toAST();
#if CLICKHOUSE_CLOUD
            if (storage_set->getName() == "SharedSet")
                sets.addFromStorage(set_key, std::move(ast), static_cast<StorageSharedSet *>(storage_set)->getSet(planner_context.getQueryContext()), second_argument_table->getStorageID());
            else
#endif
            sets.addFromTuple(set_key, std::move(ast), std::move(set), settings);
        }
        else if (in_second_argument_node_type == QueryTreeNodeType::QUERY ||
            in_second_argument_node_type == QueryTreeNodeType::UNION ||
            in_second_argument_node_type == QueryTreeNodeType::TABLE)
        {
            if (auto lookup_set = tryGetLookupSetFromTableExpression(in_second_argument, planner_context))
            {
                auto set_key = in_second_argument->getTreeHash({.ignore_cte = true});
                if (sets.findStorage(set_key))
                    return;

                auto ast = in_second_argument->toAST({ .set_subquery_cte_name = false });
                sets.addFromStorage(set_key, std::move(ast), std::move(lookup_set->set), lookup_set->storage_id);
                return;
            }

            auto set_key = in_second_argument->getTreeHash({.ignore_cte = true});
            const bool external_table_expected = isNameOfGlobalInFunction(function_node->getFunctionName());

            if (auto subquery_set = sets.findSubquery(set_key))
            {
                if (external_table_expected)
                    subquery_set->markExternalTableExpected();
                return;
            }

            auto subquery_to_execute = in_second_argument;
            if (in_second_argument->as<TableNode>())
                subquery_to_execute = buildSubqueryToReadColumnsFromTableExpression(static_pointer_cast<TableNode>(subquery_to_execute), planner_context.getQueryContext());

            auto ast = in_second_argument->toAST({ .set_subquery_cte_name = false });
            auto subquery_set = sets.addFromSubquery(set_key, std::move(ast), std::move(subquery_to_execute), settings);
            if (external_table_expected)
                subquery_set->markExternalTableExpected();
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function '{}' is supported only if second argument is constant or table expression",
                function_node->getFunctionName());
        }
    }

    static bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr & child_node)
    {
        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

private:
    PlannerContext & planner_context;
    std::vector<QueryTreeNodePtr> & pending_source_expressions;
};

}

void collectSets(const QueryTreeNodePtr & node, PlannerContext & planner_context)
{
    std::vector<QueryTreeNodePtr> pending_source_expressions{node};
    /// Every link of a `DEFAULT` chain is reachable from each link above it, so without this the
    /// worklist would visit link `i` once per link above it: `2 * N^2` root visits for a chain of N
    /// columns, against `4 * N` with it. Skipping a repeat visit changes nothing, because every set
    /// registration in `enterImpl` is already guarded by `findTuple`, `findStorage` or `findSubquery`.
    std::unordered_set<QueryTreeNodePtr> visited_source_expressions;

    while (!pending_source_expressions.empty())
    {
        auto node_to_visit = pending_source_expressions.back();
        pending_source_expressions.pop_back();

        if (!visited_source_expressions.insert(node_to_visit).second)
            continue;

        /// Each pending node is visited as a root: needChildVisit refuses QUERY and UNION
        /// children, so a source expression that is itself a query must start its own visit.
        CollectSetsVisitor visitor(planner_context, pending_source_expressions);
        visitor.visit(node_to_visit);
    }
}

}
