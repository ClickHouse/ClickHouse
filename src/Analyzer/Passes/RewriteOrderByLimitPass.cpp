#include <Analyzer/Passes/RewriteOrderByLimitPass.h>

#include <concepts>
#include <ranges>
#include <unordered_set>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{

auto constexpr DEFAULT_LIMIT_MAX_VAL = 1000000;
auto constexpr DEFAULT_MIN_COLUMNS_TO_USE_FETCH = 2;

namespace Setting
{
extern const SettingsBool query_plan_rewrite_order_by_limit;
extern const SettingsUInt64 query_plan_max_limit_for_rewrite_order_by_limit;
extern const SettingsUInt64 query_plan_min_columns_to_use_rewrite_order_by_limit;
}

namespace
{


template <typename T>
concept StringViewCompatible = std::convertible_to<T, std::string_view>;

template <typename R, typename Proj>
concept StringProjectionRange = std::ranges::input_range<R> && requires {
    typename std::indirect_result_t<Proj, std::ranges::iterator_t<R>>;
    requires StringViewCompatible<std::indirect_result_t<Proj, std::ranges::iterator_t<R>>>;
};

/// Disable this optimization when _part_starting_offset/_part_offset already exists in the projection
template <typename R, typename Proj = std::identity>
requires StringProjectionRange<R, Proj>
bool checkColumnsHelper(R && range, Proj proj = {})
{
    for (auto && name : range | std::views::transform(proj))
    {
        if (name == "_part_starting_offset" || name == "_part_offset")
            return false;
    }
    return true;
}

template <std::ranges::input_range Range>
auto collectVec(Range && range)
{
    using value_type = std::ranges::range_value_t<Range>;
    std::vector<value_type> result;

    if constexpr (std::ranges::sized_range<Range>)
    {
        result.reserve(std::ranges::size(range));
    }

    for (auto && elem : range)
    {
        result.emplace_back(std::forward<decltype(elem)>(elem));
    }

    return result;
}

/// The rewrite evaluates the `ORDER BY` key twice: once in the cloned subquery that selects the physical
/// offsets of the top rows, and once in the outer query (with `LIMIT` removed) that reads those rows back
/// and re-sorts them. A sort key that is not deterministic within the scope of a query, such as `rand()`,
/// yields different values in the two evaluations, so the rewrite can return the rows in a different order
/// (or, when the key ties, a different set) than the original `ORDER BY ... LIMIT`. Detect such a key so the
/// rewrite can be rejected. Aggregate and window functions in `ORDER BY` are rejected before this check runs;
/// for them `getFunction` returns null, which we conservatively treat as non-deterministic.
bool orderByHasNonDeterministicFunction(const QueryTreeNodePtr & order_by_node)
{
    if (!order_by_node)
        return false;

    QueryTreeNodes nodes_to_process{order_by_node};
    while (!nodes_to_process.empty())
    {
        auto node = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (const auto * function_node = node->as<FunctionNode>())
        {
            const auto & function_base = function_node->getFunction();
            if (!function_base || !function_base->isDeterministicInScopeOfQuery())
                return true;
        }

        for (const auto & child : node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child);
        }
    }

    return false;
}


struct OrderByLimitRewriteVisitor : public InDepthQueryTreeVisitorWithContext<OrderByLimitRewriteVisitor>
{
    using Base = InDepthQueryTreeVisitorWithContext<OrderByLimitRewriteVisitor>;
    using Base::Base;

    StoragePtr checkSimpleOrderByLimitQueryAndReturnStorage(const QueryNode & query_node) const
    {
        if (query_node.hasWith())
            return {};
        if (query_node.hasGroupBy())
            return {};
        if (query_node.hasWindow())
            return {};
        /// `hasWindow` only covers the named `WINDOW` clause; `QueryAnalysisPass` clears that section,
        /// so an inline window function (`row_number() OVER (...)`) in the projection or `ORDER BY` leaves
        /// it false. Window functions are evaluated over the full result set before `LIMIT`, but the rewrite
        /// filters rows by their physical offset before they are computed, which would change their values.
        if (hasWindowFunctionNodes(query_node.getProjectionNode()) || hasWindowFunctionNodes(query_node.getOrderByNode()))
            return {};
        /// The `arrayJoin` function (as opposed to the `ARRAY JOIN` clause, which turns the join tree into a
        /// non-`TableNode` and is already rejected below) expands one physical input row into several output
        /// rows. The rewrite selects whole physical rows by `_part_starting_offset + _part_offset` and
        /// re-evaluates the projection over them, so a row picked for a single expanded element is expanded
        /// again and returns all of its elements instead of just the top-N. `hasFunctionNode` does not descend
        /// into nested subqueries, so only an `arrayJoin` in this query's own scope is rejected.
        if (hasFunctionNode(query_node.getProjectionNode(), "arrayJoin")
            || hasFunctionNode(query_node.getOrderByNode(), "arrayJoin")
            || (query_node.hasWhere() && hasFunctionNode(query_node.getWhere(), "arrayJoin"))
            || (query_node.hasPrewhere() && hasFunctionNode(query_node.getPrewhere(), "arrayJoin")))
            return {};
        if (query_node.hasHaving())
            return {};
        /// `QUALIFY` filters on window-function results that are computed over the full pre-`LIMIT`
        /// result set (a window function in `QUALIFY` is not covered by the projection/`ORDER BY` scan
        /// above). The rewrite selects rows by their physical offset first, so the `QUALIFY` predicate
        /// would be re-evaluated over only the selected rows and change the result. Reject it outright,
        /// just like `HAVING`.
        if (query_node.hasQualify())
            return {};
        if (query_node.hasInterpolate())
            return {};
        if (query_node.hasLimitByLimit())
            return {};
        if (query_node.hasLimitByOffset())
            return {};
        if (query_node.hasLimitBy())
            return {};
        /// `DISTINCT` would be applied to the physical row offsets selected by the subquery
        /// instead of the original projection, which changes the result set.
        if (query_node.isDistinct())
            return {};
        /// `OFFSET` is kept on the main query while the subquery clone also carries it,
        /// so it would be applied twice. Reject the rewrite to avoid wrong results.
        if (query_node.hasOffset())
            return {};

        if (!query_node.hasLimit() || !query_node.hasOrderBy())
            return {};
        /// `ORDER BY ... WITH FILL` can synthesize rows that do not exist in the table before `LIMIT` is applied.
        /// The rewritten subquery can only return physical row offsets (`_part_starting_offset + _part_offset`),
        /// and the filled rows have no such offset, so the rewrite would change the result set.
        for (const auto & sort_node : query_node.getOrderBy().getNodes())
        {
            if (const auto * sort = sort_node->as<SortNode>(); sort && sort->withFill())
                return {};
        }
        /// A non-deterministic sort key (e.g. `ORDER BY rand()`) would be evaluated twice by the rewrite
        /// with diverging results, changing the order (or set) of the returned rows. Reject it.
        if (orderByHasNonDeterministicFunction(query_node.getOrderByNode()))
            return {};
        /// If the limit exceeds @limit_max_val, disable optimization
        if (auto * limit = query_node.getLimit()->as<ConstantNode>())
        {
            UInt64 value = 0;
            if (limit->getValue().tryGet(value))
            {
                if (limit_max_val > 0 && value > limit_max_val)
                {
                    return {};
                }
            }
            else
            {
                return {};
            }
        }

        /// Check the column name when identifier has been fully resolved
        const auto & columns = query_node.getProjectionColumns();
        if (!checkColumnsHelper(columns, &NameAndTypePair::name))
            return {};

        /// Only process single-table nodes, handle the process where the table identifier has been parsed and not parsed,
        /// and return the corresponding StoragePtr for that table, which is used for subsequent metadata validation during rewriting.
        /// Note: Statements like "select a join b order by x limit 10" are not supported for now.
        if (auto * tb_node = query_node.getJoinTreeNode()->as<TableNode>())
        {
            /// With `FINAL` the result row is not necessarily a single physical source row: engines such as
            /// `SummingMergeTree`, `AggregatingMergeTree`, and the collapsing/replacing variants can merge or
            /// suppress rows before the final result is produced. The rewrite feeds a physical-row predicate
            /// (`_part_starting_offset + _part_offset` `IN` subquery) back into the read, and `ReadFromMergeTree`
            /// turns it into a `total_offset_condition` during index analysis, so marks needed to compute the
            /// merged `FINAL` result can be pruned too early. Reject the rewrite in this case.
            if (tb_node->hasTableExpressionModifiers() && tb_node->getTableExpressionModifiers()->hasFinal())
                return {};
            if (columns.size() >= min_columns_to_use_fetch)
            {
                auto storage = tb_node->getStorage();
                /// A physical column named `_part_starting_offset`/`_part_offset` shadows the virtual
                /// row-offset columns during read planning, so the generated `ColumnNode`s would read
                /// user data instead of physical offsets and build a wrong `IN` filter.
                auto metadata = storage->getInMemoryMetadataPtr(getContext(), false);
                if (metadata
                    && (metadata->getColumns().hasPhysical("_part_starting_offset")
                        || metadata->getColumns().hasPhysical("_part_offset")))
                    return {};
                /// The rewrite communicates between the cloned subquery and the outer query through
                /// physical row positions (`_part_starting_offset + _part_offset`), which only identify
                /// base-table rows when both reads are planned on the base table. When the table defines
                /// a normal projection, `optimizeUseNormalProjection` can plan either read on the
                /// projection, whose parts have their own row numbering: `_part_offset` is remapped to
                /// the parent's `_parent_part_offset` only when the projection was written with parent
                /// part offsets, and `_part_starting_offset` is never remapped at all. The subquery could
                /// then rank rows by projection-local offsets while the outer query fetches base-table
                /// rows at those positions, returning different rows. Reject the rewrite whenever a
                /// normal projection exists (aggregate projections cannot serve these non-aggregate
                /// queries, so they are safe to ignore).
                if (metadata)
                {
                    for (const auto & projection : metadata->getProjections())
                    {
                        if (projection.type == ProjectionDescription::Type::Normal)
                            return {};
                    }
                }
                return storage;
            }
        }

        return {};
    }

    /// Do not descend into the children of a query node that was already selected for the rewrite: the
    /// rewrite clones the whole node and replaces its projection, so rewriting a subquery nested inside it
    /// is pointless. The decision is keyed on the parent node itself rather than a single shared flag, so
    /// selecting one child — for example the first arm of a `UNION ALL` — does not stop the visitor from
    /// descending into that node's other, equally eligible children.
    bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & /*child*/) const
    {
        return !selected_for_rewrite.contains(parent.get());
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (auto * query_node = node->as<QueryNode>())
        {
            if (auto table_storage = checkSimpleOrderByLimitQueryAndReturnStorage(*query_node))
            {
                if (table_storage->as<StorageMergeTree>())
                {
                    selected_for_rewrite.insert(node.get());
                    order_by_limit_nodes.emplace_back(node, std::move(table_storage));
                }
            }
        }
    }

    std::unordered_set<const IQueryTreeNode *> selected_for_rewrite;
    size_t limit_max_val = DEFAULT_LIMIT_MAX_VAL;
    size_t min_columns_to_use_fetch = DEFAULT_MIN_COLUMNS_TO_USE_FETCH;
    std::vector<std::pair<QueryTreeNodePtr, StoragePtr>> order_by_limit_nodes;
};
}

static bool rewriteOrderByLimit(QueryTreeNodePtr & original_query, const StoragePtr & table_storage, ContextPtr context)
{
    auto * main_query_node = original_query->as<QueryNode>();
    if (!main_query_node)
        return false;

    /// 1. Obtain subquery for rewrite through deep copy
    auto new_order_by_limit_subquery = main_query_node->clone();
    auto * new_order_by_limit_subquery_node = new_order_by_limit_subquery->as<QueryNode>();
    if (!new_order_by_limit_subquery_node)
        return false;
    const_cast<SettingsChanges &>(new_order_by_limit_subquery_node->getSettingsChanges()).clear();

    /// 2. Modify the projection to scan only the _part_starting_offset and _part_offset columns for row positioning
    auto & subquery_projection = new_order_by_limit_subquery_node->getProjection().getChildren();
    if (subquery_projection.empty())
        return false;
    auto get_column_name_and_type = [&table_storage, &context](const String & column_name) -> std::optional<NameAndTypePair>
    {
        auto metadata = table_storage->getInMemoryMetadataPtr(context, false);
        if (auto column = metadata->virtuals.tryGet(column_name, VirtualsKind::All, VirtualsMaterializationPlace::All))
            return column;

        if (!metadata->getColumns().has(column_name))
            return {};

        const auto & column = metadata->getColumns().get(column_name);
        return NameAndTypePair{column.name, column.type};
    };
    /// The _part_starting_offset/_part_offset column must exist in the table information
    auto part_column = get_column_name_and_type("_part_starting_offset");
    auto part_offset_column = get_column_name_and_type("_part_offset");
    if (!part_column || !part_offset_column)
        return false;

    auto create_func_with_resolve = [&context](const char * func_name, QueryTreeNodes columns) -> FunctionNodePtr
    {
        auto func = std::make_shared<FunctionNode>(func_name);
        func->getArguments().getNodes() = std::move(columns);
        func->resolveAsFunction(FunctionFactory::instance().get(func_name, context));
        return func;
    };
    auto get_column_source_from_proj = [](const QueryNode & node) -> TableExpressionNodePtr
    {
        const auto & children = node.getProjection().getChildren();
        if (children.empty())
            return {};
        if (auto * column_node = children.front()->as<ColumnNode>())
        {
            return column_node->getColumnSource();
        }
        else
        {
            return {};
        }
    };
    auto collect_column_nodes = [](const NamesAndTypes & column_names_and_types, TableExpressionNodePtr source) -> QueryTreeNodes
    {
        /// Keep `source` for every generated column: the range has more than one entry, so moving it
        /// here would leave the second and subsequent `ColumnNode`s with an empty source.
        return collectVec(
            column_names_and_types
            | std::views::transform(
                [&](const NameAndTypePair & pair) -> QueryTreeNodePtr { return std::make_shared<ColumnNode>(pair, source); }));
    };

    auto part_offset_column_info = NamesAndTypes{*part_column, *part_offset_column};


    auto subquery_column_source = get_column_source_from_proj(*new_order_by_limit_subquery_node);
    if (!subquery_column_source)
        return false;

    auto subquery_columns = collect_column_nodes(part_offset_column_info, std::move(subquery_column_source));
    auto func_plus_part_starting_and_part_offset = create_func_with_resolve("plus", std::move(subquery_columns));
    subquery_projection = {func_plus_part_starting_and_part_offset};

    new_order_by_limit_subquery_node->resolveProjectionColumns(
        {{"_cumulative_part_offset", func_plus_part_starting_and_part_offset->getResultType()}});

    /// 3. Clear the `LIMIT`/`PREWHERE`/`WHERE` and retain the `ORDER BY` in the main query,
    /// and construct `(_part_starting_offset + _part_offset) IN subquery` as the `WHERE` condition
    main_query_node->getLimit().reset();
    main_query_node->getWhere().reset();
    main_query_node->getPrewhere().reset();
    auto main_column_source = get_column_source_from_proj(*main_query_node);
    if (!main_column_source)
        return false;
    auto main_columns = collect_column_nodes(part_offset_column_info, std::move(main_column_source));
    auto where_column = create_func_with_resolve("plus", std::move(main_columns));
    new_order_by_limit_subquery_node->setIsSubquery(true);
    auto function_in = create_func_with_resolve("in", {std::move(where_column), std::move(new_order_by_limit_subquery)});
    main_query_node->getWhere() = std::move(function_in);

    return true;
}

void RewriteOrderByLimitPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    /// disable rewrite by default
    if (!context->getSettingsRef()[Setting::query_plan_rewrite_order_by_limit])
        return;

    OrderByLimitRewriteVisitor visitor(context);
    visitor.limit_max_val = context->getSettingsRef()[Setting::query_plan_max_limit_for_rewrite_order_by_limit];
    visitor.min_columns_to_use_fetch = context->getSettingsRef()[Setting::query_plan_min_columns_to_use_rewrite_order_by_limit];

    visitor.visit(query_tree_node);

    bool has_rewrite = false;
    for (auto & query_node : visitor.order_by_limit_nodes)
    {
        if (rewriteOrderByLimit(query_node.first, query_node.second, context))
        {
            has_rewrite = true;
            /// The rewrite relies on `_part_starting_offset + _part_offset` identifying the same rows
            /// in the cloned subquery and in the outer query, which only holds when both reads use one
            /// storage snapshot. Each rewritten `QueryNode` is planned from its own context
            /// (`Planner::buildPlanForUnionNode` plans every branch from `query_node->getContext()`),
            /// so the setting must be forced on every rewritten node, not only on the root — otherwise
            /// a rewritten `UNION ALL` arm would keep the user-disabled value and take independent
            /// snapshots under concurrent part changes. `QueryNode::cloneImpl` shares the parent's
            /// `ContextMutablePtr`, so this also covers the cloned inner subquery.
            query_node.first->as<QueryNode>()->getMutableContext()->setSetting(
                "enable_shared_storage_snapshot_in_query", true);
        }
    }

    if (has_rewrite)
    {
        LOG_TRACE(
            &Poco::Logger::get("RewriteOrderByLimitPass"),
            "Rewrite ORDER BY LIMIT successfully, current query: {}",
            query_tree_node->dumpTree());
    }
}

}
