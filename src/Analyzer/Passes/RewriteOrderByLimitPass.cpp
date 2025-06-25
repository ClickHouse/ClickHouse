#include <Analyzer/Passes/RewriteOrderByLimitPass.h>

#include <concepts>
#include <ranges>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/TableNode.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageDummy.h>
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


struct OrderByLimitRewriteVisitor : public InDepthQueryTreeVisitorWithContext<OrderByLimitRewriteVisitor>
{
    using Base = InDepthQueryTreeVisitorWithContext<OrderByLimitRewriteVisitor>;
    using Base::Base;

    StoragePtr getTableStorageFromTableId(const StorageID & table_id) const
    {
        auto ctx = getContext();
        return DatabaseCatalog::instance().tryGetTable(table_id, ctx);
    }

    StoragePtr checkSimpleOrderByLimitQueryAndReturnStorage(const QueryNode & query_node) const
    {
        if (query_node.hasWith())
            return {};
        if (query_node.hasGroupBy())
            return {};
        if (query_node.hasWindow())
            return {};
        if (query_node.hasHaving())
            return {};
        if (query_node.hasInterpolate())
            return {};
        if (query_node.hasLimitByLimit())
            return {};
        if (query_node.hasLimitByOffset())
            return {};
        if (query_node.hasLimitBy())
            return {};

        if (!query_node.hasLimit() || !query_node.hasOrderBy())
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
        if (auto * tb_node = query_node.getJoinTree()->as<TableNode>())
        {
            if (columns.size() >= min_columns_to_use_fetch)
            {
                const auto & storage = tb_node->getStorage();
                if (storage->as<StorageDummy>())
                {
                    return getTableStorageFromTableId(storage->getStorageID());
                }
                return storage;
            }
        }

        return {};
    }

    bool needChildVisit(QueryTreeNodePtr & /*parent*/, QueryTreeNodePtr & /*child*/) const { return need_child_visit; }

    void enterImpl(QueryTreeNodePtr & node)
    {
        need_child_visit = true;
        if (auto * query_node = node->as<QueryNode>())
        {
            if (auto table_storage = checkSimpleOrderByLimitQueryAndReturnStorage(*query_node))
            {
                if (table_storage->as<StorageMergeTree>())
                {
                    need_child_visit = false;
                    order_by_limit_nodes.emplace_back(node, std::move(table_storage));
                }
            }
        }
    }

    bool need_child_visit = false;
    size_t limit_max_val = DEFAULT_LIMIT_MAX_VAL;
    size_t min_columns_to_use_fetch = DEFAULT_MIN_COLUMNS_TO_USE_FETCH;
    std::vector<std::pair<QueryTreeNodePtr, StoragePtr>> order_by_limit_nodes;
};
}

bool rewriteOrderByLimit(QueryTreeNodePtr & original_query, const StoragePtr & table_storage, ContextPtr context)
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
    auto get_column_name_and_type = [&table_storage](const String & column_name) -> std::optional<NameAndTypePair>
    {
        if (auto column = table_storage->getVirtualsList().tryGetByName(column_name))
            return column;

        if (!table_storage->getInMemoryMetadataPtr()->getColumns().has(column_name))
            return {};

        const auto & column = table_storage->getInMemoryMetadataPtr()->getColumns().get(column_name);
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
    auto get_column_source_from_proj = [](const QueryNode & node) -> QueryTreeNodePtr
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
    auto collect_column_nodes = [](const NamesAndTypes & column_names_and_types, QueryTreeNodePtr source) -> QueryTreeNodes
    {
        return collectVec(
            column_names_and_types
            | std::views::transform(
                [&](const NameAndTypePair & pair) -> QueryTreeNodePtr { return std::make_shared<ColumnNode>(pair, std::move(source)); }));
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
