#include <Storages/TimeSeries/PrometheusQueryToSQL/applySortFunction.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    bool isSortByLabelFunction(std::string_view function_name)
    {
        return (function_name == "sort_by_label") || (function_name == "sort_by_label_desc");
    }

    /// timeSeriesGroupToTags(group)
    ASTPtr makeTagsExpression()
    {
        return makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group));
    }

    /// Builds the per-series sort key for sort() / sort_desc(): the sample value,
    /// with the full label set appended as a deterministic tiebreaker.
    ASTPtr makeSortByValueKey()
    {
        return makeASTFunction(
            "tuple",
            makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)),
            makeTagsExpression());
    }

    /// arrayMap(t -> t.2, arraySort(t -> t.1, groupArray((<sort_key>, group))))
    /// Sorting makes the result independent of the order in which groupArray() collected the rows.
    ASTPtr makeSortedGroupsByKey(ASTPtr && sort_key, bool descending)
    {
        auto sorted_pairs = makeASTFunction(
            descending ? "arrayReverseSort" : "arraySort",
            makeASTLambda({"t"},
                makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("t"), make_intrusive<ASTLiteral>(1u))),
            makeASTFunction("groupArray",
                makeASTFunction("tuple", std::move(sort_key), make_intrusive<ASTIdentifier>(ColumnNames::Group))));

        return makeASTFunction("arrayMap",
            makeASTLambda({"t"},
                makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("t"), make_intrusive<ASTLiteral>(2u))),
            std::move(sorted_pairs));
    }

    /// Orders the groups for sort_by_label() / sort_by_label_desc() exactly like Prometheus, with timeSeriesSortGroupsByTags();
    /// with `has_input_rank` it starts from the order fixed by an inner sort*() call (`sort_rank`).
    ASTPtr makeSortedGroupsByLabels(const std::vector<String> & labels, bool descending, bool has_input_rank)
    {
        Array label_names;
        for (const auto & label : labels)
            label_names.push_back(label);

        if (!has_input_rank)
        {
            return makeASTFunction("timeSeriesSortGroupsByTags",
                makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(ColumnNames::Group)),
                make_intrusive<ASTLiteral>(std::move(label_names)),
                make_intrusive<ASTLiteral>(descending));
        }

        auto sort_input = makeASTFunction("groupArray",
            makeASTFunction("tuple",
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                makeASTFunction("CAST", make_intrusive<ASTIdentifier>(ColumnNames::SortRank), make_intrusive<ASTLiteral>(String{"Array(UInt64)"}))));
        sort_input->setAlias("sort_input");

        auto element = [](ASTPtr array, UInt64 index)
        {
            return makeASTFunction("arrayMap",
                makeASTLambda({"t"},
                    makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("t"), make_intrusive<ASTLiteral>(index))),
                std::move(array));
        };

        return makeASTFunction("timeSeriesSortGroupsByTags",
            element(sort_input, 1),
            make_intrusive<ASTLiteral>(std::move(label_names)),
            make_intrusive<ASTLiteral>(descending),
            element(make_intrusive<ASTIdentifier>("sort_input"), 2));
    }

    /// Materializes `query` and makes `query_piece` continue from its `group` and `values` columns.
    /// Returns the name of the materialized subquery.
    String materializeVectorGrid(SQLQueryPiece & query_piece, ASTPtr && query, ConverterContext & context)
    {
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query), SQLSubqueryType::MATERIALIZED_TABLE});
        String table = context.subqueries.back().name;

        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        builder.from_table = table;
        query_piece.select_query = builder.getSelectQuery();

        return table;
    }

    /// Orders the vector at the sort*() call site: builds a subquery mapping each series (`sort_group`)
    /// to its position (`sort_rank`) in the required order, later applied by finalizeSQL().
    void materializeSortRank(
        SQLQueryPiece & query_piece, ASTPtr && sorted_groups, ConverterContext & context, const String & input_rank_subquery = {})
    {
        /// The other store methods hold at most one series, so there is nothing to order.
        if (query_piece.store_method != StoreMethod::VECTOR_GRID)
            return;

        /// The vector grid is read twice (by the rank map and by the rest of the evaluation),
        /// so it must be materialized.
        String vector_grid = materializeVectorGrid(query_piece, std::move(query_piece.select_query), context);

        /// Step 1:
        /// SELECT <sorted_groups> AS sorted_groups FROM <vector_grid>
        ASTPtr step1_query;
        {
            SelectQueryBuilder builder;

            builder.select_list.push_back(std::move(sorted_groups));
            builder.select_list.back()->setAlias(ColumnNames::SortedGroups);

            builder.from_table = vector_grid;

            /// Series without a sample at this step are not in the vector, so they must not take part in sorting.
            builder.where = makeASTFunction("isNotNull",
                makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)));

            /// Gives `sorted_groups` the input order as `sort_rank`.
            if (!input_rank_subquery.empty())
            {
                builder.join_table = input_rank_subquery;
                builder.join_kind = JoinKind::Left;
                builder.join_strictness = JoinStrictness::Any;
                builder.join_on = makeASTFunction("equals",
                    make_intrusive<ASTIdentifier>(ColumnNames::Group),
                    make_intrusive<ASTIdentifier>(ColumnNames::SortGroup));
            }

            step1_query = builder.getSelectQuery();
        }

        /// Step 2: unfold `sorted_groups` into one row per series with its position:
        /// SELECT (arrayJoin(arrayZip(sorted_groups, arrayEnumerate(sorted_groups))) AS p).1 AS sort_group,
        ///        [p.2] AS sort_rank
        /// FROM step1
        {
            SelectQueryBuilder builder;

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            auto array_join_expr = makeASTFunction("arrayJoin",
                makeASTFunction("arrayZip",
                    make_intrusive<ASTIdentifier>(ColumnNames::SortedGroups),
                    makeASTFunction("arrayEnumerate", make_intrusive<ASTIdentifier>(ColumnNames::SortedGroups))));
            array_join_expr->setAlias("p");

            builder.select_list.push_back(makeASTFunction("tupleElement", array_join_expr, make_intrusive<ASTLiteral>(1u)));
            builder.select_list.back()->setAlias(ColumnNames::SortGroup);

            /// The rank is an array so that `or` can prefix it with the side a row comes from.
            builder.select_list.push_back(makeASTFunction("array",
                makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("p"), make_intrusive<ASTLiteral>(2u))));
            builder.select_list.back()->setAlias(ColumnNames::SortRank);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), builder.getSelectQuery(), SQLSubqueryType::TABLE});
            query_piece.sort_rank_subquery = context.subqueries.back().name;
        }
    }

    /// Makes `query` (which has a column `new_group`) the rank map of `query_piece`:
    /// SELECT new_group AS sort_group, <rank> AS sort_rank FROM <query>
    void setSortRankSubquery(SQLQueryPiece & query_piece, ASTPtr && query, ASTPtr && rank, ConverterContext & context)
    {
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query), SQLSubqueryType::TABLE});

        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::SortGroup);

        builder.select_list.push_back(std::move(rank));
        builder.select_list.back()->setAlias(ColumnNames::SortRank);

        builder.from_table = context.subqueries.back().name;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), builder.getSelectQuery(), SQLSubqueryType::TABLE});
        query_piece.sort_rank_subquery = context.subqueries.back().name;
    }
}

bool isSortFunction(std::string_view function_name)
{
    return (function_name == "sort") || (function_name == "sort_desc") || isSortByLabelFunction(function_name);
}

SQLQueryPiece applySortFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;

    if (isSortByLabelFunction(function_name))
    {
        /// sort_by_label(v instant-vector, label string, ...) requires the vector; with no labels it sorts by all labels.
        if (arguments.empty())
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects at least 1 argument (an instant vector and optional label names), "
                            "but was called with {} arguments",
                            function_name, arguments.size());
        }

        auto & argument = arguments[0];

        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects the first argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }

        std::vector<String> labels;
        labels.reserve(arguments.size() - 1);
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            if (arguments[i].type != ResultType::STRING)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects argument #{} of type {}, but expression {} has type {}",
                                function_name, i + 1, ResultType::STRING,
                                getPromQLText(arguments[i], context), arguments[i].type);
            }
            labels.push_back(arguments[i].string_value);
        }

        /// Prometheus sorts the vector in place, so an order fixed by an inner sort*() call is the starting point.
        String input_rank_subquery = argument.sort_rank_subquery;

        /// sort_by_label() / sort_by_label_desc() do not change the values, they only order the vector, which must
        /// happen right at this call site: outer functions must see (and keep) the order produced here.
        materializeSortRank(
            argument,
            makeSortedGroupsByLabels(labels, function_name == "sort_by_label_desc", !input_rank_subquery.empty()),
            context,
            input_rank_subquery);
        argument.node = function_node;
        return std::move(argument);
    }

    if (arguments.size() != 1)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects {} argument, but was called with {} arguments",
                        function_name, 1, arguments.size());
    }

    auto & argument = arguments[0];

    if (argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects an argument of type {}, but expression {} has type {}",
                        function_name, ResultType::INSTANT_VECTOR,
                        getPromQLText(argument, context), argument.type);
    }

    /// sort() / sort_desc() do not change the values, they only order the vector at this call site.
    /// The new rank map replaces any ordering set by an inner sort*() call (e.g. sort_desc(sort_by_label(...))).
    materializeSortRank(argument, makeSortedGroupsByKey(makeSortByValueKey(), function_name == "sort_desc"), context);
    argument.node = function_node;
    return std::move(argument);
}

void setLimitAggregationRank(SQLQueryPiece & query_piece, ASTPtr && bucket_key, bool descending, ConverterContext & context)
{
    /// The order finalizeSQL() gives a topk() / bottomk() result: by bucket, then by value with NaN last, then by tags.
    ASTPtr value = makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u));
    auto is_nan = makeASTFunction("isNaN", value->clone());
    auto sort_key = makeASTFunction("tuple");
    if (bucket_key)
        sort_key->arguments->children.push_back(std::move(bucket_key));
    sort_key->arguments->children.push_back(descending ? makeASTFunction("not", is_nan->clone()) : is_nan->clone());
    sort_key->arguments->children.push_back(makeASTFunction("if", is_nan, make_intrusive<ASTLiteral>(0.0), value));
    sort_key->arguments->children.push_back(makeTagsExpression());
    materializeSortRank(query_piece, makeSortedGroupsByKey(std::move(sort_key), descending), context);
}

ASTPtr makeSortSourceOfMergedRow(ASTPtr group, ASTPtr values)
{
    return makeASTFunction("anyIf",
        std::move(group),
        makeASTFunction("isNotNull", makeASTFunction("arrayElement", std::move(values), make_intrusive<ASTLiteral>(1u))));
}

void setVectorGridRankedBySource(
    SQLQueryPiece & query_piece, ASTPtr && query, const String & source_rank_subquery, ConverterContext & context)
{
    String table = materializeVectorGrid(query_piece, std::move(query), context);

    /// SELECT group AS new_group, sort_rank
    /// FROM <result> ANY INNER JOIN <source_rank_subquery> ON sort_source = sort_group
    SelectQueryBuilder builder;

    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    builder.select_list.back()->setAlias(ColumnNames::NewGroup);

    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::SortRank));

    builder.from_table = table;
    builder.join_table = source_rank_subquery;
    builder.join_kind = JoinKind::Inner;
    builder.join_strictness = JoinStrictness::Any;
    builder.join_on = makeASTFunction("equals",
        make_intrusive<ASTIdentifier>(ColumnNames::SortSource),
        make_intrusive<ASTIdentifier>(ColumnNames::SortGroup));

    setSortRankSubquery(query_piece, builder.getSelectQuery(), make_intrusive<ASTIdentifier>(ColumnNames::SortRank), context);
}

void setOrResultWithSortRank(
    SQLQueryPiece & query_piece,
    ASTPtr && query,
    const String & left_rank_subquery,
    const String & right_rank_subquery,
    ConverterContext & context)
{
    String table = materializeVectorGrid(query_piece, std::move(query), context);

    /// Adds the rank of `key` in `rank_subquery` as column `alias`, or an empty rank if there is no rank map.
    auto add_rank = [](SelectQueryBuilder & builder, const char * key, const String & rank_subquery, const char * alias)
    {
        if (rank_subquery.empty())
        {
            /// A side without sort*() has no defined order, as in Prometheus, so its rows share one rank.
            builder.select_list.push_back(make_intrusive<ASTLiteral>(Array{}));
        }
        else
        {
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::SortRank));
            builder.join_table = rank_subquery;
            builder.join_kind = JoinKind::Left;
            builder.join_strictness = JoinStrictness::Any;
            builder.join_on = makeASTFunction("equals",
                make_intrusive<ASTIdentifier>(key),
                make_intrusive<ASTIdentifier>(ColumnNames::SortGroup));
        }
        builder.select_list.back()->setAlias(alias);
    };

    /// Step 1: add the left rank of each row.
    ASTPtr step1_query;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::SortFromRight));

        builder.from_table = table;
        add_rank(builder, ColumnNames::Group, left_rank_subquery, ColumnNames::SortLeftRank);

        step1_query = builder.getSelectQuery();
    }

    /// Step 2: add the right rank of each row.
    ASTPtr step2_query;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::SortFromRight));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::SortLeftRank));

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;
        add_rank(builder, ColumnNames::NewGroup, right_rank_subquery, ColumnNames::SortRightRank);

        step2_query = builder.getSelectQuery();
    }

    /// Step 3: a left row ranks as [0, left rank...] and a right row as [1, right rank...], so right rows come last.
    auto side_rank = [](UInt64 side, const char * rank)
    {
        return makeASTFunction("arrayConcat",
            makeASTFunction("array", make_intrusive<ASTLiteral>(side)),
            make_intrusive<ASTIdentifier>(rank));
    };

    setSortRankSubquery(
        query_piece,
        std::move(step2_query),
        makeASTFunction("if",
            make_intrusive<ASTIdentifier>(ColumnNames::SortFromRight),
            side_rank(1, ColumnNames::SortRightRank),
            side_rank(0, ColumnNames::SortLeftRank)),
        context);
}

}
