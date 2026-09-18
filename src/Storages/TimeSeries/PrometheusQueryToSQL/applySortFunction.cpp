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

    /// Builds the per-series sort key for sort_by_label() / sort_by_label_desc(): the natural sort keys
    /// of the given label values (a missing label is an empty string), with the full label set as a tiebreaker.
    ASTPtr makeSortByLabelKey(const std::vector<String> & labels)
    {
        auto key = makeASTFunction("tuple");
        for (const auto & label : labels)
        {
            /// naturalSortKey(tupleElement(arrayFirst(t -> t.1 = '<label>', timeSeriesGroupToTags(group)), 2))
            auto label_value = makeASTFunction("tupleElement",
                makeASTFunction("arrayFirst",
                    makeASTLambda({"t"},
                        makeASTFunction("equals",
                            makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("t"), make_intrusive<ASTLiteral>(1u)),
                            make_intrusive<ASTLiteral>(label))),
                    makeTagsExpression()),
                make_intrusive<ASTLiteral>(2u));
            key->arguments->children.push_back(makeASTFunction("naturalSortKey", std::move(label_value)));
        }
        key->arguments->children.push_back(makeTagsExpression());
        return key;
    }

    /// Orders the vector at the sort*() call site: builds a subquery mapping each series (`sort_group`)
    /// to its position (`sort_rank`) in the required order, later applied by finalizeSQL().
    void materializeSortRank(SQLQueryPiece & query_piece, ASTPtr && sort_key, bool descending, ConverterContext & context)
    {
        /// The other store methods hold at most one series, so there is nothing to order.
        if (query_piece.store_method != StoreMethod::VECTOR_GRID)
            return;

        /// The vector grid is read twice (by the rank map and by the rest of the evaluation),
        /// so it must be materialized.
        context.subqueries.emplace_back(
            SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::MATERIALIZED_TABLE});
        String vector_grid = context.subqueries.back().name;

        /// Step 1:
        /// SELECT arrayMap(t -> t.2, arraySort(t -> t.1, groupArray((<sort_key>, group)))) AS sorted_groups
        /// FROM <vector_grid>
        ///
        /// arraySort() (arrayReverseSort() for the descending variants) makes the result deterministic
        /// regardless of the order in which groupArray() collected the rows.
        ASTPtr step1_query;
        {
            SelectQueryBuilder builder;

            auto sorted_pairs = makeASTFunction(
                descending ? "arrayReverseSort" : "arraySort",
                makeASTLambda({"t"},
                    makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("t"), make_intrusive<ASTLiteral>(1u))),
                makeASTFunction("groupArray",
                    makeASTFunction("tuple", std::move(sort_key), make_intrusive<ASTIdentifier>(ColumnNames::Group))));

            builder.select_list.push_back(makeASTFunction("arrayMap",
                makeASTLambda({"t"},
                    makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("t"), make_intrusive<ASTLiteral>(2u))),
                std::move(sorted_pairs)));
            builder.select_list.back()->setAlias(ColumnNames::SortedGroups);

            builder.from_table = vector_grid;

            step1_query = builder.getSelectQuery();
        }

        /// Step 2: unfold `sorted_groups` into one row per series with its position:
        /// SELECT (arrayJoin(arrayZip(sorted_groups, arrayEnumerate(sorted_groups))) AS p).1 AS sort_group,
        ///        p.2 AS sort_rank
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

            builder.select_list.push_back(
                makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("p"), make_intrusive<ASTLiteral>(2u)));
            builder.select_list.back()->setAlias(ColumnNames::SortRank);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), builder.getSelectQuery(), SQLSubqueryType::TABLE});
            query_piece.sort_rank_subquery = context.subqueries.back().name;
        }

        /// The rest of the evaluation continues from the materialized vector grid.
        {
            SelectQueryBuilder builder;
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
            builder.from_table = vector_grid;
            query_piece.select_query = builder.getSelectQuery();
        }
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
        /// sort_by_label(v instant-vector, label string, ...) requires the vector and at least one label.
        if (arguments.size() < 2)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects at least 2 arguments (an instant vector and at least one label name), "
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

        /// sort_by_label() / sort_by_label_desc() do not change the values, they only order the vector, which must
        /// happen right at this call site: outer functions must see (and keep) the order produced here.
        materializeSortRank(argument, makeSortByLabelKey(labels), function_name == "sort_by_label_desc", context);
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
    materializeSortRank(argument, makeSortByValueKey(), function_name == "sort_desc", context);
    argument.node = function_node;
    return std::move(argument);
}

void rekeySortRankSubquery(
    SQLQueryPiece & query_piece, const std::function<ASTPtr(ASTPtr)> & transform_group, ConverterContext & context)
{
    if (query_piece.sort_rank_subquery.empty())
        return;

    /// Step 1:
    /// SELECT <transform_group(sort_group)> AS new_group, min(sort_rank) AS sort_rank
    /// FROM <sort_rank_subquery>
    /// GROUP BY new_group
    ASTPtr rekeying_query;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(transform_group(make_intrusive<ASTIdentifier>(ColumnNames::SortGroup)));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);

        builder.select_list.push_back(makeASTFunction("min", make_intrusive<ASTIdentifier>(ColumnNames::SortRank)));
        builder.select_list.back()->setAlias(ColumnNames::SortRank);

        builder.from_table = query_piece.sort_rank_subquery;

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        rekeying_query = builder.getSelectQuery();
    }

    /// Step 2:
    /// SELECT new_group AS sort_group, sort_rank
    /// FROM step1
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::SortGroup);

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::SortRank));

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(rekeying_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), builder.getSelectQuery(), SQLSubqueryType::TABLE});
        query_piece.sort_rank_subquery = context.subqueries.back().name;
    }
}

}
