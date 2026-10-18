#include <Storages/TimeSeries/PrometheusQueryToSQL/aggregateRangeVector.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/StorageTimeSeriesSelector.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// The column of the aggregated values in both branches of the union; the name differs from the alias of the consumers
    /// to avoid cyclic aliases.
    constexpr const char * VALUES_BY_SERIES = "values_by_series";

    constexpr std::string_view SELECTOR_TABLE_FUNCTION = "timeSeriesSelector";

    /// Calls `on_table_expression` for each table expression a SELECT query reads from, including the subqueries in its FROM clauses.
    template <typename OnTableExpression>
    void visitTableExpressions(IAST & ast, OnTableExpression && on_table_expression)
    {
        if (auto * select_with_union = ast.as<ASTSelectWithUnionQuery>())
        {
            for (auto & select : select_with_union->list_of_selects->children)
                visitTableExpressions(*select, on_table_expression);
            return;
        }

        if (auto * subquery = ast.as<ASTSubquery>())
        {
            visitTableExpressions(*subquery->children.at(0), on_table_expression);
            return;
        }

        auto * select = ast.as<ASTSelectQuery>();
        if (!select || !select->tables())
            return;

        for (auto & element_ast : select->tables()->children)
        {
            auto & element = element_ast->as<ASTTablesInSelectQueryElement &>();
            if (!element.table_expression)
                continue;
            auto & table_expression = element.table_expression->as<ASTTableExpression &>();
            if (table_expression.subquery)
                visitTableExpressions(*table_expression.subquery, on_table_expression);
            else
                on_table_expression(table_expression);
        }
    }

    const SQLSubquery * findSubqueryByName(const SQLSubqueries & subqueries, const ASTPtr & table)
    {
        const auto * table_identifier = table->as<ASTTableIdentifier>();
        if (!table_identifier)
            return nullptr;
        const String name = table_identifier->shortName();
        for (const auto & subquery : subqueries)
        {
            if (subquery.name == name)
                return &subquery;
        }
        return nullptr;
    }

    /// Finds the call of `timeSeriesSelector` the raw data of a range vector are read from, following the named subqueries.
    const ASTFunction * findSelector(const ASTPtr & raw_data, const SQLSubqueries & subqueries)
    {
        const ASTFunction * res = nullptr;
        visitTableExpressions(*raw_data, [&](const ASTTableExpression & table_expression)
        {
            if (res)
                return;
            if (table_expression.table_function)
            {
                const auto & function = table_expression.table_function->as<ASTFunction &>();
                if (function.name == SELECTOR_TABLE_FUNCTION)
                    res = &function;
            }
            else if (const auto * subquery = findSubqueryByName(subqueries, table_expression.database_and_table_name))
            {
                res = findSelector(subquery->ast, subqueries);
            }
        });
        return res;
    }

    /// Returns the call of `timeSeriesSelector` with `mode` as its last argument.
    ASTPtr cloneSelectorWithMode(const ASTFunction & selector, std::string_view mode)
    {
        auto res = selector.clone();
        res->as<ASTFunction &>().arguments->children.push_back(make_intrusive<ASTLiteral>(String{mode}));
        return res;
    }

    /// Clones the SELECT query of raw data with `mode` added to its call of `timeSeriesSelector`. The named subqueries it reads
    /// from are cloned and registered as new subqueries too, so each query reading the raw data stays referenced once.
    ASTPtr cloneRawDataWithSelectorMode(const ASTPtr & raw_data, std::string_view mode, ConverterContext & context)
    {
        auto res = raw_data->clone();
        visitTableExpressions(*res, [&](ASTTableExpression & table_expression)
        {
            if (table_expression.table_function)
            {
                const auto & function = table_expression.table_function->as<ASTFunction &>();
                if (function.name != SELECTOR_TABLE_FUNCTION)
                    return;
                auto new_table_function = cloneSelectorWithMode(function, mode);
                for (auto & child : table_expression.children)
                {
                    if (child == table_expression.table_function)
                        child = new_table_function;
                }
                table_expression.table_function = std::move(new_table_function);
            }
            else if (const auto * subquery = findSubqueryByName(context.subqueries, table_expression.database_and_table_name))
            {
                auto cloned_subquery = cloneRawDataWithSelectorMode(subquery->ast, mode, context);
                auto & subqueries = context.subqueries;
                subqueries.emplace_back(subqueries.size(), std::move(cloned_subquery), SQLSubqueryType::TABLE);
                auto new_table = make_intrusive<ASTTableIdentifier>(subqueries.back().name);
                for (auto & child : table_expression.children)
                {
                    if (child == table_expression.database_and_table_name)
                        child = new_table;
                }
                table_expression.database_and_table_name = std::move(new_table);
            }
        });
        return res;
    }

    String registerSubquery(ASTPtr ast, ConverterContext & context)
    {
        auto & subqueries = context.subqueries;
        subqueries.emplace_back(subqueries.size(), std::move(ast), SQLSubqueryType::TABLE);
        return subqueries.back().name;
    }

    /// SELECT [id,] timeSeriesIdToGroup(id) AS group, <aggregate_function> AS values_by_series FROM <raw_data> GROUP BY <group_by_key>
    String registerAggregation(ASTPtr raw_data, ASTPtr aggregate_function, ASTPtr group_by_key, bool with_id, ConverterContext & context)
    {
        SelectQueryBuilder builder;
        if (with_id)
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::ID));
        builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
        builder.select_list.back()->setAlias(ColumnNames::Group);
        builder.select_list.push_back(std::move(aggregate_function));
        builder.select_list.back()->setAlias(VALUES_BY_SERIES);
        builder.group_by.push_back(std::move(group_by_key));
        builder.from_table = registerSubquery(std::move(raw_data), context);
        return registerSubquery(builder.getSelectQuery(), context);
    }
}


RangeVectorAggregation aggregateRangeVector(SQLQueryPiece & range_vector, boost::intrusive_ptr<ASTFunction> aggregate_function, ConverterContext & context)
{
    RangeVectorAggregation res;

    switch (range_vector.store_method)
    {
        case StoreMethod::RAW_DATA:
        {
            if (!context.aggregate_range_vectors_in_order)
            {
                /// SELECT timeSeriesIdToGroup(id), <aggregate_function> ... GROUP BY timeSeriesIdToGroup(id)
                res.group_expression = makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID));
                res.group_by_key = res.group_expression->clone();
                res.aggregate_function = std::move(aggregate_function);
                break;
            }

            const auto * selector = findSelector(range_vector.select_query, context.subqueries);
            if (!selector)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The raw data of expression {} are not read by {}",
                                getPromQLText(range_vector, context), SELECTOR_TABLE_FUNCTION);
            }

            /// The raw data of the ids whose set of tags is stored under several ids are read again by the second branch,
            /// so they are cloned before the first branch takes the original query.
            auto raw_data_of_ids_sharing_tags = cloneRawDataWithSelectorMode(
                range_vector.select_query, StorageTimeSeriesSelector::MODE_SAMPLES_OF_IDS_SHARING_TAGS, context);

            /// SELECT id, timeSeriesIdToGroup(id) AS group, <aggregate_function> AS values_by_series
            /// FROM <raw_data>
            /// GROUP BY id  -- in the order of the samples table
            auto aggregated_by_id = registerAggregation(
                std::move(range_vector.select_query),
                aggregate_function->clone(),
                make_intrusive<ASTIdentifier>(ColumnNames::ID),
                /* with_id= */ true,
                context);

            /// SELECT id FROM timeSeriesSelector(<database>, <time_series_table>, <selector>, <min_time>, <max_time>, 'ids_sharing_tags')
            String ids_sharing_tags;
            {
                SelectQueryBuilder builder;
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::ID));
                builder.from_table_function = cloneSelectorWithMode(*selector, StorageTimeSeriesSelector::MODE_IDS_SHARING_TAGS);
                ids_sharing_tags = registerSubquery(builder.getSelectQuery(), context);
            }

            /// SELECT group, values_by_series
            /// FROM <aggregated_by_id> LEFT ANTI JOIN <ids_sharing_tags> ON <aggregated_by_id>.id = <ids_sharing_tags>.id
            /// A join leaves the aggregation over the raw data as is: a condition on the key `id` would be moved before the aggregation
            /// and checked for every sample instead of once per id.
            String own_tags_branch;
            {
                SelectQueryBuilder builder;
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(VALUES_BY_SERIES));
                builder.from_table = aggregated_by_id;
                builder.join_kind = JoinKind::Left;
                builder.join_strictness = JoinStrictness::Anti;
                builder.join_table = ids_sharing_tags;
                builder.join_on = makeASTFunction(
                    "equals",
                    make_intrusive<ASTIdentifier>(Strings{aggregated_by_id, ColumnNames::ID}),
                    make_intrusive<ASTIdentifier>(Strings{ids_sharing_tags, ColumnNames::ID}));
                own_tags_branch = registerSubquery(builder.getSelectQuery(), context);
            }

            /// SELECT timeSeriesIdToGroup(id) AS group, <aggregate_function> AS values_by_series
            /// FROM <raw_data of timeSeriesSelector(<database>, <time_series_table>, <selector>, <min_time>, <max_time>, 'samples_of_ids_sharing_tags')>
            /// GROUP BY timeSeriesIdToGroup(id)
            auto shared_tags_branch = registerAggregation(
                std::move(raw_data_of_ids_sharing_tags),
                std::move(aggregate_function),
                makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)),
                /* with_id= */ false,
                context);

            /// SELECT group, values_by_series FROM <own_tags_branch> UNION ALL SELECT group, values_by_series FROM <shared_tags_branch>
            SelectQueryBuilder builder;
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(VALUES_BY_SERIES));
            builder.from_table = own_tags_branch;
            builder.union_table = shared_tags_branch;
            range_vector.select_query = builder.getSelectQuery();

            res.group_expression = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            res.aggregate_function = make_intrusive<ASTIdentifier>(VALUES_BY_SERIES);
            break;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// SELECT group, <aggregate_function> ... GROUP BY group
            res.group_expression = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            res.group_by_key = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            res.aggregate_function = std::move(aggregate_function);
            break;
        }

        case StoreMethod::EMPTY:
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::CONST_STRING:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            /// These store methods have no series to group by.
            throwUnexpectedStoreMethod(range_vector, context);
        }
    }

    return res;
}

}
