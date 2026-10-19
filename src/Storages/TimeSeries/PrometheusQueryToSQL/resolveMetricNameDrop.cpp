#include <Storages/TimeSeries/PrometheusQueryToSQL/resolveMetricNameDrop.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/makeNoDuplicateSeriesPerStepCheck.h>
#include <algorithm>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    bool isVector(const Node * node)
    {
        return node->result_type == ResultType::INSTANT_VECTOR || node->result_type == ResultType::RANGE_VECTOR;
    }

    /// timeSeriesRemoveTag(<group>, '__name__')
    ASTPtr makeRemoveMetricNameAST(ASTPtr group)
    {
        return makeASTFunction("timeSeriesRemoveTag", std::move(group), make_intrusive<ASTLiteral>(kMetricName));
    }

    /// Stores `select_query` as a named subquery and returns its name.
    String addSubquery(ASTPtr select_query, SQLSubqueryType subquery_type, ConverterContext & context)
    {
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(select_query), subquery_type});
        return context.subqueries.back().name;
    }

    /// SELECT new_group AS group, values FROM <from_table> [UNION ALL SELECT new_group AS group, values FROM <union_table>]
    ASTPtr makeRenamingQuery(const String & from_table, const String & union_table)
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        builder.from_table = from_table;
        builder.union_table = union_table;
        return builder.getSelectQuery();
    }

    /// Drops the metric name from series which can't get the same tags because they have the same metric name.
    ///
    /// Step 1:
    /// SELECT timeSeriesRemoveTag(group, '__name__') AS new_group, values
    /// FROM <vector_grid>
    ///
    /// Step 2:
    /// SELECT new_group AS group, values
    /// FROM step1
    ASTPtr dropSingleMetricName(ASTPtr vector_grid, ConverterContext & context)
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(makeRemoveMetricNameAST(make_intrusive<ASTIdentifier>(ColumnNames::Group)));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        builder.from_table = addSubquery(std::move(vector_grid), SQLSubqueryType::TABLE, context);

        return makeRenamingQuery(addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context), /* union_table = */ "");
    }

    /// Drops the metric name and merges series which get the same tags, checking that they don't have values at the same step.
    /// Only such series go through the array aggregates, the others are passed as they are.
    ///
    /// Step 1 (materialized because steps 2 and 3 both read it):
    /// SELECT timeSeriesRemoveTag(group, '__name__') AS new_group, values, count() OVER (PARTITION BY new_group) AS series_count
    /// FROM <vector_grid>
    ///
    /// Step 2:
    /// SELECT new_group, values
    /// FROM step1
    /// WHERE series_count = 1
    ///
    /// Step 3:
    /// SELECT new_group, anyForEach(values) AS values
    /// FROM step1
    /// WHERE series_count > 1
    /// GROUP BY new_group
    /// HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(step1.values)), new_group) = 0
    ///
    /// Step 4:
    /// SELECT new_group AS group, values FROM step2
    /// UNION ALL
    /// SELECT new_group AS group, values FROM step3
    ASTPtr dropMetricNameAndMergeDuplicates(ASTPtr vector_grid, ConverterContext & context)
    {
        String counted_grid;
        {
            SelectQueryBuilder builder;
            builder.select_list.push_back(makeRemoveMetricNameAST(make_intrusive<ASTIdentifier>(ColumnNames::Group)));
            builder.select_list.back()->setAlias(ColumnNames::NewGroup);
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

            auto partition_by = make_intrusive<ASTExpressionList>();
            partition_by->children.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

            auto window_definition = make_intrusive<ASTWindowDefinition>();
            window_definition->partition_by = partition_by;
            window_definition->children.push_back(std::move(partition_by));

            auto series_count = makeASTFunction("count");
            series_count->setIsWindowFunction(true);
            series_count->setKind(ASTFunction::Kind::WINDOW_FUNCTION);
            series_count->window_definition = window_definition;
            series_count->children.push_back(std::move(window_definition));
            series_count->setAlias(ColumnNames::SeriesCount);
            builder.select_list.push_back(std::move(series_count));

            builder.from_table = addSubquery(std::move(vector_grid), SQLSubqueryType::TABLE, context);
            counted_grid = addSubquery(builder.getSelectQuery(), SQLSubqueryType::MATERIALIZED_TABLE, context);
        }

        String unique_series;
        {
            SelectQueryBuilder builder;
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
            builder.from_table = counted_grid;
            builder.where = makeASTFunction("equals", make_intrusive<ASTIdentifier>(ColumnNames::SeriesCount), make_intrusive<ASTLiteral>(1u));
            unique_series = addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context);
        }

        String merged_series;
        {
            SelectQueryBuilder builder;
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
            builder.select_list.push_back(makeNoDuplicateSeriesPerStepValues(make_intrusive<ASTIdentifier>(ColumnNames::Values)));
            builder.select_list.back()->setAlias(ColumnNames::Values);
            builder.from_table = counted_grid;
            builder.where = makeASTFunction("greater", make_intrusive<ASTIdentifier>(ColumnNames::SeriesCount), make_intrusive<ASTLiteral>(1u));
            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
            builder.having = makeNoDuplicateSeriesPerStepCheck(
                make_intrusive<ASTIdentifier>(Strings{counted_grid, ColumnNames::Values}),
                make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
            merged_series = addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context);
        }

        return makeRenamingQuery(unique_series, merged_series);
    }
}


bool hasSingleMetricName(const Node * node)
{
    switch (node->node_type)
    {
        case NodeType::InstantSelector:
        {
            const auto & matchers = static_cast<const PrometheusQueryTree::InstantSelector *>(node)->matchers;
            return std::any_of(matchers.begin(), matchers.end(), [](const PrometheusQueryTree::Matcher & matcher)
            {
                return matcher.label_name == kMetricName && matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ;
            });
        }

        case NodeType::RangeSelector:
            return hasSingleMetricName(static_cast<const PrometheusQueryTree::RangeSelector *>(node)->getInstantSelector());

        case NodeType::Subquery:
            return hasSingleMetricName(static_cast<const PrometheusQueryTree::Subquery *>(node)->getExpression());

        case NodeType::Offset:
            return hasSingleMetricName(static_cast<const PrometheusQueryTree::Offset *>(node)->getExpression());

        case NodeType::UnaryOperator:
            return hasSingleMetricName(static_cast<const PrometheusQueryTree::UnaryOperator *>(node)->getArgument());

        case NodeType::Function:
        {
            const auto * function_node = static_cast<const PrometheusQueryTree::Function *>(node);
            if (function_node->function_name == "label_replace" || function_node->function_name == "label_join")
                return false;

            /// A function with one vector argument keeps the labels of that argument (except the metric name).
            const Node * vector_argument = nullptr;
            for (const Node * argument : function_node->getArguments())
            {
                if (!isVector(argument))
                    continue;
                if (vector_argument)
                    return false;
                vector_argument = argument;
            }
            return vector_argument && hasSingleMetricName(vector_argument);
        }

        case NodeType::BinaryOperator:
        {
            /// An operator between a vector and a scalar keeps the labels of the vector.
            const auto * operator_node = static_cast<const PrometheusQueryTree::BinaryOperator *>(node);
            const Node * left = operator_node->getLeftArgument();
            const Node * right = operator_node->getRightArgument();
            if (left->result_type == ResultType::INSTANT_VECTOR && right->result_type == ResultType::SCALAR)
                return hasSingleMetricName(left);
            if (left->result_type == ResultType::SCALAR && right->result_type == ResultType::INSTANT_VECTOR)
                return hasSingleMetricName(right);
            return false;
        }

        case NodeType::Scalar:
        case NodeType::StringLiteral:
        case NodeType::AggregationOperator:
            return false;
    }

    UNREACHABLE();
}


SQLQueryPiece resolveMetricNameDrop(SQLQueryPiece && query_piece, ConverterContext & context)
{
    if (!query_piece.metric_name_drop_pending)
        return std::move(query_piece);

    chassert(query_piece.store_method == StoreMethod::VECTOR_GRID);
    chassert(!query_piece.metric_name_dropped);

    if (hasSingleMetricName(query_piece.node))
        query_piece.select_query = dropSingleMetricName(std::move(query_piece.select_query), context);
    else
        query_piece.select_query = dropMetricNameAndMergeDuplicates(std::move(query_piece.select_query), context);

    query_piece.metric_name_drop_pending = false;
    query_piece.metric_name_dropped = true;
    return std::move(query_piece);
}


ASTPtr foldMetricNameDropIntoAggregation(const PrometheusQueryTree::AggregationOperator * operator_node, SQLQueryPiece & argument)
{
    ASTPtr group = make_intrusive<ASTIdentifier>(ColumnNames::Group);

    if (!argument.metric_name_drop_pending || !hasSingleMetricName(argument.node))
        return group;

    /// transformGroupASTForAggregationOperator() drops the metric name itself unless `by(__name__)` keeps it.
    const auto & labels = operator_node->labels;
    if (operator_node->by && std::find(labels.begin(), labels.end(), kMetricName) != labels.end())
    {
        group = makeRemoveMetricNameAST(std::move(group));
        argument.metric_name_dropped = true;
    }

    argument.metric_name_drop_pending = false;
    return group;
}

}
