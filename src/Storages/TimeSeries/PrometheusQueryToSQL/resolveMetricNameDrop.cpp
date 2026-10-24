#include <Storages/TimeSeries/PrometheusQueryToSQL/resolveMetricNameDrop.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
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

    /// SELECT new_group AS group, values FROM <from_table>
    ASTPtr makeRenamingQuery(const String & from_table)
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        builder.from_table = from_table;
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

        return makeRenamingQuery(addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context));
    }
}


ASTPtr dropMetricNameAndMergeDuplicates(ASTPtr vector_grid, ConverterContext & context)
{
    String vector_grid_name = addSubquery(std::move(vector_grid), SQLSubqueryType::TABLE, context);

    SelectQueryBuilder builder;
    builder.select_list.push_back(makeRemoveMetricNameAST(make_intrusive<ASTIdentifier>(ColumnNames::Group)));
    builder.select_list.back()->setAlias(ColumnNames::NewGroup);
    builder.select_list.push_back(makeNoDuplicateSeriesPerStepValues(make_intrusive<ASTIdentifier>(ColumnNames::Values)));
    builder.select_list.back()->setAlias(ColumnNames::Values);
    builder.from_table = vector_grid_name;
    builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
    builder.having = makeNoDuplicateSeriesPerStepCheck(
        make_intrusive<ASTIdentifier>(Strings{vector_grid_name, ColumnNames::Values}),
        make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

    return makeRenamingQuery(addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context));
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
