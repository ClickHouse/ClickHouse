#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOffset.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Applies an offset for the evaluation time: <expression> offset 1d
    SQLQueryPiece offsetEvaluationTime(
        const PrometheusQueryTree::Offset * offset_node,
        SQLQueryPiece && expression,
        DurationType offset_value,
        ConverterContext & context)
    {
        expression.node = offset_node;

        /// A range vector keeps the timestamps of its samples, the range function consuming it shifts its grid instead.
        if (expression.type == ResultType::RANGE_VECTOR)
            return std::move(expression);

        switch (expression.store_method)
        {
            case StoreMethod::EMPTY:
            {
                return std::move(expression);
            }

            case StoreMethod::CONST_SCALAR:
            case StoreMethod::CONST_STRING:
            case StoreMethod::SINGLE_SCALAR:
            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                expression.start_time += offset_value;
                expression.end_time += offset_value;
                return std::move(expression);
            }

            case StoreMethod::RAW_DATA:
            {
                /// Can't get in here because RAW_DATA is used only for range vectors, and they are returned above as is.
                throwUnexpectedStoreMethod(expression, context);
            }
        }

        UNREACHABLE();
    }

    /// Applies a fixed evaluation time: <expression> @ 1609746000, @ start(), or @ end()
    SQLQueryPiece setEvaluationTime(
        const PrometheusQueryTree::Offset * offset_node, SQLQueryPiece && expression, ConverterContext & context)
    {
        /// A range vector already contains the samples evaluated at the fixed time. Keep its timestamps and, for a
        /// subquery, its complete inner grid intact. A range-vector function will aggregate it at the fixed time.
        if (expression.type == ResultType::RANGE_VECTOR)
        {
            expression.node = offset_node;
            return std::move(expression);
        }

        auto node_range = context.node_range_getter.get(offset_node);
        if (node_range.empty())
            return SQLQueryPiece{offset_node, offset_node->result_type, StoreMethod::EMPTY};

        /// <expression> is expected to be calculated at a fixed evaluation time.
        if (expression.start_time != expression.end_time)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Expression {} is expected to be calculated at a fixed evaluation time",
                            getPromQLText(expression, context));
        }

        expression.node = offset_node;

        switch (expression.store_method)
        {
            case StoreMethod::EMPTY:
            {
                return std::move(expression);
            }

            case StoreMethod::CONST_SCALAR:
            case StoreMethod::CONST_STRING:
            case StoreMethod::SINGLE_SCALAR:
            {
                expression.start_time = node_range.start_time;
                expression.end_time = node_range.end_time;
                expression.step = node_range.step;
                return std::move(expression);
            }

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                /// For scalar grid:
                /// SELECT arrayResize([], <count_of_time_steps>, values[1])) AS values
                /// FROM <scalar_grid>
                ///
                /// For vector grid:
                /// SELECT group,
                ///        arrayResize([], <count_of_time_steps>, values[1])) AS values
                /// FROM <vector_grid>
                SelectQueryBuilder builder;

                if (expression.store_method == StoreMethod::VECTOR_GRID)
                    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                auto new_values = makeASTFunction(
                    "arrayResize",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>(
                        stepsInTimeSeriesRange(node_range.start_time, node_range.end_time, node_range.step)),
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)));

                new_values->setAlias(ColumnNames::Values);
                builder.select_list.push_back(std::move(new_values));

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE);
                builder.from_table = subqueries.back().name;

                expression.select_query = builder.getSelectQuery();

                expression.start_time = node_range.start_time;
                expression.end_time = node_range.end_time;
                expression.step = node_range.step;
                return std::move(expression);
            }

            case StoreMethod::RAW_DATA:
            {
                /// Can't get in here because RAW_DATA is used only for range vectors, and they are returned above as is.
                throwUnexpectedStoreMethod(expression, context);
            }
        }

        UNREACHABLE();
    }
}

SQLQueryPiece applyOffset(const PrometheusQueryTree::Offset * offset_node, SQLQueryPiece && expression, ConverterContext & context)
{
    if (offset_node->hasAtModifier())
    {
        /// Set fixed evaluation time.
        return setEvaluationTime(offset_node, std::move(expression), context);
    }
    else if (auto offset_value = offset_node->offset_value)
    {
        /// Add offset to the evaluation time.
        return offsetEvaluationTime(offset_node, std::move(expression), *offset_value, context);
    }
    else
    {
        return expression;
    }
}

}
