#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>

#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFusedAggregationBinaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOffset.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySubquery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyUnaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>

#include <Common/Exception.h>


namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB::PrometheusQueryToSQL
{

namespace
{
    const NativeFragmentDescription * findNativeFragment(const Node * node, const ConverterContext & context)
    {
        const NativeFragmentDescription * result = nullptr;
        for (const auto & fragment : context.native_fragments)
        {
            if (fragment.node != node)
                continue;
            if (result)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Multiple native PromQL fragments reference the same query node");
            result = &fragment;
        }
        return result;
    }

    SQLQueryPiece makeNativeFragmentQueryPiece(
        const Node * node,
        const NativeFragmentDescription & native_fragment,
        ConverterContext & context)
    {
        chassert(native_fragment.node == node);

        const auto & range = context.node_range_getter.get(node);

        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        builder.from_table = native_fragment.table_name;

        SQLQueryPiece result{node, node->result_type, StoreMethod::VECTOR_GRID};
        result.metric_name_dropped = native_fragment.metric_name_dropped;
        result.start_time = range.start_time;
        result.end_time = range.end_time;
        result.step = range.step;
        result.select_query = builder.getSelectQuery();
        return result;
    }

    SQLQueryPiece visitNode(const Node * node, ConverterContext & context)
    {
        if (const auto * native_fragment = findNativeFragment(node, context))
            return makeNativeFragmentQueryPiece(node, *native_fragment, context);

        switch (node->node_type)
        {
            case NodeType::Scalar:
            {
                const auto * scalar_node = static_cast<const PrometheusQueryTree::Scalar *>(node);
                return fromLiteral(scalar_node, context);
            }

            case NodeType::StringLiteral:
            {
                const auto * string_node = static_cast<const PrometheusQueryTree::StringLiteral *>(node);
                return fromLiteral(string_node, context);
            }

            case NodeType::InstantSelector:
            {
                const auto * instant_selector = static_cast<const PrometheusQueryTree::InstantSelector *>(node);
                return fromSelector(instant_selector, context);
            }

            case NodeType::RangeSelector:
            {
                const auto * range_selector = static_cast<const PrometheusQueryTree::RangeSelector *>(node);
                return fromSelector(range_selector, context);
            }

            case NodeType::Subquery:
            {
                const auto * subquery_node = static_cast<const PrometheusQueryTree::Subquery *>(node);
                SQLQueryPiece expression = visitNode(subquery_node->getExpression(), context);
                return applySubquery(subquery_node, std::move(expression), context);
            }

            case NodeType::Offset:
            {
                const auto * offset_node = static_cast<const PrometheusQueryTree::Offset *>(node);
                SQLQueryPiece expression = visitNode(offset_node->getExpression(), context);
                return applyOffset(offset_node, std::move(expression), context);
            }

            case NodeType::Function:
            {
                const auto * function = static_cast<const PrometheusQueryTree::Function *>(node);
                std::vector<SQLQueryPiece> arguments;
                for (const auto * arg_node : function->getArguments())
                {
                    arguments.push_back(visitNode(arg_node, context));
                }
                return applyFunction(function, std::move(arguments), context);
            }

            case NodeType::UnaryOperator:
            {
                const auto * unary_operator = static_cast<const PrometheusQueryTree::UnaryOperator *>(node);
                SQLQueryPiece argument = visitNode(unary_operator->getArgument(), context);
                return applyUnaryOperator(unary_operator, std::move(argument), context);
            }

            case NodeType::BinaryOperator:
            {
                const auto * binary_operator = static_cast<const PrometheusQueryTree::BinaryOperator *>(node);

                if (canFuseAggregationBinaryOperator(binary_operator, context))
                {
                    const auto * aggregation
                        = static_cast<const PrometheusQueryTree::AggregationOperator *>(binary_operator->getLeftArgument());
                    SQLQueryPiece argument = visitNode(aggregation->getArguments().at(0), context);
                    return applyFusedAggregationBinaryOperator(binary_operator, std::move(argument), context);
                }

                SQLQueryPiece left_argument = visitNode(binary_operator->getLeftArgument(), context);
                SQLQueryPiece right_argument = visitNode(binary_operator->getRightArgument(), context);
                return applyBinaryOperator(binary_operator, std::move(left_argument), std::move(right_argument), context);
            }

            case NodeType::AggregationOperator:
            {
                const auto * aggregation_operator = static_cast<const PrometheusQueryTree::AggregationOperator *>(node);
                std::vector<SQLQueryPiece> arguments;
                for (const auto * arg_node : aggregation_operator->getArguments())
                {
                    arguments.push_back(visitNode(arg_node, context));
                }
                return applyAggregationOperator(aggregation_operator, std::move(arguments), context);
            }
        }

        UNREACHABLE();
    }
}


Converter::Converter(
    std::shared_ptr<const PrometheusQueryTree> promql_tree_,
    PrometheusQueryEvaluationSettings settings_,
    NativeFragmentDescriptions native_fragments_)
    : promql_tree(std::move(promql_tree_))
    , settings(std::move(settings_))
    , native_fragments(std::move(native_fragments_))
    , result_type(DB::PrometheusQueryToSQL::getResultType(*promql_tree, settings))
{
    if (native_fragments.size() > MAX_NATIVE_FRAGMENTS)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PromQL converter supports at most {} native fragments, got {}",
            MAX_NATIVE_FRAGMENTS,
            native_fragments.size());
}


ColumnsDescription Converter::getResultColumns() const
{
    return DB::PrometheusQueryToSQL::getResultColumns(*promql_tree, settings);
}


ASTPtr Converter::getSQL() const
{
    ConverterContext context{promql_tree, settings, native_fragments};
    auto query_piece = visitNode(promql_tree->getRoot(), context);
    query_piece.type = result_type;
    return finalizeSQL(std::move(query_piece), context);
}

}
