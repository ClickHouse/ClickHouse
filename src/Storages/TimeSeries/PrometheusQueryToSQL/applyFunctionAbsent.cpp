#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionAbsent.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <Common/Exception.h>

#include <map>
#include <unordered_set>


namespace DB::ErrorCodes
{
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
const PrometheusQueryTree::InstantSelector * peelToInstantSelector(const Node * node)
{
    while (node->node_type == NodeType::Offset)
        node = static_cast<const PrometheusQueryTree::Offset *>(node)->getExpression();

    if (node->node_type == NodeType::InstantSelector)
        return static_cast<const PrometheusQueryTree::InstantSelector *>(node);

    return nullptr;
}

ASTPtr makeInferredLabelsMap(const PrometheusQueryTree::Function * function_node)
{
    std::map<String, String> labels;
    /// This set deliberately stays monotonic, matching Prometheus's historic `has` map:
    /// a later matcher can delete an inferred label, but it cannot unlock that label name
    /// for a subsequent equality matcher.
    std::unordered_set<String> labels_with_equality_matcher;

    const auto * selector = peelToInstantSelector(function_node->getArguments().at(0));
    if (selector)
    {
        for (const auto & matcher : selector->matchers)
        {
            if (matcher.label_name == kMetricName)
                continue;

            if (matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ && !labels_with_equality_matcher.contains(matcher.label_name))
            {
                labels_with_equality_matcher.insert(matcher.label_name);
                if (!matcher.label_value.empty())
                    labels[matcher.label_name] = matcher.label_value;
                else
                    /// Prometheus treats an empty label value as a missing label:
                    /// labels.Builder::Set(name, "") deletes that label.
                    labels.erase(matcher.label_name);
            }
            else
            {
                labels.erase(matcher.label_name);
            }
        }
    }

    auto map = makeASTFunction("map");
    for (const auto & [label_name, label_value] : labels)
    {
        map->arguments->children.push_back(make_intrusive<ASTLiteral>(label_name));
        map->arguments->children.push_back(make_intrusive<ASTLiteral>(label_value));
    }

    /// An empty `map` has type Map(Nothing, Nothing), so cast every inferred map to a stable type.
    return makeASTFunction("CAST", std::move(map), make_intrusive<ASTLiteral>("Map(String, String)"));
}
}


SQLQueryPiece applyFunctionAbsent(const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    if (arguments.size() != 1)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function 'absent' expects 1 argument, but was called with {} arguments",
            arguments.size());
    }

    auto & argument = arguments[0];
    if (argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function 'absent' expects an argument of type {}, but expression {} has type {}",
            ResultType::INSTANT_VECTOR,
            getPromQLText(argument, context),
            argument.type);
    }

    const auto & node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};

    argument = toVectorGrid(std::move(argument), context);

    SQLQueryPiece res = argument;
    res.node = function_node;
    res.start_time = node_range.start_time;
    res.end_time = node_range.end_time;
    res.step = node_range.step;
    res.metric_name_dropped = true;

    SelectQueryBuilder builder;
    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
    builder.from_table = context.subqueries.back().name;

    builder.select_list.push_back(makeASTFunction("timeSeriesTagsToGroup", makeInferredLabelsMap(function_node)));
    builder.select_list.back()->setAlias(ColumnNames::Group);

    const size_t num_steps = stepsInTimeSeriesRange(res.start_time, res.end_time, res.step);
    auto presence_counts = makeASTFunction(
        "arrayResize",
        makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
        make_intrusive<ASTLiteral>(num_steps),
        make_intrusive<ASTLiteral>(0u));

    builder.select_list.push_back(makeASTFunction(
        "arrayMap",
        makeASTLambda(
            {"x"},
            makeASTFunction(
                "if",
                makeASTFunction("equals", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(0u)),
                timeSeriesScalarToAST(1, context.scalar_data_type),
                make_intrusive<ASTLiteral>(Field{}))),
        std::move(presence_counts)));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    res.select_query = builder.getSelectQuery();
    return res;
}

}
