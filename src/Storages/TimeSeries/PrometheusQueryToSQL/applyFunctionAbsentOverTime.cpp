#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionAbsentOverTime.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
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
/// Peels off `Offset` and `Subquery` wrappers and returns the underlying instant selector
/// (the one embedded in a `RangeSelector`, or a bare `InstantSelector` reached through a
/// subquery) if there is one, so we can derive labels from its matchers.
/// Returns nullptr if the argument is not backed by a selector (e.g.
/// `absent_over_time(sum(nonexistent)[5m:])`), in which case the produced sample has no labels.
const PrometheusQueryTree::InstantSelector * peelToInstantSelector(const Node * node)
{
    while (node->node_type == NodeType::Offset || node->node_type == NodeType::Subquery)
    {
        if (node->node_type == NodeType::Offset)
            node = static_cast<const PrometheusQueryTree::Offset *>(node)->getExpression();
        else
            node = static_cast<const PrometheusQueryTree::Subquery *>(node)->getExpression();
    }

    if (node->node_type == NodeType::RangeSelector)
        return static_cast<const PrometheusQueryTree::RangeSelector *>(node)->getInstantSelector();

    if (node->node_type == NodeType::InstantSelector)
        return static_cast<const PrometheusQueryTree::InstantSelector *>(node);

    return nullptr;
}

/// Builds the map of labels inferred from the input selector's matchers, using the same "smart"
/// label derivation logic as `absent()`. An empty `map` has type Map(Nothing, Nothing), so we
/// cast every inferred map to a stable type Map(String, String).
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

    return makeASTFunction("CAST", std::move(map), make_intrusive<ASTLiteral>("Map(String, String)"));
}
}


SQLQueryPiece applyFunctionAbsentOverTime(const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    if (arguments.size() != 1)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function 'absent_over_time' expects 1 argument, but was called with {} arguments",
            arguments.size());
    }

    auto & argument = arguments[0];
    if (argument.type != ResultType::RANGE_VECTOR)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function 'absent_over_time' expects an argument of type {}, but expression {} has type {}",
            ResultType::RANGE_VECTOR,
            getPromQLText(argument, context),
            argument.type);
    }

    const auto & node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};

    const auto start_time = node_range.start_time;
    const auto end_time = node_range.end_time;
    const auto step = node_range.step;
    const size_t num_steps = stepsInTimeSeriesRange(start_time, end_time, step);

    SQLQueryPiece res{function_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
    res.start_time = start_time;
    res.end_time = end_time;
    res.step = step;
    res.metric_name_dropped = true;

    /// timeSeriesTagsToGroup(<inferred labels>) AS group
    /// The synthetic sample's labels are derived from the input selector's matchers (same logic
    /// as `absent()`).
    ASTPtr inferred_group = makeASTFunction("timeSeriesTagsToGroup", makeInferredLabelsMap(function_node));
    inferred_group->setAlias(ColumnNames::Group);

    if (argument.store_method == StoreMethod::EMPTY)
    {
        /// The range vector is statically empty — produce a constant 1-element series (value 1 at
        /// every evaluation step) with the inferred labels. No subquery is read.
        SelectQueryBuilder builder;
        builder.select_list.push_back(std::move(inferred_group));
        builder.select_list.push_back(makeASTFunction(
            "arrayResize",
            make_intrusive<ASTLiteral>(Array{}),
            make_intrusive<ASTLiteral>(num_steps),
            timeSeriesScalarToAST(1, context.scalar_data_type)));
        builder.select_list.back()->setAlias(ColumnNames::Values);
        res.select_query = builder.getSelectQuery();
        return res;
    }

    /// The presence of samples cannot be known at translation time (the argument is a runtime
    /// subquery). Build a per-series presence grid with `present_over_time` semantics
    /// (timeSeriesPresentToGrid: 1 where the window has a sample, NULL otherwise), then aggregate
    /// across all series with `countForEach` and invert: emit 1 at grid points where no series has
    /// a sample, NULL otherwise. This mirrors `absent()` but over a range window.
    /// The grid keeps the metric name: it is a private intermediate that is collapsed across all
    /// series anyway, and dropping the name here could only manufacture duplicate label sets (e.g.
    /// a selector matching several metrics on the same tags), which the public path rejects.
    SQLQueryPiece presence_grid
        = applyFunctionOverRange(function_node, "present_over_time", std::move(arguments), context, /* drop_metric_name = */ false);

    /// A statically-empty presence grid means there is no data at all in the range: emit 1 at every
    /// step for the synthetic series.
    if (presence_grid.store_method == StoreMethod::EMPTY || !presence_grid.select_query)
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(std::move(inferred_group));
        builder.select_list.push_back(makeASTFunction(
            "arrayResize",
            make_intrusive<ASTLiteral>(Array{}),
            make_intrusive<ASTLiteral>(num_steps),
            timeSeriesScalarToAST(1, context.scalar_data_type)));
        builder.select_list.back()->setAlias(ColumnNames::Values);
        res.select_query = builder.getSelectQuery();
        return res;
    }

    SelectQueryBuilder builder;
    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(presence_grid.select_query), SQLSubqueryType::TABLE});
    builder.from_table = context.subqueries.back().name;

    builder.select_list.push_back(std::move(inferred_group));

    /// arrayResize(countForEach(values), <num_steps>, 0) — number of series present at each grid
    /// point; padded with 0 (no series) if the subquery returns no rows.
    auto presence_counts = makeASTFunction(
        "arrayResize",
        makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
        make_intrusive<ASTLiteral>(num_steps),
        make_intrusive<ASTLiteral>(0u));

    /// arrayMap(x -> if(x = 0, 1, NULL), <presence_counts>)
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
