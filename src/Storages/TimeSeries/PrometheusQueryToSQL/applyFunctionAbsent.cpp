#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionAbsent.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
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
    /// Peels off `Offset` wrappers and returns the underlying instant selector if the argument is a bare instant or
    /// range selector, so we can derive tags from its matchers.
    /// Returns nullptr for anything else, in which case the produced sample has no tags. In particular a subquery
    /// never infers tags, even a selector-backed one like `absent_over_time(nonexistent[5m:1m])`:
    /// Prometheus's createLabelsForAbsentFunction() only looks at a vector selector or a matrix selector.
    const PrometheusQueryTree::InstantSelector * peelToInstantSelector(const Node * node)
    {
        while (node->node_type == NodeType::Offset)
            node = static_cast<const PrometheusQueryTree::Offset *>(node)->getExpression();

        if (node->node_type == NodeType::RangeSelector)
            return static_cast<const PrometheusQueryTree::RangeSelector *>(node)->getInstantSelector();

        if (node->node_type == NodeType::InstantSelector)
            return static_cast<const PrometheusQueryTree::InstantSelector *>(node);

        return nullptr;
    }

    /// Builds the tags of the synthetic series from the input selector's matchers, following Prometheus's
    /// createLabelsForAbsentFunction(). An empty `map` has type Map(Nothing, Nothing), so the map is cast to a stable
    /// type Map(String, String).
    ASTPtr makeTagsForAbsentFunction(const PrometheusQueryTree::Function * function_node)
    {
        std::map<String, String> tags;
        /// This set deliberately stays monotonic, matching Prometheus's historic `has` map:
        /// a later matcher can delete a tag, but it cannot unlock that tag name
        /// for a subsequent equality matcher.
        std::unordered_set<String> tags_with_equality_matcher;

        const auto * selector = peelToInstantSelector(function_node->getArguments().at(0));
        if (selector)
        {
            for (const auto & matcher : selector->matchers)
            {
                if (matcher.label_name == kMetricName)
                    continue;

                if (matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ && !tags_with_equality_matcher.contains(matcher.label_name))
                {
                    tags_with_equality_matcher.insert(matcher.label_name);
                    if (!matcher.label_value.empty())
                        tags[matcher.label_name] = matcher.label_value;
                    else
                        /// Prometheus treats an empty label value as a missing label:
                        /// labels.Builder::Set(name, "") deletes that label.
                        tags.erase(matcher.label_name);
                }
                else
                {
                    tags.erase(matcher.label_name);
                }
            }
        }

        auto map = makeASTFunction("map");
        for (const auto & [tag_name, tag_value] : tags)
        {
            map->arguments->children.push_back(make_intrusive<ASTLiteral>(tag_name));
            map->arguments->children.push_back(make_intrusive<ASTLiteral>(tag_value));
        }

        return makeASTFunction("CAST", std::move(map), make_intrusive<ASTLiteral>("Map(String, String)"));
    }

}


SQLQueryPiece applyFunctionAbsent(const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto function_name = function_node->function_name;
    const auto expected_argument_type = (function_name == "absent_over_time") ? ResultType::RANGE_VECTOR : ResultType::INSTANT_VECTOR;

    if (arguments.size() != 1)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function '{}' expects 1 argument, but was called with {} arguments",
            function_name,
            arguments.size());
    }

    const auto & argument = arguments[0];
    if (argument.type != expected_argument_type)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function '{}' expects an argument of type {}, but expression {} has type {}",
            function_name,
            expected_argument_type,
            getPromQLText(argument, context),
            argument.type);
    }

    const auto & node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};

    /// The presence grid: one row per series, and `values` has one element per evaluation step which is non-NULL
    /// where the series is present at that step.
    SQLQueryPiece presence_grid{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    if (argument.type == ResultType::RANGE_VECTOR)
    {
        /// For `absent_over_time` a series is present at a step if its range window has at least one sample, which is
        /// what `present_over_time` calculates. The grid keeps the metric name: it is a private intermediate that is
        /// collapsed across all series anyway, and dropping the name here could only manufacture duplicate sets of tags
        /// (e.g. a selector matching several metrics on the same tags), which the public path rejects.
        presence_grid = applyFunctionOverRange(function_node, "present_over_time", std::move(arguments), context, /* drop_metric_name = */ false);
    }
    else
    {
        switch (argument.store_method)
        {
            case StoreMethod::EMPTY:
            {
                /// The instant vector is statically empty, and so is the presence grid.
                break;
            }

            case StoreMethod::CONST_SCALAR:
            case StoreMethod::SINGLE_SCALAR:
            case StoreMethod::SCALAR_GRID:
            {
                /// A scalar converted to an instant vector (e.g. `vector(1)`) has a value at every evaluation step
                /// (NaN is a value too), so `absent` of it is always empty.
                return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};
            }

            case StoreMethod::VECTOR_GRID:
            {
                /// An instant vector stored as a grid is already a presence grid.
                presence_grid = std::move(arguments[0]);
                break;
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            {
                /// Can't get in here because these store methods are incompatible with an instant vector.
                throwUnexpectedStoreMethod(argument, context);
            }
        }
    }

    const size_t num_steps = stepsInTimeSeriesRange(node_range.start_time, node_range.end_time, node_range.step);

    SQLQueryPiece res{function_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
    res.start_time = node_range.start_time;
    res.end_time = node_range.end_time;
    res.step = node_range.step;
    res.metric_name_dropped = true;

    /// timeSeriesTagsToGroup(<tags>) AS group
    ASTPtr group = makeASTFunction("timeSeriesTagsToGroup", makeTagsForAbsentFunction(function_node));
    group->setAlias(ColumnNames::Group);

    if (presence_grid.store_method == StoreMethod::EMPTY)
    {
        /// No series is present at any step, so the synthetic series has the value 1 at every step:
        /// SELECT <group>, arrayResize([], <num_steps>, 1) AS values
        SelectQueryBuilder builder;
        builder.select_list.push_back(std::move(group));
        builder.select_list.push_back(makeASTFunction(
            "arrayResize",
            make_intrusive<ASTLiteral>(Array{}),
            make_intrusive<ASTLiteral>(num_steps),
            timeSeriesScalarToAST(1)));
        builder.select_list.back()->setAlias(ColumnNames::Values);
        res.select_query = builder.getSelectQuery();
        return res;
    }

    /// The presence grid is aggregated below without keys, and such an aggregation over an empty input returns no rows
    /// if the setting `empty_result_for_aggregation_by_empty_set` is enabled. However an empty input is exactly the case
    /// when the synthetic series must be produced, so a neutral row is added to the input: an array of NULLs with one
    /// element per step. It doesn't change any count but guarantees that the aggregation always has an input row.
    ///
    /// SELECT arrayResize(CAST([], 'Array(Nullable(Float64))'), <num_steps>, NULL) AS values
    String neutral_row_subquery;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(makeASTFunction(
            "arrayResize",
            makeASTFunction(
                "CAST",
                make_intrusive<ASTLiteral>(Array{}),
                make_intrusive<ASTLiteral>("Array(Nullable(Float64))")),
            make_intrusive<ASTLiteral>(num_steps),
            make_intrusive<ASTLiteral>(Field{})));
        builder.select_list.back()->setAlias(ColumnNames::Values);
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), builder.getSelectQuery(), SQLSubqueryType::TABLE});
        neutral_row_subquery = context.subqueries.back().name;
    }

    /// SELECT values FROM <presence_grid> UNION ALL SELECT values FROM <neutral_row>
    String presence_values_subquery;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(presence_grid.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;
        builder.union_table = neutral_row_subquery;
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), builder.getSelectQuery(), SQLSubqueryType::TABLE});
        presence_values_subquery = context.subqueries.back().name;
    }

    /// countForEach(values) is the number of series present at each step. The synthetic series has the value 1 where
    /// that number is zero and no value (NULL) where at least one series is present:
    /// SELECT <group>,
    ///        arrayMap(x -> if(x = 0, 1, NULL), countForEach(values)) AS values
    /// FROM <presence_values>
    SelectQueryBuilder builder;
    builder.from_table = presence_values_subquery;
    builder.select_list.push_back(std::move(group));
    builder.select_list.push_back(makeASTFunction(
        "arrayMap",
        makeASTLambda(
            {"x"},
            makeASTFunction(
                "if",
                makeASTFunction("equals", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(0u)),
                timeSeriesScalarToAST(1),
                make_intrusive<ASTLiteral>(Field{}))),
        makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values))));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    res.select_query = builder.getSelectQuery();
    return res;
}

}
