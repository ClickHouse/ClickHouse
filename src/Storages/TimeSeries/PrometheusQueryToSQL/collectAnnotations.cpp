#include <Storages/TimeSeries/PrometheusQueryToSQL/collectAnnotations.h>

#include <IO/DoubleConverter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <base/scope_guard.h>

#include <unordered_map>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    using AggregationOperatorNode = PrometheusQueryTree::AggregationOperator;
    using FunctionNode = PrometheusQueryTree::Function;
    using OffsetNode = PrometheusQueryTree::Offset;
    using InstantSelectorNode = PrometheusQueryTree::InstantSelector;
    using RangeSelectorNode = PrometheusQueryTree::RangeSelector;
    using SubqueryNode = PrometheusQueryTree::Subquery;

    constexpr std::string_view COUNTER_SUFFIXES[] = {"_total", "_sum", "_count", "_bucket"};

    bool hasCounterSuffix(std::string_view metric_name)
    {
        for (auto suffix : COUNTER_SUFFIXES)
        {
            if (metric_name.ends_with(suffix))
                return true;
        }
        return false;
    }

    /// Formats a number like "%g" in Go: the shortest digits, in the exponent form if the exponent is below -4 or at least 6.
    String formatFloatLikeGo(Float64 value)
    {
        if (std::isnan(value))
            return "NaN";
        if (std::isinf(value))
            return (value > 0) ? "+Inf" : "-Inf";

        char digits[32];
        bool sign = false;
        int length = 0;
        int point = 0;
        double_conversion::DoubleToStringConverter::DoubleToAscii(
            value, double_conversion::DoubleToStringConverter::SHORTEST, 0, digits, sizeof(digits), &sign, &length, &point);

        /// The value is 0.<digits> * 10^point.
        String res = sign ? "-" : "";
        const int exponent = point - 1;
        if (exponent < -4 || exponent >= 6)
        {
            res += digits[0];
            if (length > 1)
            {
                res += '.';
                res.append(digits + 1, length - 1);
            }
            res += (exponent < 0) ? "e-" : "e+";
            if (std::abs(exponent) < 10)
                res += '0';
            res += std::to_string(std::abs(exponent));
        }
        else if (point <= 0)
        {
            res += "0.";
            res.append(-point, '0');
            res.append(digits, length);
        }
        else if (point >= length)
        {
            res.append(digits, length);
            res.append(point - length, '0');
        }
        else
        {
            res.append(digits, point);
            res += '.';
            res.append(digits + point, length - point);
        }
        return res;
    }

    bool isInvalidQuantile(const SQLQueryPiece & phi)
    {
        return (phi.store_method == StoreMethod::CONST_SCALAR)
            && (std::isnan(phi.scalar_value) || (phi.scalar_value < 0) || (phi.scalar_value > 1));
    }

    /// Prometheus: util/annotations/annotations.go, NewInvalidQuantileWarning().
    void addInvalidQuantileWarning(const Node * phi_node, ScalarType phi, ASTPtr probe, ConverterContext & context)
    {
        context.annotations->push_back(Annotation{
            .is_info = false,
            .message = "PromQL warning: quantile value should be between 0 and 1, got " + formatFloatLikeGo(phi),
            .position = phi_node->start_pos.value_or(0),
            .probe = std::move(probe)});
    }

    const Node * skipOffsets(const Node * node)
    {
        while (node->node_type == NodeType::Offset)
            node = static_cast<const OffsetNode *>(node)->getExpression();
        return node;
    }

    /// Returns the selector whose series give the metric names of a range-vector argument: the argument or the expression of its subquery.
    const Node * getNamedSelector(const Node * argument)
    {
        const Node * node = skipOffsets(argument);
        if (node->node_type == NodeType::Subquery)
            node = skipOffsets(static_cast<const SubqueryNode *>(node)->getExpression());
        if ((node->node_type == NodeType::RangeSelector) || (node->node_type == NodeType::InstantSelector))
            return node;
        return nullptr;
    }

    const PrometheusQueryTree::MatcherList & getMatchers(const Node * selector)
    {
        if (selector->node_type == NodeType::RangeSelector)
            return static_cast<const RangeSelectorNode *>(selector)->getInstantSelector()->matchers;
        return static_cast<const InstantSelectorNode *>(selector)->matchers;
    }

    ASTPtr makeLambdaIsNotNull()
    {
        return makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
            makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")));
    }

    /// Converts `argument` again with its own subqueries and applies `function_name` to it without dropping the metric name.
    /// Returns a query selecting the column `name` (the metric names if `by_metric_name`) of the grid rows having a value.
    ASTPtr makeProbe(
        const Node * function_node,
        std::string_view function_name,
        const Node * argument,
        bool by_metric_name,
        ConverterContext & context,
        ConvertNodeFunction convert_node)
    {
        auto saved_subqueries = std::exchange(context.subqueries, {});
        auto * saved_annotations = std::exchange(context.annotations, nullptr);
        if (by_metric_name)
            context.non_counter_selector = getNamedSelector(argument);

        SCOPE_EXIT({
            context.subqueries = std::move(saved_subqueries);
            context.annotations = saved_annotations;
            context.non_counter_selector = nullptr;
        });

        std::vector<SQLQueryPiece> arguments;
        arguments.push_back(convert_node(argument, context));
        auto grid = applyFunctionOverRange(function_node, function_name, std::move(arguments), context, /* drop_metric_name = */ false);
        if ((grid.store_method != StoreMethod::VECTOR_GRID) || (by_metric_name && grid.metric_name_dropped))
            return nullptr;

        SelectQueryBuilder builder;
        builder.with = std::move(context.subqueries);
        builder.with.emplace_back(builder.with.size(), std::move(grid.select_query), SQLSubqueryType::TABLE);
        builder.from_table = builder.with.back().name;

        ASTPtr has_value = makeASTFunction("arrayExists", makeLambdaIsNotNull(), make_intrusive<ASTIdentifier>(ColumnNames::Values));

        if (!by_metric_name)
        {
            /// SELECT '' AS name FROM <grid> WHERE arrayExists(x -> isNotNull(x), values) LIMIT 1
            builder.select_list.push_back(make_intrusive<ASTLiteral>(String{}));
            builder.select_list.back()->setAlias("name");
            builder.where = std::move(has_value);
            builder.limit = 1;
            return builder.getSelectQuery();
        }

        /// SELECT coalesce(timeSeriesExtractTag(group, '__name__'), '') AS name FROM <grid> WHERE arrayExists(x -> isNotNull(x), values)
        /// AND (name != '') AND NOT (endsWith(name, '_total') OR ...) GROUP BY name ORDER BY name
        builder.select_list.push_back(makeASTFunction(
            "coalesce",
            makeASTFunction("timeSeriesExtractTag", make_intrusive<ASTIdentifier>(ColumnNames::Group), make_intrusive<ASTLiteral>(kMetricName)),
            make_intrusive<ASTLiteral>(String{})));
        builder.select_list.back()->setAlias("name");

        ASTs has_counter_suffix;
        for (auto suffix : COUNTER_SUFFIXES)
            has_counter_suffix.push_back(
                makeASTFunction("endsWith", make_intrusive<ASTIdentifier>("name"), make_intrusive<ASTLiteral>(String{suffix})));

        builder.where = makeASTFunction(
            "and",
            std::move(has_value),
            makeASTFunction("notEquals", make_intrusive<ASTIdentifier>("name"), make_intrusive<ASTLiteral>(String{})),
            makeASTFunction("not", makeASTFunction("or", std::move(has_counter_suffix))));

        builder.group_by.push_back(make_intrusive<ASTIdentifier>("name"));
        builder.order_by.push_back(make_intrusive<ASTIdentifier>("name"));
        builder.order_direction = 1;
        return builder.getSelectQuery();
    }

    /// Prometheus: promql/engine.go:1905-1912, an info for each metric name without a counter suffix for which rate() has a value.
    void addPossibleNonCounterInfo(
        const FunctionNode * function_node, const SQLQueryPiece & argument, ConverterContext & context, ConvertNodeFunction convert_node)
    {
        if (argument.metric_name_dropped)
            return;

        const Node * argument_node = function_node->getArguments().at(0);
        if (const auto * selector = getNamedSelector(argument_node))
        {
            for (const auto & matcher : getMatchers(selector))
            {
                if ((matcher.label_name == kMetricName) && (matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ)
                    && hasCounterSuffix(matcher.label_value))
                    return;
            }
        }

        auto probe = makeProbe(function_node, function_node->function_name, argument_node, /* by_metric_name = */ true, context, convert_node);
        if (!probe)
            return;

        /// The engine replaces a subquery argument with a new matrix selector without a position, so Prometheus reports "1:1".
        context.annotations->push_back(Annotation{
            .is_info = true,
            .message = "PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: ",
            .position = (skipOffsets(argument_node)->node_type == NodeType::Subquery) ? 0 : argument_node->start_pos.value_or(0),
            .probe = std::move(probe),
            .append_name = true});
    }

    void collectPositions(const Node * left, const Node * right, std::unordered_map<size_t, size_t> & positions)
    {
        if (left->start_pos && right->start_pos)
            positions.emplace(*left->start_pos, *right->start_pos);
        for (size_t i = 0; i != std::min(left->children.size(), right->children.size()); ++i)
            collectPositions(left->children[i], right->children[i], positions);
    }
}


void collectAnnotations(
    const Node * node, const std::vector<SQLQueryPiece> & arguments, ConverterContext & context, ConvertNodeFunction convert_node)
{
    if (!context.annotations || context.node_range_getter.get(node).empty())
        return;

    if (node->node_type == NodeType::AggregationOperator)
    {
        /// Prometheus: promql/engine.go:1452-1460.
        const auto * aggregation_node = static_cast<const AggregationOperatorNode *>(node);
        if ((aggregation_node->operator_name == "quantile") && (arguments.size() == 2) && isInvalidQuantile(arguments[0]))
            addInvalidQuantileWarning(aggregation_node->getArguments().at(0), arguments[0].scalar_value, nullptr, context);
        return;
    }

    if (node->node_type != NodeType::Function)
        return;

    const auto * function_node = static_cast<const FunctionNode *>(node);
    const auto & function_name = function_node->function_name;

    if ((function_name == "histogram_quantile") && (arguments.size() == 2) && isInvalidQuantile(arguments[0]))
    {
        /// Prometheus: promql/functions.go:1430-1432, checked at every step even without data.
        addInvalidQuantileWarning(function_node->getArguments().at(0), arguments[0].scalar_value, nullptr, context);
    }
    else if ((function_name == "quantile_over_time") && (arguments.size() == 2) && isInvalidQuantile(arguments[0]))
    {
        /// Prometheus: promql/functions.go:935-943, only if some window has a sample.
        auto probe = makeProbe(
            function_node, "count_over_time", function_node->getArguments().at(1), /* by_metric_name = */ false, context, convert_node);
        if (probe)
            addInvalidQuantileWarning(function_node->getArguments().at(0), arguments[0].scalar_value, std::move(probe), context);
    }
    else if (((function_name == "rate") || (function_name == "increase")) && (arguments.size() == 1))
    {
        addPossibleNonCounterInfo(function_node, arguments[0], context, convert_node);
    }
}


void repeatAnnotationsForEqualArgument(
    const Node * left_argument, const Node * right_argument, size_t first_annotation, ConverterContext & context)
{
    if (!context.annotations)
        return;

    std::unordered_map<size_t, size_t> positions;
    collectPositions(left_argument, right_argument, positions);

    auto & annotations = *context.annotations;
    size_t end = annotations.size();
    for (size_t i = first_annotation; i != end; ++i)
    {
        Annotation annotation = annotations[i];
        if (auto it = positions.find(annotation.position); it != positions.end())
            annotation.position = it->second;
        annotations.push_back(std::move(annotation));
    }
}

}
