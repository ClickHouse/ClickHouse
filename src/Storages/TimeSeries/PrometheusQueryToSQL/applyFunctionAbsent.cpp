#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionAbsent.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <Common/Exception.h>

#include <map>
#include <unordered_set>
#include <fmt/format.h>


namespace DB::ErrorCodes
{
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
using Labels = std::map<String, String>;

bool isAbsentOverTime(std::string_view function_name)
{
    return function_name == "absent_over_time";
}

ResultType getExpectedArgumentType(std::string_view function_name)
{
    return isAbsentOverTime(function_name) ? ResultType::RANGE_VECTOR : ResultType::INSTANT_VECTOR;
}

void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    size_t expected_number_of_arguments = 1;
    if (arguments.size() != expected_number_of_arguments)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function '{}' expects {} argument, but was called with {} arguments",
            function_name,
            expected_number_of_arguments,
            arguments.size());
    }

    const auto & argument = arguments[0];
    auto expected_type = getExpectedArgumentType(function_name);
    if (argument.type != expected_type)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function '{}' expects an argument of type {}, but expression {} has type {}",
            function_name,
            expected_type,
            getPromQLText(argument, context),
            argument.type);
    }
}

const PQT::MatcherList * getSelectorMatchers(const Node * node)
{
    if (node->node_type == NodeType::Offset)
        return getSelectorMatchers(static_cast<const PQT::Offset *>(node)->getExpression());

    if (node->node_type == NodeType::InstantSelector)
        return &static_cast<const PQT::InstantSelector *>(node)->matchers;

    if (node->node_type == NodeType::RangeSelector)
        return &static_cast<const PQT::RangeSelector *>(node)->getInstantSelector()->matchers;

    return nullptr;
}

Labels getLabelsForAbsentFunction(const Node * argument_node)
{
    Labels labels;
    const auto * matchers = getSelectorMatchers(argument_node);
    if (!matchers)
        return labels;

    std::unordered_set<String> seen_label_names;
    for (const auto & matcher : *matchers)
    {
        if (matcher.label_name == kMetricName)
            continue;

        if ((matcher.matcher_type == PQT::MatcherType::EQ) && !seen_label_names.contains(matcher.label_name))
        {
            labels[matcher.label_name] = matcher.label_value;
            seen_label_names.insert(matcher.label_name);
        }
        else
        {
            labels.erase(matcher.label_name);
        }
    }

    return labels;
}

ASTPtr makeSyntheticGroupAST(const Labels & labels)
{
    auto group = makeASTFunction("timeSeriesTagsToGroup", make_intrusive<ASTLiteral>(Array{}));
    for (const auto & [name, value] : labels)
    {
        group->arguments->children.push_back(make_intrusive<ASTLiteral>(name));
        group->arguments->children.push_back(make_intrusive<ASTLiteral>(value));
    }
    return group;
}

ASTPtr makeAbsentValuesAST(const NodeEvaluationRange & node_range, ConverterContext & context)
{
    auto values = makeASTFunction(
        "arrayResize",
        make_intrusive<ASTLiteral>(Array{}),
        make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(node_range.start_time, node_range.end_time, node_range.step)),
        timeSeriesScalarToAST(1, context.scalar_data_type));

    return makeASTFunction(
        "CAST", std::move(values), make_intrusive<ASTLiteral>(fmt::format("Array(Nullable({}))", context.scalar_data_type->getName())));
}

ASTPtr makeAbsentValuesFromCountsAST(const NodeEvaluationRange & node_range, ConverterContext & context)
{
    auto counts = makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values));
    auto zero_counts = makeASTFunction(
        "arrayResize",
        make_intrusive<ASTLiteral>(Array{}),
        make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(node_range.start_time, node_range.end_time, node_range.step)),
        make_intrusive<ASTLiteral>(0u));

    auto counts_or_zero_counts
        = makeASTFunction("if", makeASTFunction("empty", counts->clone()), std::move(zero_counts), std::move(counts));

    auto lambda_args = makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"));
    auto absent_or_null = makeASTFunction(
        "if",
        makeASTFunction("equals", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(0u)),
        timeSeriesScalarToAST(1, context.scalar_data_type),
        make_intrusive<ASTLiteral>(Null{}));

    return makeASTFunction(
        "CAST",
        makeASTFunction(
            "arrayMap", makeASTFunction("lambda", std::move(lambda_args), std::move(absent_or_null)), std::move(counts_or_zero_counts)),
        make_intrusive<ASTLiteral>(fmt::format("Array(Nullable({}))", context.scalar_data_type->getName())));
}

SQLQueryPiece makeAbsentResult(const Node * node, const Labels & labels, const NodeEvaluationRange & node_range, ConverterContext & context)
{
    SQLQueryPiece res{node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};

    SelectQueryBuilder builder;

    builder.select_list.push_back(makeSyntheticGroupAST(labels));
    builder.select_list.back()->setAlias(ColumnNames::Group);

    builder.select_list.push_back(makeAbsentValuesAST(node_range, context));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    res.select_query = builder.getSelectQuery();
    res.start_time = node_range.start_time;
    res.end_time = node_range.end_time;
    res.step = node_range.step;
    res.metric_name_dropped = true;

    return res;
}

SQLQueryPiece makeEmptyResult(const Node * node)
{
    return SQLQueryPiece{node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};
}

SQLQueryPiece buildAbsentResultFromVectorGrid(
    const Node * node,
    SQLQueryPiece && vector_grid,
    const Labels & labels,
    const NodeEvaluationRange & node_range,
    ConverterContext & context)
{
    chassert(vector_grid.store_method == StoreMethod::VECTOR_GRID);

    SQLQueryPiece res{node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};

    SelectQueryBuilder builder;

    builder.select_list.push_back(makeSyntheticGroupAST(labels));
    builder.select_list.back()->setAlias(ColumnNames::Group);

    builder.select_list.push_back(makeAbsentValuesFromCountsAST(node_range, context));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    context.subqueries.emplace_back(context.subqueries.size(), std::move(vector_grid.select_query), SQLSubqueryType::TABLE);
    builder.from_table = context.subqueries.back().name;

    res.select_query = builder.getSelectQuery();
    res.start_time = node_range.start_time;
    res.end_time = node_range.end_time;
    res.step = node_range.step;
    res.metric_name_dropped = true;

    return res;
}

SQLQueryPiece
buildPresenceGridForRangeArgument(SQLQueryPiece && argument, const NodeEvaluationRange & node_range, ConverterContext & context)
{
    bool has_group = false;
    ASTPtr timestamps;
    ASTPtr values;

    switch (argument.store_method)
    {
        case StoreMethod::VECTOR_GRID:
        {
            has_group = true;

            auto ts = makeASTFunction(
                "timeSeriesFromGrid",
                timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(argument.step, context.timestamp_data_type),
                make_intrusive<ASTIdentifier>(ColumnNames::Values));
            ts->setAlias(ColumnNames::TimeSeries);

            timestamps = makeASTFunction("tupleElement", std::move(ts), make_intrusive<ASTLiteral>(1));
            values = makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries), make_intrusive<ASTLiteral>(2));
            break;
        }

        case StoreMethod::RAW_DATA:
        {
            has_group = true;
            timestamps = make_intrusive<ASTIdentifier>(ColumnNames::Timestamp);
            values = make_intrusive<ASTIdentifier>(ColumnNames::Value);
            break;
        }

        case StoreMethod::EMPTY:
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::CONST_STRING:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            throwUnexpectedStoreMethod(argument, context);
        }
    }

    chassert(has_group);
    chassert(timestamps);
    chassert(values);

    SQLQueryPiece res{argument.node, ResultType::RANGE_VECTOR, StoreMethod::VECTOR_GRID};

    SelectQueryBuilder builder;
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    builder.select_list.push_back(addParametersToAggregateFunction(
        makeASTFunction("timeSeriesLastToGrid", std::move(timestamps), std::move(values)),
        timeSeriesTimestampToAST(node_range.start_time, context.timestamp_data_type),
        timeSeriesTimestampToAST(node_range.end_time, context.timestamp_data_type),
        timeSeriesDurationToAST(node_range.step, context.timestamp_data_type),
        timeSeriesDurationToAST(node_range.window, context.timestamp_data_type)));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    context.subqueries.emplace_back(context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE);
    builder.from_table = context.subqueries.back().name;

    res.select_query = builder.getSelectQuery();
    res.start_time = node_range.start_time;
    res.end_time = node_range.end_time;
    res.step = node_range.step;

    return res;
}
}


bool isFunctionAbsent(std::string_view function_name)
{
    return (function_name == "absent") || isAbsentOverTime(function_name);
}


SQLQueryPiece applyFunctionAbsent(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionAbsent(function_name));

    checkArgumentTypes(function_node, arguments, context);

    auto node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto labels = getLabelsForAbsentFunction(function_node->getArguments().at(0));
    auto argument = std::move(arguments[0]);

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            return makeAbsentResult(function_node, labels, node_range, context);
        }

        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            return makeEmptyResult(function_node);
        }

        case StoreMethod::VECTOR_GRID:
        {
            if (isAbsentOverTime(function_name))
                argument = buildPresenceGridForRangeArgument(std::move(argument), node_range, context);
            return buildAbsentResultFromVectorGrid(function_node, std::move(argument), labels, node_range, context);
        }

        case StoreMethod::RAW_DATA:
        {
            chassert(isAbsentOverTime(function_name));
            argument = buildPresenceGridForRangeArgument(std::move(argument), node_range, context);
            return buildAbsentResultFromVectorGrid(function_node, std::move(argument), labels, node_range, context);
        }

        case StoreMethod::CONST_STRING:
        {
            throwUnexpectedStoreMethod(argument, context);
        }
    }

    UNREACHABLE();
}

}
