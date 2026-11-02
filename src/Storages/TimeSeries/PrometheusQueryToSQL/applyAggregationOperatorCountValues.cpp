#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperatorCountValues.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <Common/Exception.h>
#include <Common/isValidUTF8.h>
#include <Common/quoteString.h>

#include <algorithm>


namespace DB::ErrorCodes
{
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
constexpr const char * SAMPLE = "sample";
constexpr const char * TIME_INDEX = "time_index";
constexpr const char * LABEL_VALUE = "label_value";
constexpr const char * SET_GROUP = "set_group";
constexpr const char * VALUE_COUNT = "value_count";
constexpr const char * COUNT_PAIRS = "count_pairs";
constexpr const char * COUNTS = "counts";

void checkArgumentTypes(
    const PrometheusQueryTree::AggregationOperator * operator_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
{
    const auto & operator_name = operator_node->operator_name;

    if (arguments.size() != 2)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Aggregation operator '{}' expects 2 arguments, but was called with {} arguments",
            operator_name,
            arguments.size());
    }

    if (arguments[0].type != ResultType::STRING)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Aggregation operator '{}' expects first argument of type {}, but expression {} has type {}",
            operator_name,
            ResultType::STRING,
            getPromQLText(arguments[0], context),
            arguments[0].type);
    }

    if (arguments[1].type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Aggregation operator '{}' expects second argument of type {}, but expression {} has type {}",
            operator_name,
            ResultType::INSTANT_VECTOR,
            getPromQLText(arguments[1], context),
            arguments[1].type);
    }
}

ASTPtr setValueLabel(ASTPtr group, const String & label_name)
{
    auto tags_without_value_label = makeASTFunction(
        "timeSeriesGroupToTags", makeASTFunction("timeSeriesRemoveTag", std::move(group), make_intrusive<ASTLiteral>(label_name)));

    return makeASTFunction(
        "timeSeriesTagsToGroup",
        std::move(tags_without_value_label),
        make_intrusive<ASTLiteral>(label_name),
        make_intrusive<ASTIdentifier>(LABEL_VALUE));
}

ASTPtr transformGroup(const PrometheusQueryTree::AggregationOperator * operator_node, ASTPtr group, const String & label_name, bool & metric_name_dropped)
{
    if (operator_node->without)
    {
        std::vector<std::string_view> tags_to_remove{operator_node->labels.begin(), operator_node->labels.end()};
        tags_to_remove.push_back(kMetricName);
        std::sort(tags_to_remove.begin(), tags_to_remove.end());
        tags_to_remove.erase(std::unique(tags_to_remove.begin(), tags_to_remove.end()), tags_to_remove.end());

        metric_name_dropped = true;
        return makeASTFunction(
            "timeSeriesRemoveTags", std::move(group), make_intrusive<ASTLiteral>(Array{tags_to_remove.begin(), tags_to_remove.end()}));
    }

    std::vector<std::string_view> tags_to_keep;
    if (operator_node->by)
        tags_to_keep.assign(operator_node->labels.begin(), operator_node->labels.end());
    tags_to_keep.push_back(label_name);
    std::sort(tags_to_keep.begin(), tags_to_keep.end());
    tags_to_keep.erase(std::unique(tags_to_keep.begin(), tags_to_keep.end()), tags_to_keep.end());

    if (!std::binary_search(tags_to_keep.begin(), tags_to_keep.end(), kMetricName))
        metric_name_dropped = true;

    return makeASTFunction(
        "timeSeriesRemoveAllTagsExcept", std::move(group), make_intrusive<ASTLiteral>(Array{tags_to_keep.begin(), tags_to_keep.end()}));
}
}


SQLQueryPiece applyAggregationOperatorCountValues(
    const PrometheusQueryTree::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(operator_node, arguments, context);

    const auto * label_argument_node = operator_node->getArguments().at(0);
    const auto * label_node = static_cast<const PrometheusQueryTree::StringLiteral *>(label_argument_node);
    const String & label_name = label_node->string;
    if (label_name.empty() || !UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(label_name.data()), label_name.size()))
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Aggregation operator 'count_values' received invalid label name {}",
            quoteString(label_name));
    }

    auto & vector_argument = arguments[1];
    if (vector_argument.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};

    vector_argument = toVectorGrid(std::move(vector_argument), context);

    SQLQueryPiece res = vector_argument;
    res.node = operator_node;
    if (label_name == kMetricName)
        res.metric_name_dropped = false;

    /// Step 1: unroll each per-series values array into one row per non-null grid point.
    ASTPtr unrolled_query;
    {
        SelectQueryBuilder builder;
        context.subqueries.emplace_back(
            SQLSubquery{context.subqueries.size(), std::move(vector_argument.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        builder.select_list.back()->setAlias(ColumnNames::OriginalGroup);

        auto array_join = makeASTFunction(
            "arrayJoin",
            makeASTFunction(
                "arrayZip",
                makeASTFunction("arrayEnumerate", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        array_join->setAlias(SAMPLE);

        builder.select_list.push_back(makeASTFunction("tupleElement", std::move(array_join), make_intrusive<ASTLiteral>(1u)));
        builder.select_list.back()->setAlias(TIME_INDEX);

        auto sample_value = makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(SAMPLE), make_intrusive<ASTLiteral>(2u));
        builder.select_list.push_back(
            makeASTFunction("timeSeriesPrometheusValueToString", makeASTFunction("assumeNotNull", sample_value->clone())));
        builder.select_list.back()->setAlias(LABEL_VALUE);

        builder.where = makeASTFunction("isNotNull", std::move(sample_value));
        unrolled_query = builder.getSelectQuery();
    }

    /// Step 2: set (or overwrite) the requested value label before applying grouping.
    ASTPtr relabeled_query;
    {
        SelectQueryBuilder builder;
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(unrolled_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(setValueLabel(make_intrusive<ASTIdentifier>(ColumnNames::OriginalGroup), label_name));
        builder.select_list.back()->setAlias(SET_GROUP);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(TIME_INDEX));

        relabeled_query = builder.getSelectQuery();
    }

    /// Step 3: count rows independently for every resulting group and grid point.
    ASTPtr counted_query;
    {
        SelectQueryBuilder builder;
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(relabeled_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(
            transformGroup(operator_node, make_intrusive<ASTIdentifier>(SET_GROUP), label_name, res.metric_name_dropped));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(TIME_INDEX));
        builder.select_list.push_back(makeASTFunction("count"));
        builder.select_list.back()->setAlias(VALUE_COUNT);

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(TIME_INDEX));

        counted_query = builder.getSelectQuery();
    }

    /// Step 4: collect each group's sparse per-grid counts as index/count pairs.
    ASTPtr paired_query;
    {
        SelectQueryBuilder builder;
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(counted_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.push_back(makeASTFunction(
            "groupArray", makeASTFunction("tuple", make_intrusive<ASTIdentifier>(TIME_INDEX), make_intrusive<ASTIdentifier>(VALUE_COUNT))));
        builder.select_list.back()->setAlias(COUNT_PAIRS);
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        paired_query = builder.getSelectQuery();
    }

    /// Step 5: turn the paired arrays into maps keyed by the 1-based grid index.
    ASTPtr packed_query;
    {
        SelectQueryBuilder builder;
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(paired_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.push_back(makeASTFunction(
            "mapFromArrays",
            makeASTFunction(
                "arrayMap",
                makeASTLambda(
                    {"pair"}, makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("pair"), make_intrusive<ASTLiteral>(1u))),
                make_intrusive<ASTIdentifier>(COUNT_PAIRS)),
            makeASTFunction(
                "arrayMap",
                makeASTLambda(
                    {"pair"}, makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("pair"), make_intrusive<ASTLiteral>(2u))),
                make_intrusive<ASTIdentifier>(COUNT_PAIRS))));
        builder.select_list.back()->setAlias(COUNTS);

        packed_query = builder.getSelectQuery();
    }

    /// Step 6: expand the sparse map back to a fixed nullable values array.
    {
        SelectQueryBuilder builder;
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(packed_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        const size_t num_steps = stepsInTimeSeriesRange(res.start_time, res.end_time, res.step);
        auto grid_indices = makeASTFunction("range", make_intrusive<ASTLiteral>(1u), make_intrusive<ASTLiteral>(num_steps + 1));
        auto count_at_index = makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(COUNTS), make_intrusive<ASTIdentifier>("i"));
        auto nullable_count = makeASTFunction(
            "nullIf",
            timeSeriesScalarASTCast(std::move(count_at_index), context.scalar_data_type),
            timeSeriesScalarToAST(0, context.scalar_data_type));

        builder.select_list.push_back(
            makeASTFunction("arrayMap", makeASTLambda({"i"}, std::move(nullable_count)), std::move(grid_indices)));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
