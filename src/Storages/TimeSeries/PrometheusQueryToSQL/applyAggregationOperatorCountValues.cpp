#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperatorCountValues.h>

#include <Common/Exception.h>
#include <Common/isValidUTF8.h>
#include <Common/quoteString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <algorithm>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    constexpr const char * point_column = "point";
    constexpr const char * point_index_column = "point_index";
    constexpr const char * value_label_column = "value_label";
    constexpr const char * counts_column = "counts";

bool isValidPrometheusLabelName(std::string_view label_name)
{
    return !label_name.empty() && UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(label_name.data()), label_name.size());
}

void checkArgumentTypes(
    const PQT::AggregationOperator * operator_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
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

    const auto & label_arg = arguments[0];

    if (label_arg.type != ResultType::STRING)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Aggregation operator '{}' expects first argument of type {}, but expression {} has type {}",
            operator_name,
            ResultType::STRING,
            getPromQLText(label_arg, context),
            label_arg.type);
    }

    const auto & vector_arg = arguments[1];

    if (vector_arg.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Aggregation operator '{}' expects second argument of type {}, but expression {} has type {}",
            operator_name,
            ResultType::INSTANT_VECTOR,
            getPromQLText(vector_arg, context),
            vector_arg.type);
    }
}

String getValueLabelName(SQLQueryPiece && label_arg, const ConverterContext & context)
{
    if (label_arg.store_method != StoreMethod::CONST_STRING)
        throwUnexpectedStoreMethod(label_arg, context);

    if (!isValidPrometheusLabelName(label_arg.string_value))
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Aggregation operator 'count_values' called with invalid label name {}",
            quoteString(label_arg.string_value));
    }

    return std::move(label_arg.string_value);
}

ASTPtr makeFormattedValueLabel(ASTPtr value)
{
    return makeASTFunction("timeSeriesPrometheusFormatFloat", std::move(value));
}

ASTPtr makeGroupWithValueLabel(ASTPtr group, const String & value_label_name, ASTPtr value_label)
{
    /// count_values() overwrites any existing label with the generated value label before by/without grouping.
    auto group_as_uint64 = makeASTFunction("CAST", std::move(group), make_intrusive<ASTLiteral>("UInt64"));
    return makeASTFunction(
        "timeSeriesTagsToGroup",
        makeASTFunction(
            "timeSeriesGroupToTags",
            makeASTFunction("timeSeriesRemoveTag", std::move(group_as_uint64), make_intrusive<ASTLiteral>(value_label_name))),
        make_intrusive<ASTLiteral>(value_label_name),
        std::move(value_label));
}

ASTPtr transformGroupASTForCountValues(
    const PQT::AggregationOperator * operator_node,
    ASTPtr && group_with_value_label,
    const String & value_label_name,
    bool input_metric_name_dropped,
    bool & output_metric_name_dropped)
{
    if (operator_node->by)
    {
        std::vector<std::string_view> tags_to_keep{operator_node->labels.begin(), operator_node->labels.end()};
        tags_to_keep.push_back(value_label_name);
        std::sort(tags_to_keep.begin(), tags_to_keep.end());
        tags_to_keep.erase(std::unique(tags_to_keep.begin(), tags_to_keep.end()), tags_to_keep.end());

        bool keeps_metric_name = std::binary_search(tags_to_keep.begin(), tags_to_keep.end(), kMetricName);
        output_metric_name_dropped = !((value_label_name == kMetricName) || (!input_metric_name_dropped && keeps_metric_name));

        return makeASTFunction(
            "timeSeriesRemoveAllTagsExcept",
            std::move(group_with_value_label),
            make_intrusive<ASTLiteral>(Array{tags_to_keep.begin(), tags_to_keep.end()}));
    }

    if (!operator_node->without)
    {
        output_metric_name_dropped = (value_label_name != kMetricName);
        return makeASTFunction(
            "timeSeriesRemoveAllTagsExcept", std::move(group_with_value_label), make_intrusive<ASTLiteral>(Array{value_label_name}));
    }

    std::vector<std::string_view> tags_to_remove{operator_node->labels.begin(), operator_node->labels.end()};
    tags_to_remove.push_back(kMetricName);
    std::sort(tags_to_remove.begin(), tags_to_remove.end());
    tags_to_remove.erase(std::unique(tags_to_remove.begin(), tags_to_remove.end()), tags_to_remove.end());

    output_metric_name_dropped = true;
    return makeASTFunction(
        "timeSeriesRemoveTags",
        std::move(group_with_value_label),
        make_intrusive<ASTLiteral>(Array{tags_to_remove.begin(), tags_to_remove.end()}));
}

ASTPtr makeCountValuesArray(TimestampType start_time, TimestampType end_time, DurationType step, const DataTypePtr & scalar_data_type)
{
    size_t num_steps = stepsInTimeSeriesRange(start_time, end_time, step);

    auto count = makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(counts_column), make_intrusive<ASTIdentifier>("i"));

    return makeASTFunction(
        "arrayMap",
        makeASTLambda(
            {"i"},
            makeASTFunction(
                "if",
                makeASTFunction("mapContains", make_intrusive<ASTIdentifier>(counts_column), make_intrusive<ASTIdentifier>("i")),
                makeASTFunction("toNullable", timeSeriesScalarASTCast(std::move(count), scalar_data_type)),
                make_intrusive<ASTLiteral>(Field{} /* NULL */))),
        makeASTFunction("range", make_intrusive<ASTLiteral>(1u), make_intrusive<ASTLiteral>(num_steps + 1)));
}
}


SQLQueryPiece applyAggregationOperatorCountValues(
    const PQT::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(operator_node, arguments, context);

    auto & label_arg = arguments[0];
    auto & vector_arg = arguments[1];

    /// If either argument is empty then the result is also empty.
    if (label_arg.store_method == StoreMethod::EMPTY || vector_arg.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};

    String value_label_name = getValueLabelName(std::move(label_arg), context);
    vector_arg = toVectorGrid(std::move(vector_arg), context);

    auto res = vector_arg;
    res.node = operator_node;
    const bool input_metric_name_dropped = res.metric_name_dropped;

    /// Step 1: expand each series grid row into one row per timestamp position.
    ASTPtr exploded_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(vector_arg.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        auto point = makeASTFunction(
            "arrayJoin",
            makeASTFunction(
                "arrayZip",
                makeASTFunction("arrayEnumerate", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        point->setAlias(point_column);
        builder.select_list.push_back(std::move(point));

        exploded_query = builder.getSelectQuery();
    }

    /// Step 2: keep present samples and format their values as Prometheus label strings.
    ASTPtr present_samples_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(exploded_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        builder.select_list.push_back(
            makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(point_column), make_intrusive<ASTLiteral>(1u)));
        builder.select_list.back()->setAlias(point_index_column);

        builder.select_list.push_back(makeFormattedValueLabel(makeASTFunction(
            "ifNull",
            makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(point_column), make_intrusive<ASTLiteral>(2u)),
            timeSeriesScalarToAST(0, context.scalar_data_type))));
        builder.select_list.back()->setAlias(value_label_column);

        builder.where = makeASTFunction(
            "isNotNull", makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(point_column), make_intrusive<ASTLiteral>(2u)));

        present_samples_query = builder.getSelectQuery();
    }

    /// Step 3: count samples per timestamp and generated label set.
    ASTPtr per_point_counts_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(present_samples_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        ASTPtr group_with_value_label = makeGroupWithValueLabel(
            make_intrusive<ASTIdentifier>(ColumnNames::Group), value_label_name, make_intrusive<ASTIdentifier>(value_label_column));

        ASTPtr new_group = transformGroupASTForCountValues(
            operator_node, std::move(group_with_value_label), value_label_name, input_metric_name_dropped, res.metric_name_dropped);

        builder.select_list.push_back(std::move(new_group));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(point_index_column));
        builder.select_list.back()->setAlias(point_index_column);

        builder.select_list.push_back(makeASTFunction("count"));
        builder.select_list.back()->setAlias(ColumnNames::Value);

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(point_index_column));

        per_point_counts_query = builder.getSelectQuery();
    }

    /// Step 4: pack sparse per-timestamp counts back onto the vector grid.
    ASTPtr packed_counts_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(per_point_counts_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        builder.select_list.push_back(makeASTFunction(
            "mapFromArrays",
            makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(point_index_column)),
            makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(ColumnNames::Value))));
        builder.select_list.back()->setAlias(counts_column);

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        packed_counts_query = builder.getSelectQuery();
    }

    /// Step 5: rename `new_group` back to `group` and materialize NULLs where a generated series has no sample.
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(packed_counts_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(makeCountValuesArray(res.start_time, res.end_time, res.step, context.scalar_data_type));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
