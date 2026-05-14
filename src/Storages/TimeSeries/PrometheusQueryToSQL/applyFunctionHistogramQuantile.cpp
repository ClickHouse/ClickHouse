#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionHistogramQuantile.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
constexpr const char * point_column = "point";
constexpr const char * point_index_column = "point_index";
constexpr const char * upper_bound_column = "upper_bound";
constexpr const char * bucket_pairs_column = "bucket_pairs";
constexpr const char * values_by_index_column = "values_by_index";
constexpr const char * le_label = "le";

void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    if (arguments.size() != 2)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function '{}' expects 2 arguments, but was called with {} arguments",
            function_name,
            arguments.size());
    }

    const auto & quantile_arg = arguments[0];
    if (quantile_arg.type != ResultType::SCALAR)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function '{}' expects first argument of type {}, but expression {} has type {}",
            function_name,
            ResultType::SCALAR,
            getPromQLText(quantile_arg, context),
            quantile_arg.type);
    }

    const auto & vector_arg = arguments[1];
    if (vector_arg.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function '{}' expects second argument of type {}, but expression {} has type {}",
            function_name,
            ResultType::INSTANT_VECTOR,
            getPromQLText(vector_arg, context),
            vector_arg.type);
    }
}

ASTPtr getQuantileParameter(SQLQueryPiece && quantile_arg, ConverterContext & context)
{
    switch (quantile_arg.store_method)
    {
        case StoreMethod::CONST_SCALAR: {
            return makeASTFunction("toFloat64", timeSeriesScalarToAST(quantile_arg.scalar_value, context.scalar_data_type));
        }
        case StoreMethod::SINGLE_SCALAR: {
            context.subqueries.emplace_back(
                SQLSubquery{context.subqueries.size(), std::move(quantile_arg.select_query), SQLSubqueryType::SCALAR});
            auto subquery_id = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
            return makeASTFunction("toFloat64", makeASTFunction("assumeNotNull", std::move(subquery_id)));
        }
        case StoreMethod::SCALAR_GRID: {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Function 'histogram_quantile' with a non-constant scalar parameter is not supported");
        }
        default: {
            throwUnexpectedStoreMethod(quantile_arg, context);
        }
    }
}

ASTPtr getPointElement(UInt64 index)
{
    return makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(point_column), make_intrusive<ASTLiteral>(index));
}

ASTPtr makeUpperBound(ASTPtr group)
{
    return makeASTFunction(
        "toFloat64OrNull", makeASTFunction("timeSeriesExtractTag", std::move(group), make_intrusive<ASTLiteral>(le_label)));
}

ASTPtr makePresentUpperBoundCondition(ASTPtr upper_bound)
{
    return makeASTFunction("isNotNull", std::move(upper_bound));
}

ASTPtr makeHistogramIdentityGroup(ASTPtr group)
{
    auto group_as_uint64 = makeASTFunction("CAST", std::move(group), make_intrusive<ASTLiteral>("UInt64"));
    return makeASTFunction("timeSeriesRemoveTag", std::move(group_as_uint64), make_intrusive<ASTLiteral>(le_label));
}

ASTPtr makeHistogramQuantileArray(TimestampType start_time, TimestampType end_time, DurationType step, const DataTypePtr & scalar_data_type)
{
    size_t num_steps = stepsInTimeSeriesRange(start_time, end_time, step);
    auto point_index = make_intrusive<ASTIdentifier>("i");
    auto value = makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(values_by_index_column), point_index->clone());

    return makeASTFunction(
        "arrayMap",
        makeASTLambda(
            {"i"},
            makeASTFunction(
                "if",
                makeASTFunction("mapContains", make_intrusive<ASTIdentifier>(values_by_index_column), point_index->clone()),
                makeASTFunction("toNullable", timeSeriesScalarASTCast(std::move(value), scalar_data_type)),
                make_intrusive<ASTLiteral>(Field{} /* NULL */))),
        makeASTFunction("range", make_intrusive<ASTLiteral>(1u), make_intrusive<ASTLiteral>(num_steps + 1)));
}
}


bool isFunctionHistogramQuantile(std::string_view function_name)
{
    return function_name == "histogram_quantile";
}


SQLQueryPiece
applyFunctionHistogramQuantile(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);

    auto & quantile_arg = arguments[0];
    auto & vector_arg = arguments[1];
    if (quantile_arg.store_method == StoreMethod::EMPTY || vector_arg.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};

    ASTPtr quantile = getQuantileParameter(std::move(quantile_arg), context);
    vector_arg = toVectorGrid(std::move(vector_arg), context);

    auto res = vector_arg;
    res.node = function_node;

    /// Work point-by-point because classic histogram buckets are separate float series,
    /// while histogram_quantile() groups them by timestamp and labels except `le` and `__name__`.
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

    /// Non-bucket inputs must disappear, not become an empty histogram with a NaN result.
    /// Prometheus ignores unparsable `le` labels but keeps parseable NaN bounds, which make the quantile NaN.
    ASTPtr present_bucket_samples_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(exploded_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        builder.select_list.push_back(getPointElement(1));
        builder.select_list.back()->setAlias(point_index_column);

        builder.select_list.push_back(getPointElement(2));
        builder.select_list.back()->setAlias(ColumnNames::Value);

        builder.select_list.push_back(makeUpperBound(make_intrusive<ASTIdentifier>(ColumnNames::Group)));
        builder.select_list.back()->setAlias(upper_bound_column);

        builder.where = makeASTFunction(
            "and",
            makeASTFunction("isNotNull", getPointElement(2)),
            makePresentUpperBoundCondition(makeUpperBound(make_intrusive<ASTIdentifier>(ColumnNames::Group))));

        present_bucket_samples_query = builder.getSelectQuery();
    }

    /// Prometheus identifies classic histograms by all labels except `le`. Keep `__name__`
    /// here so different metric names stay separate until the final name drop, where duplicate
    /// output label sets are rejected.
    ASTPtr bucket_counts_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(
            SQLSubquery{context.subqueries.size(), std::move(present_bucket_samples_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(makeHistogramIdentityGroup(make_intrusive<ASTIdentifier>(ColumnNames::Group)));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(point_index_column));
        builder.select_list.back()->setAlias(point_index_column);

        builder.select_list.push_back(makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(upper_bound_column)));
        builder.select_list.back()->setAlias(upper_bound_column);

        builder.select_list.push_back(makeASTFunction(
            "toFloat64", makeASTFunction("sum", makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(ColumnNames::Value)))));
        builder.select_list.back()->setAlias(ColumnNames::Value);

        builder.where = makePresentUpperBoundCondition(make_intrusive<ASTIdentifier>(upper_bound_column));

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(point_index_column));
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(upper_bound_column));

        bucket_counts_query = builder.getSelectQuery();
    }

    ASTPtr bucket_arrays_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(bucket_counts_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(point_index_column));

        /// Keep bounds and counts zipped so later groupArray() order cannot pair a bound with a count from a different bucket.
        builder.select_list.push_back(makeASTFunction(
            "groupArray",
            makeASTFunction(
                "tuple", make_intrusive<ASTIdentifier>(upper_bound_column), make_intrusive<ASTIdentifier>(ColumnNames::Value))));
        builder.select_list.back()->setAlias(bucket_pairs_column);

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(point_index_column));

        bucket_arrays_query = builder.getSelectQuery();
    }

    ASTPtr per_group_values_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(bucket_arrays_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        auto upper_bounds = makeASTFunction(
            "arrayMap",
            makeASTLambda(
                {"bucket"}, makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("bucket"), make_intrusive<ASTLiteral>(1u))),
            make_intrusive<ASTIdentifier>(bucket_pairs_column));

        auto bucket_counts = makeASTFunction(
            "arrayMap",
            makeASTLambda(
                {"bucket"}, makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("bucket"), make_intrusive<ASTLiteral>(2u))),
            make_intrusive<ASTIdentifier>(bucket_pairs_column));

        auto histogram_quantile = makeASTFunction(
            "timeSeriesPrometheusHistogramQuantile", quantile->clone(), std::move(upper_bounds), std::move(bucket_counts));

        builder.select_list.push_back(makeASTFunction(
            "mapFromArrays",
            makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(point_index_column)),
            makeASTFunction("groupArray", std::move(histogram_quantile))));
        builder.select_list.back()->setAlias(values_by_index_column);

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        per_group_values_query = builder.getSelectQuery();
    }

    /// Re-expand sparse point-index results into the vector grid shape expected by finalization.
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(per_group_values_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(makeHistogramQuantileArray(res.start_time, res.end_time, res.step, context.scalar_data_type));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        res.select_query = builder.getSelectQuery();
    }

    return dropMetricName(std::move(res), context);
}

}
