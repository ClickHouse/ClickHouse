#include <Storages/TimeSeries/makeASTSelectLatestSamples.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLSubquery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <base/defines.h>


namespace DB
{

namespace
{
    constexpr const char * LAST_VALUE_COLUMN = "last_value";
    constexpr const char * LAST_TIMESTAMP_COLUMN = "last_timestamp";
    constexpr UInt64 STALE_NAN_BITS = 0x7ff0000000000002ULL;

    /// SELECT <columns>
    /// FROM timeSeriesSelector(<database>, <time_series_table>, <selector>, <min_time>, <max_time>, <return_columns>)
    /// UNION ALL ...
    ASTPtr makeASTSelectFromSelectors(
        const StorageID & time_series_storage_id,
        const Strings & selectors,
        const ASTPtr & min_time,
        const ASTPtr & max_time,
        const String & return_columns,
        const Strings & columns)
    {
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        for (const auto & selector : selectors)
        {
            PrometheusQueryToSQL::SelectQueryBuilder builder;
            for (const auto & column : columns)
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(column));
            builder.from_table_function = makeASTFunction(
                "timeSeriesSelector",
                make_intrusive<ASTLiteral>(time_series_storage_id.getDatabaseName()),
                make_intrusive<ASTLiteral>(time_series_storage_id.getTableName()),
                make_intrusive<ASTLiteral>(selector),
                min_time->clone(),
                max_time->clone(),
                make_intrusive<ASTLiteral>(return_columns));

            auto select = builder.getSelectQuery();
            list_of_selects->children.push_back(select->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0));
        }

        auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
        union_query->union_mode = SelectUnionMode::UNION_ALL;
        union_query->children.push_back(std::move(list_of_selects));
        union_query->list_of_selects = union_query->children.back();
        return union_query;
    }
}


ASTPtr makeASTSelectLatestSamples(
    const StorageID & time_series_storage_id,
    const Strings & selectors,
    DateTime64 time,
    Decimal64 lookback_delta,
    UInt32 time_scale)
{
    chassert(!selectors.empty());

    auto time_type = std::make_shared<DataTypeDateTime64>(time_scale);

    /// The range is (time - lookback_delta, time]. The table function converts the bounds to the scale of the table itself,
    /// rounding them towards the inside of the range.
    auto min_time_ast = timeSeriesTimestampToAST(DateTime64(time.value - lookback_delta.value + 1), time_type);
    auto max_time_ast = timeSeriesTimestampToAST(time, time_type);

    PrometheusQueryToSQL::SQLSubqueries with;

    /// WITH prometheus_query_step_1 AS (SELECT id, timestamp, value FROM timeSeriesSelector(..., 'timestamp_and_value') UNION ALL ...)
    with.emplace_back(
        with.size(),
        makeASTSelectFromSelectors(
            time_series_storage_id, selectors, min_time_ast, max_time_ast, "timestamp_and_value",
            {TimeSeriesColumnNames::ID, TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value}),
        PrometheusQueryToSQL::SQLSubqueryType::TABLE);
    const String samples_table = with.back().name;

    /// prometheus_query_step_2 AS (SELECT id, tags FROM timeSeriesSelector(..., 'tags') UNION ALL ...)
    with.emplace_back(
        with.size(),
        makeASTSelectFromSelectors(
            time_series_storage_id, selectors, min_time_ast, max_time_ast, "tags",
            {TimeSeriesColumnNames::ID, TimeSeriesColumnNames::Tags}),
        PrometheusQueryToSQL::SQLSubqueryType::TABLE);
    const String series_table = with.back().name;

    /// prometheus_query_step_3 AS (SELECT id, <...> AS last_value, <...> AS last_timestamp FROM prometheus_query_step_1 GROUP BY id HAVING ...)
    {
        PrometheusQueryToSQL::SelectQueryBuilder builder;

        /// The latest sample of a series is found on a grid of the single point `time`. If several samples have the same
        /// timestamp, a real value beats a NaN, then the greatest wins (the rule of the instant query).
        /// <function_name>(<time>, <time>, 0, <lookback_delta>)(timestamp, value)[1]
        auto latest_sample_of = [&](const char * function_name)
        {
            auto aggregate = addParametersToAggregateFunction(
                makeASTFunction(
                    function_name, make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp), make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value)),
                timeSeriesTimestampToAST(time, time_type),
                timeSeriesTimestampToAST(time, time_type),
                timeSeriesDurationToAST(Decimal64{0}, time_type),
                timeSeriesDurationToAST(lookback_delta, time_type));
            return makeASTFunction("arrayElement", std::move(aggregate), make_intrusive<ASTLiteral>(UInt64{1}));
        };

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));

        /// timeSeriesLastToGrid(...)(timestamp, value)[1] AS last_value
        builder.select_list.push_back(latest_sample_of("timeSeriesLastToGrid"));
        builder.select_list.back()->setAlias(LAST_VALUE_COLUMN);

        /// toDateTime64(timeSeriesTimestampOfLastToGrid(...)(timestamp, value)[1], 3) AS last_timestamp
        builder.select_list.push_back(
            makeASTFunction("toDateTime64", latest_sample_of("timeSeriesTimestampOfLastToGrid"), make_intrusive<ASTLiteral>(UInt64{3})));
        builder.select_list.back()->setAlias(LAST_TIMESTAMP_COLUMN);

        builder.from_table = samples_table;

        /// A series matched by several selectors has its samples repeated, they are aggregated together.
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));

        /// Skips the series whose latest sample is a stale marker (and the ones without samples, whose value is NULL).
        builder.having = makeASTFunction(
            "notEquals",
            makeASTFunction("reinterpretAsUInt64", makeASTFunction("toFloat64", make_intrusive<ASTIdentifier>(LAST_VALUE_COLUMN))),
            make_intrusive<ASTLiteral>(STALE_NAN_BITS));

        with.emplace_back(with.size(), builder.getSelectQuery(), PrometheusQueryToSQL::SQLSubqueryType::TABLE);
    }
    const String latest_samples_table = with.back().name;

    /// SELECT prometheus_query_step_2.tags AS tags,
    ///        prometheus_query_step_3.last_value AS last_value,
    ///        prometheus_query_step_3.last_timestamp AS last_timestamp
    /// FROM prometheus_query_step_3
    /// ANY INNER JOIN prometheus_query_step_2 ON prometheus_query_step_3.id = prometheus_query_step_2.id
    PrometheusQueryToSQL::SelectQueryBuilder builder;
    builder.with = std::move(with);

    builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{series_table, TimeSeriesColumnNames::Tags}));
    builder.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{latest_samples_table, LAST_VALUE_COLUMN}));
    builder.select_list.back()->setAlias(LAST_VALUE_COLUMN);
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{latest_samples_table, LAST_TIMESTAMP_COLUMN}));
    builder.select_list.back()->setAlias(LAST_TIMESTAMP_COLUMN);

    builder.from_table = latest_samples_table;

    /// The tags of a series can be repeated (several selectors, unmerged parts of the tags table), ANY joins one of them.
    builder.join_kind = JoinKind::Inner;
    builder.join_strictness = JoinStrictness::Any;
    builder.join_table = series_table;
    builder.join_on = makeASTFunction(
        "equals",
        make_intrusive<ASTIdentifier>(Strings{latest_samples_table, TimeSeriesColumnNames::ID}),
        make_intrusive<ASTIdentifier>(Strings{series_table, TimeSeriesColumnNames::ID}));

    /// ORDER BY arrayFirst(tag -> tag.1 = '__name__', tags).2, tags
    /// `tags` resolves to `prometheus_query_step_2.tags` both as the alias of the SELECT list and as a column of the join.
    builder.order_by.push_back(makeASTFunction(
        "tupleElement",
        makeASTFunction(
            "arrayFirst",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("tag")),
                makeASTFunction(
                    "equals",
                    makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("tag"), make_intrusive<ASTLiteral>(UInt64{1})),
                    make_intrusive<ASTLiteral>(String{TimeSeriesTagNames::MetricName}))),
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags)),
        make_intrusive<ASTLiteral>(UInt64{2})));
    builder.order_by.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
    builder.order_direction = 1;

    return builder.getSelectQuery();
}

}
