#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    constexpr UInt64 STALE_NAN_BITS = 0x7ff0000000000002ULL;

    ASTPtr isStaleMarker(ASTPtr value)
    {
        return makeASTFunction(
            "equals",
            makeASTFunction("reinterpretAsUInt64", std::move(value)),
            make_intrusive<ASTLiteral>(STALE_NAN_BITS));
    }

    /// Reads a selector from the TimeSeries tables of the shards of a cluster, selecting on each shard
    /// and evaluating centrally.
    ASTPtr fromRangeSelectorOnCluster(std::string_view instant_selector_text,
                                      TimestampType min_time,
                                      TimestampType max_time,
                                      bool filter_stale_markers,
                                      const ConverterContext & context)
    {
        /// SELECT timeSeriesIdToTags(id) AS tags, timestamp, value
        /// FROM timeSeriesSelector(<database>, <time_series_table>, <selector>, <min_time>, <max_time>)
        SelectQueryBuilder shard_builder;

        shard_builder.select_list.push_back(makeASTFunction("timeSeriesIdToTags", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
        shard_builder.select_list.back()->setAlias(ColumnNames::Tags);

        shard_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        shard_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        const auto & remote_storage_id = context.remote_time_series_storage_id;
        auto selector_function = makeASTFunction("timeSeriesSelector");
        /// An empty database name means each shard uses its own default database.
        if (remote_storage_id.hasDatabase())
            selector_function->arguments->children.push_back(make_intrusive<ASTLiteral>(remote_storage_id.database_name));
        selector_function->arguments->children.push_back(make_intrusive<ASTLiteral>(remote_storage_id.getTableName()));
        selector_function->arguments->children.push_back(make_intrusive<ASTLiteral>(String{instant_selector_text}));
        selector_function->arguments->children.push_back(timeSeriesTimestampToAST(min_time, context.result_timestamp_type));
        selector_function->arguments->children.push_back(timeSeriesTimestampToAST(max_time, context.result_timestamp_type));
        shard_builder.from_table_function = std::move(selector_function);

        /// Filtered on each shard, as on the local path, see fromRangeSelector().
        if (filter_stale_markers)
            shard_builder.where = makeASTFunction("not", isStaleMarker(make_intrusive<ASTIdentifier>(ColumnNames::Value)));

        /// SELECT tags, timestamp, value FROM cluster(<cluster>, view(<shard query>))
        SelectQueryBuilder cluster_builder;

        cluster_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Tags));
        cluster_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        cluster_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        /// Declared on the generated call itself, so the ephemeral Distributed storage carries the
        /// wrapper's fan-out semantics and the caller's own setting still overrides them normally.
        auto cluster_settings = make_intrusive<ASTSetQuery>();
        cluster_settings->is_standalone = false;
        cluster_settings->changes.emplace_back("skip_unavailable_shards", context.skip_unavailable_shards);
        cluster_settings->changes.emplace_back("skip_unavailable_shards_mode", context.skip_unavailable_shards_mode);

        cluster_builder.from_table_function = makeASTFunction(
            "cluster",
            make_intrusive<ASTLiteral>(context.cluster_name),
            makeASTFunction("view", shard_builder.getSelectQuery()),
            std::move(cluster_settings));

        /// Always ship the query text: a serialized plan binds an unqualified name on the
        /// initiator, and shards do not apply their own row policies to a shipped plan (#112891).
        cluster_builder.settings_changes.emplace_back("serialize_query_plan", false);

        /// SELECT timeSeriesTagsToGroup(tags) AS group, timestamp, value FROM view(<cluster query>)
        /// Groups are node-local: without the view() the whole query goes to the shards, which each restart their own counter.
        SelectQueryBuilder builder;

        builder.select_list.push_back(makeASTFunction("timeSeriesTagsToGroup", make_intrusive<ASTIdentifier>(ColumnNames::Tags)));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        builder.from_table_function = makeASTFunction("view", cluster_builder.getSelectQuery());

        return builder.getSelectQuery();
    }

    SQLQueryPiece fromRangeSelector(std::string_view instant_selector_text,
                                    const Node * node,
                                    bool filter_stale_markers,
                                    ConverterContext & context)
    {
        auto node_range = context.node_range_getter.get(node);
        if (node_range.empty())
            return SQLQueryPiece{node, ResultType::RANGE_VECTOR, StoreMethod::EMPTY};

        SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

        /// The range is (start_time - window, end_time] at the result scale. The table function converts the bounds to the scale
        /// of the table itself, rounding them towards the inside of the range.
        TimestampType min_time = node_range.start_time - node_range.window + 1;
        TimestampType max_time = node_range.end_time;

        if (!context.cluster_name.empty())
        {
            res.select_query = fromRangeSelectorOnCluster(instant_selector_text, min_time, max_time, filter_stale_markers, context);
            return res;
        }

        /// SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
        /// FROM timeSeriesSelector(<database>, <time_series_table>, <selector>, <min_time>, <max_time>)
        SelectQueryBuilder builder;

        builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        /// The columns `timestamp` and `value` keep the types they have in the table, see the comment for StoreMethod::RAW_DATA.
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        builder.from_table_function = makeASTFunction(
            "timeSeriesSelector",
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getTableName()),
            make_intrusive<ASTLiteral>(String{instant_selector_text}),
            timeSeriesTimestampToAST(min_time, context.result_timestamp_type),
            timeSeriesTimestampToAST(max_time, context.result_timestamp_type));

        /// Prometheus range selectors omit the dedicated stale-NaN payload while preserving
        /// ordinary NaN samples as data.
        if (filter_stale_markers)
        {
            builder.where = makeASTFunction(
                "not",
                isStaleMarker(make_intrusive<ASTIdentifier>(ColumnNames::Value)));
        }

        res.select_query = builder.getSelectQuery();
        return res;
    }

    SQLQueryPiece replaceStaleMarkersWithNulls(SQLQueryPiece && vector_grid, ConverterContext & context)
    {
        if (vector_grid.store_method != StoreMethod::VECTOR_GRID)
            return std::move(vector_grid);

        /// Instant selectors must let last_over_time observe the stale marker first.
        /// Once it is selected as the newest sample, turn it into absence before
        /// aggregations, vector matching, scalar(), absent(), or finalization see it.
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        const String iterator_name = "x";
        ASTPtr is_stale_marker = makeASTFunction(
            "and",
            makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>(iterator_name)),
            isStaleMarker(
                makeASTFunction(
                    "assumeNotNull",
                    make_intrusive<ASTIdentifier>(iterator_name))));

        ASTPtr value = makeASTFunction(
            "if",
            std::move(is_stale_marker),
            make_intrusive<ASTLiteral>(Field{}),
            make_intrusive<ASTIdentifier>(iterator_name));

        auto values = makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>(iterator_name)),
                std::move(value)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values));
        values->setAlias(ColumnNames::Values);
        builder.select_list.push_back(std::move(values));

        context.subqueries.emplace_back(
            context.subqueries.size(),
            std::move(vector_grid.select_query),
            SQLSubqueryType::TABLE);
        builder.from_table = context.subqueries.back().name;

        /// A vector-grid row whose every step is NULL represents no series at all.
        /// Drop it here so downstream operators which validate uniqueness by row count
        /// don't mistake a stale series for a duplicate.
        context.subqueries.emplace_back(
            context.subqueries.size(),
            builder.getSelectQuery(),
            SQLSubqueryType::TABLE);

        SelectQueryBuilder filter_builder;
        filter_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        filter_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        filter_builder.from_table = context.subqueries.back().name;

        const String filter_iterator_name = "x";
        filter_builder.where = makeASTFunction(
            "arrayExists",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>(filter_iterator_name)),
                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>(filter_iterator_name))),
            make_intrusive<ASTIdentifier>(ColumnNames::Values));

        vector_grid.select_query = filter_builder.getSelectQuery();
        return std::move(vector_grid);
    }
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(
        instant_selector_text, instant_selector_node, /* filter_stale_markers = */ false, context);
    auto vector_grid = applyFunctionOverRange(
        instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
    return replaceStaleMarkersWithNulls(std::move(vector_grid), context);
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(
        instant_selector_text, range_selector_node, /* filter_stale_markers = */ true, context);
}

}
