#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Reads a selector from the TimeSeries tables of the shards of a cluster, selecting on each shard
    /// and evaluating centrally.
    ASTPtr fromRangeSelectorOnCluster(std::string_view instant_selector_text,
                                      TimestampType min_time,
                                      TimestampType max_time,
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
        selector_function->arguments->children.push_back(timeSeriesTimestampToAST(min_time, context.timestamp_data_type));
        selector_function->arguments->children.push_back(timeSeriesTimestampToAST(max_time, context.timestamp_data_type));
        shard_builder.from_table_function = std::move(selector_function);

        /// SELECT tags, timestamp, value FROM cluster(<cluster>, view(<shard query>))
        SelectQueryBuilder cluster_builder;

        cluster_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Tags));
        cluster_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        cluster_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        cluster_builder.from_table_function = makeASTFunction(
            "cluster",
            make_intrusive<ASTLiteral>(context.cluster_name),
            makeASTFunction("view", shard_builder.getSelectQuery()));

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
                                    ConverterContext & context)
    {
        auto node_range = context.node_range_getter.get(node);
        if (node_range.empty())
            return SQLQueryPiece{node, ResultType::RANGE_VECTOR, StoreMethod::EMPTY};

        SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

        TimestampType min_time = node_range.start_time - node_range.window + 1;
        TimestampType max_time = node_range.end_time;

        if (!context.cluster_name.empty())
        {
            res.select_query = fromRangeSelectorOnCluster(instant_selector_text, min_time, max_time, context);
            return res;
        }

        /// SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
        /// FROM timeSeriesSelectorToGrid(<selector>, <start_time>, <end_time>, <step>, <window>)
        SelectQueryBuilder builder;

        builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        builder.from_table_function = makeASTFunction(
            "timeSeriesSelector",
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getTableName()),
            make_intrusive<ASTLiteral>(String{instant_selector_text}),
            timeSeriesTimestampToAST(min_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(max_time, context.timestamp_data_type));

        res.select_query = builder.getSelectQuery();
        return res;
    }
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(instant_selector_text, instant_selector_node, context);
    return applyFunctionOverRange(instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(instant_selector_text, range_selector_node, context);
}

}
