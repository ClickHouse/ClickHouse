#include <Storages/TimeSeries/PrometheusRemoteReadProtocol.h>

#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Core/DecimalFunctions.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
}

namespace
{
    /// Converts label matchers to an instant selector format in PromQL,
    /// i.e. for example the function can return "{__name__='up'}"
    PrometheusQueryTree labelMatchersToPromQL(const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matchers)
    {
        auto instant_selector = std::make_unique<PrometheusQueryTree::InstantSelector>();
        auto & res_matchers = instant_selector->matchers;

        for (const auto & label_matcher : label_matchers)
        {
            const auto & label_name = label_matcher.name();
            const auto & label_value = label_matcher.value();
            auto type = label_matcher.type();

            PrometheusQueryTree::Matcher res_matcher;
            res_matcher.label_name = label_name;
            res_matcher.label_value = label_value;

            if (type == prometheus::LabelMatcher::EQ)
                res_matcher.matcher_type = PrometheusQueryTree::MatcherType::EQ;
            else if (type == prometheus::LabelMatcher::NEQ)
                res_matcher.matcher_type = PrometheusQueryTree::MatcherType::NE;
            else if (type == prometheus::LabelMatcher::RE)
                res_matcher.matcher_type = PrometheusQueryTree::MatcherType::RE;
            else if (type == prometheus::LabelMatcher::NRE)
                res_matcher.matcher_type = PrometheusQueryTree::MatcherType::NRE;
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown label matcher type: {}", type);

            res_matchers.emplace_back(std::move(res_matcher));
        }

        /// RemoteRead matchers are not PromQL text, and the protocol permits matcher sets
        /// such as job=~".*" that match an empty label value. TimeSeries rows always have
        /// a non-empty metric name, so preserve that invariant when the matchers are later
        /// serialized to PromQL and reparsed by timeSeriesSelector.
        res_matchers.emplace_back(PrometheusQueryTree::Matcher{
            .label_name = "__name__",
            .label_value = "",
            .matcher_type = PrometheusQueryTree::MatcherType::NE});

        return PrometheusQueryTree{std::move(instant_selector)};
    }

    /// The function builds a SELECT query for reading time series:
    /// SELECT series.tags AS tags, per_id.samples AS samples
    /// FROM
    /// (
    ///     SELECT id, timeSeriesGroupArray(samples) AS samples
    ///     FROM timeSeriesSelector(time_series_storage_id, "label_matchers", min_time, max_time, 'samples')
    ///     GROUP BY id
    /// ) AS per_id
    /// ANY INNER JOIN timeSeriesSelector(time_series_storage_id, "label_matchers", min_time, max_time, 'tags') AS series
    /// ON per_id.id = series.id
    /// If the table stores one sample per row, the subquery uses timeSeriesGroupArray(timestamp, value) and 'timestamp_and_value'.
    ASTPtr buildSelectQueryForReadingTimeSeries(
        const StorageID & time_series_storage_id,
        bool table_has_bucketed_samples,
        const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matchers,
        Int64 min_time_ms,
        Int64 max_time_ms)
    {
        const char * per_id_alias = "per_id";
        const char * series_alias = "series";
        const String selector = labelMatchersToPromQL(label_matchers).toString();

        /// timeSeriesSelector(time_series_storage_id, "label_matchers", min_time, max_time, return_columns)
        auto make_selector = [&](const String & return_columns)
        {
            return makeASTFunction(
                "timeSeriesSelector",
                make_intrusive<ASTLiteral>(time_series_storage_id.getDatabaseName()),
                make_intrusive<ASTLiteral>(time_series_storage_id.getTableName()),
                make_intrusive<ASTLiteral>(selector),
                make_intrusive<ASTLiteral>(DecimalField<Decimal64>{min_time_ms, 3}),
                make_intrusive<ASTLiteral>(DecimalField<Decimal64>{max_time_ms, 3}),
                make_intrusive<ASTLiteral>(return_columns));
        };

        auto per_id_select = make_intrusive<ASTSelectQuery>();

        {
            /// SELECT id, timeSeriesGroupArray(samples) AS samples
            auto select_list_exp = make_intrusive<ASTExpressionList>();

            select_list_exp->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));

            if (table_has_bucketed_samples)
            {
                select_list_exp->children.push_back(makeASTFunction("timeSeriesGroupArray", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Samples)));
            }
            else
            {
                select_list_exp->children.push_back(makeASTFunction(
                    "timeSeriesGroupArray",
                    make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                    make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value)));
            }

            select_list_exp->children.back()->setAlias(TimeSeriesColumnNames::Samples);

            per_id_select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));
        }

        {
            /// FROM timeSeriesSelector(time_series_storage_id, "label_matchers", min_time, max_time, 'samples' | 'timestamp_and_value')
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->table_function = make_selector(table_has_bucketed_samples ? "samples" : "timestamp_and_value");
            table_exp->children.emplace_back(table_exp->table_function);

            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            table->table_expression = table_exp;
            table->children.push_back(std::move(table_exp));

            auto tables = make_intrusive<ASTTablesInSelectQuery>();
            tables->children.push_back(std::move(table));

            per_id_select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
        }

        {
            /// GROUP BY id
            auto group_by_list = make_intrusive<ASTExpressionList>();
            group_by_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
            per_id_select->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_by_list));
        }

        auto select_query = make_intrusive<ASTSelectQuery>();

        {
            /// SELECT series.tags AS tags, per_id.samples AS samples
            auto select_list_exp = make_intrusive<ASTExpressionList>();

            select_list_exp->children.push_back(make_intrusive<ASTIdentifier>(std::vector<String>{series_alias, TimeSeriesColumnNames::Tags}));
            select_list_exp->children.back()->setAlias(TimeSeriesColumnNames::Tags);

            select_list_exp->children.push_back(make_intrusive<ASTIdentifier>(std::vector<String>{per_id_alias, TimeSeriesColumnNames::Samples}));
            select_list_exp->children.back()->setAlias(TimeSeriesColumnNames::Samples);

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));
        }

        auto tables = make_intrusive<ASTTablesInSelectQuery>();

        {
            /// FROM (<per_id_select>) AS per_id
            auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
            auto list_of_selects = make_intrusive<ASTExpressionList>();
            list_of_selects->children.push_back(std::move(per_id_select));
            select_with_union_query->children.push_back(std::move(list_of_selects));
            select_with_union_query->list_of_selects = select_with_union_query->children.back();

            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->subquery = make_intrusive<ASTSubquery>(std::move(select_with_union_query));
            table_exp->subquery->setAlias(per_id_alias);
            table_exp->children.push_back(table_exp->subquery);

            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            table->table_expression = table_exp;
            table->children.push_back(std::move(table_exp));

            tables->children.push_back(std::move(table));
        }

        {
            /// ANY INNER JOIN timeSeriesSelector(time_series_storage_id, "label_matchers", min_time, max_time, 'tags') AS series
            /// ON per_id.id = series.id
            /// The table function can return a series more than once (from unmerged parts of the tags table), ANY joins one of them.
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->table_function = make_selector("tags");
            table_exp->table_function->setAlias(series_alias);
            table_exp->children.push_back(table_exp->table_function);

            auto table_join = make_intrusive<ASTTableJoin>();
            table_join->kind = JoinKind::Inner;
            table_join->strictness = JoinStrictness::Any;
            table_join->on_expression = makeASTFunction(
                "equals",
                make_intrusive<ASTIdentifier>(std::vector<String>{per_id_alias, TimeSeriesColumnNames::ID}),
                make_intrusive<ASTIdentifier>(std::vector<String>{series_alias, TimeSeriesColumnNames::ID}));
            table_join->children.push_back(table_join->on_expression);

            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            table->table_expression = table_exp;
            table->table_join = table_join;
            table->children.push_back(table->table_expression);
            table->children.push_back(table->table_join);

            tables->children.push_back(std::move(table));
        }

        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

        return select_query;
    }

    /// Converts a block generated by the SELECT query for converting time series to the protobuf format.
    void convertBlockToProtobuf(
        Block && block,
        google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & out_time_series)
    {
        size_t num_rows = block.rows();
        if (!num_rows)
            return;

        /// The first column contains tags.
        /// These tags are already sorted lexicographically.
        /// The type of the first column is Array(Tuple(String, String)).
        const auto & tags_column = checkAndGetColumn<ColumnArray>(*block.getByName(TimeSeriesColumnNames::Tags).column);
        const auto & tags_offsets = tags_column.getOffsets();
        const auto & tag_name_value_tuples = checkAndGetColumn<ColumnTuple>(tags_column.getData());
        const auto & tags_names = tag_name_value_tuples.getColumn(0);
        const auto & tags_values = tag_name_value_tuples.getColumn(1);

        /// The second column contains tuples (timestamp, value).
        /// These tuples are already sorted by timestamp.
        /// The type of the second column is Array(Tuple(timestamp_data_type, value_data_type)).
        const auto & time_series_column = checkAndGetColumn<ColumnArray>(*block.getByName(TimeSeriesColumnNames::Samples).column);
        const auto & time_series_offsets = time_series_column.getOffsets();
        const auto & timestamp_value_tuples = checkAndGetColumn<ColumnTuple>(time_series_column.getData());
        const auto & timestamps = timestamp_value_tuples.getColumn(0);
        const auto & values = timestamp_value_tuples.getColumn(1);

        auto timestamp_data_type
            = typeid_cast<const DataTypeTuple &>(
                  *typeid_cast<const DataTypeArray &>(*block.getByName(TimeSeriesColumnNames::Samples).type).getNestedType())
                  .getElement(0);

        UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

        for (size_t i = 0; i != num_rows; ++i)
        {
            auto & new_time_series = *out_time_series.Add();

            size_t tags_start_offset = tags_offsets[i - 1];
            size_t tags_end_offset = tags_offsets[i];

            for (size_t j = tags_start_offset; j != tags_end_offset; ++j)
            {
                auto & new_label = *new_time_series.add_labels();
                new_label.set_name(tags_names.getDataAt(j));
                new_label.set_value(tags_values.getDataAt(j));
            }

            size_t time_series_start_offset = time_series_offsets[i - 1];
            size_t time_series_end_offset = time_series_offsets[i];

            for (size_t j = time_series_start_offset; j != time_series_end_offset; ++j)
            {
                Int64 timestamp = timestamps.getInt(j);
                Float64 value = values.getFloat64(j);
                Int64 timestamp_ms = DecimalUtils::convertTo<Decimal64>(3, Decimal64{timestamp}, timestamp_scale).value;
                auto & new_sample = *new_time_series.add_samples();
                new_sample.set_timestamp(timestamp_ms);
                new_sample.set_value(value);
            }
        }
    }
}


PrometheusRemoteReadProtocol::PrometheusRemoteReadProtocol(ConstStoragePtr time_series_storage_, const ContextPtr & context_)
    : WithContext{context_}
    , time_series_storage(storagePtrToTimeSeries(time_series_storage_))
    , log(getLogger("PrometheusRemoteReadProtocol"))
{
}

PrometheusRemoteReadProtocol::~PrometheusRemoteReadProtocol() = default;

void PrometheusRemoteReadProtocol::readTimeSeries(google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & out_time_series,
                                                  Int64 start_timestamp_ms,
                                                  Int64 end_timestamp_ms,
                                                  const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matcher,
                                                  const prometheus::ReadHints &)
{
    out_time_series.Clear();

    auto time_series_storage_id = time_series_storage->getStorageID();

    ASTPtr select_query = buildSelectQueryForReadingTimeSeries(
        time_series_storage_id, time_series_storage->getStorageSettings()->hasBucketedSamples(), label_matcher, start_timestamp_ms, end_timestamp_ms);

    LOG_TRACE(log, "{}: Executing query {}",
              time_series_storage_id.getNameForLogs(), select_query->formatForLogging());

    auto context = getContext();
    InterpreterSelectQueryAnalyzer interpreter(select_query, context, SelectQueryOptions{});
    BlockIO io = interpreter.execute();
    PullingPipelineExecutor executor(io.pipeline);

    Block block;
    while (executor.pull(block))
    {
        LOG_TRACE(log, "{}: Pulled block with {} columns and {} rows",
                  time_series_storage_id.getNameForLogs(), block.columns(), block.rows());

        if (!block.empty())
            convertBlockToProtobuf(std::move(block), out_time_series);
    }

    LOG_TRACE(log, "{}: {} time series read",
              time_series_storage_id.getNameForLogs(), out_time_series.size());
}

}

#endif
