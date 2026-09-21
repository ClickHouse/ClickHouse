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
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
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

        return PrometheusQueryTree{std::move(instant_selector)};
    }

    /// The function builds a SELECT query for reading time series:
    /// SELECT timeSeriesGroupToTags(group) AS tags, timeSeriesGroupArray(timestamp, value) AS time_series
    /// FROM timeSeriesSelector(time_series_storage_id, "label_matchers", min_time, max_time)
    /// GROUP BY timeSeriesIdToGroup(id) AS group
    ASTPtr buildSelectQueryForReadingTimeSeries(
        const StorageID & time_series_storage_id,
        const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matchers,
        Int64 min_time_ms,
        Int64 max_time_ms)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        {
            /// SELECT timeSeriesGroupToTags(group) AS tags, timeSeriesGroupArray(timestamp, value) AS time_series
            auto select_list_exp = make_intrusive<ASTExpressionList>();

            select_list_exp->children.push_back(
                makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Group)));

            select_list_exp->children.back()->setAlias(TimeSeriesColumnNames::Tags);

            select_list_exp->children.push_back(makeASTFunction(
                "timeSeriesGroupArray",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value)));

            select_list_exp->children.back()->setAlias(TimeSeriesColumnNames::TimeSeries);

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));
        }

        {
            /// FROM timeSeriesSelector(time_series_storage_id, "label_matchers", min_time, max_time)
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->table_function = makeASTFunction(
                "timeSeriesSelector",
                make_intrusive<ASTLiteral>(time_series_storage_id.getDatabaseName()),
                make_intrusive<ASTLiteral>(time_series_storage_id.getTableName()),
                make_intrusive<ASTLiteral>(labelMatchersToPromQL(label_matchers).toString()),
                make_intrusive<ASTLiteral>(DecimalField<Decimal64>{min_time_ms, 3}),
                make_intrusive<ASTLiteral>(DecimalField<Decimal64>{max_time_ms, 3}));

            table_exp->children.emplace_back(table_exp->table_function);

            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            table->table_expression = table_exp;
            table->children.push_back(std::move(table_exp));

            auto tables = make_intrusive<ASTTablesInSelectQuery>();
            tables->children.push_back(std::move(table));

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
        }

        {
            /// GROUP BY timeSeriesIdToGroup(id) AS group
            auto group_by_list = make_intrusive<ASTExpressionList>();

            group_by_list->children.push_back(
                makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID)));

            group_by_list->children.back()->setAlias(TimeSeriesColumnNames::Group);

            select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_by_list));
        }

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
        /// The type of the second column is Array(Tuple(timestamp_data_type, scalar_data_type)).
        const auto & time_series_column = checkAndGetColumn<ColumnArray>(*block.getByName(TimeSeriesColumnNames::TimeSeries).column);
        const auto & time_series_offsets = time_series_column.getOffsets();
        const auto & timestamp_value_tuples = checkAndGetColumn<ColumnTuple>(time_series_column.getData());
        const auto & timestamps = timestamp_value_tuples.getColumn(0);
        const auto & values = timestamp_value_tuples.getColumn(1);

        auto timestamp_data_type
            = typeid_cast<const DataTypeTuple &>(
                  *typeid_cast<const DataTypeArray &>(*block.getByName(TimeSeriesColumnNames::TimeSeries).type).getNestedType())
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
        time_series_storage_id, label_matcher, start_timestamp_ms, end_timestamp_ms);

    LOG_TRACE(log, "{}: Executing query {}",
              time_series_storage_id.getNameForLogs(), select_query->formatForLogging());

    auto context = getContext();
    BlockIO io;
    std::optional<InterpreterSelectQuery> interpreter_holder;
    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(select_query, context, SelectQueryOptions{});
        io = interpreter.execute();
    }
    else
    {
        interpreter_holder.emplace(select_query, context, SelectQueryOptions{});
        io = interpreter_holder->execute();
    }
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
