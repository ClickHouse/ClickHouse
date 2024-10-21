#include <Storages/TimeSeries/PrometheusRemoteReadProtocol.h>

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesColumnsValidator.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
}

namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
}


namespace
{
    /// Makes an ASTIdentifier for a column of the specified table.
    ASTPtr makeASTColumn(const StorageID & table_id, const String & column_name)
    {
        return std::make_shared<ASTIdentifier>(Strings{table_id.database_name, table_id.table_name, column_name});
    }

    /// Makes an AST for condition `data_table.timestamp >= min_timestamp_ms`
    ASTPtr makeASTTimestampGreaterOrEquals(Int64 min_timestamp_ms, const StorageID & data_table_id)
    {
        return makeASTFunction("greaterOrEquals",
                               makeASTColumn(data_table_id, TimeSeriesColumnNames::Timestamp),
                               std::make_shared<ASTLiteral>(Field{DecimalField{DateTime64{min_timestamp_ms}, 3}}));
    }

    /// Makes an AST for condition `data_table.timestamp <= max_timestamp_ms`
    ASTPtr makeASTTimestampLessOrEquals(Int64 max_timestamp_ms, const StorageID & data_table_id)
    {
        return makeASTFunction("lessOrEquals",
                               makeASTColumn(data_table_id, TimeSeriesColumnNames::Timestamp),
                               std::make_shared<ASTLiteral>(Field{DecimalField{DateTime64{max_timestamp_ms}, 3}}));
    }

    /// Makes an AST for condition `tags_table.max_time >= min_timestamp_ms`
    ASTPtr makeASTMaxTimeGreaterOrEquals(Int64 min_timestamp_ms, const StorageID & tags_table_id)
    {
        return makeASTFunction("greaterOrEquals",
                               makeASTColumn(tags_table_id, TimeSeriesColumnNames::MaxTime),
                               std::make_shared<ASTLiteral>(Field{DecimalField{DateTime64{min_timestamp_ms}, 3}}));
    }

    /// Makes an AST for condition `tags_table.min_time <= max_timestamp_ms`
    ASTPtr makeASTMinTimeLessOrEquals(Int64 max_timestamp_ms, const StorageID & tags_table_id)
    {
        return makeASTFunction("lessOrEquals",
                               makeASTColumn(tags_table_id, TimeSeriesColumnNames::MinTime),
                               std::make_shared<ASTLiteral>(Field{DecimalField{DateTime64{max_timestamp_ms}, 3}}));
    }

    /// Makes an AST for the expression referencing a tag value.
    ASTPtr makeASTLabelName(const String & label_name, const StorageID & tags_table_id, const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        if (label_name == TimeSeriesTagNames::MetricName)
            return makeASTColumn(tags_table_id, TimeSeriesColumnNames::MetricName);

        auto it = column_name_by_tag_name.find(label_name);
        if (it != column_name_by_tag_name.end())
            return makeASTColumn(tags_table_id, it->second);

        /// arrayElement() can be used to extract a value from a Map too.
        return makeASTFunction("arrayElement", makeASTColumn(tags_table_id, TimeSeriesColumnNames::Tags), std::make_shared<ASTLiteral>(label_name));
    }

    /// Makes an AST for a label matcher, for example `metric_name == 'value'` or `NOT match(labels['label_name'], 'regexp')`.
    ASTPtr makeASTLabelMatcher(
        const prometheus::LabelMatcher & label_matcher,
        const StorageID & tags_table_id,
        const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        const auto & label_name = label_matcher.name();
        const auto & label_value = label_matcher.value();
        auto type = label_matcher.type();

        if (type == prometheus::LabelMatcher::EQ)
            return makeASTFunction("equals", makeASTLabelName(label_name, tags_table_id, column_name_by_tag_name), std::make_shared<ASTLiteral>(label_value));
        if (type == prometheus::LabelMatcher::NEQ)
            return makeASTFunction(
                "notEquals",
                makeASTLabelName(label_name, tags_table_id, column_name_by_tag_name),
                std::make_shared<ASTLiteral>(label_value));
        if (type == prometheus::LabelMatcher::RE)
            return makeASTFunction(
                "match", makeASTLabelName(label_name, tags_table_id, column_name_by_tag_name), std::make_shared<ASTLiteral>(label_value));
        if (type == prometheus::LabelMatcher::NRE)
            return makeASTFunction(
                "not",
                makeASTFunction(
                    "match",
                    makeASTLabelName(label_name, tags_table_id, column_name_by_tag_name),
                    std::make_shared<ASTLiteral>(label_value)));
        throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Unexpected type of label matcher: {}", type);
    }

    /// Makes an AST checking that tags match a specified label matcher and that timestamp is in range [min_timestamp_ms, max_timestamp_ms].
    ASTPtr makeASTFilterForReadingTimeSeries(
        const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matcher,
        Int64 min_timestamp_ms,
        Int64 max_timestamp_ms,
        const StorageID & data_table_id,
        const StorageID & tags_table_id,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        bool filter_by_min_time_and_max_time)
    {
        ASTs filters;

        if (min_timestamp_ms)
        {
            filters.push_back(makeASTTimestampGreaterOrEquals(min_timestamp_ms, data_table_id));
            if (filter_by_min_time_and_max_time)
                filters.push_back(makeASTMaxTimeGreaterOrEquals(min_timestamp_ms, tags_table_id));
        }

        if (max_timestamp_ms)
        {
            filters.push_back(makeASTTimestampLessOrEquals(max_timestamp_ms, data_table_id));
            if (filter_by_min_time_and_max_time)
                filters.push_back(makeASTMinTimeLessOrEquals(max_timestamp_ms, tags_table_id));
        }

        for (const auto & label_matcher_element : label_matcher)
            filters.push_back(makeASTLabelMatcher(label_matcher_element, tags_table_id, column_name_by_tag_name));

        if (filters.empty())
            return nullptr;

        return makeASTForLogicalAnd(std::move(filters));
    }

    /// Makes a mapping from a tag name to a column name.
    std::unordered_map<String, String> makeColumnNameByTagNameMap(const TimeSeriesSettings & storage_settings)
    {
        std::unordered_map<String, String> res;
        const Map & tags_to_columns = storage_settings[TimeSeriesSetting::tags_to_columns];
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            res[tag_name] = column_name;
        }
        return res;
    }

    /// The function builds a SELECT query for reading time series:
    /// SELECT tags_table.metric_name, tags_table.tag_column1, ... tags_table.tag_columnN, tags_table.tags,
    ///        groupArray(CAST(data_table.timestamp, 'DateTime64(3)'), CAST(data_table.value, 'Float64'))
    /// FROM data_table
    /// SEMI LEFT JOIN tag_table ON data_table.id = tags_table.id
    /// WHERE filter
    /// GROUP BY tags_table.tag_column1, ..., tags_table.tag_columnN, tags_table.tags
    ASTPtr buildSelectQueryForReadingTimeSeries(
        Int64 min_timestamp_ms,
        Int64 max_timestamp_ms,
        const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matcher,
        const TimeSeriesSettings & time_series_settings,
        const StorageID & data_table_id,
        const StorageID & tags_table_id)
    {
        auto select_query = std::make_shared<ASTSelectQuery>();

            /// SELECT tags_table.metric_name, any(tags_table.tag_column1), ... any(tags_table.tag_columnN), any(tags_table.tags),
            ///        groupArray(data_table.timestamp, data_table.value)
            {
            auto exp_list = std::make_shared<ASTExpressionList>();

            exp_list->children.push_back(
                makeASTColumn(tags_table_id, TimeSeriesColumnNames::MetricName));

            const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
            for (const auto & tag_name_and_column_name : tags_to_columns)
            {
                const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
                const auto & column_name = tuple.at(1).safeGet<String>();
                exp_list->children.push_back(
                    makeASTColumn(tags_table_id, column_name));
            }

            exp_list->children.push_back(
                makeASTColumn(tags_table_id, TimeSeriesColumnNames::Tags));

            exp_list->children.push_back(
                makeASTFunction("groupArray",
                                makeASTFunction("tuple",
                                                makeASTFunction("CAST", makeASTColumn(data_table_id, TimeSeriesColumnNames::Timestamp), std::make_shared<ASTLiteral>("DateTime64(3)")),
                                                makeASTFunction("CAST", makeASTColumn(data_table_id, TimeSeriesColumnNames::Value), std::make_shared<ASTLiteral>("Float64")))));

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, exp_list);
        }

        /// FROM data_table
        auto tables = std::make_shared<ASTTablesInSelectQuery>();

        {
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_exp = std::make_shared<ASTTableExpression>();
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(data_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);
        }

        /// SEMI LEFT JOIN tags_table ON data_table.id = tags_table.id
        {
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();

            auto table_join = std::make_shared<ASTTableJoin>();
            table_join->kind = JoinKind::Left;
            table_join->strictness = JoinStrictness::Semi;

            table_join->on_expression = makeASTFunction("equals", makeASTColumn(data_table_id, TimeSeriesColumnNames::ID), makeASTColumn(tags_table_id, TimeSeriesColumnNames::ID));
            table->table_join = table_join;

            auto table_exp = std::make_shared<ASTTableExpression>();
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(tags_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        auto column_name_by_tag_name = makeColumnNameByTagNameMap(time_series_settings);

        /// WHERE <filter>
        if (auto where = makeASTFilterForReadingTimeSeries(label_matcher, min_timestamp_ms, max_timestamp_ms, data_table_id, tags_table_id,
                                                           column_name_by_tag_name, time_series_settings[TimeSeriesSetting::filter_by_min_time_and_max_time]))
        {
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where));
        }

        /// GROUP BY tags_table.metric_name, tags_table.tag_column1, ..., tags_table.tag_columnN, tags_table.tags
        {
            auto exp_list = std::make_shared<ASTExpressionList>();

            exp_list->children.push_back(
                makeASTColumn(tags_table_id, TimeSeriesColumnNames::MetricName));

            const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
            for (const auto & tag_name_and_column_name : tags_to_columns)
            {
                const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
                const auto & column_name = tuple.at(1).safeGet<String>();
                exp_list->children.push_back(
                    makeASTColumn(tags_table_id, column_name));
            }

            exp_list->children.push_back(makeASTColumn(tags_table_id, TimeSeriesColumnNames::Tags));

            select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, exp_list);
        }

        return select_query;
    }

    /// Sorts a list of pairs {tag_name, tag_value} by tag name.
    void sortLabelsByName(std::vector<std::pair<std::string_view /* label_name */, std::string_view /* label_value */>> & labels)
    {
        auto less_by_label_name = [](const std::pair<std::string_view, std::string_view> & left, const std::pair<std::string_view, std::string_view> & right)
        {
            return left.first < right.first;
        };
        std::sort(labels.begin(), labels.end(), less_by_label_name);
    }

    /// Sorts a list of pairs {timestamp, value} by timestamp.
    void sortTimeSeriesByTimestamp(std::vector<std::pair<Int64 /* timestamp_ms */, Float64 /* value */>> & time_series)
    {
        auto less_by_timestamp = [](const std::pair<Int64, Float64> & left, const std::pair<Int64, Float64> & right)
        {
            return left.first < right.first;
        };
        std::sort(time_series.begin(), time_series.end(), less_by_timestamp);
    }

    /// Converts a block generated by the SELECT query for converting time series to the protobuf format.
    void convertBlockToProtobuf(
        Block && block,
        google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & out_time_series,
        const StorageID & time_series_storage_id,
        const TimeSeriesSettings & time_series_settings)
    {
        size_t num_rows = block.rows();
        if (!num_rows)
            return;

        size_t column_index = 0;

        /// We analyze columns sequentially.
        auto get_next_column_with_type = [&] -> const ColumnWithTypeAndName & { return block.getByPosition(column_index++); };
        auto get_next_column = [&] -> const IColumn & { return *(get_next_column_with_type().column); };

        /// Column "metric_name".
        const auto & metric_name_column_with_type = get_next_column_with_type();
        TimeSeriesColumnsValidator validator{time_series_storage_id, time_series_settings};
        validator.validateColumnForMetricName(metric_name_column_with_type);
        const auto & metric_name_column = *metric_name_column_with_type.column;

        /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
        std::unordered_map<String, const IColumn *> column_by_tag_name;
        const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_with_type = get_next_column_with_type();
            validator.validateColumnForTagValue(column_with_type);
            const auto & column = *column_with_type.column;
            column_by_tag_name[tag_name] = &column;
        }

        /// Column "tags".
        const auto & tags_column_with_type = get_next_column_with_type();
        validator.validateColumnForTagsMap(tags_column_with_type);
        const auto & tags_column = checkAndGetColumn<ColumnMap>(*tags_column_with_type.column);
        const auto & tags_names = tags_column.getNestedData().getColumn(0);
        const auto & tags_values = tags_column.getNestedData().getColumn(1);
        const auto & tags_offsets = tags_column.getNestedColumn().getOffsets();

        /// Column containing time series: groupArray(CAST(data_table.timestamp, 'DateTime64(3)'), CAST(data_table.value, 'Float64'))
        const auto & time_series_column = checkAndGetColumn<ColumnArray>(get_next_column());
        const auto & time_series_timestamps = checkAndGetColumn<ColumnDecimal<DateTime64>>(checkAndGetColumn<ColumnTuple>(time_series_column.getData()).getColumn(0));
        const auto & time_series_values = checkAndGetColumn<ColumnFloat64>(checkAndGetColumn<ColumnTuple>(time_series_column.getData()).getColumn(1));
        const auto & time_series_offsets = time_series_column.getOffsets();

        /// We will sort labels lexicographically and time series by timestamp before sending them to a client.
        std::vector<std::pair<std::string_view, std::string_view>> labels;
        std::vector<std::pair<Int64, Float64>> time_series;

        for (size_t i = 0; i != num_rows; ++i)
        {
            /// Collect labels.
            size_t num_labels = 1; /* 1 for a metric name */

            for (const auto & [_, column] : column_by_tag_name)
            {
                if (!column->isNullAt(i) && !column->getDataAt(i).empty())
                    ++num_labels;
            }

            size_t tags_start_offset = tags_offsets[i - 1];
            size_t tags_end_offset = tags_offsets[i];
            num_labels += tags_end_offset - tags_start_offset;

            labels.clear();
            labels.reserve(num_labels);

            labels.emplace_back(TimeSeriesTagNames::MetricName, metric_name_column.getDataAt(i));

            for (const auto & [tag_name, column] : column_by_tag_name)
            {
                if (!column->isNullAt(i) && !column->getDataAt(i).empty())
                    labels.emplace_back(tag_name, column->getDataAt(i));
            }

            for (size_t j = tags_start_offset; j != tags_end_offset; ++j)
            {
                std::string_view tag_name{tags_names.getDataAt(j)};
                std::string_view tag_value{tags_values.getDataAt(j)};
                labels.emplace_back(tag_name, tag_value);
            }

            /// Sort labels.
            sortLabelsByName(labels);

            /// Collect time series.
            size_t time_series_start_offset = time_series_offsets[i - 1];
            size_t time_series_end_offset = time_series_offsets[i];
            size_t num_time_series = time_series_end_offset - time_series_start_offset;

            time_series.clear();
            time_series.reserve(num_time_series);

            for (size_t j = time_series_start_offset; j != time_series_end_offset; ++j)
                time_series.emplace_back(time_series_timestamps.getElement(j), time_series_values.getElement(j));

            /// Sort time series.
            sortTimeSeriesByTimestamp(time_series);

            /// Prepare a result.
            auto & new_time_series = *out_time_series.Add();

            for (const auto & [label_name, label_value] : labels)
            {
                auto & new_label = *new_time_series.add_labels();
                new_label.set_name(label_name);
                new_label.set_value(label_value);
            }

            for (const auto & [timestamp, value] : time_series)
            {
                auto & new_sample = *new_time_series.add_samples();
                new_sample.set_timestamp(timestamp);
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
    const auto & time_series_settings = time_series_storage->getStorageSettings();
    auto data_table_id = time_series_storage->getTargetTableId(ViewTarget::Data);
    auto tags_table_id = time_series_storage->getTargetTableId(ViewTarget::Tags);

    ASTPtr select_query = buildSelectQueryForReadingTimeSeries(
        start_timestamp_ms, end_timestamp_ms, label_matcher, time_series_settings, data_table_id, tags_table_id);

    LOG_TRACE(log, "{}: Executing query {}",
              time_series_storage_id.getNameForLogs(), select_query);

    InterpreterSelectQuery interpreter(select_query, getContext(), SelectQueryOptions{});
    BlockIO io = interpreter.execute();
    PullingPipelineExecutor executor(io.pipeline);

    Block block;
    while (executor.pull(block))
    {
        LOG_TRACE(log, "{}: Pulled block with {} columns and {} rows",
                  time_series_storage_id.getNameForLogs(), block.columns(), block.rows());

        if (block)
            convertBlockToProtobuf(std::move(block), out_time_series, time_series_storage_id, time_series_settings);
    }

    LOG_TRACE(log, "{}: {} time series read",
              time_series_storage_id.getNameForLogs(), out_time_series.size());
}

}

#endif
