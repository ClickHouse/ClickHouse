#include <Storages/System/StorageSystemMetricLogView.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/IColumn.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/IInflatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Parsers/ASTOrderByElement.h>
#include <Storages/SelectQueryInfo.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Core/Settings.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Common/logger_useful.h>
#include <fmt/ranges.h>
#include <Processors/ISimpleTransform.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/TransposedMetricLog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace Setting
{
    extern const SettingsUInt64 max_bytes_in_set;
    extern const SettingsUInt64 max_rows_in_set;
    extern const SettingsOverflowMode set_overflow_mode;
    extern const SettingsBool transform_null_in;
}

namespace
{

constexpr auto VIEW_COLUMNS_ORDER =
{
    TransposedMetricLog::EVENT_TIME_NAME,
    TransposedMetricLog::VALUE_NAME,
    TransposedMetricLog::METRIC_NAME,
    TransposedMetricLog::HOSTNAME_NAME,
    TransposedMetricLog::EVENT_DATE_NAME,
};

constexpr auto HOUR_ALIAS_NAME = "hour";

/// Order for elements in view
constexpr size_t EVENT_TIME_POSITION = 0;
constexpr size_t VALUE_POSITION = 1;
constexpr size_t METRIC_POSITION = 2;
constexpr size_t HOSTNAME_POSITION = 3;
constexpr size_t EVENT_DATE_POSITION = 4;
constexpr size_t EVENT_TIME_HOUR_POSITION = 5;

/// SELECT event_time, value ..., metric FROM system.transposed_metric_log ORDER BY event_time;
std::shared_ptr<ASTSelectWithUnionQuery> getSelectQuery(const StorageID & source_storage_id)
{
    std::shared_ptr<ASTSelectWithUnionQuery> result = std::make_shared<ASTSelectWithUnionQuery>();
    std::shared_ptr<ASTSelectQuery> select_query = std::make_shared<ASTSelectQuery>();
    std::shared_ptr<ASTExpressionList> expression_list = std::make_shared<ASTExpressionList>();

    for (const auto & column_name : VIEW_COLUMNS_ORDER)
        expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));

    auto last_element = makeASTFunction("toStartOfHour", std::make_shared<ASTIdentifier>(TransposedMetricLog::EVENT_TIME_NAME));

    auto select_list_last_element = last_element->clone();
    select_list_last_element->setAlias(HOUR_ALIAS_NAME);
    expression_list->children.push_back(select_list_last_element);


    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(expression_list));

    auto tables = std::make_shared<ASTTablesInSelectQuery>();
    auto table = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expression = std::make_shared<ASTTableExpression>();
    auto database_and_table_name = std::make_shared<ASTTableIdentifier>(source_storage_id);
    table_expression->database_and_table_name = database_and_table_name;
    table->table_expression = table_expression;
    tables->children.emplace_back(table);
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

    std::shared_ptr<ASTExpressionList> order_by = std::make_shared<ASTExpressionList>();
    std::shared_ptr<ASTOrderByElement> order_by_date = std::make_shared<ASTOrderByElement>();
    order_by_date->children.emplace_back(std::make_shared<ASTIdentifier>(TransposedMetricLog::EVENT_DATE_NAME));
    order_by_date->direction = 1;
    std::shared_ptr<ASTOrderByElement> order_by_time = std::make_shared<ASTOrderByElement>();
    order_by_time->children.emplace_back(last_element);
    order_by_time->direction = 1;
    std::shared_ptr<ASTOrderByElement> order_by_metric = std::make_shared<ASTOrderByElement>();
    order_by_metric->children.emplace_back(std::make_shared<ASTIdentifier>(TransposedMetricLog::METRIC_NAME));
    order_by_metric->direction = 1;
    order_by->children.emplace_back(order_by_date);
    order_by->children.emplace_back(order_by_time);
    order_by->children.emplace_back(order_by_metric);

    select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, order_by);

    result->list_of_selects = std::make_shared<ASTExpressionList>();
    result->list_of_selects->children.emplace_back(select_query);

    return result;
}

ASTCreateQuery getCreateQuery(const StorageID & source_storage_id)
{
    ASTCreateQuery query;
    query.children.emplace_back(getSelectQuery(source_storage_id));
    query.select = query.children[0]->as<ASTSelectWithUnionQuery>();
    return query;
}

ColumnsDescription getColumnsDescription()
{
    NamesAndTypesList result;
    result.push_back(NameAndTypePair(TransposedMetricLog::HOSTNAME_NAME, std::make_shared<DataTypeString>()));
    result.push_back(NameAndTypePair(TransposedMetricLog::EVENT_DATE_NAME, std::make_shared<DataTypeDate>()));
    result.push_back(NameAndTypePair(TransposedMetricLog::EVENT_TIME_NAME, std::make_shared<DataTypeDateTime>()));
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        result.push_back(NameAndTypePair(std::string{TransposedMetricLog::PROFILE_EVENT_PREFIX} + ProfileEvents::getName(ProfileEvents::Event(i)), std::make_shared<DataTypeInt64>()));

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        result.push_back(NameAndTypePair(std::string{TransposedMetricLog::CURRENT_METRIC_PREFIX} + CurrentMetrics::getName(CurrentMetrics::Metric(i)), std::make_shared<DataTypeInt64>()));

    /// Doesn't support subsecond precision, it's just an alias
    NamesAndAliases aliases;
    aliases.push_back(NameAndAliasPair(TransposedMetricLog::EVENT_TIME_MICROSECONDS_NAME, std::make_shared<DataTypeDateTime64>(6), "toDateTime64(event_time, 6)"));

    return ColumnsDescription{result, aliases};
}

ColumnsDescription getColumnsDescriptionForView()
{
    NamesAndTypesList result;
    result.push_back(NameAndTypePair(TransposedMetricLog::EVENT_TIME_NAME, std::make_shared<DataTypeDateTime>()));
    result.push_back(NameAndTypePair(TransposedMetricLog::VALUE_NAME, std::make_shared<DataTypeInt64>()));
    result.push_back(NameAndTypePair(TransposedMetricLog::METRIC_NAME, std::make_shared<DataTypeString>()));
    result.push_back(NameAndTypePair(TransposedMetricLog::HOSTNAME_NAME, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())));
    result.push_back(NameAndTypePair(TransposedMetricLog::EVENT_DATE_NAME, std::make_shared<DataTypeDate>()));
    result.push_back(NameAndTypePair(HOUR_ALIAS_NAME, std::make_shared<DataTypeDateTime>()));

    return ColumnsDescription{result};
}

}

StorageSystemMetricLogView::StorageSystemMetricLogView(const StorageID & table_id, const StorageID & source_storage_id)
    : IStorage(table_id)
    , internal_view(table_id, getCreateQuery(source_storage_id), getColumnsDescriptionForView(), "")
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(getColumnsDescription());
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemMetricLogView::checkAlterIsPossible(const AlterCommands &, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alters of system tables are not supported");
}

namespace
{

/// Transpose structure with metrics/events as rows to
/// to columnar view.
class CustomFilterTransform : public IInflatingTransform
{
    HashMap<StringRef, size_t> mapping;
    Names column_names;
public:
    static constexpr auto SECONDS_IN_HOUR = 3600;
    String getName() const override { return "MetricLogCustomTransform"; }

    PODArray<UInt32> times;
    PODArray<UInt16> dates;
    std::vector<std::string> hostnames;
    size_t current_hour = 0;
    Int64 max_second_in_hour = -1;

    std::unordered_map<size_t, PODArray<Int64>> buffer;

    std::vector<Chunk> ready_hours;

    bool need_hostname = false;
    bool need_date = false;

    size_t max_block_size;

    CustomFilterTransform(Block input_header, Block output_header, size_t max_block_size_)
        : IInflatingTransform(input_header, output_header)
        , max_block_size(max_block_size_)
    {
        size_t counter = 0;
        column_names.reserve(output_header.columns());

        /// Preserve memory for each column
        for (const auto & column : output_header)
        {
            column_names.push_back(column.name);
            const auto & column_name = column_names.back();
            if (column_name.starts_with(TransposedMetricLog::PROFILE_EVENT_PREFIX) || column_name.starts_with(TransposedMetricLog::CURRENT_METRIC_PREFIX))
            {
                mapping[column_name] = counter;
                buffer[counter].resize_fill(SECONDS_IN_HOUR, 0L);
                counter++;
            }
            else if (column_name == TransposedMetricLog::HOSTNAME_NAME)
            {
                need_hostname = true;
            }
            else if (column_name == TransposedMetricLog::EVENT_DATE_NAME)
            {
                need_date = true;
            }
        }

        times.resize_fill(SECONDS_IN_HOUR, static_cast<UInt32>(0));

        if (need_date)
            dates.resize_fill(SECONDS_IN_HOUR, static_cast<UInt16>(0));
        if (need_hostname)
            hostnames.resize(SECONDS_IN_HOUR, "");
    }

    bool canGenerate() override
    {
        return !ready_hours.empty();
    }

    Chunk generate() override
    {
        auto result = std::move(ready_hours.back());
        ready_hours.pop_back();
        return result;
    }

    Chunk getRemaining() override
    {
        flushToChunk();

        if (ready_hours.empty())
            return Chunk();

        return generate();
    }

    void flushToChunk()
    {
        Chunk result;

        size_t rows_count = max_second_in_hour + 1;
        if (rows_count == 0)
            return;

        MutableColumns output_columns;
        output_columns.reserve(buffer.size() + need_date + need_hostname + 1);

        for (const auto & column_name : column_names)
        {
            if (column_name == TransposedMetricLog::EVENT_TIME_NAME)
            {
                output_columns.push_back(ColumnDateTime::create(times.begin(), times.begin() + rows_count));
                times.assign(rows_count, static_cast<UInt32>(0));
            }
            else if (column_name == TransposedMetricLog::EVENT_DATE_NAME)
            {
                output_columns.push_back(ColumnDate::create(dates.begin(), dates.begin() + rows_count));
                dates.assign(rows_count, static_cast<UInt16>(0));
            }
            else if (column_name == TransposedMetricLog::HOSTNAME_NAME)
            {
                auto string_column = ColumnString::create();
                for (size_t i = 0; i < rows_count; ++i)
                    string_column->insertData(hostnames[i].data(), hostnames[i].size());
                output_columns.push_back(std::move(string_column));
                hostnames.assign(rows_count, "");
            }
            else if (column_name.starts_with(TransposedMetricLog::PROFILE_EVENT_PREFIX) || column_name.starts_with(TransposedMetricLog::CURRENT_METRIC_PREFIX))
            {
                auto & column = buffer[mapping.at(column_name)];
                output_columns.push_back(ColumnInt64::create(column.begin(), column.begin() + rows_count));
                column.assign(rows_count, 0L);
            }
        }

        result.setColumns(std::move(output_columns), rows_count);
        ready_hours.emplace_back(std::move(result));
    }

    void consume(Chunk chunk) override
    {
        size_t rows_count = chunk.getNumRows();

        const auto & columns = chunk.getColumns();
        const auto & event_time_column = checkAndGetColumn<ColumnDateTime>(*columns[EVENT_TIME_POSITION]);
        const auto & value_column = checkAndGetColumn<ColumnInt64>(*columns[VALUE_POSITION]);
        const auto & metric_column = checkAndGetColumn<ColumnString>(*columns[METRIC_POSITION]);
        const auto & date_column = checkAndGetColumn<ColumnDate>(*columns[EVENT_DATE_POSITION]);
        const auto & hostname_column = checkAndGetColumn<ColumnLowCardinality>(*columns[HOSTNAME_POSITION]);
        const auto & hour_column = checkAndGetColumn<ColumnDateTime>(*columns[EVENT_TIME_HOUR_POSITION]);

        if (rows_count && current_hour == 0)
        {
            current_hour = hour_column.getInt(0);
        }

        for (size_t i = 0; i < rows_count; ++i)
        {
            size_t hour = hour_column.getInt(i);
            if (hour != current_hour)
            {
                flushToChunk();

                current_hour = hour;
                max_second_in_hour = -1;
            }

            auto time = event_time_column.getInt(i);

            auto second_in_hour = time - hour;
            max_second_in_hour = std::max<Int64>(second_in_hour, max_second_in_hour);
            times[second_in_hour] = time;

            if (need_date)
                dates[second_in_hour] = date_column.getUInt(i);

            if (need_hostname)
                hostnames[second_in_hour] = hostname_column.getDataAt(i).toString();

            StringRef metric_name = metric_column.getDataAt(i);
            auto * it = mapping.find(metric_name);
            if (it == mapping.end())
                continue;

            size_t event_index = it->value.second;

            Int64 value = value_column.getInt(i);
            buffer[event_index][second_in_hour] = value;
        }
    }
};

}

CustomMetricLogStep::CustomMetricLogStep(
    Block input_header_, Block output_header_, size_t max_block_size_)
     : ITransformingStep(
         input_header_, output_header_,
         ITransformingStep::Traits
         {
            .data_stream_traits = ITransformingStep::DataStreamTraits{.returns_single_stream = true, .preserves_number_of_streams = false, .preserves_sorting = true},
            .transform_traits = ITransformingStep::TransformTraits{.preserves_number_of_rows = false}
         })
    , max_block_size(max_block_size_)
{
}

void CustomMetricLogStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addTransform(std::make_shared<CustomFilterTransform>(input_headers[0], *output_header, max_block_size));
}

/// Adds filter to view similar to WHERE metric_name IN ('Metric1', 'Event1', ...)
void StorageSystemMetricLogView::addFilterByMetricNameStep(QueryPlan & query_plan, const Names & column_names, ContextPtr context)
{
    MutableColumnPtr column_for_set = ColumnString::create();
    for (const auto & column_name : column_names)
    {
        if (column_name.starts_with(TransposedMetricLog::PROFILE_EVENT_PREFIX) || column_name.starts_with(TransposedMetricLog::CURRENT_METRIC_PREFIX))
            column_for_set->insertData(column_name.data(), column_name.size());
    }

    if (column_for_set->empty())
        return;

    ColumnWithTypeAndName set_column(std::move(column_for_set), std::make_shared<DataTypeString>(), "__set");
    ColumnsWithTypeAndName set_columns;
    set_columns.push_back(set_column);
    const auto & settings = context->getSettingsRef();
    SizeLimits size_limits_for_set = {settings[Setting::max_rows_in_set], settings[Setting::max_bytes_in_set], settings[Setting::set_overflow_mode]};

    auto in_function = FunctionFactory::instance().get("in", context->getQueryContext());
    auto future_set = std::make_shared<FutureSetFromTuple>(CityHash_v1_0_2::uint128{}, nullptr, set_columns, false, size_limits_for_set);
    auto column_set = ColumnSet::create(1, std::move(future_set));
    ColumnWithTypeAndName set_for_dag(std::move(column_set), std::make_shared<DataTypeSet>(), "_filter");

    ActionsDAG dag(query_plan.getCurrentHeader().getColumnsWithTypeAndName());
    const auto & metric_input = dag.findInOutputs(TransposedMetricLog::METRIC_NAME);
    const auto & filter_dag_column = dag.addColumn(set_for_dag);
    const auto & output = dag.addFunction(in_function, {&metric_input, &filter_dag_column}, "_special_filter_for_metric_log");
    dag.getOutputs().push_back(&output);

    query_plan.addStep(std::make_unique<FilterStep>(query_plan.getCurrentHeader(), std::move(dag), "_special_filter_for_metric_log", true));
}

void StorageSystemMetricLogView::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr &,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    std::shared_ptr<StorageSnapshot> snapshot_for_view = std::make_shared<StorageSnapshot>(internal_view, internal_view.getInMemoryMetadataPtr());
    Block input_header = snapshot_for_view->metadata->getSampleBlock();

    internal_view.read(query_plan, input_header.getNames(), snapshot_for_view, query_info, context, processed_stage, max_block_size, num_streams);

    Block full_output_header = getInMemoryMetadataPtr()->getSampleBlock();

    /// Doesn't make sense to filter by metric, we will not filter out anything
    bool read_all_columns = full_output_header.columns() == column_names.size();
    if (!read_all_columns)
        addFilterByMetricNameStep(query_plan, column_names, context);

    Block output_header;
    for (const auto & name : column_names)
        output_header.insert(full_output_header.getByName(name));

    /// Otherwise we can allocate too much memory
    if (column_names.size() > 100)
        max_block_size = std::min(8192UL, max_block_size);

    query_plan.addStep(std::make_unique<CustomMetricLogStep>(input_header, output_header, max_block_size));
}

}
