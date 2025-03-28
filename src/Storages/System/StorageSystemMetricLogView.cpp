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
#include <Processors/ISimpleTransform.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>

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

constexpr auto EVENT_TIME_NAME = "event_time";
constexpr auto VALUE_NAME = "value";
constexpr auto EVENT_DATE_NAME = "event_date";
constexpr auto METRIC_NAME = "metric";
constexpr auto HOSTNAME_NAME = "hostname";

constexpr auto VIEW_COLUMNS_ORDER = {
    EVENT_TIME_NAME, VALUE_NAME, METRIC_NAME, HOSTNAME_NAME, EVENT_DATE_NAME
};

constexpr size_t EVENT_TIME_POSITION = 0;
constexpr size_t VALUE_POSITION = 1;
constexpr size_t METRIC_POSITION = 2;
constexpr size_t HOSTNAME_POSITION = 3;
constexpr size_t EVENT_DATE_POSITION = 4;

constexpr std::string_view PROFILE_EVENT_PREFIX = "ProfileEvent_";
constexpr std::string_view CURRENT_METRIC_PREFIX = "CurrentMetric_";


std::shared_ptr<ASTSelectWithUnionQuery> getSelectQuery()
{
    std::shared_ptr<ASTSelectWithUnionQuery> result = std::make_shared<ASTSelectWithUnionQuery>();
    std::shared_ptr<ASTSelectQuery> select_query = std::make_shared<ASTSelectQuery>();
    std::shared_ptr<ASTExpressionList> expression_list = std::make_shared<ASTExpressionList>();

    for (const auto & column_name : VIEW_COLUMNS_ORDER)
        expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(expression_list));

    auto tables = std::make_shared<ASTTablesInSelectQuery>();
    auto table = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expression = std::make_shared<ASTTableExpression>();
    auto database_and_table_name = std::make_shared<ASTTableIdentifier>("system", "transposed_metric_log");
    table_expression->database_and_table_name = database_and_table_name;
    table->table_expression = table_expression;
    tables->children.emplace_back(table);
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

    std::shared_ptr<ASTExpressionList> order_by = std::make_shared<ASTExpressionList>();
    std::shared_ptr<ASTOrderByElement> order_by_date = std::make_shared<ASTOrderByElement>();
    order_by_date->children.emplace_back(std::make_shared<ASTIdentifier>(EVENT_DATE_NAME));
    order_by_date->direction = 1;
    std::shared_ptr<ASTOrderByElement> order_by_time = std::make_shared<ASTOrderByElement>();
    order_by_time->children.emplace_back(std::make_shared<ASTIdentifier>(HOSTNAME_NAME));
    order_by_time->direction = 1;
    order_by->children.emplace_back(order_by_date);
    order_by->children.emplace_back(order_by_time);

    select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, order_by);

    result->list_of_selects = std::make_shared<ASTExpressionList>();
    result->list_of_selects->children.emplace_back(select_query);

    return result;
}


ASTCreateQuery getCreateQuery()
{
    ASTCreateQuery query;
    query.children.emplace_back(getSelectQuery());
    query.select = query.children[0]->as<ASTSelectWithUnionQuery>();
    return query;
}



ColumnsDescription getColumnsDescription()
{
    NamesAndTypesList result;
    result.push_back(NameAndTypePair(HOSTNAME_NAME, std::make_shared<DataTypeString>()));
    result.push_back(NameAndTypePair(EVENT_DATE_NAME, std::make_shared<DataTypeDate>()));
    result.push_back(NameAndTypePair(EVENT_TIME_NAME, std::make_shared<DataTypeDateTime>()));
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        result.push_back(NameAndTypePair(std::string{PROFILE_EVENT_PREFIX} + ProfileEvents::getName(ProfileEvents::Event(i)), std::make_shared<DataTypeInt64>()));

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        result.push_back(NameAndTypePair(std::string{CURRENT_METRIC_PREFIX} + CurrentMetrics::getName(CurrentMetrics::Metric(i)), std::make_shared<DataTypeInt64>()));

    return ColumnsDescription{result};
}

ColumnsDescription getColumnsDescriptionForView()
{
    NamesAndTypesList result;
    result.push_back(NameAndTypePair(EVENT_TIME_NAME, std::make_shared<DataTypeDateTime>()));
    result.push_back(NameAndTypePair(VALUE_NAME, std::make_shared<DataTypeInt64>()));
    result.push_back(NameAndTypePair(METRIC_NAME, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())));
    result.push_back(NameAndTypePair(HOSTNAME_NAME, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())));
    result.push_back(NameAndTypePair(EVENT_DATE_NAME, std::make_shared<DataTypeDate>()));

    return ColumnsDescription{result};
}

}

StorageSystemMetricLogView::StorageSystemMetricLogView(const StorageID & table_id)
    : IStorage(table_id)
    , internal_view(table_id, getCreateQuery(), getColumnsDescriptionForView(), "")
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

class CustomFilterTransform : public IInflatingTransform
{
    HashMap<StringRef, size_t> mapping;
    Names column_names;
public:
    String getName() const override { return "MetricLogCustomTransform"; }

    PODArray<UInt32> times;
    PODArray<UInt16> dates;
    std::vector<std::string> hostnames;

    std::unordered_map<size_t, PODArray<Int64>> buffer;

    bool need_hostname = false;
    bool need_date = false;

    CustomFilterTransform(Block input_header, Block output_header)
        : IInflatingTransform(input_header, output_header)
    {
        size_t counter = 0;
        column_names.reserve(output_header.columns());

        for (const auto & column : output_header)
        {
            column_names.push_back(column.name);
            const auto & column_name = column_names.back();
            if (column_name.starts_with(PROFILE_EVENT_PREFIX) || column_name.starts_with(CURRENT_METRIC_PREFIX))
            {
                mapping[column_name] = counter;
                buffer[counter].reserve(8192);
                counter++;
            }
            else if (column_name == HOSTNAME_NAME)
                need_hostname = true;
            else if (column_name == EVENT_DATE_NAME)
                need_date = true;
        }

        times.reserve(8192);

        if (need_date)
            dates.reserve(8192);
        if (need_hostname)
            hostnames.reserve(8192);
    }

    bool canGenerate() override
    {
        return times.size() >= 8192;
    }

    Chunk generate() override
    {
        Chunk result;

        size_t dest_rows_count = times.size();

        alignMissingRows();

        MutableColumns output_columns;
        output_columns.reserve(buffer.size() + need_date + need_hostname + 1);

        for (const auto & column_name : column_names)
        {
            if (column_name == EVENT_TIME_NAME)
            {
                output_columns.push_back(ColumnDateTime::create(times.begin(), times.end()));
            }
            else if (column_name == EVENT_DATE_NAME)
            {
                output_columns.push_back(ColumnDate::create(dates.begin(), dates.end()));
            }
            else if (column_name == HOSTNAME_NAME)
            {
                auto string_column = ColumnString::create();
                for (const auto & hostname : hostnames)
                    string_column->insertData(hostname.data(), hostname.size());
                output_columns.push_back(std::move(string_column));
            }
            else if (column_name.starts_with(PROFILE_EVENT_PREFIX) || column_name.starts_with(CURRENT_METRIC_PREFIX))
            {
                auto & column = buffer[mapping.at(column_name)];
                output_columns.push_back(ColumnInt64::create(column.begin(), column.end()));
                column.clear();
            }
        }

        hostnames.clear();
        dates.clear();
        times.clear();

        result.setColumns(std::move(output_columns), dest_rows_count);
        return result;
    }

    Chunk getRemaining() override
    {
        if (times.empty())
            return Chunk();

        return generate();
    }

    void alignMissingRows()
    {
        for (auto & [_, buf] : buffer)
            if (buf.size() < times.size())
                buf.push_back(0);
    }

    void consume(Chunk chunk) override
    {
        size_t rows_count = chunk.getNumRows();

        const auto & columns = chunk.getColumns();
        const auto & event_time_column = checkAndGetColumn<ColumnDateTime>(*columns[EVENT_TIME_POSITION]);
        const auto & value_column = checkAndGetColumn<ColumnInt64>(*columns[VALUE_POSITION]);
        const auto & metric_column = checkAndGetColumn<ColumnLowCardinality>(*columns[METRIC_POSITION]);
        const auto & date_column = checkAndGetColumn<ColumnDate>(*columns[EVENT_DATE_POSITION]);
        const auto & hostname_column = checkAndGetColumn<ColumnLowCardinality>(*columns[HOSTNAME_POSITION]);

        if (rows_count && times.empty())
        {
            times.push_back(event_time_column.getInt(0));
            if (need_date)
                dates.push_back(date_column.getUInt(0));
            if (need_hostname)
                hostnames.push_back(hostname_column.getDataAt(0).toString());
        }

        for (size_t i = 0; i < rows_count; ++i)
        {
            auto time = event_time_column.getInt(i);

            if (time != times.back())
            {
                alignMissingRows();

                times.push_back(time);

                if (need_date)
                    dates.push_back(date_column.getUInt(i));

                if (need_hostname)
                    hostnames.push_back(hostname_column.getDataAt(i).toString());
            }

            StringRef metric_name = metric_column.getDataAt(i);
            auto * it = mapping.find(metric_name);
            if (it == mapping.end())
                continue;

            size_t event_index = it->value.second;

            Int64 value = value_column.getInt(i);
            if (buffer[event_index].size() == times.size())
                buffer[event_index].back() = value;
            else
                buffer[event_index].push_back(value);
        }
    }
};

}

CustomMetricLogStep::CustomMetricLogStep(
    Block input_header_, Block output_header_)
     : ITransformingStep(
         input_header_, output_header_,
         ITransformingStep::Traits{
            .data_stream_traits = ITransformingStep::DataStreamTraits{.returns_single_stream = true, .preserves_number_of_streams = false, .preserves_sorting = true},
            .transform_traits = ITransformingStep::TransformTraits{.preserves_number_of_rows = false}
     })
{
}

void CustomMetricLogStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addTransform(std::make_shared<CustomFilterTransform>(input_headers[0], *output_header));
}


void StorageSystemMetricLogView::addFilterByMetricNameStep(QueryPlan & query_plan, const Names & column_names, ContextPtr context)
{
    MutableColumnPtr column_for_set = ColumnString::create();
    for (const auto & column_name : column_names)
    {
        if (column_name.starts_with(PROFILE_EVENT_PREFIX) || column_name.starts_with(CURRENT_METRIC_PREFIX))
            column_for_set->insertData(column_name.data(), column_name.size());
    }

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
    const auto & metric_input = dag.findInOutputs(METRIC_NAME);
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

    addFilterByMetricNameStep(query_plan, column_names, context);

    Block full_output_header = getInMemoryMetadataPtr()->getSampleBlock();

    Block output_header;
    for (const auto & name : column_names)
        output_header.insert(full_output_header.getByName(name));

    query_plan.addStep(std::make_unique<CustomMetricLogStep>(input_header, output_header));
}

}
