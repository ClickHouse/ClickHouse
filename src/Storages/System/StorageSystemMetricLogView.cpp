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
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/IInflatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Parsers/ASTOrderByElement.h>
#include <Storages/SelectQueryInfo.h>

#include <Common/logger_useful.h>
#include <Processors/ISimpleTransform.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

//constexpr auto FIXED_COLUMNS = {"hostname", "event_date", "event_time"};
//
//ASTPtr getExpressionForProfileEvent(const std::string & prefix, const std::string & name)
//{
//    auto equals = makeASTFunction("equals", std::make_shared<ASTIdentifier>("metric"), std::make_shared<ASTLiteral>(name));
//    auto sumif = makeASTFunction("sumIf", std::make_shared<ASTIdentifier>("value"), equals);
//
//    sumif->alias = prefix + "_" + name;
//    return sumif;
//}

//std::shared_ptr<ASTSelectWithUnionQuery> getSelectQuery()
//{
//    std::shared_ptr<ASTSelectWithUnionQuery> result = std::make_shared<ASTSelectWithUnionQuery>();
//    std::shared_ptr<ASTSelectQuery> select_query = std::make_shared<ASTSelectQuery>();
//    std::shared_ptr<ASTExpressionList> expression_list = std::make_shared<ASTExpressionList>();
//
//    for (const auto & name : FIXED_COLUMNS)
//        expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(name));
//
//    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
//        expression_list->children.push_back(getExpressionForProfileEvent("ProfileEvent", ProfileEvents::getName(ProfileEvents::Event(i))));
//
//    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
//        expression_list->children.push_back(getExpressionForProfileEvent("CurrentMetric", CurrentMetrics::getName(CurrentMetrics::Metric(i))));
//
//    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(expression_list));
//
//    auto tables = std::make_shared<ASTTablesInSelectQuery>();
//    auto table = std::make_shared<ASTTablesInSelectQueryElement>();
//    auto table_expression = std::make_shared<ASTTableExpression>();
//    auto database_and_table_name = std::make_shared<ASTTableIdentifier>("system", "transposed_metric_log");
//    table_expression->database_and_table_name = database_and_table_name;
//    table->table_expression = table_expression;
//    tables->children.emplace_back(table);
//    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
//
//    std::shared_ptr<ASTExpressionList> group_by = std::make_shared<ASTExpressionList>();
//    for (const auto & name : FIXED_COLUMNS)
//        group_by->children.emplace_back(std::make_shared<ASTIdentifier>(name));
//
//    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);
//
//    result->list_of_selects = std::make_shared<ASTExpressionList>();
//    result->list_of_selects->children.emplace_back(select_query);
//
//    return result;
//}

std::shared_ptr<ASTSelectWithUnionQuery> getSelectQuery()
{
    std::shared_ptr<ASTSelectWithUnionQuery> result = std::make_shared<ASTSelectWithUnionQuery>();
    std::shared_ptr<ASTSelectQuery> select_query = std::make_shared<ASTSelectQuery>();
    std::shared_ptr<ASTExpressionList> expression_list = std::make_shared<ASTExpressionList>();

    expression_list->children.emplace_back(std::make_shared<ASTIdentifier>("event_time"));
    expression_list->children.emplace_back(std::make_shared<ASTIdentifier>("is_event"));
    expression_list->children.emplace_back(std::make_shared<ASTIdentifier>("value"));
    expression_list->children.emplace_back(std::make_shared<ASTIdentifier>("metric"));
    expression_list->children.emplace_back(std::make_shared<ASTIdentifier>("hostname"));
    expression_list->children.emplace_back(std::make_shared<ASTIdentifier>("event_date"));

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
    order_by_date->children.emplace_back(std::make_shared<ASTIdentifier>("event_date"));
    std::shared_ptr<ASTOrderByElement> order_by_time = std::make_shared<ASTOrderByElement>();
    order_by_time->children.emplace_back(std::make_shared<ASTIdentifier>("event_time"));
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
    result.push_back(NameAndTypePair("hostname", std::make_shared<DataTypeString>()));
    result.push_back(NameAndTypePair("event_date", std::make_shared<DataTypeDate>()));
    result.push_back(NameAndTypePair("event_time", std::make_shared<DataTypeDateTime>()));
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        static const std::string profile_event_prefix = "ProfileEvent_";
        result.push_back(NameAndTypePair(profile_event_prefix + ProfileEvents::getName(ProfileEvents::Event(i)), std::make_shared<DataTypeInt64>()));
    }

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        static const std::string current_metric_prefix = "CurrentMetric_";
        result.push_back(NameAndTypePair(current_metric_prefix + CurrentMetrics::getName(CurrentMetrics::Metric(i)), std::make_shared<DataTypeInt64>()));
    }

    return ColumnsDescription{result};
}

ColumnsDescription getColumnsDescriptionForView()
{
    NamesAndTypesList result;
    result.push_back(NameAndTypePair("event_time", std::make_shared<DataTypeDateTime>()));
    result.push_back(NameAndTypePair("is_event", std::make_shared<DataTypeUInt8>()));
    result.push_back(NameAndTypePair("value", std::make_shared<DataTypeInt64>()));
    result.push_back(NameAndTypePair("metric", std::make_shared<DataTypeString>()));
    result.push_back(NameAndTypePair("hostname", std::make_shared<DataTypeString>()));
    result.push_back(NameAndTypePair("event_date", std::make_shared<DataTypeDate>()));



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
    events_count = ProfileEvents::end();
    metrics_count = CurrentMetrics::end();

    events_mapping.reserve(events_count);
    metrics_mapping.reserve(metrics_count);

    for (ProfileEvents::Event e = ProfileEvents::Event(0), end = ProfileEvents::end(); e < end; ++e)
        events_mapping[ProfileEvents::getName(e)] = e;

    for (size_t c = 0, end = CurrentMetrics::end(); c < end; ++c)
        metrics_mapping[CurrentMetrics::getName(CurrentMetrics::Metric(c))] = c;
}

void StorageSystemMetricLogView::checkAlterIsPossible(const AlterCommands &, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alters of system tables are not supported");
}

namespace
{

class CustomFilterTransform : public IInflatingTransform
{
    const HashMap<StringRef, size_t> & events_mapping;
    const HashMap<StringRef, size_t> & metrics_mapping;
public:
    size_t event_time_position = 0;
    size_t is_event_position = 1;
    size_t value_position = 2;
    size_t metric_position = 3;
    size_t hostname_position = 4;
    size_t date_position = 5;

    String getName() const override { return "MetricLogCustomTransform"; }

    PODArray<UInt32> times;
    PODArray<UInt16> dates;
    std::vector<std::string> hostnames;

    std::vector<PODArray<Int64>> buffer_events;
    std::vector<PODArray<Int64>> buffer_metrics;
    std::vector<ColumnInt64 *> value_pointers;

    CustomFilterTransform(Block input_header, Block output_header, const HashMap<StringRef, size_t> & events_,  const HashMap<StringRef, size_t> & metrics_)
        : IInflatingTransform(input_header, output_header)
        , events_mapping(events_)
        , metrics_mapping(metrics_)

    {
        buffer_events.resize(events_mapping.size());
        for (auto & event_array : buffer_events)
            event_array.reserve(8192);

        buffer_metrics.resize(metrics_mapping.size());
        for (auto & event_array : buffer_metrics)
            event_array.reserve(8192);

        times.reserve(8192);
        dates.reserve(8192);
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

        for (auto & buf : buffer_events)
            if (buf.size() < times.size())
                buf.push_back(0);

        for (auto & buf : buffer_metrics)
            if (buf.size() < times.size())
                buf.push_back(0);


        MutableColumns output_columns;
        output_columns.reserve(buffer_events.size() + buffer_metrics.size() + 3);

        auto string_column = ColumnString::create();
        for (const auto & hostname : hostnames)
            string_column->insertData(hostname.data(), hostname.size());

        output_columns.push_back(std::move(string_column));
        output_columns.push_back(ColumnDate::create(dates.begin(), dates.end()));
        output_columns.push_back(ColumnDateTime::create(times.begin(), times.end()));

        hostnames.clear();
        dates.clear();
        times.clear();

        for (PODArray<Int64> & buffer_event : buffer_events)
        {
            output_columns.push_back(ColumnInt64::create(buffer_event.begin(), buffer_event.end()));
            buffer_event.clear();
        }

        for (PODArray<Int64> & buffer_metric : buffer_metrics)
        {
            output_columns.push_back(ColumnInt64::create(buffer_metric.begin(), buffer_metric.end()));
            buffer_metric.clear();
        }

        result.setColumns(std::move(output_columns), dest_rows_count);
        return result;
    }

    Chunk getRemaining() override
    {
        if (times.empty())
            return Chunk();

        return generate();
    }

    void consume(Chunk chunk) override
    {
        size_t rows_count = chunk.getNumRows();

        const auto & columns = chunk.getColumns();
        const auto & event_time_column = checkAndGetColumn<ColumnDateTime>(*columns[event_time_position]);
        const auto & is_event_column = checkAndGetColumn<ColumnUInt8>(*columns[is_event_position]);
        const auto & value_column = checkAndGetColumn<ColumnInt64>(*columns[value_position]);
        const auto & metric_column = checkAndGetColumn<ColumnString>(*columns[metric_position]);
        const auto & date_column = checkAndGetColumn<ColumnDate>(*columns[date_position]);
        const auto & hostname_column = checkAndGetColumn<ColumnString>(*columns[hostname_position]);

        if (rows_count && times.empty())
        {
            times.push_back(event_time_column.getInt(0));
            dates.push_back(date_column.getUInt(0));
            hostnames.push_back(hostname_column.getDataAt(0).toString());
        }

        for (size_t i = 0; i < rows_count; ++i)
        {
            auto time = event_time_column.getInt(i);
            bool is_event = is_event_column.getBool(i);
            Int64 value = value_column.getInt(i);

            if (time != times.back())
            {
                for (auto & buf : buffer_events)
                    if (buf.size() < times.size())
                        buf.push_back(0);

                for (auto & buf : buffer_metrics)
                    if (buf.size() < times.size())
                        buf.push_back(0);

                times.push_back(time);
                dates.push_back(date_column.getUInt(i));
                hostnames.push_back(hostname_column.getDataAt(i).toString());
            }

            if (is_event)
            {
                buffer_events[events_mapping.at(metric_column.getDataAt(i))].push_back(value);
            }
            else
                buffer_metrics[metrics_mapping.at(metric_column.getDataAt(i))].push_back(value);
        }
    }
};


}

CustomMetricLogStep::CustomMetricLogStep(Block input_header_, Block output_header_, const HashMap<StringRef, size_t> & events_,  const HashMap<StringRef, size_t> & metrics_)
     : ITransformingStep(
         input_header_, output_header_,
         ITransformingStep::Traits{
            .data_stream_traits = ITransformingStep::DataStreamTraits{ .returns_single_stream = true, .preserves_number_of_streams = false, .preserves_sorting = true},
            .transform_traits = ITransformingStep::TransformTraits{.preserves_number_of_rows = false}
     })
    , events_mapping(events_)
    , metrics_mapping(metrics_)
{
}

void CustomMetricLogStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addTransform(std::make_shared<CustomFilterTransform>(input_headers[0], *output_header, events_mapping, metrics_mapping));
}


void StorageSystemMetricLogView::read(
    QueryPlan & query_plan,
    const Names &,
    const StorageSnapshotPtr &,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    std::shared_ptr<StorageSnapshot> snapshot_for_view = std::make_shared<StorageSnapshot>(internal_view, internal_view.getInMemoryMetadataPtr());
    Block input_header;
    input_header.insert({nullptr, std::make_shared<DataTypeDateTime>(), "event_time"});
    input_header.insert({nullptr, std::make_shared<DataTypeUInt8>(), "is_event"});
    input_header.insert({nullptr, std::make_shared<DataTypeInt64>(), "value"});
    input_header.insert({nullptr, std::make_shared<DataTypeString>(), "metric"});
    input_header.insert({nullptr, std::make_shared<DataTypeString>(), "hostname"});
    input_header.insert({nullptr, std::make_shared<DataTypeDate>(), "event_date"});

    internal_view.read(query_plan, input_header.getNames(), snapshot_for_view, query_info, context, processed_stage, max_block_size, num_streams);

    Block output_header = getInMemoryMetadataPtr()->getSampleBlock();

    query_plan.addStep(std::make_unique<CustomMetricLogStep>(input_header, output_header, events_mapping, metrics_mapping));

}


}
