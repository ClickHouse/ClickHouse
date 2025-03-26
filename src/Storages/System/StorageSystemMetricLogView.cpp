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
    expression_list->children.emplace_back(std::make_shared<ASTIdentifier>("number"));
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
    std::shared_ptr<ASTOrderByElement> order_by_element = std::make_shared<ASTOrderByElement>();
    order_by_element->children.emplace_back(std::make_shared<ASTIdentifier>("event_time"));
    order_by->children.emplace_back(order_by_element);

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
    result.push_back(NameAndTypePair("number", std::make_shared<DataTypeUInt32>()));
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
}

void StorageSystemMetricLogView::checkAlterIsPossible(const AlterCommands &, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alters of system tables are not supported");
}

namespace
{

class CustomFilterTransform : public IInflatingTransform
{
public:
    size_t event_time_position = 0;
    size_t is_event_position = 1;
    size_t value_position = 2;
    size_t number_position = 3;
    size_t hostname_position = 4;
    size_t date_position = 5;
    static inline const auto PROFILE_EVENTS_COUNT = ProfileEvents::end();
    static inline const auto METRICS_EVENTS_COUNT = CurrentMetrics::end();

    Int64 current_second{0};
    UInt64 current_date{0};
    std::string current_hostname;
    std::array<Int64, 1000> events_array{};
    std::array<Int64, 1000> metrics_array{};

    String getName() const override { return "MetricLogCustomTransform"; }

    MutableColumns output_columns;
    std::vector<ColumnInt64 *> value_pointers;

    CustomFilterTransform(Block input_header, Block output_header)
        : IInflatingTransform(input_header, output_header)
    {

    }

    void initOutputColumns()
    {
        output_columns.push_back(ColumnString::create());
        output_columns.push_back(ColumnDate::create());
        output_columns.push_back(ColumnDateTime::create());

        for (size_t i = 0; i < PROFILE_EVENTS_COUNT + METRICS_EVENTS_COUNT; ++i)
        {
            output_columns.push_back(ColumnInt64::create());
            value_pointers.emplace_back(typeid_cast<ColumnInt64 *>(output_columns.back().get()));
        }
    }

    bool canGenerate() override
    {
        return !output_columns.empty();
    }

    Chunk generate() override
    {
        Chunk result;
        if (!output_columns.empty())
        {
            size_t dest_rows_count = output_columns[0]->size();
            result.setColumns(std::move(output_columns), dest_rows_count);
            output_columns.clear();
            value_pointers.clear();
        }
        return result;
    }

    Chunk getRemaining() override
    {
        if (current_second == 0)
            return Chunk();

        if (output_columns.empty())
            initOutputColumns();

        output_columns[0]->insert(current_hostname);
        output_columns[1]->insert(current_date);
        output_columns[2]->insert(current_second);
        for (ProfileEvents::Event e = ProfileEvents::Event(0), end = ProfileEvents::end(); e < end; ++e)
            value_pointers[e]->insertValue(events_array[e]);

        for (size_t c = 0, end = CurrentMetrics::end(); c < end; ++c)
            value_pointers[c + PROFILE_EVENTS_COUNT]->insertValue(metrics_array[c]);

        return generate();
    }

    void consume(Chunk chunk) override
    {
        size_t rows_count = chunk.getNumRows();

        const auto & columns = chunk.getColumns();
        const auto & event_time_column = checkAndGetColumn<ColumnDateTime>(*columns[event_time_position]);
        const auto & is_event_column = checkAndGetColumn<ColumnUInt8>(*columns[is_event_position]);
        const auto & value_column = checkAndGetColumn<ColumnInt64>(*columns[value_position]);
        const auto & number_column = checkAndGetColumn<ColumnUInt32>(*columns[number_position]);
        const auto & date_column = checkAndGetColumn<ColumnDate>(*columns[date_position]);
        const auto & hostname_column = checkAndGetColumn<ColumnString>(*columns[hostname_position]);

        if (rows_count)
        {
            current_hostname = hostname_column.getDataAt(0).toString();
            current_date = date_column.getUInt(0);
        }

        for (size_t i = 0; i < rows_count; ++i)
        {
            auto time = event_time_column.getInt(i);
            bool is_event = is_event_column.getBool(i);
            Int64 value = value_column.getInt(i);
            size_t event_number = number_column.getUInt(i);

            if (time != current_second)
            {
                if (output_columns.empty())
                {
                    initOutputColumns();
                }

                output_columns[0]->insert(current_hostname);
                output_columns[1]->insert(current_date);
                output_columns[2]->insert(current_second);
                for (ProfileEvents::Event e = ProfileEvents::Event(0), end = ProfileEvents::end(); e < end; ++e)
                    value_pointers[e]->insertValue(events_array[e]);

                for (size_t c = 0, end = CurrentMetrics::end(); c < end; ++c)
                    value_pointers[c + PROFILE_EVENTS_COUNT]->insertValue(metrics_array[c]);

                current_second = time;
                current_hostname = hostname_column.getDataAt(i).toString();
                current_date = date_column.getUInt(i);
                std::fill(events_array.begin(), events_array.end(), 0);
                std::fill(metrics_array.begin(), metrics_array.end(), 0);
            }
            if (is_event)
                events_array[event_number] = value;
            else
                metrics_array[event_number] = value;
        }
    }
};


}

CustomMetricLogStep::CustomMetricLogStep(Block input_header_, Block output_header_)
     : ITransformingStep(
         input_header_, output_header_,
         ITransformingStep::Traits{
            .data_stream_traits = ITransformingStep::DataStreamTraits{ .returns_single_stream = true, .preserves_number_of_streams = false, .preserves_sorting = true},
            .transform_traits = ITransformingStep::TransformTraits{.preserves_number_of_rows = false}
     })
{
}

void CustomMetricLogStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addTransform(std::make_shared<CustomFilterTransform>(input_headers[0], *output_header));
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
    input_header.insert({nullptr, std::make_shared<DataTypeUInt32>(), "number"});
    input_header.insert({nullptr, std::make_shared<DataTypeString>(), "hostname"});
    input_header.insert({nullptr, std::make_shared<DataTypeDate>(), "event_date"});

    internal_view.read(query_plan, input_header.getNames(), snapshot_for_view, query_info, context, processed_stage, max_block_size, num_streams);

    Block output_header = getInMemoryMetadataPtr()->getSampleBlock();

    query_plan.addStep(std::make_unique<CustomMetricLogStep>(input_header, output_header));

}


}
