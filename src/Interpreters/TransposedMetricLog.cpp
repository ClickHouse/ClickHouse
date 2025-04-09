#include <Interpreters/TransposedMetricLog.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeSet.h>
#include <Storages/StorageView.h>
#include <Columns/ColumnsCommon.h>
#include <Storages/SelectQueryInfo.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnTuple.h>
#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/CurrentMetrics.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/parseQuery.h>
#include <Processors/QueryPlan/CustomMetricLogViewStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/logger_useful.h>
#include <Processors/QueryPlan/FilterStep.h>

#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Interpreters/InterpreterRenameQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Core/Settings.h>


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

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
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
    order_by_time->children.emplace_back(std::make_shared<ASTIdentifier>("hour"));
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
    result.push_back(NameAndTypePair(TransposedMetricLog::EVENT_TIME_MICROSECONDS_NAME, std::make_shared<DataTypeDateTime64>(6)));
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        result.push_back(NameAndTypePair(std::string{TransposedMetricLog::PROFILE_EVENT_PREFIX} + ProfileEvents::getName(ProfileEvents::Event(i)), std::make_shared<DataTypeUInt64>()));

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        result.push_back(NameAndTypePair(std::string{TransposedMetricLog::CURRENT_METRIC_PREFIX} + CurrentMetrics::getName(CurrentMetrics::Metric(i)), std::make_shared<DataTypeInt64>()));

    return ColumnsDescription{result};
}

ColumnsDescription getColumnsDescriptionForView()
{
    NamesAndTypesList result;
    result.push_back(NameAndTypePair(TransposedMetricLog::EVENT_TIME_NAME, std::make_shared<DataTypeDateTime>()));
    result.push_back(NameAndTypePair(TransposedMetricLog::VALUE_NAME, std::make_shared<DataTypeInt64>()));
    result.push_back(NameAndTypePair(TransposedMetricLog::METRIC_NAME, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())));
    result.push_back(NameAndTypePair(TransposedMetricLog::HOSTNAME_NAME, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())));
    result.push_back(NameAndTypePair(TransposedMetricLog::EVENT_DATE_NAME, std::make_shared<DataTypeDate>()));
    result.push_back(NameAndTypePair(HOUR_ALIAS_NAME, std::make_shared<DataTypeDateTime>()));

    return ColumnsDescription{result};
}

}

/// Special view for transposed representation of system.metric_log.
/// Can be used as compatibility layer, when you want to store transposed table, but your queries want wide table.
///
/// This view is not attached by default, it's attached by TransposedMetricLog, because
/// it depend on it.
class StorageSystemMetricLogView final : public IStorage
{
public:

    StorageSystemMetricLogView(const StorageID & table_id_, const StorageID & source_storage_id)
        : IStorage(table_id_)
        , internal_view(table_id_, getCreateQuery(source_storage_id), getColumnsDescriptionForView(), "")
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(getColumnsDescription());
        setInMemoryMetadata(storage_metadata);
    }

    std::string getName() const override { return "SystemMetricLogView"; }

    bool isSystemStorage() const override { return true; }

    void checkAlterIsPossible(const AlterCommands &, ContextPtr) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alters of system tables are not supported");
    }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr &,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        std::shared_ptr<StorageSnapshot> snapshot_for_view = std::make_shared<StorageSnapshot>(internal_view, internal_view.getInMemoryMetadataPtr());
        Block input_header = snapshot_for_view->metadata->getSampleBlock();

        internal_view.read(query_plan, input_header.getNames(), snapshot_for_view, query_info, context, processed_stage, max_block_size, num_streams);

        Block full_output_header = getInMemoryMetadataPtr()->getSampleBlock();

        /// Doesn't make sense to filter by metric, we will not filter out anything
        bool read_all_columns = full_output_header.columns() == column_names.size();
        std::optional<String> additional_name;
        if (!read_all_columns)
            additional_name = addFilterByMetricNameStep(query_plan, column_names, context);

        Block output_header;
        for (const auto & name : column_names)
            output_header.insert(full_output_header.getByName(name));

        if (additional_name.has_value())
            output_header.insert(full_output_header.getByName(*additional_name));

        query_plan.addStep(std::make_unique<CustomMetricLogViewStep>(input_header, output_header));
    }

    std::optional<String> addFilterByMetricNameStep(QueryPlan & query_plan, const Names & column_names, ContextPtr context)
    {
        std::optional<String> additional_name;
        MutableColumnPtr column_for_set = ColumnString::create();
        for (const auto & column_name : column_names)
        {
            if (column_name.starts_with(TransposedMetricLog::PROFILE_EVENT_PREFIX) || column_name.starts_with(TransposedMetricLog::CURRENT_METRIC_PREFIX))
                column_for_set->insertData(column_name.data(), column_name.size());
        }

        if (column_for_set->empty())
        {
            additional_name.emplace(std::string{TransposedMetricLog::PROFILE_EVENT_PREFIX} + ProfileEvents::getName(ProfileEvents::Event(0)));
            column_for_set->insertData(additional_name->data(), additional_name->size());
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
        const auto & metric_input = dag.findInOutputs(TransposedMetricLog::METRIC_NAME);
        const auto & filter_dag_column = dag.addColumn(set_for_dag);
        const auto & output = dag.addFunction(in_function, {&metric_input, &filter_dag_column}, "_special_filter_for_metric_log");
        dag.getOutputs().push_back(&output);

        query_plan.addStep(std::make_unique<FilterStep>(query_plan.getCurrentHeader(), std::move(dag), "_special_filter_for_metric_log", true));
        return additional_name;
    }

private:
    StorageView internal_view;
};

void TransposedMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);
    columns[column_idx++]->insert(metric_name);
    columns[column_idx++]->insert(value);
}


ColumnsDescription TransposedMetricLogElement::getColumnsDescription()
{
    ParserCodec codec_parser;
    return ColumnsDescription
    {
        {
            "hostname",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Hostname of the server executing the query."
        },
        {
            "event_date",
            std::make_shared<DataTypeDate>(),
            parseQuery(codec_parser, "(Delta(2), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event date."
        },
        {
            "event_time",
            std::make_shared<DataTypeDateTime>(),
            parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event time."
        },
        {
            "event_time_microseconds",
            std::make_shared<DataTypeDateTime64>(6),
            parseQuery(codec_parser, "(DoubleDelta, ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event time with microseconds resolution."
        },
        {
            "metric",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Metric name."
        },
        {
            "value",
            std::make_shared<DataTypeInt64>(),
            parseQuery(codec_parser, "(ZSTD(3))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Metric value."
        }
    };
}

void TransposedMetricLog::stepFunction(TimePoint current_time)
{
    /// Static lazy initialization to avoid polluting the header with implementation details
    /// For differentiation of ProfileEvents counters.
    static std::vector<ProfileEvents::Count> prev_profile_events(ProfileEvents::end());

    TransposedMetricLogElement elem;
    elem.event_date = DateLUT::instance().toDayNum(elem.event_time);
    elem.event_time = std::chrono::system_clock::to_time_t(current_time);
    elem.event_time_microseconds = timeInMicroseconds(current_time);

    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        const ProfileEvents::Count new_value = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
        auto & old_value = prev_profile_events[i];

        /// Profile event counters are supposed to be monotonic. However, at least the `NetworkReceiveBytes` can be inaccurate.
        /// So, since in the future the counter should always have a bigger value than in the past, we skip this event.
        /// It can be reproduced with the following integration tests:
        /// - test_hedged_requests/test.py::test_receive_timeout2
        /// - test_secure_socket::test
        if (new_value < old_value)
            continue;

        elem.metric_name = PROFILE_EVENT_PREFIX;
        elem.metric_name += ProfileEvents::getName(ProfileEvents::Event(i));
        elem.value = new_value - old_value;
        old_value = new_value;
        this->add(elem);
    }

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        elem.metric_name = CURRENT_METRIC_PREFIX;
        elem.metric_name += CurrentMetrics::getName(CurrentMetrics::Metric(i));
        elem.value = CurrentMetrics::values[i];
        this->add(elem);
    }
}


void TransposedMetricLog::prepareTable()
{
    SystemLog<TransposedMetricLogElement>::prepareTable();

    /// Now we need to create a view and potentially rotate old
    /// system.metric_log if it existed
    if (!view_name.empty())
    {
        auto storage_id = getTableID();
        auto database = DatabaseCatalog::instance().tryGetDatabase(storage_id.getDatabaseName());
        if (database)
        {
            auto table = database->tryGetTable(view_name, getContext());
            if (table && table->getName() != "SystemMetricLogView")
            {
                /// Rename the existing table.
                int suffix = 0;
                while (DatabaseCatalog::instance().isTableExist(
                    {database->getDatabaseName(), view_name + "_" + toString(suffix)}, getContext()))
                    ++suffix;

                ASTRenameQuery::Element elem
                {
                    ASTRenameQuery::Table
                    {
                        std::make_shared<ASTIdentifier>(database->getDatabaseName()),
                        std::make_shared<ASTIdentifier>(view_name)
                    },
                    ASTRenameQuery::Table
                    {
                        std::make_shared<ASTIdentifier>(database->getDatabaseName()),
                        std::make_shared<ASTIdentifier>(view_name + "_" + toString(suffix))
                    }
                };

                auto rename = std::make_shared<ASTRenameQuery>(ASTRenameQuery::Elements{std::move(elem)});

                ActionLock merges_lock;
                if (DatabaseCatalog::instance().getDatabase(database->getDatabaseName())->getUUID() == UUIDHelpers::Nil)
                    merges_lock = table->getActionLock(ActionLocks::PartsMerge);

                auto query_context = Context::createCopy(context);
                query_context->makeQueryContext();
                /// As this operation is performed automatically we don't want it to fail because of user dependencies on log tables
                query_context->setSetting("check_table_dependencies", Field{false});
                query_context->setSetting("check_referential_table_dependencies", Field{false});

                InterpreterRenameQuery(rename, query_context).execute();

                attachNoDescription<StorageSystemMetricLogView>(getContext(), *database, "metric_log", "Metric log view", storage_id);
            }
            else if (!table)
            {
                attachNoDescription<StorageSystemMetricLogView>(getContext(), *database, "metric_log", "Metric log view", storage_id);
            }
        }
    }
}

}
