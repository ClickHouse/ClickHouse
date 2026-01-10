#include <Interpreters/TransposedMetricLog.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeSet.h>
#include <Storages/SelectQueryInfo.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnTuple.h>
#include <base/getFQDNOrHostName.h>
#include <Common/CurrentMetrics.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Common/ProfileEvents.h>
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
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>

#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

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
        result.push_back(NameAndTypePair(std::string{TransposedMetricLog::PROFILE_EVENT_PREFIX} + std::string(ProfileEvents::getName(ProfileEvents::Event(i))), std::make_shared<DataTypeUInt64>()));

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        result.push_back(NameAndTypePair(std::string{TransposedMetricLog::CURRENT_METRIC_PREFIX} + std::string(CurrentMetrics::getName(CurrentMetrics::Metric(i))), std::make_shared<DataTypeInt64>()));

    return ColumnsDescription{result};
}

}


void TransposedMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(event_date);
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
    std::lock_guard lock(previous_profile_events_mutex);

    TransposedMetricLogElement elem;
    elem.event_time = std::chrono::system_clock::to_time_t(current_time);
    elem.event_date = DateLUT::instance().toDayNum(elem.event_time);
    elem.event_time_microseconds = timeInMicroseconds(current_time);

    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        const ProfileEvents::Count new_value = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
        auto & old_value = previous_profile_events[i];

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


ASTPtr TransposedMetricLog::getDefaultOrderByAST()
{
    /// Always use default ORDER BY because it's the most effective for view
    std::string order_by_str = std::string{"("} + getDefaultOrderBy() + ")";
    ParserStorageOrderByClause order_by_p(/*allow_order_*/ false);
    return parseQuery(order_by_p, order_by_str, "Order by for transposed metric log", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
}

}
