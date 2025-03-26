#include <Storages/System/StorageSystemMetricLogView.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

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

constexpr auto FIXED_COLUMNS = {"hostname", "event_date", "event_time"};

ASTPtr getExpressionForProfileEvent(const std::string & prefix, const std::string & name)
{
    auto equals = makeASTFunction("equals", std::make_shared<ASTIdentifier>("metric"), std::make_shared<ASTLiteral>(name));
    auto sumif = makeASTFunction("sumIf", std::make_shared<ASTIdentifier>("value"), equals);

    sumif->alias = prefix + "_" + name;
    return sumif;
}

std::shared_ptr<ASTSelectWithUnionQuery> getSelectQuery()
{
    std::shared_ptr<ASTSelectWithUnionQuery> result = std::make_shared<ASTSelectWithUnionQuery>();
    std::shared_ptr<ASTSelectQuery> select_query = std::make_shared<ASTSelectQuery>();
    std::shared_ptr<ASTExpressionList> expression_list = std::make_shared<ASTExpressionList>();

    for (const auto & name : FIXED_COLUMNS)
        expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(name));

    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        expression_list->children.push_back(getExpressionForProfileEvent("ProfileEvent", ProfileEvents::getName(ProfileEvents::Event(i))));

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        expression_list->children.push_back(getExpressionForProfileEvent("CurrentMetric", CurrentMetrics::getName(CurrentMetrics::Metric(i))));

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(expression_list));

    auto tables = std::make_shared<ASTTablesInSelectQuery>();
    auto table = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expression = std::make_shared<ASTTableExpression>();
    auto database_and_table_name = std::make_shared<ASTTableIdentifier>("system", "transposed_metric_log");
    table_expression->database_and_table_name = database_and_table_name;
    table->table_expression = table_expression;
    tables->children.emplace_back(table);
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

    std::shared_ptr<ASTExpressionList> group_by = std::make_shared<ASTExpressionList>();
    for (const auto & name : FIXED_COLUMNS)
        group_by->children.emplace_back(std::make_shared<ASTIdentifier>(name));

    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);

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

}

StorageSystemMetricLogView::StorageSystemMetricLogView(const StorageID & table_id)
    : StorageView(table_id, getCreateQuery(), getColumnsDescription(), "")
{
}

void StorageSystemMetricLogView::checkAlterIsPossible(const AlterCommands &, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alters of system tables are not supported");
}

}
