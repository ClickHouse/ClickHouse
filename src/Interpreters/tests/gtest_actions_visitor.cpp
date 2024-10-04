#include <iostream>
#include <memory>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsVisitor.h>
#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>

using namespace DB;


TEST(ActionsVisitor, VisitLiteral)
{
    DataTypePtr date_type = std::make_shared<DataTypeDate32>();
    DataTypePtr expect_type = std::make_shared<DataTypeInt16>();
    const NamesAndTypesList name_and_types =
    {
        {"year", date_type}
    };

    const auto ast = std::make_shared<ASTLiteral>(19870);
    auto context = Context::createCopy(getContext().context);
    NamesAndTypesList aggregation_keys;
    ColumnNumbersList aggregation_keys_indexes_list;
    AggregationKeysInfo info(aggregation_keys, aggregation_keys_indexes_list, GroupByKind::NONE);
    SizeLimits size_limits_for_set;
    ActionsMatcher::Data visitor_data(
        context,
        size_limits_for_set,
        size_t(0),
        name_and_types,
        ActionsDAG(name_and_types),
        std::make_shared<PreparedSets>(),
        false /* no_subqueries */,
        false /* no_makeset */,
        false /* only_consts */,
        info);
    ActionsVisitor(visitor_data).visit(ast);
    auto actions = visitor_data.getActions();
    ASSERT_EQ(actions.getResultColumns().back().type->getTypeId(), expect_type->getTypeId());
}

TEST(ActionsVisitor, VisitLiteralWithType)
{
    DataTypePtr date_type = std::make_shared<DataTypeDate32>();
    const NamesAndTypesList name_and_types =
    {
        {"year", date_type}
    };

    const auto ast = std::make_shared<ASTLiteral>(19870, date_type);
    auto context = Context::createCopy(getContext().context);
    NamesAndTypesList aggregation_keys;
    ColumnNumbersList aggregation_keys_indexes_list;
    AggregationKeysInfo info(aggregation_keys, aggregation_keys_indexes_list, GroupByKind::NONE);
    SizeLimits size_limits_for_set;
    ActionsMatcher::Data visitor_data(
        context,
        size_limits_for_set,
        size_t(0),
        name_and_types,
        ActionsDAG(name_and_types),
        std::make_shared<PreparedSets>(),
        false /* no_subqueries */,
        false /* no_makeset */,
        false /* only_consts */,
        info);
    ActionsVisitor(visitor_data).visit(ast);
    auto actions = visitor_data.getActions();
    ASSERT_EQ(actions.getResultColumns().back().type->getTypeId(), date_type->getTypeId());
}
