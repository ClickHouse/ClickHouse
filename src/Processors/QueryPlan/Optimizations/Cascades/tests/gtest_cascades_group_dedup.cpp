#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Core/SortDescription.h>

using namespace DB;

namespace
{

SortDescription sortByColumn(const String & name, int direction)
{
    SortColumnDescription column;
    column.column_name = name;
    column.direction = direction;
    SortDescription description;
    description.push_back(column);
    return description;
}

/// A physical expression with no plan step (getName/getDescription are empty), so structural
/// identity is decided by the output properties and the inputs — exactly the fields that
/// distinguish e.g. an ASC sort from a DESC sort, or two different distributions.
GroupExpressionPtr exprWithSorting(const String & column, int direction)
{
    auto expression = std::make_shared<GroupExpression>(QueryPlanStepPtr{});
    expression->properties.sorting = sortByColumn(column, direction);
    return expression;
}

GroupExpressionPtr exprWithInput(GroupId input_group_id)
{
    auto expression = std::make_shared<GroupExpression>(QueryPlanStepPtr{});
    expression->inputs.push_back({.group_id = input_group_id, .required_properties = {}});
    return expression;
}

}

TEST(CascadesGroupDedup, StructuralEqualityDistinguishesProperties)
{
    EXPECT_TRUE(exprWithSorting("k", 1)->structurallyEqualTo(*exprWithSorting("k", 1)));
    /// ASC and DESC are distinct physical alternatives, not duplicates.
    EXPECT_FALSE(exprWithSorting("k", 1)->structurallyEqualTo(*exprWithSorting("k", -1)));
}

TEST(CascadesGroupDedup, StructuralEqualityDistinguishesInputs)
{
    EXPECT_TRUE(exprWithInput(1)->structurallyEqualTo(*exprWithInput(1)));
    EXPECT_FALSE(exprWithInput(1)->structurallyEqualTo(*exprWithInput(2)));

    auto a = exprWithInput(1);
    auto b = exprWithInput(1);
    b->inputs[0].required_properties.sorting = sortByColumn("k", 1);
    /// Same input group but different required properties on that input -> distinct.
    EXPECT_FALSE(a->structurallyEqualTo(*b));
}

/// addPhysicalExpression drops a structurally-equal duplicate but keeps genuinely distinct
/// alternatives. A duplicate is dropped only via structural equality, never by a bare hash
/// match, so a fingerprint collision could never silently discard a distinct alternative.
TEST(CascadesGroupDedup, AddPhysicalExpressionKeepsDistinctAlternatives)
{
    Group group(0);

    group.addPhysicalExpression(exprWithSorting("k", 1));
    group.addPhysicalExpression(exprWithSorting("k", 1)); /// exact duplicate
    EXPECT_EQ(group.physical_expressions.size(), 1u);

    group.addPhysicalExpression(exprWithSorting("k", -1)); /// distinct: opposite direction
    EXPECT_EQ(group.physical_expressions.size(), 2u);
}
