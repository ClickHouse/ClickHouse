#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>

using namespace DB;

/// Within-group deduplication runs on the total full identity (`GroupExpression::fullyEqualTo`):
/// the expression-level frame plus the step's full digest. These tests pin the two halves of it -
/// the frame, and the fresh-instance equality that the enforcer fixed-point loop depends on.

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

SortDescription sortByColumn(const String & name, int direction)
{
    SortColumnDescription column;
    column.column_name = name;
    column.direction = direction;
    SortDescription description;
    description.push_back(column);
    return description;
}

/// The frame tests below share one step pointer across all their expressions, so `fullyEqualTo`
/// answers on the frame alone - exactly the fields that distinguish e.g. an ASC sort from a DESC
/// sort, or two different input groups.
std::shared_ptr<const IQueryPlanStep> makeSharedStep()
{
    return std::make_shared<const OffsetStep>(makeHeader(), 10);
}

GroupExpressionPtr exprWithSorting(const std::shared_ptr<const IQueryPlanStep> & step, const String & column, int direction)
{
    auto expression = std::make_shared<GroupExpression>(QueryPlanStepPtr{});
    expression->plan_step = step;
    expression->properties.sorting = sortByColumn(column, direction);
    return expression;
}

GroupExpressionPtr exprWithInput(const std::shared_ptr<const IQueryPlanStep> & step, GroupId input_group_id)
{
    auto expression = std::make_shared<GroupExpression>(QueryPlanStepPtr{});
    expression->plan_step = step;
    expression->inputs.push_back({.group_id = input_group_id, .required_properties = {}});
    return expression;
}

SortingStep::Settings makeSortSettings()
{
    QueryPlanSerializationSettings serialization_settings;
    return SortingStep::Settings(serialization_settings);
}

/// A self-referential enforcer expression over a freshly built step, as `makeEnforcerExpression`
/// builds one: same group, one input pointing back at that group with the enforced axis relaxed.
GroupExpressionPtr makeEnforcer(QueryPlanStepPtr step, const SortDescription & sorting, EnforcedProperty enforced)
{
    auto expression = std::make_shared<GroupExpression>(std::move(step));
    expression->group_id = 0;
    expression->inputs.push_back({.group_id = 0, .required_properties = {}});
    expression->properties.sorting = sorting;
    expression->enforced_property = enforced;
    return expression;
}

}

TEST(CascadesGroupDedup, FullEqualityDistinguishesProperties)
{
    auto step = makeSharedStep();

    EXPECT_TRUE(exprWithSorting(step, "k", 1)->fullyEqualTo(*exprWithSorting(step, "k", 1)));
    /// ASC and DESC are distinct physical alternatives, not duplicates.
    EXPECT_FALSE(exprWithSorting(step, "k", 1)->fullyEqualTo(*exprWithSorting(step, "k", -1)));
}

TEST(CascadesGroupDedup, FullEqualityDistinguishesInputs)
{
    auto step = makeSharedStep();

    EXPECT_TRUE(exprWithInput(step, 1)->fullyEqualTo(*exprWithInput(step, 1)));
    EXPECT_FALSE(exprWithInput(step, 1)->fullyEqualTo(*exprWithInput(step, 2)));

    auto a = exprWithInput(step, 1);
    auto b = exprWithInput(step, 1);
    b->inputs[0].required_properties.sorting = sortByColumn("k", 1);
    /// Same input group but different required properties on that input -> distinct.
    EXPECT_FALSE(a->fullyEqualTo(*b));
}

/// addPhysicalExpression drops a fully-equal duplicate but keeps genuinely distinct alternatives.
/// A duplicate is dropped only via `fullyEqualTo`, never by a bare hash match, so a fingerprint
/// collision could never silently discard a distinct alternative.
TEST(CascadesGroupDedup, AddPhysicalExpressionKeepsDistinctAlternatives)
{
    Group group(0);
    auto step = makeSharedStep();

    EXPECT_TRUE(group.addPhysicalExpression(exprWithSorting(step, "k", 1)));
    EXPECT_FALSE(group.addPhysicalExpression(exprWithSorting(step, "k", 1))); /// exact duplicate
    EXPECT_EQ(group.physical_expressions.size(), 1u);

    EXPECT_TRUE(group.addPhysicalExpression(exprWithSorting(step, "k", -1))); /// distinct: opposite direction
    EXPECT_EQ(group.physical_expressions.size(), 2u);
}

/// `addLogicalExpression` drops fully-equal duplicates the same way, so a transformation that
/// derives an already-known alternative (e.g. a join swapped twice) does not grow the group.
TEST(CascadesGroupDedup, AddLogicalExpressionDropsDuplicates)
{
    Group group(0);
    auto step = makeSharedStep();

    EXPECT_TRUE(group.addLogicalExpression(exprWithInput(step, 1)));
    EXPECT_FALSE(group.addLogicalExpression(exprWithInput(step, 1))); /// exact duplicate
    EXPECT_EQ(group.logical_expressions.size(), 1u);

    EXPECT_TRUE(group.addLogicalExpression(exprWithInput(step, 2))); /// distinct input group
    EXPECT_EQ(group.logical_expressions.size(), 2u);
}

/// The guarantee the enforcer fixed-point loop in `Task.cpp` rests on: an enforcer re-derives its
/// output from scratch, so two runs produce two DISTINCT step objects with identical content. They
/// must compare fully equal, or the loop would keep counting duplicates as progress. Both step types
/// an enforcer constructs are covered.
TEST(CascadesGroupDedup, FreshEnforcerOutputsAreDuplicates)
{
    auto header = makeHeader();
    auto sorting = sortByColumn("k", 1);

    auto first_sort = makeEnforcer(
        std::make_unique<SortingStep>(header, sorting, /*limit_=*/0, makeSortSettings()), sorting, EnforcedProperty::Sorting);
    auto second_sort = makeEnforcer(
        std::make_unique<SortingStep>(header, sorting, /*limit_=*/0, makeSortSettings()), sorting, EnforcedProperty::Sorting);

    ASSERT_NE(first_sort->plan_step, second_sort->plan_step);
    EXPECT_EQ(first_sort->fullFingerprint(), second_sort->fullFingerprint());
    EXPECT_TRUE(first_sort->fullyEqualTo(*second_sort));

    auto first_gather = makeEnforcer(
        std::make_unique<GatherExchangeStep>(header, /*source_bucket_count_=*/4), SortDescription{}, EnforcedProperty::Distribution);
    auto second_gather = makeEnforcer(
        std::make_unique<GatherExchangeStep>(header, /*source_bucket_count_=*/4), SortDescription{}, EnforcedProperty::Distribution);

    ASSERT_NE(first_gather->plan_step, second_gather->plan_step);
    EXPECT_EQ(first_gather->fullFingerprint(), second_gather->fullFingerprint());
    EXPECT_TRUE(first_gather->fullyEqualTo(*second_gather));

    /// And the group really drops the second one, which is what stops the loop.
    Group group(0);
    EXPECT_TRUE(group.addPhysicalExpression(first_sort));
    EXPECT_FALSE(group.addPhysicalExpression(second_sort));
    EXPECT_TRUE(group.addPhysicalExpression(first_gather));
    EXPECT_FALSE(group.addPhysicalExpression(second_gather));
    EXPECT_EQ(group.physical_expressions.size(), 2u);
}

/// A gather that has to preserve the input order is a different exchange from a plain one, and two
/// gathers over different bucket counts are different too - the content digest must see both.
TEST(CascadesGroupDedup, GatherExchangeFieldsAreNotDuplicates)
{
    auto header = makeHeader();
    auto sorting = sortByColumn("k", 1);

    auto plain = makeEnforcer(
        std::make_unique<GatherExchangeStep>(header, /*source_bucket_count_=*/4), SortDescription{}, EnforcedProperty::Distribution);
    auto sorted = makeEnforcer(
        std::make_unique<GatherExchangeStep>(header, /*source_bucket_count_=*/4, sorting),
        SortDescription{},
        EnforcedProperty::Distribution);
    auto other_bucket_count = makeEnforcer(
        std::make_unique<GatherExchangeStep>(header, /*source_bucket_count_=*/8), SortDescription{}, EnforcedProperty::Distribution);

    EXPECT_FALSE(plain->fullyEqualTo(*sorted));
    EXPECT_FALSE(plain->fullyEqualTo(*other_bucket_count));

    Group group(0);
    EXPECT_TRUE(group.addPhysicalExpression(plain));
    EXPECT_TRUE(group.addPhysicalExpression(sorted));
    EXPECT_TRUE(group.addPhysicalExpression(other_bucket_count));
    EXPECT_EQ(group.physical_expressions.size(), 3u);
}
