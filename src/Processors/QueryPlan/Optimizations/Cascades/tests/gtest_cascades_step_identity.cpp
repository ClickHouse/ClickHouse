#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StepIdentity.h>
#include <Processors/QueryPlan/StepIdentity.h>

using namespace DB;

/// The Cascades cross-group identity compares the content of a step (its wire encoding plus the
/// audited non-wire fields), not its name and description like the within-group
/// `structurallyEqualTo`. It fails closed: a step type that has not opted in compares by pointer.

namespace
{

SharedHeader makeHeader()
{
    ColumnWithTypeAndName column;
    column.name = "x";
    column.type = std::make_shared<DataTypeUInt64>();
    column.column = column.type->createColumn();
    return std::make_shared<const Block>(Block{{column}});
}

/// Input `x` plus a constant output named `c`, so two DAGs differing only in `constant` produce
/// the same output header and differ only in the serialized DAG payload.
ActionsDAG makeDag(UInt64 constant)
{
    auto type = std::make_shared<DataTypeUInt64>();
    ActionsDAG dag(NamesAndTypesList{{"x", type}});
    dag.getOutputs().push_back(&dag.addColumn(type->createColumnConst(1, Field(constant)), type, "c"));
    return dag;
}

GroupExpressionPtr exprWithExpressionStep(const SharedHeader & header, UInt64 constant)
{
    return std::make_shared<GroupExpression>(std::make_unique<ExpressionStep>(header, makeDag(constant)));
}

}

TEST(CascadesStepIdentity, ClonesOfSameStepAreEqual)
{
    auto header = makeHeader();
    ExpressionStep step(header, makeDag(1));

    auto a = std::make_shared<GroupExpression>(step.clone());
    auto b = std::make_shared<GroupExpression>(step.clone());

    EXPECT_NE(a->plan_step, b->plan_step);
    EXPECT_EQ(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_TRUE(a->globallyEqualTo(*b));
}

TEST(CascadesStepIdentity, DifferentDagContentIsUnequal)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 2);

    /// Same input and output headers - only the constant inside the DAG differs.
    EXPECT_EQ(
        a->plan_step->getOutputHeader()->getNamesAndTypesList().toString(),
        b->plan_step->getOutputHeader()->getNamesAndTypesList().toString());
    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

TEST(CascadesStepIdentity, StepDescriptionIsExcluded)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    auto b_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    b_step->setStepDescription("projection over a very different reason");

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_EQ(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_TRUE(a->globallyEqualTo(*b));
}

/// `prevent_input_removal` is not on the wire, but it blocks later input pruning, so the audit puts
/// it in the extras: the two steps below are not interchangeable.
TEST(CascadesStepIdentity, PreventInputRemovalIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    auto b_step = std::make_unique<ExpressionStep>(header, makeDag(1));
    b_step->setPreventInputRemoval();

    auto a = std::make_shared<GroupExpression>(std::move(a_step));
    auto b = std::make_shared<GroupExpression>(std::move(b_step));

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

/// `Group` uses `enforced_property` to keep a self-referential enforcer from satisfying its own
/// input, so an enforcer and a plain expression over the same step must never be judged equal.
TEST(CascadesStepIdentity, EnforcedPropertyIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 1);
    ASSERT_TRUE(a->globallyEqualTo(*b));

    b->enforced_property = EnforcedProperty::Sorting;

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

/// `description_suffix` is GroupExpression state set by rules (e.g. "(by col)"), not the step's
/// display description, and nothing guarantees it is free of meaning - so it is compared.
TEST(CascadesStepIdentity, DescriptionSuffixIsPartOfIdentity)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 1);
    ASSERT_TRUE(a->globallyEqualTo(*b));

    b->description_suffix = "(by k)";

    EXPECT_NE(a->globalFingerprint(), b->globalFingerprint());
    EXPECT_FALSE(a->globallyEqualTo(*b));
}

/// The hash must be usable before any equality call, so that a memo can bucket expressions eagerly.
TEST(CascadesStepIdentity, IndependentlyBuiltStepsShareFingerprint)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 7);
    auto b = exprWithExpressionStep(header, 7);

    EXPECT_EQ(a->globalFingerprint(), b->globalFingerprint());
}

TEST(CascadesStepIdentity, StepWithoutOptInComparesByPointer)
{
    auto header = makeHeader();
    auto a = std::make_shared<GroupExpression>(std::make_unique<LimitStep>(header, 10, 0));
    auto b = std::make_shared<GroupExpression>(std::make_unique<LimitStep>(header, 10, 0));

    EXPECT_EQ(a->getStepIdentity(), nullptr);
    /// Equal-looking but distinct instances are not interchangeable without a field audit.
    EXPECT_FALSE(a->globallyEqualTo(*b));

    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization) - the shallow copy is the test subject
    GroupExpression shared_step_copy(*a);
    EXPECT_EQ(shared_step_copy.plan_step, a->plan_step);
    EXPECT_TRUE(a->globallyEqualTo(shared_step_copy));
    EXPECT_EQ(a->globalFingerprint(), shared_step_copy.globalFingerprint());
}

/// A rule may shallow-copy an expression and then replace its step. The inherited identity cache
/// must not be trusted for the new step.
TEST(CascadesStepIdentity, ReplacedStepInvalidatesInheritedIdentity)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    const auto fingerprint_a = a->globalFingerprint();

    auto b = std::make_shared<GroupExpression>(*a);
    ASSERT_EQ(b->globalFingerprint(), fingerprint_a);

    b->plan_step = std::make_shared<const ExpressionStep>(header, makeDag(2));

    EXPECT_NE(b->globalFingerprint(), fingerprint_a);
    EXPECT_FALSE(a->globallyEqualTo(*b));
    EXPECT_FALSE(b->globallyEqualTo(*a));
}

/// Moving a value between tags, or re-splitting two adjacent variable-length components, must
/// change the bytes - otherwise different field assignments could collide.
TEST(CascadesStepIdentity, ExtrasFramingIsInjective)
{
    SerializedSetsRegistry registry;
    auto dag = makeDag(1);

    WriteBufferFromOwnString dag_first;
    {
        CascadesIdentityExtras extras(dag_first, registry);
        extras.addDAG(1, &dag);
        extras.addAbsent(2);
    }

    WriteBufferFromOwnString dag_second;
    {
        CascadesIdentityExtras extras(dag_second, registry);
        extras.addAbsent(1);
        extras.addDAG(2, &dag);
    }

    EXPECT_NE(dag_first.str(), dag_second.str());

    WriteBufferFromOwnString split_left;
    {
        CascadesIdentityExtras extras(split_left, registry);
        extras.addStrings(1, Names{"a", "b"});
        extras.addStrings(2, Names{});
    }

    WriteBufferFromOwnString split_right;
    {
        CascadesIdentityExtras extras(split_right, registry);
        extras.addStrings(1, Names{"a"});
        extras.addStrings(2, Names{"b"});
    }

    EXPECT_NE(split_left.str(), split_right.str());
}

TEST(CascadesStepIdentity, MetricsCountEncodingPasses)
{
    auto header = makeHeader();
    auto a = exprWithExpressionStep(header, 1);
    auto b = exprWithExpressionStep(header, 1);

    CascadesIdentityMetrics::reset();

    a->globalFingerprint();
    EXPECT_EQ(CascadesIdentityMetrics::encoded_steps.load(), 1u);
    EXPECT_GT(CascadesIdentityMetrics::encoded_bytes.load(), 0u);
    EXPECT_EQ(CascadesIdentityMetrics::exact_reencodes.load(), 0u);

    EXPECT_TRUE(a->globallyEqualTo(*b));
    /// One pass hashes `b`, then both steps are re-encoded to be compared byte for byte.
    EXPECT_EQ(CascadesIdentityMetrics::exact_reencodes.load(), 2u);
    EXPECT_EQ(CascadesIdentityMetrics::encoded_steps.load(), 4u);
}
