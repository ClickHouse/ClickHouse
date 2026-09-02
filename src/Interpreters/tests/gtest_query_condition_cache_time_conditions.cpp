#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Common/FieldVisitorToString.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Cache/QueryConditionCacheTimeConditions.h>

using namespace DB;

namespace
{

/// 2026-08-09 00:00:00 UTC, a whole day.
constexpr Int64 day_start = 1786233600;
constexpr Int64 day = 86400;

const ActionsDAG::Node & addNonDeterministicConstant(ActionsDAG & dag, const DataTypePtr & type, const Field & value)
{
    /// Mimics a folded current-time function: the name embeds the value, like the analyzer does.
    auto column = type->createColumnConst(1, value);
    String name = fmt::format("_CAST({}_{})", applyVisitor(FieldVisitorToString(), value), type->getName());
    return dag.addColumn(std::move(column), type, std::move(name), /*is_deterministic_constant=*/false);
}

const ActionsDAG::Node & addDeterministicConstant(ActionsDAG & dag, const DataTypePtr & type, const Field & value)
{
    auto column = type->createColumnConst(1, value);
    String name = fmt::format("{}_{}", applyVisitor(FieldVisitorToString(), value), type->getName());
    return dag.addColumn(std::move(column), type, std::move(name));
}

const ActionsDAG::Node & addFunction(ActionsDAG & dag, const String & name, ActionsDAG::NodeRawConstPtrs children)
{
    tryRegisterFunctions();
    auto resolver = FunctionFactory::instance().get(name, getContext().context);
    return dag.addFunction(resolver, std::move(children), "");
}

/// Builds `time <cmp> <constant>` (or the flipped variant) over a fresh DAG and derives from it.
struct ComparisonCase
{
    ActionsDAG dag;
    const ActionsDAG::Node * condition;

    ComparisonCase(const String & cmp, const DataTypePtr & type, const Field & constant, bool constant_on_the_left = false)
    {
        const auto & input = dag.addInput("time", type);
        const auto & rounded_constant = addNonDeterministicConstant(dag, type, constant);
        if (constant_on_the_left)
            condition = &addFunction(dag, cmp, {&rounded_constant, &input});
        else
            condition = &addFunction(dag, cmp, {&input, &rounded_constant});
    }
};

DataTypePtr dateTimeUTC()
{
    return std::make_shared<DataTypeDateTime>("UTC");
}

}

TEST(QueryConditionCacheTimeConditions, DeterministicConditionIsNotDerived)
{
    ActionsDAG dag;
    const auto & input = dag.addInput("time", dateTimeUTC());
    const auto & constant = addDeterministicConstant(dag, dateTimeUTC(), Field(static_cast<UInt64>(day_start)));
    const auto & condition = addFunction(dag, "greaterOrEquals", {&input, &constant});

    EXPECT_FALSE(deriveDeterministicTimeCondition(&condition, TimeConditionRounding::Weaken, 1.0, day_start + 10 * day, /*allow_top_k_filter=*/false));
    EXPECT_FALSE(deriveDeterministicTimeCondition(&condition, TimeConditionRounding::Strengthen, 1.0, day_start + 10 * day, /*allow_top_k_filter=*/false));
}

TEST(QueryConditionCacheTimeConditions, LowerBoundRounding)
{
    /// `time >= <noon>`, 10 days in the past: the grid is one day.
    Int64 noon = day_start + day / 2;
    ComparisonCase test("greaterOrEquals", dateTimeUTC(), Field(static_cast<UInt64>(noon)));

    auto weakened = deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Weaken, 1.0, noon + 10 * day, /*allow_top_k_filter=*/false);
    auto strengthened = deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Strengthen, 1.0, noon + 10 * day, /*allow_top_k_filter=*/false);
    ASSERT_TRUE(weakened);
    ASSERT_TRUE(strengthened);

    /// Weakening a lower bound rounds it down, strengthening rounds it up.
    EXPECT_NE(weakened->condition.find(std::to_string(day_start)), String::npos) << weakened->condition;
    EXPECT_NE(strengthened->condition.find(std::to_string(day_start + day)), String::npos) << strengthened->condition;
    EXPECT_NE(weakened->hash, strengthened->hash);
}

TEST(QueryConditionCacheTimeConditions, WriteKeyMeetsNextCellReadKey)
{
    /// A write with a constant in one grid cell must produce the same key as a read whose constant
    /// is in the next grid cell: that is how entries written during one cell serve the next one.
    Int64 constant_of_writer = day_start + day / 2;
    Int64 constant_of_reader = day_start + day + 3600;

    ComparisonCase writer("greaterOrEquals", dateTimeUTC(), Field(static_cast<UInt64>(constant_of_writer)));
    ComparisonCase reader("greaterOrEquals", dateTimeUTC(), Field(static_cast<UInt64>(constant_of_reader)));

    auto write_key = deriveDeterministicTimeCondition(writer.condition, TimeConditionRounding::Strengthen, 1.0, constant_of_writer + 10 * day, /*allow_top_k_filter=*/false);
    auto read_key = deriveDeterministicTimeCondition(reader.condition, TimeConditionRounding::Weaken, 1.0, constant_of_reader + 10 * day, /*allow_top_k_filter=*/false);
    ASSERT_TRUE(write_key);
    ASSERT_TRUE(read_key);
    EXPECT_EQ(write_key->hash, read_key->hash);
}

TEST(QueryConditionCacheTimeConditions, AlignedConstantIsUnchangedInBothDirections)
{
    /// A constant aligned to the grid (e.g. from `toStartOfDay(now())`) survives the rounding
    /// unchanged, so reads and writes share the key within the same grid cell.
    ComparisonCase test("greaterOrEquals", dateTimeUTC(), Field(static_cast<UInt64>(day_start)));

    auto weakened = deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Weaken, 1.0, day_start + 10 * day, /*allow_top_k_filter=*/false);
    auto strengthened = deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Strengthen, 1.0, day_start + 10 * day, /*allow_top_k_filter=*/false);
    ASSERT_TRUE(weakened);
    ASSERT_TRUE(strengthened);
    EXPECT_EQ(weakened->hash, strengthened->hash);
    EXPECT_EQ(weakened->condition, strengthened->condition);
}

TEST(QueryConditionCacheTimeConditions, DateConstantIsIdentityRounded)
{
    /// Date values are whole days, and every grid step divides a day, so both directions agree,
    /// and the derivation does not even depend on the current time (only the value does).
    auto date = std::make_shared<DataTypeDate>();
    ActionsDAG dag;
    const auto & input = dag.addInput("d", date);
    const auto & constant = addNonDeterministicConstant(dag, date, Field(static_cast<UInt64>(20664))); /// today() - 10
    const auto & condition = addFunction(dag, "greaterOrEquals", {&input, &constant});

    auto weakened = deriveDeterministicTimeCondition(&condition, TimeConditionRounding::Weaken, 1.0, day_start + day / 2, /*allow_top_k_filter=*/false);
    auto strengthened = deriveDeterministicTimeCondition(&condition, TimeConditionRounding::Strengthen, 1.0, day_start + day / 2, /*allow_top_k_filter=*/false);
    auto weakened_later = deriveDeterministicTimeCondition(&condition, TimeConditionRounding::Weaken, 1.0, day_start + day - 1, /*allow_top_k_filter=*/false);
    ASSERT_TRUE(weakened);
    ASSERT_TRUE(strengthened);
    ASSERT_TRUE(weakened_later);
    EXPECT_EQ(weakened->hash, strengthened->hash);
    EXPECT_EQ(weakened->hash, weakened_later->hash);
    EXPECT_NE(weakened->condition.find("20664"), String::npos) << weakened->condition;
}

TEST(QueryConditionCacheTimeConditions, UpperBoundRoundsTheOtherWay)
{
    Int64 noon = day_start + day / 2;
    ComparisonCase test("lessOrEquals", dateTimeUTC(), Field(static_cast<UInt64>(noon)));

    auto weakened = deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Weaken, 1.0, noon + 10 * day, /*allow_top_k_filter=*/false);
    auto strengthened = deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Strengthen, 1.0, noon + 10 * day, /*allow_top_k_filter=*/false);
    ASSERT_TRUE(weakened);
    ASSERT_TRUE(strengthened);

    /// Weakening an upper bound rounds it up, strengthening rounds it down.
    EXPECT_NE(weakened->condition.find(std::to_string(day_start + day)), String::npos) << weakened->condition;
    EXPECT_NE(strengthened->condition.find(std::to_string(day_start)), String::npos) << strengthened->condition;
}

TEST(QueryConditionCacheTimeConditions, ConstantOnTheLeftMirrorsTheBound)
{
    /// `<noon> <= time` is a lower bound on `time`, like `time >= <noon>`.
    Int64 noon = day_start + day / 2;
    ComparisonCase test("lessOrEquals", dateTimeUTC(), Field(static_cast<UInt64>(noon)), /*constant_on_the_left=*/true);

    auto weakened = deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Weaken, 1.0, noon + 10 * day, /*allow_top_k_filter=*/false);
    ASSERT_TRUE(weakened);
    EXPECT_NE(weakened->condition.find(std::to_string(day_start)), String::npos) << weakened->condition;
}

TEST(QueryConditionCacheTimeConditions, NotFlipsTheDirection)
{
    Int64 noon = day_start + day / 2;
    ActionsDAG dag;
    const auto & input = dag.addInput("time", dateTimeUTC());
    const auto & constant = addNonDeterministicConstant(dag, dateTimeUTC(), Field(static_cast<UInt64>(noon)));
    const auto & comparison = addFunction(dag, "greaterOrEquals", {&input, &constant});
    const auto & condition = addFunction(dag, "not", {&comparison});

    /// Weakening `NOT (time >= K)` strengthens the inner lower bound, i.e. rounds it up.
    auto weakened = deriveDeterministicTimeCondition(&condition, TimeConditionRounding::Weaken, 1.0, noon + 10 * day, /*allow_top_k_filter=*/false);
    ASSERT_TRUE(weakened);
    EXPECT_NE(weakened->condition.find(std::to_string(day_start + day)), String::npos) << weakened->condition;
}

TEST(QueryConditionCacheTimeConditions, ConjunctionWithDeterministicPart)
{
    Int64 noon = day_start + day / 2;
    ActionsDAG dag;
    const auto & time_input = dag.addInput("time", dateTimeUTC());
    const auto & flag_input = dag.addInput("flag", std::make_shared<DataTypeUInt8>());
    const auto & constant = addNonDeterministicConstant(dag, dateTimeUTC(), Field(static_cast<UInt64>(noon)));
    const auto & comparison = addFunction(dag, "greaterOrEquals", {&time_input, &constant});
    const auto & condition = addFunction(dag, "and", {&comparison, &flag_input});

    auto weakened = deriveDeterministicTimeCondition(&condition, TimeConditionRounding::Weaken, 1.0, noon + 10 * day, /*allow_top_k_filter=*/false);
    ASSERT_TRUE(weakened);
    EXPECT_NE(weakened->condition.find("flag"), String::npos) << weakened->condition;
    EXPECT_NE(weakened->condition.find(std::to_string(day_start)), String::npos) << weakened->condition;
}

TEST(QueryConditionCacheTimeConditions, GridStepFollowsTheInterval)
{
    /// One hour in the past: the grid is one hour, not one day.
    Int64 constant = day_start + day / 2 + 1234;
    ComparisonCase test("greaterOrEquals", dateTimeUTC(), Field(static_cast<UInt64>(constant)));

    auto weakened = deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Weaken, 1.0, constant + 3600, /*allow_top_k_filter=*/false);
    ASSERT_TRUE(weakened);
    EXPECT_NE(weakened->condition.find(std::to_string(day_start + day / 2)), String::npos) << weakened->condition;
}

TEST(QueryConditionCacheTimeConditions, DateTime64IsRoundedInTicks)
{
    Int64 noon = day_start + day / 2;
    auto type = std::make_shared<DataTypeDateTime64>(3, "UTC");
    Field constant = DecimalField<DateTime64>(DateTime64(noon * 1000 + 123), 3);
    ComparisonCase test("greaterOrEquals", std::static_pointer_cast<const IDataType>(type), constant);

    auto weakened = deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Weaken, 1.0, noon + 10 * day, /*allow_top_k_filter=*/false);
    auto strengthened = deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Strengthen, 1.0, noon + 10 * day, /*allow_top_k_filter=*/false);
    ASSERT_TRUE(weakened);
    ASSERT_TRUE(strengthened);
    EXPECT_NE(weakened->condition.find(std::to_string(day_start)), String::npos) << weakened->condition;
    EXPECT_NE(strengthened->condition.find(std::to_string(day_start + day)), String::npos) << strengthened->condition;
}

TEST(QueryConditionCacheTimeConditions, UnsupportedShapesAreNotDerived)
{
    Int64 noon = day_start + day / 2;
    time_t now = noon + 10 * day;

    {
        /// Equality cannot be weakened or strengthened by rounding.
        ComparisonCase test("equals", dateTimeUTC(), Field(static_cast<UInt64>(noon)));
        EXPECT_FALSE(deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Weaken, 1.0, now, /*allow_top_k_filter=*/false));
    }
    {
        /// A non-deterministic constant of a non-temporal type cannot be rounded.
        ComparisonCase test("greaterOrEquals", std::make_shared<DataTypeUInt64>(), Field(static_cast<UInt64>(noon)));
        EXPECT_FALSE(deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Weaken, 1.0, now, /*allow_top_k_filter=*/false));
    }
    {
        /// A roundable comparison under a function of unknown monotonicity cannot be used.
        ActionsDAG dag;
        const auto & time_input = dag.addInput("time", dateTimeUTC());
        const auto & flag_input = dag.addInput("flag", std::make_shared<DataTypeUInt8>());
        const auto & constant = addNonDeterministicConstant(dag, dateTimeUTC(), Field(static_cast<UInt64>(noon)));
        const auto & comparison = addFunction(dag, "greaterOrEquals", {&time_input, &constant});
        const auto & condition = addFunction(dag, "xor", {&comparison, &flag_input});
        EXPECT_FALSE(deriveDeterministicTimeCondition(&condition, TimeConditionRounding::Weaken, 1.0, now, /*allow_top_k_filter=*/false));
    }
    {
        /// A non-positive grid factor disables the derivation.
        ComparisonCase test("greaterOrEquals", dateTimeUTC(), Field(static_cast<UInt64>(noon)));
        EXPECT_FALSE(deriveDeterministicTimeCondition(test.condition, TimeConditionRounding::Weaken, 0.0, now, /*allow_top_k_filter=*/false));
    }
}
