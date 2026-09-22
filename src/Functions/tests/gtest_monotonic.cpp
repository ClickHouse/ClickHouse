#include <gtest/gtest.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

using namespace DB;

namespace
{

FunctionBasePtr buildFunction(const String & name, const DataTypes & argument_types)
{
    tryRegisterFunctions();

    auto context = getContext().context;
    auto resolver = FunctionFactory::instance().get(name, context);

    ColumnsWithTypeAndName arguments;
    arguments.reserve(argument_types.size());
    for (const auto & argument_type : argument_types)
        arguments.emplace_back(ColumnWithTypeAndName{nullptr, argument_type, ""});

    return resolver->build(arguments);
}

void checkMonotonicity(
    const FunctionBasePtr & function_base,
    const IDataType & argument_type,
    const Field & right_bound,
    bool expected_is_monotonic,
    bool expected_is_always_monotonic)
{
    auto monotonicity = function_base->getMonotonicityForRange(argument_type, Field{}, right_bound);

    ASSERT_EQ(monotonicity.is_monotonic, expected_is_monotonic);
    ASSERT_EQ(monotonicity.is_always_monotonic, expected_is_always_monotonic);

    if (expected_is_monotonic)
        ASSERT_TRUE(monotonicity.is_positive);
}

void testNullWrapperMonotonicity(const String & name, const DataTypes & function_args)
{
    auto function_base = buildFunction(name, function_args);
    ASSERT_TRUE(function_base->hasInformationAboutMonotonicity());

    const auto nested_type = std::make_shared<DataTypeUInt8>();
    const auto nullable_type = makeNullable(nested_type);
    const auto low_cardinality_nullable_type = std::make_shared<DataTypeLowCardinality>(nullable_type);

    /// Unbounded range (right = NULL) is monotonic only for non-Nullable argument types.
    checkMonotonicity(function_base, *nested_type, Field{}, true, true);
    checkMonotonicity(function_base, *nullable_type, Field{}, false, false);
    checkMonotonicity(function_base, *low_cardinality_nullable_type, Field{}, false, false);

    /// Bounded range (right != NULL) is monotonic for Nullable/LowCardinality(Nullable) too.
    checkMonotonicity(function_base, *nullable_type, UInt64(1), true, false);
    checkMonotonicity(function_base, *low_cardinality_nullable_type, UInt64(1), true, false);
}

}

TEST(Monotonicity, AssumeNotNull)
{
    testNullWrapperMonotonicity("assumeNotNull", {std::make_shared<DataTypeUInt8>()});
}

TEST(Monotonicity, IfNull)
{
    testNullWrapperMonotonicity("ifNull", {std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeUInt8>()});
}

TEST(Monotonicity, Coalesce)
{
    testNullWrapperMonotonicity("coalesce", {std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeUInt8>()});
}

TEST(Monotonicity, ToNullable)
{
    /// `toNullable` only wraps the value, so it is strictly increasing on the whole range of any argument type.
    const DataTypes argument_types = {
        std::make_shared<DataTypeUInt8>(),
        std::make_shared<DataTypeString>(),
        makeNullable(std::make_shared<DataTypeString>()),
        std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
    };

    for (const auto & argument_type : argument_types)
    {
        SCOPED_TRACE(argument_type->getName());

        auto function_base = buildFunction("toNullable", {argument_type});
        ASSERT_TRUE(function_base->hasInformationAboutMonotonicity());

        const auto monotonicity = function_base->getMonotonicityForRange(*argument_type, Field{}, Field{});

        ASSERT_TRUE(monotonicity.is_monotonic);
        ASSERT_TRUE(monotonicity.is_positive);
        ASSERT_TRUE(monotonicity.is_always_monotonic);
        ASSERT_TRUE(monotonicity.is_strict);
    }
}

TEST(Monotonicity, ToDayOfWeekConstantMode)
{
    tryRegisterFunctions();
    auto resolver = FunctionFactory::instance().get("toDayOfWeek", getContext().context);
    auto date_type = std::make_shared<DataTypeDate>();
    auto mode_type = std::make_shared<DataTypeUInt8>();
    const UInt64 monday = DateLUT::instance().makeDayNum(2026, 8, 3).toUnderType();

    /// Sunday lies inside the Monday-based factor interval, so Sunday-first numbering is not monotonic.
    for (UInt64 mode = 0; mode < 8; ++mode)
    {
        SCOPED_TRACE(mode);
        const auto function = resolver->build({
            {nullptr, date_type, "d"}, {mode_type->createColumnConst(1, mode), mode_type, "mode"}});
        const auto monotonicity = function->getMonotonicityForRange(*date_type, monday, monday + 6);
        EXPECT_EQ(monotonicity.is_monotonic, mode % 4 < 2);
        EXPECT_FALSE(monotonicity.is_always_monotonic);
        EXPECT_FALSE(function->getMonotonicityForRange(*date_type, monday, monday + 7).is_monotonic);
    }
}

TEST(Monotonicity, ToDayOfWeekUnknownMode)
{
    const auto date_type = std::make_shared<DataTypeDate>();
    const auto mode_type = std::make_shared<DataTypeUInt8>();
    const UInt64 monday = DateLUT::instance().makeDayNum(2026, 8, 3).toUnderType();

    /// Type-only construction cannot prove that an explicit mode uses Monday-first numbering.
    const auto unknown_mode = buildFunction("toDayOfWeek", {date_type, mode_type});
    EXPECT_FALSE(unknown_mode->getMonotonicityForRange(*date_type, monday, monday + 6).is_monotonic);

    /// Omitting the mode selects Monday-first numbering without a constant argument.
    const auto default_mode = buildFunction("toDayOfWeek", {date_type});
    EXPECT_TRUE(default_mode->getMonotonicityForRange(*date_type, monday, monday + 6).is_monotonic);

    const auto resolver = FunctionFactory::instance().get("toDayOfWeek", getContext().context);
    const ColumnPtr mode_column = mode_type->createColumnConst(1, UInt64(0));
    const auto nonconstant_mode = resolver->build({
        {nullptr, date_type, "d"}, {mode_column->convertToFullColumnIfConst(), mode_type, "mode"}});
    EXPECT_FALSE(nonconstant_mode->getMonotonicityForRange(*date_type, monday, monday + 6).is_monotonic);

    /// An explicit time zone is not part of the default factor analysis, even with a known mode.
    const auto timezone_type = std::make_shared<DataTypeString>();
    const auto explicit_timezone = resolver->build({
        {nullptr, date_type, "d"}, {mode_column, mode_type, "mode"},
        {timezone_type->createColumnConst(1, String("UTC")), timezone_type, "timezone"}});
    EXPECT_FALSE(explicit_timezone->getMonotonicityForRange(*date_type, monday, monday + 6).is_monotonic);
}
