#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
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
