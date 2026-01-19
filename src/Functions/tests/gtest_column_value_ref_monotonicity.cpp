#include <gtest/gtest.h>

#include <limits>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/RangeRef.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Common/DateLUT.h>

using namespace DB;

namespace
{

FunctionBasePtr buildFunctionBase(const String & name, const ColumnsWithTypeAndName & arguments)
{
    tryRegisterFunctions();
    auto context = getContext().context;
    auto resolver = FunctionFactory::instance().get(name, context);
    return resolver->build(arguments);
}

ColumnWithTypeAndName makeColumn(const DataTypePtr & type, const String & name = {})
{
    auto column = type->createColumn();
    column->insertDefault();
    return {std::move(column), type, name};
}

ColumnWithTypeAndName makeConstStringColumn(const String & value, const String & name = {})
{
    auto type = std::make_shared<DataTypeString>();
    return {type->createColumnConst(1, value), type, name};
}

ColumnWithTypeAndName makeConstInt64Column(Int64 value, const String & name = {})
{
    auto type = std::make_shared<DataTypeInt64>();
    return {type->createColumnConst(1, value), type, name};
}

ColumnWithTypeAndName makeConstUInt64Column(UInt64 value, const String & name = {})
{
    auto type = std::make_shared<DataTypeUInt64>();
    return {type->createColumnConst(1, value), type, name};
}

ColumnWithTypeAndName makeConstIntervalSecondColumn(Int64 value, const String & name = {})
{
    auto type = std::make_shared<DataTypeInterval>(IntervalKind::Kind::Second);
    return {type->createColumnConst(1, value), type, name};
}

/// This can check that the function does not require materialization of Field values because
/// materialization is not possible for ColumnNothing and it will throw in that case.
void expectNoFieldMaterialization(
    const FunctionBasePtr & function_base,
    bool expected_is_monotonic = true,
    bool expected_is_always_monotonic = true,
    bool expected_is_strict = false)
{
    ASSERT_TRUE(function_base);
    ASSERT_FALSE(function_base->getArgumentTypes().empty());

    auto dummy_column = ColumnNothing::create(1);
    const ColumnValueRef left = ColumnValueRef::normal(dummy_column.get(), 0);
    const ColumnValueRef right = ColumnValueRef::normal(dummy_column.get(), 0);

    const IDataType & type = *function_base->getArgumentTypes().front();
    const auto monotonicity = function_base->getMonotonicityForRange(type, left, right);

    EXPECT_EQ(monotonicity.is_monotonic, expected_is_monotonic);
    EXPECT_EQ(monotonicity.is_always_monotonic, expected_is_always_monotonic);
    EXPECT_EQ(monotonicity.is_strict, expected_is_strict);
}

void expectMonotonicity(const IFunctionBase::Monotonicity & actual, const IFunctionBase::Monotonicity & expected)
{
    EXPECT_EQ(actual.is_monotonic, expected.is_monotonic);
    EXPECT_EQ(actual.is_always_monotonic, expected.is_always_monotonic);

    /// `is_positive` and `is_strict` are meaningful only for monotonic functions.
    if (expected.is_monotonic && actual.is_monotonic)
    {
        EXPECT_EQ(actual.is_positive, expected.is_positive);
        EXPECT_EQ(actual.is_strict, expected.is_strict);
    }
}

template <typename ColumnT>
ColumnPtr makeNumericColumn(std::initializer_list<typename ColumnT::ValueType> values)
{
    auto col = ColumnT::create();
    auto & data = col->getData();
    data.reserve(values.size());
    for (const auto & v : values)
        data.push_back(v);
    return col;
}

}

TEST(ColumnValueRefMonotonicity, Materialize)
{
    auto func = buildFunctionBase("materialize", {makeColumn(std::make_shared<DataTypeUInt64>())});
    expectNoFieldMaterialization(func);
}

TEST(ColumnValueRefMonotonicity, ConversionTypeOnlyNoFieldMaterialization)
{
    auto dummy_column = ColumnNothing::create(1);
    const ColumnValueRef left = ColumnValueRef::normal(dummy_column.get(), 0);
    const ColumnValueRef right = ColumnValueRef::normal(dummy_column.get(), 0);

    {
        auto func = buildFunctionBase("toUInt64", {makeColumn(std::make_shared<DataTypeUInt64>())});
        const IDataType & arg_type = *func->getArgumentTypes().front();
        const auto mono = func->getMonotonicityForRange(arg_type, left, right);
        expectMonotonicity(mono, {.is_monotonic = true, .is_always_monotonic = true, .is_strict = true});
    }

    {
        auto func = buildFunctionBase("toDateTime", {makeColumn(std::make_shared<DataTypeUInt16>())});
        const IDataType & arg_type = *func->getArgumentTypes().front();
        const auto mono = func->getMonotonicityForRange(arg_type, left, right);
        expectMonotonicity(mono, {.is_monotonic = true, .is_always_monotonic = true, .is_strict = true});
    }
}

TEST(ColumnValueRefMonotonicity, ToUnixTimestamp64)
{
    auto dt64_type = std::make_shared<DataTypeDateTime64>(9);
    auto arg = makeColumn(dt64_type);

    expectNoFieldMaterialization(buildFunctionBase("toUnixTimestamp64Nano", {arg}));
    expectNoFieldMaterialization(buildFunctionBase("toUnixTimestamp64Micro", {arg}));
    expectNoFieldMaterialization(buildFunctionBase("toUnixTimestamp64Milli", {arg}));
    expectNoFieldMaterialization(buildFunctionBase("toUnixTimestamp64Second", {arg}));
}

TEST(ColumnValueRefMonotonicity, IcebergTruncate)
{
    auto func = buildFunctionBase("icebergTruncate", {makeConstUInt64Column(3), makeColumn(std::make_shared<DataTypeUInt64>())});
    expectNoFieldMaterialization(func);
}

TEST(ColumnValueRefMonotonicity, Rounding)
{
    auto arg = makeColumn(std::make_shared<DataTypeFloat64>());

    expectNoFieldMaterialization(buildFunctionBase("floor", {arg}));
    expectNoFieldMaterialization(buildFunctionBase("ceiling", {arg}));
    expectNoFieldMaterialization(buildFunctionBase("truncate", {arg}));
    expectNoFieldMaterialization(buildFunctionBase("round", {arg}));
    expectNoFieldMaterialization(buildFunctionBase("roundBankers", {arg}));
}

TEST(ColumnValueRefMonotonicity, ToInterval)
{
    auto func = buildFunctionBase("toInterval", {makeColumn(std::make_shared<DataTypeInt64>()), makeConstStringColumn("second")});
    expectNoFieldMaterialization(func);
}

TEST(ColumnValueRefMonotonicity, DateTrunc)
{
    auto dt_type = std::make_shared<DataTypeDateTime>();
    auto func = buildFunctionBase("dateTrunc", {makeConstStringColumn("day"), makeColumn(dt_type)});
    expectNoFieldMaterialization(func);
}

TEST(ColumnValueRefMonotonicity, ToStartOfInterval)
{
    auto dt_type = std::make_shared<DataTypeDateTime>();
    auto func = buildFunctionBase("toStartOfInterval", {makeColumn(dt_type), makeConstIntervalSecondColumn(1)});
    expectNoFieldMaterialization(func);
}

TEST(ColumnValueRefMonotonicity, ToTimezone)
{
    auto dt_type = std::make_shared<DataTypeDateTime>();
    auto func = buildFunctionBase("toTimezone", {makeColumn(dt_type), makeConstStringColumn("UTC")});
    expectNoFieldMaterialization(
        func, /* expected_is_monotonic = */ true, /* expected_is_always_monotonic = */ true, /* expected_is_strict = */ true);
}

TEST(ColumnValueRefMonotonicity, ModifiedJulianDay)
{
    auto func_to = buildFunctionBase("toModifiedJulianDay", {makeColumn(std::make_shared<DataTypeString>())});
    expectNoFieldMaterialization(
        func_to, /* expected_is_monotonic = */ true, /* expected_is_always_monotonic = */ true, /* expected_is_strict = */ true);

    auto func_to_or_null = buildFunctionBase("toModifiedJulianDayOrNull", {makeColumn(std::make_shared<DataTypeString>())});
    expectNoFieldMaterialization(
        func_to_or_null, /* expected_is_monotonic = */ true, /* expected_is_always_monotonic = */ true, /* expected_is_strict = */ true);

    auto func_from = buildFunctionBase("fromModifiedJulianDay", {makeColumn(std::make_shared<DataTypeInt32>())});
    expectNoFieldMaterialization(
        func_from, /* expected_is_monotonic = */ true, /* expected_is_always_monotonic = */ true, /* expected_is_strict = */ true);

    auto func_from_or_null = buildFunctionBase("fromModifiedJulianDayOrNull", {makeColumn(std::make_shared<DataTypeInt32>())});
    expectNoFieldMaterialization(
        func_from_or_null, /* expected_is_monotonic = */ true, /* expected_is_always_monotonic = */ true, /* expected_is_strict = */ true);
}

TEST(ColumnValueRefMonotonicity, AbsRanges)
{
    auto type = std::make_shared<DataTypeInt64>();
    auto func = buildFunctionBase("abs", {makeColumn(type)});
    const IDataType & arg_type = *func->getArgumentTypes().front();

    {
        auto col = makeNumericColumn<ColumnInt64>({-5, -1});
        const auto mono
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono, {.is_monotonic = true, .is_positive = false, .is_strict = true});
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({1, 5});
        const auto mono
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono, {.is_monotonic = true, .is_positive = true, .is_strict = true});
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({-1, 5});
        const auto mono
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono, {});
    }

    /// Boundary exactly at 0.
    {
        auto col = makeNumericColumn<ColumnInt64>({-1, 0});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-1)), Field(Int64(0)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({0, 1});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field(Int64(1)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({0});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 0));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field(Int64(0)));
        expectMonotonicity(mono_ref, mono_field);
    }

    /// Whole universe range.
    {
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field());
        expectMonotonicity(mono_ref, mono_field);
    }

    /// isNullAt() on ColumnNullable.
    {
        auto nullable_type = makeNullable(type);
        auto col = nullable_type->createColumn();
        col->insert(Field()); /// NULL
        col->insert(Field(Int64(-1))); /// -1

        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field(Int64(-1)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto nullable_type = makeNullable(type);
        auto col = nullable_type->createColumn();
        col->insert(Field()); /// NULL
        col->insert(Field(Int64(-1))); /// -1

        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 1), ColumnValueRef::normal(col.get(), 0));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-1)), Field());
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({-1});
        const auto mono = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::normal(col.get(), 0));
        expectMonotonicity(mono, {.is_monotonic = true, .is_positive = false, .is_strict = true});
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({-1});
        const auto mono = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::positiveInfinity());
        expectMonotonicity(mono, {});
    }
}

TEST(ColumnValueRefMonotonicity, NegateRanges)
{
    {
        auto type = std::make_shared<DataTypeInt64>();
        auto func = buildFunctionBase("negate", {makeColumn(type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto type = std::make_shared<DataTypeUInt64>();
        auto func = buildFunctionBase("negate", {makeColumn(type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        {
            auto col = makeNumericColumn<ColumnUInt64>({0, 1ULL << 63});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt64(0)), Field(UInt64(1ULL << 63)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt64>({0, (1ULL << 63) + 1});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt64(0)), Field(UInt64((1ULL << 63) + 1)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt64>({(1ULL << 63) + 2, std::numeric_limits<UInt64>::max()});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

            const auto mono_field
                = func->getMonotonicityForRange(arg_type, Field(UInt64((1ULL << 63) + 2)), Field(std::numeric_limits<UInt64>::max()));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt64>({0});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt64(0)), Field(POSITIVE_INFINITY));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt64>({1ULL << 63});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::normal(col.get(), 0));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(NEGATIVE_INFINITY), Field(UInt64(1ULL << 63)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt64>({(1ULL << 63) + 1});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::normal(col.get(), 0));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(NEGATIVE_INFINITY), Field(UInt64((1ULL << 63) + 1)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt64>({(1ULL << 63) + 1});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt64((1ULL << 63) + 1)), Field(POSITIVE_INFINITY));
            expectMonotonicity(mono_ref, mono_field);
        }

        /// Whole universe range.
        {
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(NEGATIVE_INFINITY), Field(POSITIVE_INFINITY));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto nullable_type = makeNullable(type);
            auto col = nullable_type->createColumn();
            col->insert(Field(UInt64(0)));
            col->insert(Field()); /// NULL

            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt64(0)), Field(POSITIVE_INFINITY));
            expectMonotonicity(mono_ref, mono_field);
        }
    }

    {
        auto type = std::make_shared<DataTypeUInt128>();
        auto func = buildFunctionBase("negate", {makeColumn(type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        {
            auto col = makeNumericColumn<ColumnUInt128>({UInt128(0), UInt128(1) << 127});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt128(0)), Field(UInt128(1) << 127));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt128>({UInt128(0), (UInt128(1) << 127) + 1});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt128(0)), Field((UInt128(1) << 127) + 1));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt128>({UInt128(1) << 127});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::normal(col.get(), 0));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(NEGATIVE_INFINITY), Field(UInt128(1) << 127));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt128>({(UInt128(1) << 127) + 1});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field((UInt128(1) << 127) + 1), Field(POSITIVE_INFINITY));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(NEGATIVE_INFINITY), Field(POSITIVE_INFINITY));
            expectMonotonicity(mono_ref, mono_field);
        }
    }

    {
        auto type = std::make_shared<DataTypeUInt256>();
        auto func = buildFunctionBase("negate", {makeColumn(type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        {
            auto col = makeNumericColumn<ColumnUInt256>({UInt256(0), UInt256(1) << 255});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt256(0)), Field(UInt256(1) << 255));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt256>({UInt256(0), (UInt256(1) << 255) + 1});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt256(0)), Field((UInt256(1) << 255) + 1));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt256>({UInt256(1) << 255});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::normal(col.get(), 0));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(NEGATIVE_INFINITY), Field(UInt256(1) << 255));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(NEGATIVE_INFINITY), Field(POSITIVE_INFINITY));
            expectMonotonicity(mono_ref, mono_field);
        }
    }
}

TEST(ColumnValueRefMonotonicity, SignIsMonotonic)
{
    auto type = std::make_shared<DataTypeInt64>();
    auto func = buildFunctionBase("sign", {makeColumn(type)});
    const IDataType & arg_type = *func->getArgumentTypes().front();

    auto col = makeNumericColumn<ColumnInt64>({-10, 10});
    expectMonotonicity(
        func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
        {.is_monotonic = true});

    expectMonotonicity(
        func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity()),
        {.is_monotonic = true});
}

TEST(ColumnValueRefMonotonicity, IntExpRanges)
{
    auto type = std::make_shared<DataTypeInt64>();

    {
        auto func = buildFunctionBase("intExp2", {makeColumn(type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        {
            auto col_good = makeNumericColumn<ColumnInt64>({0, 63});
            const auto mono_ref = func->getMonotonicityForRange(
                arg_type, ColumnValueRef::normal(col_good.get(), 0), ColumnValueRef::normal(col_good.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field(Int64(63)));
            expectMonotonicity(mono_ref, mono_field);
        }

        /// Single point boundaries.
        {
            auto col = makeNumericColumn<ColumnInt64>({0});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 0));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field(Int64(0)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnInt64>({63});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 0));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(63)), Field(Int64(63)));
            expectMonotonicity(mono_ref, mono_field);
        }

        /// Negative range.
        {
            auto col_neg = makeNumericColumn<ColumnInt64>({-1, 10});
            const auto mono_ref = func->getMonotonicityForRange(
                arg_type, ColumnValueRef::normal(col_neg.get(), 0), ColumnValueRef::normal(col_neg.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-1)), Field(Int64(10)));
            expectMonotonicity(mono_ref, mono_field);
        }

        /// Upper overflow range.
        {
            auto col_over = makeNumericColumn<ColumnInt64>({0, 64});
            const auto mono_ref = func->getMonotonicityForRange(
                arg_type, ColumnValueRef::normal(col_over.get(), 0), ColumnValueRef::normal(col_over.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field(Int64(64)));
            expectMonotonicity(mono_ref, mono_field);
        }

        /// One-sided infinity bounds.
        {
            auto col = makeNumericColumn<ColumnInt64>({63});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::normal(col.get(), 0));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field(Int64(63)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnInt64>({0});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field());
            expectMonotonicity(mono_ref, mono_field);
        }

        /// Whole universe range.
        {
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field());
            expectMonotonicity(mono_ref, mono_field);
        }

        /// isNullAt() on ColumnNullable.
        {
            auto nullable_type = makeNullable(type);
            auto col = nullable_type->createColumn();
            col->insert(Field()); /// NULL
            col->insert(Field(Int64(10))); /// 10

            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field(Int64(10)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto nullable_type = makeNullable(type);
            auto col = nullable_type->createColumn();
            col->insert(Field(Int64(0))); /// 0
            col->insert(Field()); /// NULL

            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field());
            expectMonotonicity(mono_ref, mono_field);
        }
    }

    {
        auto func = buildFunctionBase("intExp10", {makeColumn(type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        {
            auto col_good = makeNumericColumn<ColumnInt64>({0, 19});
            const auto mono_ref = func->getMonotonicityForRange(
                arg_type, ColumnValueRef::normal(col_good.get(), 0), ColumnValueRef::normal(col_good.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field(Int64(19)));
            expectMonotonicity(mono_ref, mono_field);
        }

        /// Single point boundaries.
        {
            auto col = makeNumericColumn<ColumnInt64>({0});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 0));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field(Int64(0)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnInt64>({19});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 0));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(19)), Field(Int64(19)));
            expectMonotonicity(mono_ref, mono_field);
        }

        /// Negative range.
        {
            auto col_neg = makeNumericColumn<ColumnInt64>({-1, 10});
            const auto mono_ref = func->getMonotonicityForRange(
                arg_type, ColumnValueRef::normal(col_neg.get(), 0), ColumnValueRef::normal(col_neg.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-1)), Field(Int64(10)));
            expectMonotonicity(mono_ref, mono_field);
        }

        /// Upper overflow range.
        {
            auto col_over = makeNumericColumn<ColumnInt64>({0, 20});
            const auto mono_ref = func->getMonotonicityForRange(
                arg_type, ColumnValueRef::normal(col_over.get(), 0), ColumnValueRef::normal(col_over.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field(Int64(20)));
            expectMonotonicity(mono_ref, mono_field);
        }

        /// One-sided infinity bounds.
        {
            auto col = makeNumericColumn<ColumnInt64>({19});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::normal(col.get(), 0));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field(Int64(19)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnInt64>({0});
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field());
            expectMonotonicity(mono_ref, mono_field);
        }

        /// Whole universe range.
        {
            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field());
            expectMonotonicity(mono_ref, mono_field);
        }

        /// isNullAt() on ColumnNullable.
        {
            auto nullable_type = makeNullable(type);
            auto col = nullable_type->createColumn();
            col->insert(Field()); /// NULL
            col->insert(Field(Int64(10))); /// 10

            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field(Int64(10)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto nullable_type = makeNullable(type);
            auto col = nullable_type->createColumn();
            col->insert(Field(Int64(0))); /// 0
            col->insert(Field()); /// NULL

            const auto mono_ref
                = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field());
            expectMonotonicity(mono_ref, mono_field);
        }
    }
}

TEST(ColumnValueRefMonotonicity, FactorialRanges)
{
    auto type = std::make_shared<DataTypeInt64>();
    auto func = buildFunctionBase("factorial", {makeColumn(type)});
    const IDataType & arg_type = *func->getArgumentTypes().front();

    {
        auto col = makeNumericColumn<ColumnInt64>({1, 20});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(1)), Field(Int64(20)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({0, 20});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = false});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field(Int64(20)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({1, 21});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = false});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(1)), Field(Int64(21)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({20});

        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::normal(col.get(), 0));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = false});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field(Int64(20)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({1});

        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::positiveInfinity());
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = false});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(1)), Field());
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({-5, 0});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = false});
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({-5, 2});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = false});
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({1});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 0));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = true});
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({20});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 0));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = true});
    }

    {
        auto nullable_type = makeNullable(type);
        auto col = nullable_type->createColumn();
        col->insert(Field()); /// NULL
        col->insert(Field(Int64(20))); /// 20

        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = false});
    }

    {
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = false});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field());
        expectMonotonicity(mono_ref, mono_field);
    }
}

TEST(ColumnValueRefMonotonicity, ToStartOfDayIsAlwaysMonotonic)
{
    auto dt_type = std::make_shared<DataTypeDateTime>();
    auto func = buildFunctionBase("toStartOfDay", {makeColumn(dt_type)});
    const IDataType & arg_type = *func->getArgumentTypes().front();

    const auto & date_lut = DateLUT::instance();
    const auto t_1 = static_cast<UInt32>(date_lut.makeDateTime(2020, 1, 1, 23, 59, 59));
    const auto t_2 = static_cast<UInt32>(date_lut.makeDateTime(2020, 1, 2, 0, 0, 0));

    auto col = makeNumericColumn<ColumnUInt32>({t_1, t_2});

    {
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_always_monotonic = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt32(t_1)), Field(UInt32(t_2)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_always_monotonic = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(NEGATIVE_INFINITY), Field(POSITIVE_INFINITY));
        expectMonotonicity(mono_ref, mono_field);
    }
}

TEST(ColumnValueRefMonotonicity, CustomWeekRanges)
{
    auto dt_type = std::make_shared<DataTypeDateTime>();
    auto to_week = buildFunctionBase("toWeek", {makeColumn(dt_type)});
    const IDataType & arg_type = *to_week->getArgumentTypes().front();

    const auto & date_lut = DateLUT::instance();
    const auto t_2020_01_01 = static_cast<UInt32>(date_lut.makeDateTime(2020, 1, 1, 0, 0, 0));
    const auto t_2020_12_31 = static_cast<UInt32>(date_lut.makeDateTime(2020, 12, 31, 23, 59, 59));
    const auto t_2021_01_01 = static_cast<UInt32>(date_lut.makeDateTime(2021, 1, 1, 0, 0, 0));

    {
        auto col = makeNumericColumn<ColumnUInt32>({t_2020_01_01, t_2020_12_31});
        const auto mono_ref
            = to_week->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true});

        const auto mono_field = to_week->getMonotonicityForRange(arg_type, Field(UInt32(t_2020_01_01)), Field(UInt32(t_2020_12_31)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnUInt32>({t_2020_12_31, t_2021_01_01});
        const auto mono_ref
            = to_week->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = to_week->getMonotonicityForRange(arg_type, Field(UInt32(t_2020_12_31)), Field(UInt32(t_2021_01_01)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        const auto mono_ref
            = to_week->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
        expectMonotonicity(mono_ref, {});

        const auto mono_field = to_week->getMonotonicityForRange(arg_type, Field(NEGATIVE_INFINITY), Field(POSITIVE_INFINITY));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto nullable_type = makeNullable(dt_type);
        auto col = nullable_type->createColumn();
        col->insert(Field()); /// NULL
        col->insert(Field(UInt32(t_2020_01_01))); /// 2020-01-01 00:00:00

        const auto mono_ref
            = to_week->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = to_week->getMonotonicityForRange(arg_type, Field(), Field(UInt32(t_2020_01_01)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto to_year_week = buildFunctionBase("toYearWeek", {makeColumn(dt_type)});
        const IDataType & arg_type_yw = *to_year_week->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnUInt32>({t_2020_12_31, t_2021_01_01});
        const auto mono_ref = to_year_week->getMonotonicityForRange(
            arg_type_yw, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_always_monotonic = true});

        const auto mono_field
            = to_year_week->getMonotonicityForRange(arg_type_yw, Field(UInt32(t_2020_12_31)), Field(UInt32(t_2021_01_01)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto date_type = std::make_shared<DataTypeDate>();
        auto to_week_date = buildFunctionBase("toWeek", {makeColumn(date_type)});
        const IDataType & arg_type_date = *to_week_date->getArgumentTypes().front();

        const UInt16 d_2020_01_01 = static_cast<UInt16>(date_lut.makeDayNum(2020, 1, 1).toUnderType());
        const UInt16 d_2020_12_31 = static_cast<UInt16>(date_lut.makeDayNum(2020, 12, 31).toUnderType());
        const UInt16 d_2021_01_01 = static_cast<UInt16>(date_lut.makeDayNum(2021, 1, 1).toUnderType());

        {
            auto col = makeNumericColumn<ColumnUInt16>({d_2020_01_01, d_2020_12_31});
            const auto mono_ref = to_week_date->getMonotonicityForRange(
                arg_type_date, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true});

            const auto mono_field
                = to_week_date->getMonotonicityForRange(arg_type_date, Field(UInt16(d_2020_01_01)), Field(UInt16(d_2020_12_31)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt16>({d_2020_12_31, d_2021_01_01});
            const auto mono_ref = to_week_date->getMonotonicityForRange(
                arg_type_date, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field
                = to_week_date->getMonotonicityForRange(arg_type_date, Field(UInt16(d_2020_12_31)), Field(UInt16(d_2021_01_01)));
            expectMonotonicity(mono_ref, mono_field);
        }
    }

    {
        auto date32_type = std::make_shared<DataTypeDate32>();
        auto to_week_date32 = buildFunctionBase("toWeek", {makeColumn(date32_type)});
        const IDataType & arg_type_date32 = *to_week_date32->getArgumentTypes().front();

        const Int32 d32_1969_01_01 = date_lut.makeDayNum(1969, 1, 1).toUnderType();
        const Int32 d32_1969_12_31 = date_lut.makeDayNum(1969, 12, 31).toUnderType();
        const Int32 d32_1970_01_01 = date_lut.makeDayNum(1970, 1, 1).toUnderType();

        const Int32 d32_2020_01_01 = date_lut.makeDayNum(2020, 1, 1).toUnderType();
        const Int32 d32_2020_12_31 = date_lut.makeDayNum(2020, 12, 31).toUnderType();
        const Int32 d32_2021_01_01 = date_lut.makeDayNum(2021, 1, 1).toUnderType();

        {
            auto col = makeNumericColumn<ColumnInt32>({d32_2020_01_01, d32_2020_12_31});
            const auto mono_ref = to_week_date32->getMonotonicityForRange(
                arg_type_date32, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true});

            const auto mono_field
                = to_week_date32->getMonotonicityForRange(arg_type_date32, Field(Int64(d32_2020_01_01)), Field(Int64(d32_2020_12_31)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnInt32>({d32_2020_12_31, d32_2021_01_01});
            const auto mono_ref = to_week_date32->getMonotonicityForRange(
                arg_type_date32, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field
                = to_week_date32->getMonotonicityForRange(arg_type_date32, Field(Int64(d32_2020_12_31)), Field(Int64(d32_2021_01_01)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnInt32>({d32_1969_01_01, d32_1969_12_31});
            const auto mono_ref = to_week_date32->getMonotonicityForRange(
                arg_type_date32, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true});

            const auto mono_field
                = to_week_date32->getMonotonicityForRange(arg_type_date32, Field(Int64(d32_1969_01_01)), Field(Int64(d32_1969_12_31)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnInt32>({d32_1969_12_31, d32_1970_01_01});
            const auto mono_ref = to_week_date32->getMonotonicityForRange(
                arg_type_date32, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field
                = to_week_date32->getMonotonicityForRange(arg_type_date32, Field(Int64(d32_1969_12_31)), Field(Int64(d32_1970_01_01)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto nullable_type = makeNullable(date32_type);
            auto col = nullable_type->createColumn();
            col->insert(Field()); /// NULL
            col->insert(Field(Int64(d32_2020_01_01))); /// 2020-01-01

            const auto mono_ref = to_week_date32->getMonotonicityForRange(
                arg_type_date32, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = to_week_date32->getMonotonicityForRange(arg_type_date32, Field(), Field(Int64(d32_2020_01_01)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnInt32>({d32_2020_01_01, d32_2020_12_31});

            auto lc_type = std::make_shared<DataTypeLowCardinality>(date32_type);
            auto nullable_type = makeNullable(date32_type);
            auto lc_nullable_type = std::make_shared<DataTypeLowCardinality>(nullable_type);

            for (const auto & type : {DataTypePtr{lc_type}, nullable_type, DataTypePtr{lc_nullable_type}})
            {
                const auto mono_ref = to_week_date32->getMonotonicityForRange(
                    *type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
                expectMonotonicity(mono_ref, {.is_monotonic = true});

                const auto mono_field
                    = to_week_date32->getMonotonicityForRange(*type, Field(Int64(d32_2020_01_01)), Field(Int64(d32_2020_12_31)));
                expectMonotonicity(mono_ref, mono_field);
            }
        }
    }

    {
        auto dt64_type = std::make_shared<DataTypeDateTime64>(0);
        auto to_week_dt64 = buildFunctionBase("toWeek", {makeColumn(dt64_type)});
        const IDataType & arg_type_dt64 = *to_week_dt64->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({Int64(t_2020_12_31), Int64(t_2021_01_01)});
        const auto mono_ref = to_week_dt64->getMonotonicityForRange(
            arg_type_dt64, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = to_week_dt64->getMonotonicityForRange(
            arg_type_dt64,
            Field(DecimalField<DateTime64>(DateTime64(Int64(t_2020_12_31)), 0)),
            Field(DecimalField<DateTime64>(DateTime64(Int64(t_2021_01_01)), 0)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto str_type = std::make_shared<DataTypeString>();
        auto to_week_str = buildFunctionBase("toWeek", {makeColumn(str_type)});
        const IDataType & arg_type_str = *to_week_str->getArgumentTypes().front();

        auto col = str_type->createColumn();
        col->insert(Field(String("not a datetime")));
        col->insert(Field(String("2020-01-01 00:00:00")));

        const auto mono_ref = to_week_str->getMonotonicityForRange(
            arg_type_str, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field
            = to_week_str->getMonotonicityForRange(arg_type_str, Field(String("not a datetime")), Field(String("2020-01-01 00:00:00")));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto str_type = std::make_shared<DataTypeString>();
        auto to_year_week_str = buildFunctionBase("toYearWeek", {makeColumn(str_type)});
        const IDataType & arg_type_str = *to_year_week_str->getArgumentTypes().front();

        auto col = str_type->createColumn();
        col->insert(Field(String("2020-12-31 23:59:59")));
        col->insert(Field(String("2021-01-01 00:00:00")));

        const auto mono_ref = to_year_week_str->getMonotonicityForRange(
            arg_type_str, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = to_year_week_str->getMonotonicityForRange(
            arg_type_str, Field(String("2020-12-31 23:59:59")), Field(String("2021-01-01 00:00:00")));
        expectMonotonicity(mono_ref, mono_field);
    }
}

TEST(ColumnValueRefMonotonicity, ToHourRanges)
{
    auto dt_type = std::make_shared<DataTypeDateTime>();
    auto func = buildFunctionBase("toHour", {makeColumn(dt_type)});
    const IDataType & arg_type = *func->getArgumentTypes().front();

    const auto & date_lut = DateLUT::instance();
    const auto t_day_start = static_cast<UInt32>(date_lut.makeDateTime(2020, 1, 1, 0, 0, 0));
    const auto t_day_end = static_cast<UInt32>(date_lut.makeDateTime(2020, 1, 1, 23, 59, 59));
    const auto t_next_day = static_cast<UInt32>(date_lut.makeDateTime(2020, 1, 2, 0, 0, 0));

    {
        auto col = makeNumericColumn<ColumnUInt32>({t_day_start, t_day_end});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt32(t_day_start)), Field(UInt32(t_day_end)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnUInt32>({t_day_end, t_next_day});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(UInt32(t_day_end)), Field(UInt32(t_next_day)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(NEGATIVE_INFINITY), Field(POSITIVE_INFINITY));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto nullable_type = makeNullable(dt_type);
        auto col = nullable_type->createColumn();
        col->insert(Field()); /// NULL
        col->insert(Field(UInt32(t_day_start))); /// 2020-01-01 00:00:00

        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field(UInt32(t_day_start)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnUInt32>({t_day_start, t_day_end, t_next_day});

        auto lc_type = std::make_shared<DataTypeLowCardinality>(dt_type);
        auto nullable_type = makeNullable(dt_type);
        auto lc_nullable_type = std::make_shared<DataTypeLowCardinality>(nullable_type);

        for (const auto & type : {DataTypePtr{lc_type}, nullable_type, DataTypePtr{lc_nullable_type}})
        {
            const auto mono_ref
                = func->getMonotonicityForRange(*type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true});

            const auto mono_field = func->getMonotonicityForRange(*type, Field(UInt32(t_day_start)), Field(UInt32(t_day_end)));
            expectMonotonicity(mono_ref, mono_field);

            const auto mono_ref_boundary
                = func->getMonotonicityForRange(*type, ColumnValueRef::normal(col.get(), 1), ColumnValueRef::normal(col.get(), 2));
            expectMonotonicity(mono_ref_boundary, {});

            const auto mono_field_boundary = func->getMonotonicityForRange(*type, Field(UInt32(t_day_end)), Field(UInt32(t_next_day)));
            expectMonotonicity(mono_ref_boundary, mono_field_boundary);
        }
    }

    {
        auto dt64_type = std::make_shared<DataTypeDateTime64>(0);
        auto func_dt64 = buildFunctionBase("toHour", {makeColumn(dt64_type)});
        const IDataType & arg_type_dt64 = *func_dt64->getArgumentTypes().front();

        {
            auto col = makeNumericColumn<ColumnInt64>({Int64(t_day_start), Int64(t_day_end)});
            const auto mono_ref = func_dt64->getMonotonicityForRange(
                arg_type_dt64, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true});

            const auto mono_field = func_dt64->getMonotonicityForRange(
                arg_type_dt64,
                Field(DecimalField<DateTime64>(DateTime64(Int64(t_day_start)), 0)),
                Field(DecimalField<DateTime64>(DateTime64(Int64(t_day_end)), 0)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnInt64>({Int64(t_day_end), Int64(t_next_day)});
            const auto mono_ref = func_dt64->getMonotonicityForRange(
                arg_type_dt64, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func_dt64->getMonotonicityForRange(
                arg_type_dt64,
                Field(DecimalField<DateTime64>(DateTime64(Int64(t_day_end)), 0)),
                Field(DecimalField<DateTime64>(DateTime64(Int64(t_next_day)), 0)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            const auto mono_ref
                = func_dt64->getMonotonicityForRange(arg_type_dt64, ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity());
            expectMonotonicity(mono_ref, {});

            const auto mono_field = func_dt64->getMonotonicityForRange(arg_type_dt64, Field(NEGATIVE_INFINITY), Field(POSITIVE_INFINITY));
            expectMonotonicity(mono_ref, mono_field);
        }
    }
}

TEST(ColumnValueRefMonotonicity, ToMonthRangesDateAndDate32)
{
    const auto & date_lut = DateLUT::instance();
    const UInt16 d_2020_01_01 = static_cast<UInt16>(date_lut.makeDayNum(2020, 1, 1).toUnderType());
    const UInt16 d_2020_12_31 = static_cast<UInt16>(date_lut.makeDayNum(2020, 12, 31).toUnderType());
    const UInt16 d_2021_01_01 = static_cast<UInt16>(date_lut.makeDayNum(2021, 1, 1).toUnderType());

    {
        auto date_type = std::make_shared<DataTypeDate>();
        auto func_date = buildFunctionBase("toMonth", {makeColumn(date_type)});
        const IDataType & arg_type_date = *func_date->getArgumentTypes().front();

        {
            auto col = makeNumericColumn<ColumnUInt16>({d_2020_01_01, d_2020_12_31});
            const auto mono_ref = func_date->getMonotonicityForRange(
                arg_type_date, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true});

            const auto mono_field
                = func_date->getMonotonicityForRange(arg_type_date, Field(UInt16(d_2020_01_01)), Field(UInt16(d_2020_12_31)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnUInt16>({d_2020_12_31, d_2021_01_01});
            const auto mono_ref = func_date->getMonotonicityForRange(
                arg_type_date, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field
                = func_date->getMonotonicityForRange(arg_type_date, Field(UInt16(d_2020_12_31)), Field(UInt16(d_2021_01_01)));
            expectMonotonicity(mono_ref, mono_field);
        }
    }

    {
        auto date32_type = std::make_shared<DataTypeDate32>();
        auto func_date32 = buildFunctionBase("toMonth", {makeColumn(date32_type)});
        const IDataType & arg_type_date32 = *func_date32->getArgumentTypes().front();

        {
            auto col = makeNumericColumn<ColumnInt32>({Int32(d_2020_01_01), Int32(d_2020_12_31)});
            const auto mono_ref = func_date32->getMonotonicityForRange(
                arg_type_date32, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {.is_monotonic = true});

            const auto mono_field
                = func_date32->getMonotonicityForRange(arg_type_date32, Field(Int32(d_2020_01_01)), Field(Int32(d_2020_12_31)));
            expectMonotonicity(mono_ref, mono_field);
        }

        {
            auto col = makeNumericColumn<ColumnInt32>({Int32(d_2020_12_31), Int32(d_2021_01_01)});
            const auto mono_ref = func_date32->getMonotonicityForRange(
                arg_type_date32, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
            expectMonotonicity(mono_ref, {});

            const auto mono_field
                = func_date32->getMonotonicityForRange(arg_type_date32, Field(Int32(d_2020_12_31)), Field(Int32(d_2021_01_01)));
            expectMonotonicity(mono_ref, mono_field);
        }
    }
}

TEST(ColumnValueRefMonotonicity, BinaryArithmeticPlusMinusRanges)
{
    auto int64_type = std::make_shared<DataTypeInt64>();

    {
        auto func = buildFunctionBase("plus", {makeColumn(int64_type), makeConstInt64Column(5)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("plus", {makeConstInt64Column(5), makeColumn(int64_type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("plus", {makeColumn(int64_type), makeConstInt64Column(5)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({7, 7});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(7)), Field(Int64(7)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("plus", {makeColumn(int64_type), makeConstInt64Column(1)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({std::numeric_limits<Int64>::max() - 1, std::numeric_limits<Int64>::max()});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(
            arg_type, Field(std::numeric_limits<Int64>::max() - 1), Field(std::numeric_limits<Int64>::max()));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("minus", {makeColumn(int64_type), makeConstInt64Column(5)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("minus", {makeConstInt64Column(5), makeColumn(int64_type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("plus", {makeColumn(int64_type), makeConstInt64Column(5)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::normal(col.get(), 0));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("plus", {makeColumn(int64_type), makeConstInt64Column(5)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::positiveInfinity());
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(10)), Field());
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("minus", {makeColumn(int64_type), makeConstInt64Column(5)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto nullable_type = makeNullable(int64_type);
        auto col = nullable_type->createColumn();
        col->insert(Field()); /// NULL
        col->insert(Field(Int64(10))); /// 10

        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }
}

TEST(ColumnValueRefMonotonicity, BinaryArithmeticDivideIntDivRanges)
{
    auto int64_type = std::make_shared<DataTypeInt64>();

    /// variable / constant
    {
        auto func = buildFunctionBase("divide", {makeColumn(int64_type), makeConstInt64Column(2)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    /// variable / 0
    {
        auto func = buildFunctionBase("divide", {makeColumn(int64_type), makeConstInt64Column(0)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    /// constant / variable
    {
        auto func = buildFunctionBase("divide", {makeConstInt64Column(1), makeColumn(int64_type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({1, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(1)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    /// constant / variable (negative interval)
    {
        auto func = buildFunctionBase("divide", {makeConstInt64Column(1), makeColumn(int64_type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, -1});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(-1)));
        expectMonotonicity(mono_ref, mono_field);
    }

    /// intDiv variable / constant
    {
        auto func = buildFunctionBase("intDiv", {makeColumn(int64_type), makeConstInt64Column(2)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    /// intDiv constant / variable
    {
        auto func = buildFunctionBase("intDiv", {makeConstInt64Column(1), makeColumn(int64_type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({1, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(1)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("divide", {makeColumn(int64_type), makeConstInt64Column(2)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({7, 7});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(7)), Field(Int64(7)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("divide", {makeColumn(int64_type), makeConstInt64Column(-2)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_always_monotonic = true, .is_strict = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("intDiv", {makeColumn(int64_type), makeConstInt64Column(-2)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-10, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = false, .is_always_monotonic = true, .is_strict = false});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-10)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("divide", {makeColumn(int64_type), makeConstInt64Column(2)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::negativeInfinity(), ColumnValueRef::normal(col.get(), 0));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true, .is_always_monotonic = true, .is_strict = false});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    /// 0 / variable special case.
    {
        auto func = buildFunctionBase("divide", {makeConstInt64Column(0), makeColumn(int64_type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({1, 10});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(1)), Field(Int64(10)));
        expectMonotonicity(mono_ref, mono_field);
    }

    /// constant / variable that crosses zero is not monotonic.
    {
        auto func = buildFunctionBase("divide", {makeConstInt64Column(1), makeColumn(int64_type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-1, 1});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-1)), Field(Int64(1)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto func = buildFunctionBase("intDiv", {makeConstInt64Column(1), makeColumn(int64_type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({-1, 1});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(-1)), Field(Int64(1)));
        expectMonotonicity(mono_ref, mono_field);
    }

    /// constant / variable with 0 as an endpoint is not monotonic.
    {
        auto func = buildFunctionBase("divide", {makeConstInt64Column(1), makeColumn(int64_type)});
        const IDataType & arg_type = *func->getArgumentTypes().front();

        auto col = makeNumericColumn<ColumnInt64>({0, 1});
        const auto mono_ref
            = func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = func->getMonotonicityForRange(arg_type, Field(Int64(0)), Field(Int64(1)));
        expectMonotonicity(mono_ref, mono_field);
    }
}

TEST(ColumnValueRefMonotonicity, CastRanges)
{
    auto uint64_type = std::make_shared<DataTypeUInt64>();
    auto cast_func = buildFunctionBase("CAST", {makeColumn(uint64_type), makeConstStringColumn("UInt32")});
    const IDataType & arg_type = *cast_func->getArgumentTypes().front();

    {
        auto col = makeNumericColumn<ColumnUInt64>({0, 0xFFFFFFFFULL});
        const auto mono_ref
            = cast_func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {.is_monotonic = true, .is_positive = true});

        const auto mono_field = cast_func->getMonotonicityForRange(arg_type, Field(UInt64(0)), Field(UInt64(0xFFFFFFFFULL)));
        expectMonotonicity(mono_ref, mono_field);
    }

    {
        auto col = makeNumericColumn<ColumnUInt64>({0, 0x1'0000'0000ULL});
        const auto mono_ref
            = cast_func->getMonotonicityForRange(arg_type, ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1));
        expectMonotonicity(mono_ref, {});

        const auto mono_field = cast_func->getMonotonicityForRange(arg_type, Field(UInt64(0)), Field(UInt64(0x1'0000'0000ULL)));
        expectMonotonicity(mono_ref, mono_field);
    }
}
