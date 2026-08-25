#include <initializer_list>
#include <limits>
#include <ostream>
#include <Core/Field.h>
#include <Interpreters/convertFieldToType.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>
#include <base/Decimal.h>
#include <base/types.h>

using namespace DB;

struct ConvertFieldToTypeTestParams
{
    const char * from_type; // MUST NOT BE NULL
    const Field from_value;
    const char * to_type; // MUST NOT BE NULL
    const std::optional<Field> expected_value;
};

static std::ostream & operator << (std::ostream & ostr, const ConvertFieldToTypeTestParams & params)
{
    return ostr << "{"
            << "\n\tfrom_type  : " << params.from_type
            << "\n\tto_type    : " << params.to_type
            << "\n}";
}

class ConvertFieldToTypeTest : public ::testing::TestWithParam<ConvertFieldToTypeTestParams>
{};

TEST_P(ConvertFieldToTypeTest, convert)
{
    const auto & params = GetParam();

    ASSERT_NE(nullptr, params.from_type);
    ASSERT_NE(nullptr, params.to_type);

    const auto & type_factory = DataTypeFactory::instance();
    const auto from_type = type_factory.get(params.from_type);
    const auto to_type = type_factory.get(params.to_type);

    if (params.expected_value)
    {
        const auto result = convertFieldToType(params.from_value, *to_type, from_type.get());
        EXPECT_EQ(*params.expected_value, result);
    }
    else
    {
        EXPECT_ANY_THROW(convertFieldToType(params.from_value, *to_type, from_type.get()));
    }
}

// Basically, the number of seconds in a day works for UTC here
const Int64 Day = 24 * 60 * 60;

// 123 is arbitrary value here

INSTANTIATE_TEST_SUITE_P(
    DateToDateTime64,
    ConvertFieldToTypeTest,
    ::testing::ValuesIn(std::initializer_list<ConvertFieldToTypeTestParams>{
        // min value of Date
        {
            "Date",
            Field(0),
            "DateTime64(0, 'UTC')",
            DecimalField<DateTime64>(DateTime64(0), 0)
        },
        // Max value of Date
        {
            "Date",
            Field(std::numeric_limits<UInt16>::max()),
            "DateTime64(0, 'UTC')",
            DecimalField<DateTime64>(DateTime64(std::numeric_limits<UInt16>::max() * Day), 0)
        },
        // check that scale is respected
        {
            "Date",
            Field(123),
            "DateTime64(0, 'UTC')",
            DecimalField<DateTime64>(DateTime64(123 * Day), 0)
        },
        {
            "Date",
            Field(1),
            "DateTime64(1, 'UTC')",
            DecimalField<DateTime64>(DateTime64(Day * 10), 1)
        },
        {
            "Date",
            Field(123),
            "DateTime64(3, 'UTC')",
            DecimalField<DateTime64>(DateTime64(123 * Day * 1000), 3)
        },
        {
            "Date",
            Field(123),
            "DateTime64(6, 'UTC')",
            DecimalField<DateTime64>(DateTime64(123 * Day * 1'000'000), 6)
        },
    })
);

INSTANTIATE_TEST_SUITE_P(
    Date32ToDateTime64,
    ConvertFieldToTypeTest,
    ::testing::ValuesIn(std::initializer_list<ConvertFieldToTypeTestParams>{
        // 1st Jan 1900 (see DATE_LUT_MIN_YEAR)
        {
            "Date32",
            Field(-25'567),
            "DateTime64(0, 'UTC')",
            DecimalField<DateTime64>(DateTime64(-25'567 * Day), 0)
        },
        // 31 Dec 2299 (see DATE_LUT_MAX_YEAR)
        {
            "Date32",
            Field(120'529),
            "DateTime64(0, 'UTC')",
            DecimalField<DateTime64>(DateTime64(120'529 * Day), 0)
        },
        // min value of Date32: 1st Jan 0000 (see DATE_LUT_MIN_EXTEND_DAY_NUM)
        {
            "Date32",
            Field(-719'528),
            "DateTime64(0, 'UTC')",
            DecimalField<DateTime64>(DateTime64(-719'528 * Day), 0)
        },
        // max value of Date32: 31 Dec 9999 (see DATE_LUT_MAX_EXTEND_DAY_NUM)
        {
            "Date32",
            Field(2'932'896),
            "DateTime64(0, 'UTC')",
            DecimalField<DateTime64>(DateTime64(2'932'896 * Day), 0)
        },
        // check that scale is respected
        {
            "Date32",
            Field(123),
            "DateTime64(0, 'UTC')",
            DecimalField<DateTime64>(DateTime64(123 * Day), 0)
        },
        {
            "Date32",
            Field(123),
            "DateTime64(1, 'UTC')",
            DecimalField<DateTime64>(DateTime64(123 * Day * 10), 1)
        },
        {
            "Date32",
            Field(123),
            "DateTime64(3, 'UTC')",
            DecimalField<DateTime64>(DateTime64(123 * Day * 1000), 3)
        },
        {
            "Date32",
            Field(123),
            "DateTime64(6, 'UTC')",
            DecimalField<DateTime64>(DateTime64(123 * Day * 1'000'000), 6)
        }
    })
);

/// A `Date32` day number does not fit into `UInt16`, so the coercion must keep the exact timestamp
/// instead of narrowing the day number and wrapping around. A timestamp outside the range of `DateTime`
/// simply matches nothing, which is what the exact-bound users of `convertFieldToType` need.
INSTANTIATE_TEST_SUITE_P(
    Date32ToDateTime,
    ConvertFieldToTypeTest,
    ::testing::ValuesIn(std::initializer_list<ConvertFieldToTypeTestParams>{
        // An ordinary day number.
        {
            "Date32",
            Field(19'723),
            "DateTime('UTC')",
            Field(19'723 * Day)
        },
        // min value of Date32: 1st Jan 0000 (see DATE_LUT_MIN_EXTEND_DAY_NUM).
        // It would wrap to day 1368 (30 Sep 1973) if the day number were narrowed to UInt16.
        {
            "Date32",
            Field(-719'528),
            "DateTime('UTC')",
            Field(-719'528 * Day)
        },
        // 31 Dec 1899, one day below the previous lower boundary of Date32.
        {
            "Date32",
            Field(-25'568),
            "DateTime('UTC')",
            Field(-25'568 * Day)
        },
        // max value of Date32: 31 Dec 9999 (see DATE_LUT_MAX_EXTEND_DAY_NUM).
        {
            "Date32",
            Field(2'932'896),
            "DateTime('UTC')",
            Field(2'932'896 * Day)
        },
    })
);

/// An exact numeric constant converted to `Date32` must respect the representable window
/// `[0000-01-01, 9999-12-31]` = day numbers `[-719528, 2932896]`. Out-of-window day numbers
/// convert to Null (the constant matches nothing) instead of materializing an impossible
/// `Date32` value that raw numeric consumers would see while formatting clamps it.
INSTANTIATE_TEST_SUITE_P(
    NumericToDate32,
    ConvertFieldToTypeTest,
    ::testing::ValuesIn(std::initializer_list<ConvertFieldToTypeTestParams>{
        // An ordinary positive day number (as unsigned and signed literal).
        {
            "UInt64",
            Field(UInt64(19'723)),
            "Date32",
            Field(Int64(19'723))
        },
        {
            "Int64",
            Field(Int64(-25'568)),
            "Date32",
            Field(Int64(-25'568))
        },
        // The exact boundaries stay valid.
        {
            "Int64",
            Field(Int64(-719'528)),
            "Date32",
            Field(Int64(-719'528))
        },
        {
            "UInt64",
            Field(UInt64(2'932'896)),
            "Date32",
            Field(Int64(2'932'896))
        },
        // One step outside either boundary converts to Null instead of passing through.
        {
            "Int64",
            Field(Int64(-719'529)),
            "Date32",
            Field()
        },
        {
            "UInt64",
            Field(UInt64(2'932'897)),
            "Date32",
            Field()
        },
        // A value that does not even fit into the underlying Int32.
        {
            "UInt64",
            Field(std::numeric_limits<UInt64>::max()),
            "Date32",
            Field()
        },
    })
);

INSTANTIATE_TEST_SUITE_P(
    DateTimeToDateTime64,
    ConvertFieldToTypeTest,
    ::testing::ValuesIn(std::initializer_list<ConvertFieldToTypeTestParams>{
        {
            "DateTime",
            Field(1),
            "DateTime64(0, 'UTC')",
            DecimalField<DateTime64>(DateTime64(1), 0)
        },
        {
            "DateTime",
            Field(1),
            "DateTime64(1, 'UTC')",
            DecimalField<DateTime64>(DateTime64(1'0), 1)
        },
        {
            "DateTime",
            Field(123),
            "DateTime64(3, 'UTC')",
            DecimalField<DateTime64>(DateTime64(123'000), 3)
        },
        {
            "DateTime",
            Field(123),
            "DateTime64(6, 'UTC')",
            DecimalField<DateTime64>(DateTime64(123'000'000), 6)
        },
    })
);

INSTANTIATE_TEST_SUITE_P(
    StringToNumber,
    ConvertFieldToTypeTest,
    ::testing::ValuesIn(std::initializer_list<ConvertFieldToTypeTestParams>{
        {
            "String",
            Field("1"),
            "Int8",
            Field(1)
        },
        {
            "String",
            Field("256"),
            "Int8",
            Field()
        },
        {
            "String",
            Field("not a number"),
            "Int8",
            {}
        },
        {
            "String",
            Field("1.1"),
            "Int8",
            {} /// we can not convert '1.1' to Int8
        },
        {
            "String",
            Field("1.1"),
            "Float64",
            Field(1.1)
        },
    })
);

INSTANTIATE_TEST_SUITE_P(
    NumberToString,
    ConvertFieldToTypeTest,
    ::testing::ValuesIn(std::initializer_list<ConvertFieldToTypeTestParams>{
        {
            "Int8",
            Field(1),
            "String",
            Field("1")
        },
        {
            "Int8",
            Field(-1),
            "String",
            Field("-1")
        },
        {
            "Float64",
            Field(1.1),
            "String",
            Field("1.1")
        },
    })
);

INSTANTIATE_TEST_SUITE_P(
    StringToDate,
    ConvertFieldToTypeTest,
    ::testing::ValuesIn(std::initializer_list<ConvertFieldToTypeTestParams>{
        {
            "String",
            Field("2024-07-12"),
            "Date",
            Field(static_cast<UInt16>(19916))
        },
        {
            "String",
            Field("not a date"),
            "Date",
            {}
        },
    })
);

/// https://github.com/ClickHouse/ClickHouse/issues/43144
/// Conversion to a floating-point type is exact by default (a value that is not exactly
/// representable returns Null), so optimizer/pruning callers do not build a wrong comparison
/// bound from a rounded constant. Materialization paths (the `values` table function, `INSERT`)
/// reach `convertFieldToTypeOrThrow` and pass `convert_inexact_floats=true` to opt into rounding to
/// the nearest value, like `CAST`.
/// The strict conversion used by the `IN` operator always stays exact.
TEST(ConvertFieldToTypeStrictness, Float64ToFloat32)
{
    const auto & type_factory = DataTypeFactory::instance();
    const auto from_type = type_factory.get("Float64");
    const auto to_type = type_factory.get("Float32");

    /// 0.1 is not exactly representable in Float32.
    const Field inexact{0.1};
    /// 0.5 is exactly representable in Float32.
    const Field exact{0.5};

    /// Default (non-strict, no opt-in): the conversion stays exact, so an inexact value is Null.
    /// This keeps lossy float rounding out of optimizer/pruning paths (e.g. `KeyCondition`).
    EXPECT_TRUE(convertFieldToType(inexact, *to_type, from_type.get()).isNull());

    /// Opt-in (convert_inexact_floats=true): the nearest Float32 value is returned, matching CAST.
    const Field nearest = convertFieldToType(inexact, *to_type, from_type.get(), {}, /*strict=*/ false, /*convert_inexact_floats=*/ true);
    EXPECT_FALSE(nearest.isNull());
    EXPECT_EQ(nearest, Field(static_cast<Float32>(0.1)));

    /// `convertFieldToTypeOrThrow` is exact by default: a non-representable value is rejected. This keeps
    /// lossy rounding out of callers such as `ALTER ... PARTITION` resolution (`getPartitionIDFromQuery`).
    EXPECT_THROW(convertFieldToTypeOrThrow(inexact, *to_type, from_type.get()), Exception);

    /// The `values`/insert path passes `convert_inexact_floats=true`, so it rounds to the nearest value and does not throw.
    const Field materialized = convertFieldToTypeOrThrow(inexact, *to_type, from_type.get(), {}, /*convert_inexact_floats=*/ true);
    EXPECT_EQ(materialized, Field(static_cast<Float32>(0.1)));

    /// Strict: a value that is not exactly representable becomes Null (excluded from an IN set).
    const Field strict_inexact = convertFieldToType(inexact, *to_type, from_type.get(), {}, /*strict=*/ true);
    EXPECT_TRUE(strict_inexact.isNull());

    /// An exactly representable value converts identically in all modes.
    EXPECT_EQ(convertFieldToType(exact, *to_type, from_type.get()), Field(static_cast<Float32>(0.5)));
    EXPECT_EQ(convertFieldToType(exact, *to_type, from_type.get(), {}, /*strict=*/ false, /*convert_inexact_floats=*/ true), Field(static_cast<Float32>(0.5)));
    EXPECT_EQ(convertFieldToType(exact, *to_type, from_type.get(), {}, /*strict=*/ true), Field(static_cast<Float32>(0.5)));

    /// Out-of-range values are rejected in every mode (no silent overflow to inf).
    const Field too_big{1e300};
    EXPECT_TRUE(convertFieldToType(too_big, *to_type, from_type.get()).isNull());
    EXPECT_TRUE(convertFieldToType(too_big, *to_type, from_type.get(), {}, /*strict=*/ false, /*convert_inexact_floats=*/ true).isNull());
    EXPECT_TRUE(convertFieldToType(too_big, *to_type, from_type.get(), {}, /*strict=*/ true).isNull());
}
