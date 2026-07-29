#include <gtest/gtest.h>

#include <DataTypes/DataTypeFactory.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>

using namespace DB;

/// Pins both branches of `runtimeFilterTypeContainsFloat`, which decides whether the join runtime
/// filter may take its single-distinct-value `== const` fast path. The FALSE branch is what keeps the
/// optimization for types where `=` is key identity; it is not observable from SQL, because
/// `ValuesCount::ONE` and `ValuesCount::MANY` produce the same rows for such a type and update the same
/// stats, so an over-broad helper would silently cost the optimization while every SQL assertion still
/// passed.

namespace
{
bool containsFloat(const String & type_name)
{
    return runtimeFilterTypeContainsFloat(DataTypeFactory::instance().get(type_name));
}
}

TEST(RuntimeFilterContainsFloat, FloatBearingTypesDeclineTheFastPath)
{
    EXPECT_TRUE(containsFloat("Float64"));
    EXPECT_TRUE(containsFloat("Float32"));
    EXPECT_TRUE(containsFloat("BFloat16"));
    EXPECT_TRUE(containsFloat("LowCardinality(Float64)"));
    EXPECT_TRUE(containsFloat("Nullable(Float64)"));
    EXPECT_TRUE(containsFloat("Tuple(Float64, Int64)"));
    EXPECT_TRUE(containsFloat("Array(Float64)"));
    EXPECT_TRUE(containsFloat("Map(String, Float64)"));
    /// Two levels deep, so the type walk has to be transitive.
    EXPECT_TRUE(containsFloat("Array(Tuple(Float64, Int64))"));
    /// A declared JSON path is part of the static type and is found by the walk.
    EXPECT_TRUE(containsFloat("JSON(a Float64)"));
    /// A JSON without declared float paths still declines: its dynamic paths are not in the type.
    EXPECT_TRUE(containsFloat("JSON"));
    EXPECT_TRUE(containsFloat("Tuple(JSON)"));
}

TEST(RuntimeFilterContainsFloat, FloatFreeTypesKeepTheFastPath)
{
    EXPECT_FALSE(containsFloat("Int64"));
    EXPECT_FALSE(containsFloat("UInt8"));
    EXPECT_FALSE(containsFloat("Decimal64(2)"));
    EXPECT_FALSE(containsFloat("String"));
    EXPECT_FALSE(containsFloat("Date"));
    EXPECT_FALSE(containsFloat("DateTime64(3)"));
    EXPECT_FALSE(containsFloat("Tuple(Int64, String)"));
    EXPECT_FALSE(containsFloat("Array(Int64)"));
    EXPECT_FALSE(containsFloat("Map(String, Int64)"));
    EXPECT_FALSE(containsFloat("LowCardinality(String)"));
}

TEST(RuntimeFilterContainsFloat, NullTypeIsFloatFree)
{
    EXPECT_FALSE(runtimeFilterTypeContainsFloat(nullptr));
}
