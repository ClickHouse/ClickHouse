#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/transformTypesRecursively.h>
#include <Common/assert_cast.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

DataTypePtr typeFromString(const String & name)
{
    return DataTypeFactory::instance().get(name);
}

std::vector<String> getNames(const DataTypes & types)
{
    std::vector<String> names;
    for (const auto & type : types)
        names.push_back(type->getName());
    return names;
}

/// Replaces UInt8 with UInt16 at every level of simple types that are all UInt8.
struct Walk
{
    size_t simple_calls = 0;
    size_t complex_calls = 0;

    DataTypes operator()(DataTypes types, const TypeIndexesSet & types_to_not_descend = {})
    {
        transformTypesRecursively(
            types,
            [&](DataTypes & leaves, TypeIndexesSet &)
            {
                ++simple_calls;
                if (std::ranges::all_of(leaves, [](const DataTypePtr & leaf) { return isUInt8(leaf); }))
                    for (auto & leaf : leaves)
                        leaf = std::make_shared<DataTypeUInt16>();
            },
            [&](DataTypes &, TypeIndexesSet &) { ++complex_calls; },
            /*format_settings=*/ nullptr,
            types_to_not_descend);
        return types;
    }
};

}

TEST(TransformTypesRecursively, LowCardinality)
{
    EXPECT_EQ(getNames(Walk{}({typeFromString("LowCardinality(UInt8)"), typeFromString("UInt8")})),
        (Names{"LowCardinality(UInt16)", "UInt16"}));
    EXPECT_EQ(getNames(Walk{}({typeFromString("LowCardinality(Nullable(UInt8))"), typeFromString("Nullable(UInt8)")})),
        (Names{"LowCardinality(Nullable(UInt16))", "Nullable(UInt16)"}));

    Walk walk;
    EXPECT_EQ(getNames(walk({typeFromString("LowCardinality(UInt8)"), typeFromString("LowCardinality(UInt8)")}, {TypeIndex::LowCardinality})),
        (Names{"LowCardinality(UInt8)", "LowCardinality(UInt8)"}));
    EXPECT_EQ(walk.simple_calls, 0);
    EXPECT_EQ(walk.complex_calls, 1);
}

TEST(TransformTypesRecursively, CustomName)
{
    /// A level whose children do not change is kept as is, with its custom name.
    auto point = typeFromString("Point");
    auto result = Walk{}({typeFromString("Tuple(p Point, u UInt8)"), typeFromString("Tuple(p Point, u UInt8)")});
    EXPECT_EQ(getNames(result), (Names{"Tuple(p Point, u UInt16)", "Tuple(p Point, u UInt16)"}));

    /// A rebuilt level is the plain type.
    EXPECT_EQ(getNames(Walk{}({typeFromString("Nested(a UInt8)"), typeFromString("Nested(a UInt8)")})),
        (Names{"Array(Tuple(a UInt16))", "Array(Tuple(a UInt16))"}));

    DataTypes unchanged{point, point};
    EXPECT_EQ(Walk{}(unchanged)[0].get(), point.get());
}

TEST(TransformTypesRecursively, JSON)
{
    EXPECT_EQ(getNames(Walk{}({typeFromString("JSON(a UInt8, b String)"), typeFromString("JSON(b String, a UInt8)")})),
        (Names{"JSON(a UInt16, b String)", "JSON(a UInt16, b String)"}));

    Walk walk;
    EXPECT_EQ(getNames(walk({typeFromString("JSON(a UInt8)"), typeFromString("JSON(a UInt8, b String)")})),
        (Names{"JSON(a UInt8)", "JSON(a UInt8, b String)"}));
    EXPECT_EQ(walk.simple_calls, 0);
    EXPECT_EQ(walk.complex_calls, 1);
}

TEST(TransformTypesRecursively, Variant)
{
    auto uint8 = std::make_shared<DataTypeUInt8>();
    auto string = std::make_shared<DataTypeString>();

    /// Alternatives are paired by `equals`, and each Variant keeps its discriminator order.
    auto result = Walk{}({
        std::make_shared<DataTypeVariant>(DataTypes{uint8, string}, DataTypeVariant::FixedDiscriminatorOrder{}),
        std::make_shared<DataTypeVariant>(DataTypes{string, uint8}, DataTypeVariant::FixedDiscriminatorOrder{})});
    EXPECT_EQ(getNames(assert_cast<const DataTypeVariant &>(*result[0]).getVariants()), (Names{"UInt16", "String"}));
    EXPECT_EQ(getNames(assert_cast<const DataTypeVariant &>(*result[1]).getVariants()), (Names{"String", "UInt16"}));

    /// A Variant holding two `equals`-equal alternatives is not walked.
    Walk ambiguous;
    auto ambiguous_variant = std::make_shared<DataTypeVariant>(
        DataTypes{std::make_shared<DataTypeDateTime>("UTC"), std::make_shared<DataTypeDateTime>("Asia/Tokyo"), uint8},
        DataTypeVariant::FixedDiscriminatorOrder{});
    EXPECT_EQ(ambiguous({ambiguous_variant, ambiguous_variant})[0].get(), ambiguous_variant.get());
    EXPECT_EQ(ambiguous.simple_calls, 0);
    EXPECT_EQ(ambiguous.complex_calls, 1);

    /// Neither is one with a different set of alternatives.
    EXPECT_EQ(getNames(Walk{}({typeFromString("Variant(UInt8, String)"), typeFromString("Variant(UInt8, Int8)")})),
        (Names{"Variant(String, UInt8)", "Variant(Int8, UInt8)"}));
}

TEST(TransformTypesRecursively, TupleNamingMode)
{
    auto result = Walk{}({typeFromString("Tuple(UInt8, String)"), typeFromString("Tuple(UInt8, String)")});
    EXPECT_EQ(getNames(result), (Names{"Tuple(UInt16, String)", "Tuple(UInt16, String)"}));
    EXPECT_FALSE(assert_cast<const DataTypeTuple &>(*result[0]).hasExplicitNames());

    Walk walk;
    EXPECT_EQ(getNames(walk({typeFromString("Tuple(`1` UInt8)"), typeFromString("Tuple(UInt8)")})),
        (Names{"Tuple(`1` UInt8)", "Tuple(UInt8)"}));
    EXPECT_EQ(walk.simple_calls, 0);
    EXPECT_EQ(walk.complex_calls, 1);

    EXPECT_EQ(getNames(Walk{}({typeFromString("Tuple(a UInt8)"), typeFromString("Tuple(b UInt8)")})),
        (Names{"Tuple(a UInt8)", "Tuple(b UInt8)"}));
}
