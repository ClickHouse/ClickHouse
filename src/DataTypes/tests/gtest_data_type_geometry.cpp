#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/assert_cast.h>

#include <gtest/gtest.h>

using namespace DB;

/// For backward compatibility, new geo types should be appended at the end with
/// the next free discriminator.
GTEST_TEST(DataTypeGeometry, StableDiscriminators)
{
    auto geometry = DataTypeFactory::instance().get("Geometry");
    const auto & variant = assert_cast<const DataTypeVariant &>(*geometry);

    auto check = [&](const String & name, ColumnVariant::Discriminator expected)
    {
        auto discriminator = variant.tryGetVariantDiscriminator(name);
        ASSERT_TRUE(discriminator.has_value()) << name;
        EXPECT_EQ(*discriminator, expected) << name;
    };

    check("LineString", 0);
    check("MultiLineString", 1);
    check("MultiPolygon", 2);
    check("Point", 3);
    check("Polygon", 4);
    check("Ring", 5);
    check("MultiPoint", 6);

    EXPECT_EQ(variant.getVariants().size(), 7u);
}
