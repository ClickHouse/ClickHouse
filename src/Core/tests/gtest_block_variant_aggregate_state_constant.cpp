#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

/// A constant `Variant` of a single row whose active alternative is `global_discriminator`.
/// The value of the alternative is a default (empty) aggregate state.
ColumnPtr makeConstantVariantOfAggregateState(const DataTypeVariant & type, size_t global_discriminator)
{
    MutableColumns variants;
    for (const auto & alternative : type.getVariants())
        variants.push_back(alternative->createColumn());

    variants[global_discriminator]->insertDefault();

    auto local_discriminators = ColumnVariant::ColumnDiscriminators::create();
    local_discriminators->insertValue(static_cast<ColumnVariant::Discriminator>(global_discriminator));

    auto offsets = ColumnVariant::ColumnOffsets::create();
    offsets->insertValue(0);

    /// The local order of the variants is the global one, so no discriminators mapping is needed.
    auto variant = ColumnVariant::create(std::move(local_discriminators), std::move(offsets), std::move(variants));
    return ColumnConst::create(std::move(variant), 1);
}

Block makeBlock(const ColumnPtr & column, const DataTypePtr & type)
{
    return Block{ColumnWithTypeAndName{column, type, "v"}};
}

}

/// Aggregate states whose functions have the same state representation are compatible, and for
/// constants the comparison of their values is relaxed to the serialized state, because the
/// function names are allowed to differ. Under a `Variant` this relaxation must not apply: the
/// `Field` of a `Variant` value is the value of its active alternative and no longer says which
/// alternative it is, so two constants on different alternatives with the same serialized state
/// must not be reported as the same value.
GTEST_TEST(BlockStructure, ConstantVariantKeepsTheActiveAggregateStateAlternative)
{
    tryRegisterAggregateFunctions();

    auto quantile_type = DataTypeFactory::instance().get("AggregateFunction(quantile(0.5), UInt8)");
    auto quantiles_type = DataTypeFactory::instance().get("AggregateFunction(quantiles(0.9), UInt8)");
    auto variant_type = std::make_shared<DataTypeVariant>(DataTypes{quantile_type, quantiles_type});

    Block first = makeBlock(makeConstantVariantOfAggregateState(*variant_type, 0), variant_type);
    Block second = makeBlock(makeConstantVariantOfAggregateState(*variant_type, 1), variant_type);

    EXPECT_FALSE(blocksHaveEqualStructure(first, second));
    EXPECT_FALSE(blocksHaveEqualStructure(second, first));

    /// The same alternative on both sides is still the same constant.
    EXPECT_TRUE(blocksHaveEqualStructure(first, makeBlock(makeConstantVariantOfAggregateState(*variant_type, 0), variant_type)));
    EXPECT_TRUE(blocksHaveEqualStructure(second, makeBlock(makeConstantVariantOfAggregateState(*variant_type, 1), variant_type)));
}
