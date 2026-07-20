#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(ColumnVariant, CreateFromEmptyColumns)
{
    MutableColumns columns;
    columns.push_back(ColumnUInt32::create());
    columns.push_back(ColumnString::create());
    auto column = ColumnVariant::create(std::move(columns));
    ASSERT_TRUE(column->empty() && column->getLocalDiscriminators().empty() && column->getOffsets().empty());
}

TEST(ColumnVariant, CreateFromEmptyColumnsWithLocalOrder)
{
    MutableColumns columns;
    columns.push_back(ColumnUInt32::create());
    columns.push_back(ColumnString::create());
    std::vector<ColumnVariant::Discriminator> local_to_global_discriminators;
    local_to_global_discriminators.push_back(1);
    local_to_global_discriminators.push_back(0);
    auto column = ColumnVariant::create(std::move(columns), local_to_global_discriminators);
    ASSERT_TRUE(column->empty() && column->getLocalDiscriminators().empty() && column->getOffsets().empty());
    ASSERT_EQ(column->localDiscriminatorByGlobal(0), 0);
    ASSERT_EQ(column->localDiscriminatorByGlobal(1), 1);
    ASSERT_EQ(column->globalDiscriminatorByLocal(0), 0);
    ASSERT_EQ(column->globalDiscriminatorByLocal(1), 1);
}

MutableColumns createColumns1()
{
    MutableColumns columns;
    auto column1 = ColumnUInt64::create();
    column1->insertValue(42);
    columns.push_back(std::move(column1));
    auto column2 = ColumnString::create();
    column2->insertData("Hello", 5);
    column2->insertData("World", 5);
    columns.push_back(std::move(column2));
    auto column3 = ColumnUInt32::create();
    columns.push_back(std::move(column3));
    return columns;
}

MutableColumnPtr createDiscriminators1()
{
    auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();
    discriminators_column->insertValue(0);
    discriminators_column->insertValue(1);
    discriminators_column->insertValue(ColumnVariant::NULL_DISCRIMINATOR);
    discriminators_column->insertValue(1);
    discriminators_column->insertValue(ColumnVariant::NULL_DISCRIMINATOR);
    return discriminators_column;
}

void reorderColumns(const std::vector<ColumnVariant::Discriminator> & local_to_global_order, MutableColumns & columns)
{
    MutableColumns res;
    for (auto global_discr : local_to_global_order)
        res.push_back(std::move(columns[global_discr]));
    columns = std::move(res);
}

template <typename Ptr>
void reorderDiscriminators(const std::vector<ColumnVariant::Discriminator> & local_to_global_order, Ptr & discriminators)
{
    std::vector<ColumnVariant::Discriminator> global_to_local_order(local_to_global_order.size());
    for (size_t i = 0; i != local_to_global_order.size(); ++i)
        global_to_local_order[local_to_global_order[i]] = i;

    auto & discriminators_data = assert_cast<ColumnVariant::ColumnDiscriminators *>(discriminators.get())->getData();
    for (auto & discr : discriminators_data)
    {
        if (discr != ColumnVariant::NULL_DISCRIMINATOR)
            discr = global_to_local_order[discr];
    }
}

MutableColumnPtr createOffsets1()
{
    auto offsets = ColumnVariant::ColumnOffsets::create();
    offsets->insertValue(0);
    offsets->insertValue(0);
    offsets->insertValue(0);
    offsets->insertValue(1);
    offsets->insertValue(0);
    return offsets;
}

std::vector<ColumnVariant::Discriminator> createLocalToGlobalOrder1()
{
    std::vector<ColumnVariant::Discriminator> local_to_global_discriminators;
    local_to_global_discriminators.push_back(1);
    local_to_global_discriminators.push_back(2);
    local_to_global_discriminators.push_back(0);
    return local_to_global_discriminators;
}

void checkColumnVariant1(ColumnVariant * column)
{
    const auto & offsets = column->getOffsets();
    ASSERT_EQ(column->size(), 5);
    ASSERT_EQ(offsets[0], 0);
    ASSERT_EQ(offsets[1], 0);
    ASSERT_EQ(offsets[3], 1);
    ASSERT_TRUE(column->isDefaultAt(2) && column->isDefaultAt(4));
    ASSERT_EQ((*column)[0].safeGet<UInt32>(), 42);
    ASSERT_EQ((*column)[1].safeGet<String>(), "Hello");
    ASSERT_TRUE((*column)[2].isNull());
    ASSERT_EQ((*column)[3].safeGet<String>(), "World");
    ASSERT_TRUE((*column)[4].isNull());
}

void checkColumnVariant1Order(ColumnVariant * column)
{
    ASSERT_EQ(column->localDiscriminatorByGlobal(0), 2);
    ASSERT_EQ(column->localDiscriminatorByGlobal(1), 0);
    ASSERT_EQ(column->localDiscriminatorByGlobal(2), 1);
    ASSERT_EQ(column->globalDiscriminatorByLocal(0), 1);
    ASSERT_EQ(column->globalDiscriminatorByLocal(1), 2);
    ASSERT_EQ(column->globalDiscriminatorByLocal(2), 0);
    ASSERT_EQ(column->localDiscriminatorAt(0), 2);
    ASSERT_EQ(column->localDiscriminatorAt(1), 0);
    ASSERT_EQ(column->localDiscriminatorAt(2), ColumnVariant::NULL_DISCRIMINATOR);
    ASSERT_EQ(column->localDiscriminatorAt(3), 0);
    ASSERT_EQ(column->localDiscriminatorAt(4), ColumnVariant::NULL_DISCRIMINATOR);
    ASSERT_EQ(column->globalDiscriminatorAt(0), 0);
    ASSERT_EQ(column->globalDiscriminatorAt(1), 1);
    ASSERT_EQ(column->globalDiscriminatorAt(2), ColumnVariant::NULL_DISCRIMINATOR);
    ASSERT_EQ(column->globalDiscriminatorAt(3), 1);
    ASSERT_EQ(column->globalDiscriminatorAt(4), ColumnVariant::NULL_DISCRIMINATOR);
}

TEST(ColumnVariant, CreateFromDiscriminatorsAndColumns)
{
    auto columns = createColumns1();
    auto discriminators = createDiscriminators1();
    auto column = ColumnVariant::create(std::move(discriminators), std::move(columns));
    checkColumnVariant1(column.get());
}

TEST(ColumnVariant, CreateFromDiscriminatorsAndColumnsWithLocalOrder)
{
    auto local_to_global_order = createLocalToGlobalOrder1();
    auto columns = createColumns1();
    reorderColumns(local_to_global_order, columns);
    auto discriminators = createDiscriminators1();
    reorderDiscriminators(local_to_global_order, discriminators);
    auto column = ColumnVariant::create(std::move(discriminators), std::move(columns), createLocalToGlobalOrder1());
    checkColumnVariant1(column.get());
    checkColumnVariant1Order(column.get());
}

TEST(ColumnVariant, CreateFromDiscriminatorsOffsetsAndColumns)
{
    auto columns = createColumns1();
    auto discriminators = createDiscriminators1();
    auto offsets = createOffsets1();
    auto column = ColumnVariant::create(std::move(discriminators), std::move(offsets), std::move(columns));
    checkColumnVariant1(column.get());
}

TEST(ColumnVariant, CreateFromDiscriminatorsOffsetsAndColumnsWithLocalOrder)
{
    auto local_to_global_order = createLocalToGlobalOrder1();
    auto columns = createColumns1();
    reorderColumns(local_to_global_order, columns);
    auto discriminators = createDiscriminators1();
    reorderDiscriminators(local_to_global_order, discriminators);
    auto offsets = createOffsets1();
    auto column = ColumnVariant::create(std::move(discriminators), std::move(offsets), std::move(columns), createLocalToGlobalOrder1());
    checkColumnVariant1(column.get());
    checkColumnVariant1Order(column.get());
}

ColumnVariant::MutablePtr createVariantWithOneFullColumNoNulls(size_t size, bool change_order)
{
    MutableColumns columns;
    auto column1 = ColumnUInt64::create();
    for (size_t i = 0; i != size; ++i)
        column1->insertValue(i);
    columns.push_back(std::move(column1));
    auto column2 = ColumnString::create();
    columns.push_back(std::move(column2));
    auto column3 = ColumnUInt32::create();
    columns.push_back(std::move(column3));
    auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();
    for (size_t i = 0; i != size; ++i)
        discriminators_column->insertValue(0);
    if (change_order)
    {
        auto local_to_global_order = createLocalToGlobalOrder1();
        reorderColumns(local_to_global_order, columns);
        reorderDiscriminators(local_to_global_order, discriminators_column);
        return ColumnVariant::create(std::move(discriminators_column), std::move(columns), createLocalToGlobalOrder1());
    }
    return ColumnVariant::create(std::move(discriminators_column), std::move(columns));
}

TEST(ColumnVariant, CreateFromDiscriminatorsAndOneFullColumnNoNulls)
{
    auto column = createVariantWithOneFullColumNoNulls(3, false);
    const auto & offsets = column->getOffsets();
    ASSERT_EQ(column->size(), 3);
    ASSERT_EQ(offsets[0], 0);
    ASSERT_EQ(offsets[1], 1);
    ASSERT_EQ(offsets[2], 2);
    ASSERT_EQ((*column)[0].safeGet<UInt64>(), 0);
    ASSERT_EQ((*column)[1].safeGet<UInt64>(), 1);
    ASSERT_EQ((*column)[2].safeGet<UInt64>(), 2);
}

TEST(ColumnVariant, CreateFromDiscriminatorsAndOneFullColumnNoNullsWithLocalOrder)
{
    auto column = createVariantWithOneFullColumNoNulls(3, true);
    const auto & offsets = column->getOffsets();
    ASSERT_EQ(column->size(), 3);
    ASSERT_EQ(offsets[0], 0);
    ASSERT_EQ(offsets[1], 1);
    ASSERT_EQ(offsets[2], 2);
    ASSERT_EQ((*column)[0].safeGet<UInt64>(), 0);
    ASSERT_EQ((*column)[1].safeGet<UInt64>(), 1);
    ASSERT_EQ((*column)[2].safeGet<UInt64>(), 2);
    ASSERT_EQ(column->localDiscriminatorAt(0), 2);
    ASSERT_EQ(column->localDiscriminatorAt(1), 2);
    ASSERT_EQ(column->localDiscriminatorAt(2), 2);
    ASSERT_EQ(column->globalDiscriminatorAt(0), 0);
    ASSERT_EQ(column->globalDiscriminatorAt(0), 0);
    ASSERT_EQ(column->globalDiscriminatorAt(0), 0);
}

TEST(ColumnVariant, CloneResizedToEmpty)
{
    auto column = ColumnVariant::create(createDiscriminators1(), createOffsets1(), createColumns1());
    auto resized_column = column->cloneResized(0);
    ASSERT_TRUE(resized_column->empty());
}

TEST(ColumnVariant, CloneResizedToLarge)
{
    auto column = ColumnVariant::create(createDiscriminators1(), createOffsets1(), createColumns1());
    auto resized_column = column->cloneResized(7);
    const auto * resized_column_variant = assert_cast<const ColumnVariant *>(resized_column.get());
    ASSERT_EQ(resized_column_variant->size(), 7);
    const auto & offsets = resized_column_variant->getOffsets();
    for (size_t i = 0; i != 7; ++i)
    {
        if (i == 3)
            ASSERT_EQ(offsets[i], 1);
        else
            ASSERT_EQ(offsets[i], 0);
    }

    const auto & discriminators = resized_column_variant->getLocalDiscriminators();
    std::vector<size_t> null_indexes = {2, 4, 5, 6};
    for (size_t i : null_indexes)
        ASSERT_EQ(discriminators[i], ColumnVariant::NULL_DISCRIMINATOR);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(0).size(), 1);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(1).size(), 2);
}

TEST(ColumnVariant, CloneResizedWithOneFullColumnNoNulls)
{
    auto column = createVariantWithOneFullColumNoNulls(5, false);
    auto resized_column = column->cloneResized(3);
    const auto * resized_column_variant = assert_cast<const ColumnVariant *>(resized_column.get());
    ASSERT_EQ(resized_column_variant->size(), 3);
    const auto & offsets = resized_column_variant->getOffsets();
    for (size_t i = 0; i != 3; ++i)
        ASSERT_EQ(offsets[i], i);
    const auto & discriminators = resized_column_variant->getLocalDiscriminators();
    for (size_t i = 0; i != 3; ++i)
        ASSERT_EQ(discriminators[i], 0);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(0).size(), 3);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(1).size(), 0);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(2).size(), 0);
}

MutableColumns createColumns2()
{
    MutableColumns columns;
    auto column1 = ColumnUInt64::create();
    column1->insertValue(42);
    column1->insertValue(43);
    column1->insertValue(44);
    columns.push_back(std::move(column1));
    auto column2 = ColumnString::create();
    column2->insertData("Hello", 5);
    column2->insertData("World", 5);
    columns.push_back(std::move(column2));
    auto column3 = ColumnUInt8::create();
    columns.push_back(std::move(column3));
    return columns;
}

TEST(ColumnVariant, CloneResizedGeneral1)
{
    ///   D     c1    c2    c3
    ///   0     42   Hello
    ///   1     43   World
    ///  NULL   44
    ///   0
    ///   1
    ///  NULL
    ///   0
    auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();
    discriminators_column->insertValue(0);
    discriminators_column->insertValue(1);
    discriminators_column->insertValue(ColumnVariant::NULL_DISCRIMINATOR);
    discriminators_column->insertValue(0);
    discriminators_column->insertValue(1);
    discriminators_column->insertValue(ColumnVariant::NULL_DISCRIMINATOR);
    discriminators_column->insertValue(0);
    auto column = ColumnVariant::create(std::move(discriminators_column), createColumns2());
    auto resized_column = column->cloneResized(4);
    const auto * resized_column_variant = assert_cast<const ColumnVariant *>(resized_column.get());
    ASSERT_EQ(resized_column_variant->size(), 4);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(0).size(), 2);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(1).size(), 1);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(2).size(), 0);
    const auto & discriminators = resized_column_variant->getLocalDiscriminators();
    ASSERT_EQ(discriminators[0], 0);
    ASSERT_EQ(discriminators[1], 1);
    ASSERT_EQ(discriminators[2], ColumnVariant::NULL_DISCRIMINATOR);
    ASSERT_EQ(discriminators[3], 0);
    const auto & offsets = resized_column_variant->getOffsets();
    ASSERT_EQ(offsets[0], 0);
    ASSERT_EQ(offsets[1], 0);
    ASSERT_EQ(offsets[3], 1);
    ASSERT_EQ((*resized_column_variant)[0].safeGet<UInt64>(), 42);
    ASSERT_EQ((*resized_column_variant)[1].safeGet<String>(), "Hello");
    ASSERT_EQ((*resized_column_variant)[3].safeGet<UInt64>(), 43);
}

TEST(ColumnVariant, CloneResizedGeneral2)
{
    ///   D     c1    c2    c3
    ///   0     42   Hello
    ///  NULL   43   World
    ///  NULL   44
    ///   0
    ///   1
    ///   1
    ///   0
    auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();
    discriminators_column->insertValue(0);
    discriminators_column->insertValue(ColumnVariant::NULL_DISCRIMINATOR);
    discriminators_column->insertValue(ColumnVariant::NULL_DISCRIMINATOR);
    discriminators_column->insertValue(0);
    discriminators_column->insertValue(1);
    discriminators_column->insertValue(1);
    discriminators_column->insertValue(0);
    auto column = ColumnVariant::create(std::move(discriminators_column), createColumns2());
    auto resized_column = column->cloneResized(3);
    const auto * resized_column_variant = assert_cast<const ColumnVariant *>(resized_column.get());
    ASSERT_EQ(resized_column_variant->size(), 3);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(0).size(), 1);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(1).size(), 0);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(2).size(), 0);
    const auto & discriminators = resized_column_variant->getLocalDiscriminators();
    ASSERT_EQ(discriminators[0], 0);
    ASSERT_EQ(discriminators[1], ColumnVariant::NULL_DISCRIMINATOR);
    ASSERT_EQ(discriminators[2], ColumnVariant::NULL_DISCRIMINATOR);
    const auto & offsets = resized_column_variant->getOffsets();
    ASSERT_EQ(offsets[0], 0);
    ASSERT_EQ((*resized_column_variant)[0].safeGet<UInt64>(), 42);
}

TEST(ColumnVariant, CloneResizedGeneral3)
{
    ///   D     c1    c2    c3
    ///   0     42   Hello
    ///   1     43   World
    ///   1     44
    ///   0
    ///  NULL
    ///  NULL
    ///   0
    auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();
    discriminators_column->insertValue(0);
    discriminators_column->insertValue(1);
    discriminators_column->insertValue(1);
    discriminators_column->insertValue(0);
    discriminators_column->insertValue(ColumnVariant::NULL_DISCRIMINATOR);
    discriminators_column->insertValue(ColumnVariant::NULL_DISCRIMINATOR);
    discriminators_column->insertValue(0);
    auto column = ColumnVariant::create(std::move(discriminators_column), createColumns2());
    auto resized_column = column->cloneResized(5);
    const auto * resized_column_variant = assert_cast<const ColumnVariant *>(resized_column.get());
    ASSERT_EQ(resized_column_variant->size(), 5);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(0).size(), 2);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(1).size(), 2);
    ASSERT_EQ(resized_column_variant->getVariantByLocalDiscriminator(2).size(), 0);
    const auto & discriminators = resized_column_variant->getLocalDiscriminators();
    ASSERT_EQ(discriminators[0], 0);
    ASSERT_EQ(discriminators[1], 1);
    ASSERT_EQ(discriminators[2], 1);
    ASSERT_EQ(discriminators[3], 0);
    const auto & offsets = resized_column_variant->getOffsets();
    ASSERT_EQ(offsets[0], 0);
    ASSERT_EQ(offsets[1], 0);
    ASSERT_EQ(offsets[2], 1);
    ASSERT_EQ(offsets[3], 1);
    ASSERT_EQ((*resized_column_variant)[0].safeGet<UInt64>(), 42);
    ASSERT_EQ((*resized_column_variant)[1].safeGet<String>(), "Hello");
    ASSERT_EQ((*resized_column_variant)[2].safeGet<String>(), "World");
    ASSERT_EQ((*resized_column_variant)[3].safeGet<UInt64>(), 43);
}

MutableColumnPtr createDiscriminators2()
{
    auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();
    discriminators_column->insertValue(0);
    discriminators_column->insertValue(1);
    discriminators_column->insertValue(ColumnVariant::NULL_DISCRIMINATOR);
    discriminators_column->insertValue(0);
    discriminators_column->insertValue(1);
    discriminators_column->insertValue(ColumnVariant::NULL_DISCRIMINATOR);
    discriminators_column->insertValue(0);
    return discriminators_column;
}

std::vector<ColumnVariant::Discriminator> createLocalToGlobalOrder2()
{
    std::vector<ColumnVariant::Discriminator> local_to_global_discriminators;
    local_to_global_discriminators.push_back(2);
    local_to_global_discriminators.push_back(0);
    local_to_global_discriminators.push_back(1);
    return local_to_global_discriminators;
}

ColumnVariant::MutablePtr createVariantColumn1(bool reorder)
{
    auto columns = createColumns1();
    auto discriminators = createDiscriminators1();
    if (!reorder)
        return ColumnVariant::create(std::move(discriminators), std::move(columns));
    auto local_to_global_order = createLocalToGlobalOrder1();
    reorderColumns(local_to_global_order, columns);
    reorderDiscriminators(local_to_global_order, discriminators);
    return ColumnVariant::create(std::move(discriminators), std::move(columns), local_to_global_order);
}

ColumnVariant::MutablePtr createVariantColumn2(bool reorder)
{
    auto columns = createColumns2();
    auto discriminators = createDiscriminators2();
    if (!reorder)
        return ColumnVariant::create(std::move(discriminators), std::move(columns));
    auto local_to_global_order = createLocalToGlobalOrder2();
    reorderColumns(local_to_global_order, columns);
    reorderDiscriminators(local_to_global_order, discriminators);
    return ColumnVariant::create(std::move(discriminators), std::move(columns), local_to_global_order);
}

TEST(ColumnVariant, InsertFrom)
{
    for (bool change_order : {false, true})
    {
        auto column_to = createVariantColumn1(change_order);
        auto column_from = createVariantColumn2(change_order);
        column_to->insertFrom(*column_from, 3);
        ASSERT_EQ(column_to->globalDiscriminatorAt(5), 0);
        ASSERT_EQ((*column_to)[5].safeGet<UInt64>(), 43);
    }
}

TEST(ColumnVariant, InsertRangeFromOneColumnNoNulls)
{
    for (bool change_order : {false, true})
    {
        auto column_to = createVariantColumn2(change_order);
        auto column_from = createVariantWithOneFullColumNoNulls(5, change_order);
        column_to->insertRangeFrom(*column_from, 2, 2);
        ASSERT_EQ(column_to->globalDiscriminatorAt(7), 0);
        ASSERT_EQ(column_to->globalDiscriminatorAt(8), 0);
        ASSERT_EQ((*column_to)[7].safeGet<UInt64>(), 2);
        ASSERT_EQ((*column_to)[8].safeGet<UInt64>(), 3);
    }
}

TEST(ColumnVariant, InsertRangeFromGeneral)
{
    for (bool change_order : {false, true})
    {
        auto column_to = createVariantColumn1(change_order);
        auto column_from = createVariantColumn2(change_order);
        column_to->insertRangeFrom(*column_from, 1, 4);
        ASSERT_EQ(column_to->globalDiscriminatorAt(5), 1);
        ASSERT_EQ(column_to->globalDiscriminatorAt(6), ColumnVariant::NULL_DISCRIMINATOR);
        ASSERT_EQ(column_to->globalDiscriminatorAt(7), 0);
        ASSERT_EQ(column_to->globalDiscriminatorAt(8), 1);
        ASSERT_EQ((*column_to)[5].safeGet<String>(), "Hello");
        ASSERT_EQ((*column_to)[7].safeGet<UInt64>(), 43);
        ASSERT_EQ((*column_to)[8].safeGet<String>(), "World");
    }
}

TEST(ColumnVariant, InsertManyFrom)
{
    for (bool change_order : {false, true})
    {
        auto column_to = createVariantColumn1(change_order);
        auto column_from = createVariantColumn2(change_order);
        column_to->insertManyFrom(*column_from, 3, 2);
        ASSERT_EQ(column_to->globalDiscriminatorAt(5), 0);
        ASSERT_EQ(column_to->globalDiscriminatorAt(6), 0);
        ASSERT_EQ((*column_to)[5].safeGet<UInt64>(), 43);
        ASSERT_EQ((*column_to)[6].safeGet<UInt64>(), 43);
    }
}

TEST(ColumnVariant, PopBackOneColumnNoNulls)
{
    auto column = createVariantWithOneFullColumNoNulls(5, false);
    column->popBack(3);
    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ(column->getVariantByLocalDiscriminator(0).size(), 2);
    ASSERT_EQ((*column)[0].safeGet<UInt64>(), 0);
    ASSERT_EQ((*column)[1].safeGet<UInt64>(), 1);
}

TEST(ColumnVariant, PopBackGeneral)
{
    auto column = ColumnVariant::create(createDiscriminators2(), createColumns2());
    column->popBack(4);
    ASSERT_EQ(column->size(), 3);
    ASSERT_EQ(column->getVariantByLocalDiscriminator(0).size(), 1);
    ASSERT_EQ(column->getVariantByLocalDiscriminator(1).size(), 1);
    ASSERT_EQ((*column)[0].safeGet<UInt64>(), 42);
    ASSERT_EQ((*column)[1].safeGet<String>(), "Hello");
    ASSERT_TRUE((*column)[2].isNull());
}

TEST(ColumnVariant, FilterOneColumnNoNulls)
{
    auto column = createVariantWithOneFullColumNoNulls(3, false);
    IColumn::Filter filter;
    filter.push_back(1);
    filter.push_back(0);
    filter.push_back(1);
    auto filtered_column = column->filter(filter, -1);
    ASSERT_EQ(filtered_column->size(), 2);
    ASSERT_EQ((*filtered_column)[0].safeGet<UInt64>(), 0);
    ASSERT_EQ((*filtered_column)[1].safeGet<UInt64>(), 2);
}

TEST(ColumnVariant, FilterGeneral)
{
    auto column = ColumnVariant::create(createDiscriminators2(), createColumns2());
    IColumn::Filter filter;
    filter.push_back(0);
    filter.push_back(1);
    filter.push_back(1);
    filter.push_back(0);
    filter.push_back(0);
    filter.push_back(1);
    filter.push_back(0);
    auto filtered_column = column->filter(filter, -1);
    ASSERT_EQ(filtered_column->size(), 3);
    ASSERT_EQ((*filtered_column)[0].safeGet<String>(), "Hello");
    ASSERT_TRUE((*filtered_column)[1].isNull());
    ASSERT_TRUE((*filtered_column)[2].isNull());
}

TEST(ColumnVariant, PermuteAndIndexOneColumnNoNulls)
{
    auto column = createVariantWithOneFullColumNoNulls(4, false);
    IColumn::Permutation permutation;
    permutation.push_back(1);
    permutation.push_back(3);
    permutation.push_back(2);
    permutation.push_back(0);
    auto permuted_column = column->permute(permutation, 3);
    ASSERT_EQ(permuted_column->size(), 3);
    ASSERT_EQ((*permuted_column)[0].safeGet<UInt64>(), 1);
    ASSERT_EQ((*permuted_column)[1].safeGet<UInt64>(), 3);
    ASSERT_EQ((*permuted_column)[2].safeGet<UInt64>(), 2);

    auto index = ColumnUInt64::create();
    index->getData().push_back(1);
    index->getData().push_back(3);
    index->getData().push_back(2);
    index->getData().push_back(0);
    auto indexed_column = column->index(*index, 3);
    ASSERT_EQ(indexed_column->size(), 3);
    ASSERT_EQ((*indexed_column)[0].safeGet<UInt64>(), 1);
    ASSERT_EQ((*indexed_column)[1].safeGet<UInt64>(), 3);
    ASSERT_EQ((*indexed_column)[2].safeGet<UInt64>(), 2);
}

TEST(ColumnVariant, PermuteGeneral)
{
    auto column = ColumnVariant::create(createDiscriminators2(), createColumns2());
    IColumn::Permutation permutation;
    permutation.push_back(3);
    permutation.push_back(4);
    permutation.push_back(1);
    permutation.push_back(5);
    auto permuted_column = column->permute(permutation, 4);
    ASSERT_EQ(permuted_column->size(), 4);
    ASSERT_EQ((*permuted_column)[0].safeGet<UInt64>(), 43);
    ASSERT_EQ((*permuted_column)[1].safeGet<String>(), "World");
    ASSERT_EQ((*permuted_column)[2].safeGet<String>(), "Hello");
    ASSERT_TRUE((*permuted_column)[3].isNull());
}

TEST(ColumnVariant, ReplicateOneColumnNoNull)
{
    auto column = createVariantWithOneFullColumNoNulls(3, false);
    IColumn::Offsets offsets;
    offsets.push_back(0);
    offsets.push_back(3);
    offsets.push_back(6);
    auto replicated_column = column->replicate(offsets);
    ASSERT_EQ(replicated_column->size(), 6);
    ASSERT_EQ((*replicated_column)[0].safeGet<UInt64>(), 1);
    ASSERT_EQ((*replicated_column)[1].safeGet<UInt64>(), 1);
    ASSERT_EQ((*replicated_column)[2].safeGet<UInt64>(), 1);
    ASSERT_EQ((*replicated_column)[3].safeGet<UInt64>(), 2);
    ASSERT_EQ((*replicated_column)[4].safeGet<UInt64>(), 2);
    ASSERT_EQ((*replicated_column)[5].safeGet<UInt64>(), 2);
}

TEST(ColumnVariant, ReplicateGeneral)
{
    auto column = ColumnVariant::create(createDiscriminators1(), createColumns1());
    IColumn::Offsets offsets;
    offsets.push_back(1);
    offsets.push_back(3);
    offsets.push_back(5);
    offsets.push_back(5);
    offsets.push_back(7);
    auto replicated_column = column->replicate(offsets);
    ASSERT_EQ(replicated_column->size(), 7);
    ASSERT_EQ((*replicated_column)[0].safeGet<UInt64>(), 42);
    ASSERT_EQ((*replicated_column)[1].safeGet<String>(), "Hello");
    ASSERT_EQ((*replicated_column)[2].safeGet<String>(), "Hello");
    ASSERT_TRUE((*replicated_column)[3].isNull());
    ASSERT_TRUE((*replicated_column)[4].isNull());
    ASSERT_TRUE((*replicated_column)[5].isNull());
    ASSERT_TRUE((*replicated_column)[6].isNull());
}

TEST(ColumnVariant, ScatterOneColumnNoNulls)
{
    auto column = createVariantWithOneFullColumNoNulls(5, false);
    IColumn::Selector selector;
    selector.push_back(0);
    selector.push_back(1);
    selector.push_back(2);
    selector.push_back(0);
    selector.push_back(1);
    auto columns = column->scatter(3, selector);
    ASSERT_EQ(columns[0]->size(), 2);
    ASSERT_EQ((*columns[0])[0].safeGet<UInt64>(), 0);
    ASSERT_EQ((*columns[0])[1].safeGet<UInt64>(), 3);
    ASSERT_EQ(columns[1]->size(), 2);
    ASSERT_EQ((*columns[1])[0].safeGet<UInt64>(), 1);
    ASSERT_EQ((*columns[1])[1].safeGet<UInt64>(), 4);
    ASSERT_EQ(columns[2]->size(), 1);
    ASSERT_EQ((*columns[2])[0].safeGet<UInt64>(), 2);
}

TEST(ColumnVariant, ScatterGeneral)
{
    auto column = ColumnVariant::create(createDiscriminators2(), createColumns2());
    IColumn::Selector selector;
    selector.push_back(0);
    selector.push_back(0);
    selector.push_back(2);
    selector.push_back(0);
    selector.push_back(1);
    selector.push_back(2);
    selector.push_back(1);

    auto columns = column->scatter(3, selector);
    ASSERT_EQ(columns[0]->size(), 3);
    ASSERT_EQ((*columns[0])[0].safeGet<UInt64>(), 42);
    ASSERT_EQ((*columns[0])[1].safeGet<String>(), "Hello");
    ASSERT_EQ((*columns[0])[2].safeGet<UInt64>(), 43);
    ASSERT_EQ(columns[1]->size(), 2);
    ASSERT_EQ((*columns[1])[0].safeGet<String>(), "World");
    ASSERT_EQ((*columns[1])[1].safeGet<UInt64>(), 44);
    ASSERT_EQ(columns[2]->size(), 2);
    ASSERT_TRUE((*columns[2])[0].isNull());
    ASSERT_TRUE((*columns[2])[1].isNull());
}
