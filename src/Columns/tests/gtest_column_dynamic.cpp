#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(ColumnDynamic, CreateEmpty)
{
    auto column = ColumnDynamic::create(254);
    ASSERT_TRUE(column->empty());
    ASSERT_EQ(column->getVariantInfo().variant_type->getName(), "Variant(SharedVariant)");
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 1);
    ASSERT_EQ(column->getVariantInfo().variant_names[0], "SharedVariant");
    ASSERT_EQ(column->getVariantInfo().variant_name_to_discriminator.size(), 1);
    ASSERT_EQ(column->getVariantInfo().variant_name_to_discriminator.at("SharedVariant"), 0);
    ASSERT_TRUE(column->getVariantColumn().getVariantByGlobalDiscriminator(0).empty());
}

TEST(ColumnDynamic, InsertDefault)
{
    auto column = ColumnDynamic::create(254);
    column->insertDefault();
    ASSERT_TRUE(column->size() == 1);
    ASSERT_EQ(column->getVariantInfo().variant_type->getName(), "Variant(SharedVariant)");
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 1);
    ASSERT_EQ(column->getVariantInfo().variant_names[0], "SharedVariant");
    ASSERT_EQ(column->getVariantInfo().variant_name_to_discriminator.size(), 1);
    ASSERT_EQ(column->getVariantInfo().variant_name_to_discriminator.at("SharedVariant"), 0);
    ASSERT_TRUE(column->getVariantColumn().getVariantByGlobalDiscriminator(0).empty());
    ASSERT_TRUE(column->isNullAt(0));
    ASSERT_EQ((*column)[0], Field(Null()));
}

TEST(ColumnDynamic, InsertFields)
{
    auto column = ColumnDynamic::create(254);
    column->insert(Field(42));
    column->insert(Field(-42));
    column->insert(Field("str1"));
    column->insert(Field(Null()));
    column->insert(Field(42.42));
    column->insert(Field(43));
    column->insert(Field(-43));
    column->insert(Field("str2"));
    column->insert(Field(Null()));
    column->insert(Field(43.43));
    ASSERT_TRUE(column->size() == 10);

    ASSERT_EQ(column->getVariantInfo().variant_type->getName(), "Variant(Float64, Int8, SharedVariant, String)");
    std::vector<String> expected_names = {"Float64", "Int8", "SharedVariant", "String"};
    ASSERT_EQ(column->getVariantInfo().variant_names, expected_names);
    std::unordered_map<String, UInt8> expected_variant_name_to_discriminator = {{"Float64", 0}, {"Int8", 1}, {"SharedVariant", 2}, {"String", 3}};
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
}

ColumnDynamic::MutablePtr getDynamicWithManyVariants(size_t num_variants, Field tuple_element = Field(42))
{
    auto column = ColumnDynamic::create(254);
    for (size_t i = 0; i != num_variants; ++i)
    {
        Tuple tuple;
        for (size_t j = 0; j != i + 1; ++j)
            tuple.push_back(tuple_element);
        column->insert(tuple);
    }

    return column;
}

TEST(ColumnDynamic, InsertFieldsOverflow1)
{
    auto column = getDynamicWithManyVariants(253);

    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 254);

    column->insert(Field(42.42));
    ASSERT_EQ(column->size(), 254);
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator.contains("Float64"));

    column->insert(Field(42));
    ASSERT_EQ(column->size(), 255);
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_EQ(column->getSharedVariant().size(), 1);
    Field field = (*column)[column->size() - 1];
    ASSERT_EQ(field, 42);

    column->insert(Field(43));
    ASSERT_EQ(column->size(), 256);
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_EQ(column->getSharedVariant().size(), 2);
    field = (*column)[column->size() - 1];
    ASSERT_EQ(field, 43);

    column->insert(Field("str1"));
    ASSERT_EQ(column->size(), 257);
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column->getSharedVariant().size(), 3);
    field = (*column)[column->size() - 1];
    ASSERT_EQ(field, "str1");

    column->insert(Field(Array({Field(42), Field(43)})));
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("Array(Int8)"));
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column->getSharedVariant().size(), 4);
    field = (*column)[column->size() - 1];
    ASSERT_EQ(field, Field(Array({Field(42), Field(43)})));
}

TEST(ColumnDynamic, InsertFieldsOverflow2)
{
    auto column = getDynamicWithManyVariants(254);
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);

    column->insert(Field("str1"));
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column->getSharedVariant().size(), 1);
    Field field = (*column)[column->size() - 1];
    ASSERT_EQ(field, "str1");

    column->insert(Field(42));
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column->getSharedVariant().size(), 2);
    field = (*column)[column->size() - 1];
    ASSERT_EQ(field, 42);
}

ColumnDynamic::MutablePtr getInsertFromColumn(size_t num = 1)
{
    auto column_from = ColumnDynamic::create(254);
    for (size_t i = 0; i != num; ++i)
    {
        column_from->insert(Field(42));
        column_from->insert(Field(42.42));
        column_from->insert(Field("str"));
    }
    return column_from;
}

void checkInsertFrom(const ColumnDynamic::MutablePtr & column_from, ColumnDynamic::MutablePtr & column_to, const std::string & expected_variant, const std::vector<String> & expected_names, const std::unordered_map<String, UInt8> & expected_variant_name_to_discriminator)
{
    column_to->insertFrom(*column_from, 0);
    ASSERT_EQ(column_to->getVariantInfo().variant_type->getName(), expected_variant);
    ASSERT_EQ(column_to->getVariantInfo().variant_names, expected_names);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
    auto field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertFrom(*column_from, 1);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42.42);

    column_to->insertFrom(*column_from, 2);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "str");

    ASSERT_EQ(column_to->getVariantInfo().variant_type->getName(), expected_variant);
    ASSERT_EQ(column_to->getVariantInfo().variant_names, expected_names);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
}

TEST(ColumnDynamic, InsertFrom1)
{
    auto column_to = ColumnDynamic::create(254);
    checkInsertFrom(getInsertFromColumn(), column_to, "Variant(Float64, Int8, SharedVariant, String)", {"Float64", "Int8", "SharedVariant", "String"}, {{"Float64", 0}, {"Int8", 1}, {"SharedVariant", 2}, {"String", 3}});
}

TEST(ColumnDynamic, InsertFrom2)
{
    auto column_to = ColumnDynamic::create(254);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str"));

    checkInsertFrom(getInsertFromColumn(), column_to, "Variant(Float64, Int8, SharedVariant, String)", {"Float64", "Int8", "SharedVariant", "String"}, {{"Float64", 0}, {"Int8", 1}, {"SharedVariant", 2}, {"String", 3}});
}

TEST(ColumnDynamic, InsertFrom3)
{
    auto column_to = ColumnDynamic::create(254);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str"));
    column_to->insert(Array({Field(42)}));

    checkInsertFrom(getInsertFromColumn(), column_to, "Variant(Array(Int8), Float64, Int8, SharedVariant, String)", {"Array(Int8)", "Float64", "Int8", "SharedVariant", "String"}, {{"Array(Int8)", 0}, {"Float64", 1}, {"Int8", 2}, {"SharedVariant", 3}, {"String", 4}});
}

TEST(ColumnDynamic, InsertFromOverflow1)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertFrom(*column_from, 0);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    auto field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertFrom(*column_from, 1);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 1);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42.42);

    column_to->insertFrom(*column_from, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 2);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "str");
}

TEST(ColumnDynamic, InsertFromOverflow2)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertFrom(*column_from, 0);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    auto field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertFrom(*column_from, 1);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 1);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42.42);
}

TEST(ColumnDynamic, InsertFromOverflow3)
{
    auto column_from = ColumnDynamic::create(1);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));

    auto column_to = ColumnDynamic::create(254);
    column_to->insert(Field(41));

    column_to->insertFrom(*column_from, 0);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 0);
    auto field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertFrom(*column_from, 1);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 1);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42.42);
}

void checkInsertManyFrom(const ColumnDynamic::MutablePtr & column_from, ColumnDynamic::MutablePtr & column_to, const std::string & expected_variant, const std::vector<String> & expected_names, const std::unordered_map<String, UInt8> & expected_variant_name_to_discriminator)
{
    column_to->insertManyFrom(*column_from, 0, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_type->getName(), expected_variant);
    ASSERT_EQ(column_to->getVariantInfo().variant_names, expected_names);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
    auto field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertManyFrom(*column_from, 1, 2);
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42.42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42.42);

    column_to->insertManyFrom(*column_from, 2, 2);
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, "str");
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "str");

    ASSERT_EQ(column_to->getVariantInfo().variant_type->getName(), expected_variant);
    ASSERT_EQ(column_to->getVariantInfo().variant_names, expected_names);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
}

TEST(ColumnDynamic, InsertManyFrom1)
{
    auto column_to = ColumnDynamic::create(254);
    checkInsertManyFrom(getInsertFromColumn(), column_to, "Variant(Float64, Int8, SharedVariant, String)", {"Float64", "Int8", "SharedVariant", "String"}, {{"Float64", 0}, {"Int8", 1}, {"SharedVariant", 2}, {"String", 3}});
}

TEST(ColumnDynamic, InsertManyFrom2)
{
    auto column_to = ColumnDynamic::create(254);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str"));

    checkInsertManyFrom(getInsertFromColumn(), column_to, "Variant(Float64, Int8, SharedVariant, String)", {"Float64", "Int8", "SharedVariant", "String"}, {{"Float64", 0}, {"Int8", 1}, {"SharedVariant", 2}, {"String", 3}});
}

TEST(ColumnDynamic, InsertManyFrom3)
{
    auto column_to = ColumnDynamic::create(254);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str"));
    column_to->insert(Array({Field(42)}));

    checkInsertManyFrom(getInsertFromColumn(), column_to, "Variant(Array(Int8), Float64, Int8, SharedVariant, String)", {"Array(Int8)", "Float64", "Int8", "SharedVariant", "String"}, {{"Array(Int8)", 0}, {"Float64", 1}, {"Int8", 2}, {"SharedVariant", 3}, {"String", 4}});
}

TEST(ColumnDynamic, InsertManyFromOverflow1)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertManyFrom(*column_from, 0, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 0);
    auto field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertManyFrom(*column_from, 1, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 2);
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42.42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42.42);

    column_to->insertManyFrom(*column_from, 2, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 4);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "str");
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, "str");
}

TEST(ColumnDynamic, InsertManyFromOverflow2)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertManyFrom(*column_from, 0, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 0);
    auto field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertManyFrom(*column_from, 1, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 2);
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42.42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42.42);
}


TEST(ColumnDynamic, InsertManyFromOverflow3)
{
    auto column_from = ColumnDynamic::create(1);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));

    auto column_to = ColumnDynamic::create(254);
    column_to->insert(Field(41));

    column_to->insertManyFrom(*column_from, 0, 2);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 0);
    auto field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertManyFrom(*column_from, 1, 2);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 2);
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42.42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42.42);
}

void checkInsertRangeFrom(const ColumnDynamic::MutablePtr & column_from, ColumnDynamic::MutablePtr & column_to, const std::string & expected_variant, const std::vector<String> & expected_names, const std::unordered_map<String, UInt8> & expected_variant_name_to_discriminator)
{
    column_to->insertRangeFrom(*column_from, 0, 3);
    ASSERT_EQ(column_to->getVariantInfo().variant_type->getName(), expected_variant);
    ASSERT_EQ(column_to->getVariantInfo().variant_names, expected_names);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
    auto field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, 42);
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42.42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "str");

    column_to->insertRangeFrom(*column_from, 3, 3);
    field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, 42);
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42.42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "str");

    ASSERT_EQ(column_to->getVariantInfo().variant_type->getName(), expected_variant);
    ASSERT_EQ(column_to->getVariantInfo().variant_names, expected_names);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
}

TEST(ColumnDynamic, InsertRangeFrom1)
{
    auto column_to = ColumnDynamic::create(254);
    checkInsertRangeFrom(getInsertFromColumn(2), column_to, "Variant(Float64, Int8, SharedVariant, String)", {"Float64", "Int8", "SharedVariant", "String"}, {{"Float64", 0}, {"Int8", 1}, {"SharedVariant", 2}, {"String", 3}});
}

TEST(ColumnDynamic, InsertRangeFrom2)
{
    auto column_to = ColumnDynamic::create(254);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str1"));

    checkInsertRangeFrom(getInsertFromColumn(2), column_to, "Variant(Float64, Int8, SharedVariant, String)", {"Float64", "Int8", "SharedVariant", "String"}, {{"Float64", 0}, {"Int8", 1}, {"SharedVariant", 2}, {"String", 3}});
}

TEST(ColumnDynamic, InsertRangeFrom3)
{
    auto column_to = ColumnDynamic::create(254);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str1"));
    column_to->insert(Array({Field(42)}));

    checkInsertRangeFrom(getInsertFromColumn(2), column_to, "Variant(Array(Int8), Float64, Int8, SharedVariant, String)", {"Array(Int8)", "Float64", "Int8", "SharedVariant", "String"}, {{"Array(Int8)", 0}, {"Float64", 1}, {"Int8", 2}, {"SharedVariant", 3}, {"String", 4}});
}

TEST(ColumnDynamic, InsertRangeFromOverflow1)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(43));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertRangeFrom(*column_from, 0, 4);
    ASSERT_EQ(column_to->size(), 257);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 2);
    auto field = (*column_to)[column_to->size() - 4];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field(42.42));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field("str"));
}

TEST(ColumnDynamic, InsertRangeFromOverflow2)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(43));
    column_from->insert(Field(42.42));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertRangeFrom(*column_from, 0, 3);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 1);
    auto field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field(42.42));
}

TEST(ColumnDynamic, InsertRangeFromOverflow3)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(43));
    column_from->insert(Field(42.42));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insert(Field("Str"));
    column_to->insertRangeFrom(*column_from, 0, 3);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 3);
    auto field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field(42.42));
}

TEST(ColumnDynamic, InsertRangeFromOverflow4)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));

    auto column_to = getDynamicWithManyVariants(254);
    column_to->insertRangeFrom(*column_from, 0, 3);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 3);
    auto field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field(42.42));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field("str"));
}

TEST(ColumnDynamic, InsertRangeFromOverflow5)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(43));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insert(Field("str"));
    column_to->insertRangeFrom(*column_from, 0, 4);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 3);
    auto field = (*column_to)[column_to->size() - 4];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field(42.42));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field("str"));
}

TEST(ColumnDynamic, InsertRangeFromOverflow6)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(43));
    column_from->insert(Field(44));
    column_from->insert(Field(42.42));
    column_from->insert(Field(43.43));
    column_from->insert(Field("str"));
    column_from->insert(Field(Array({Field(42)})));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertRangeFrom(*column_from, 2, 5);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Array(Int8)"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 4);
    auto field = (*column_to)[column_to->size() - 5];

    ASSERT_EQ(field, Field(44));
    field = (*column_to)[column_to->size() - 4];
    ASSERT_EQ(field, Field(42.42));
    field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(43.43));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field("str"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field(Array({Field(42)})));
}

TEST(ColumnDynamic, InsertRangeFromOverflow7)
{
    auto column_from = ColumnDynamic::create(2);
    column_from->insert(Field(42.42));
    column_from->insert(Field("str1"));
    column_from->insert(Field(42));
    column_from->insert(Field(43.43));
    column_from->insert(Field(Array({Field(41)})));
    column_from->insert(Field(43));
    column_from->insert(Field("str2"));
    column_from->insert(Field(Array({Field(42)})));

    auto column_to = ColumnDynamic::create(254);
    column_to->insert(Field(42));

    column_to->insertRangeFrom(*column_from, 0, 8);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 4);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Array(Int8)"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 2);
    auto field = (*column_to)[column_to->size() - 8];
    ASSERT_EQ(field, Field(42.42));
    field = (*column_to)[column_to->size() - 7];
    ASSERT_EQ(field, Field("str1"));
    field = (*column_to)[column_to->size() - 6];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 5];
    ASSERT_EQ(field, Field(43.43));
    field = (*column_to)[column_to->size() - 4];
    ASSERT_EQ(field, Field(Array({Field(41)})));
    field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field("str2"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field(Array({Field(42)})));
}

TEST(ColumnDynamic, InsertRangeFromOverflow8)
{
    auto column_from = ColumnDynamic::create(2);
    column_from->insert(Field(42.42));
    column_from->insert(Field("str1"));
    column_from->insert(Field(42));
    column_from->insert(Field(43.43));
    column_from->insert(Field(Array({Field(41)})));
    column_from->insert(Field(43));
    column_from->insert(Field("str2"));
    column_from->insert(Field(Array({Field(42)})));

    auto column_to = ColumnDynamic::create(2);
    column_to->insert(Field(42));
    column_from->insert(Field("str1"));

    column_to->insertRangeFrom(*column_from, 0, 8);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 3);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Array(Int8)"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 4);
    auto field = (*column_to)[column_to->size() - 8];
    ASSERT_EQ(field, Field(42.42));
    field = (*column_to)[column_to->size() - 7];
    ASSERT_EQ(field, Field("str1"));
    field = (*column_to)[column_to->size() - 6];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 5];
    ASSERT_EQ(field, Field(43.43));
    field = (*column_to)[column_to->size() - 4];
    ASSERT_EQ(field, Field(Array({Field(41)})));
    field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field("str2"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field(Array({Field(42)})));
}

TEST(ColumnDynamic, InsertRangeFromOverflow9)
{
    auto column_from = ColumnDynamic::create(3);
    column_from->insert(Field("str1"));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str2"));
    column_from->insert(Field(42));
    column_from->insert(Field(43.43));
    column_from->insert(Field(Array({Field(41)})));
    column_from->insert(Field(43));
    column_from->insert(Field("str2"));
    column_from->insert(Field(Array({Field(42)})));

    auto column_to = ColumnDynamic::create(2);
    column_to->insert(Field(42));

    column_to->insertRangeFrom(*column_from, 0, 9);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 3);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Array(Int8)"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 4);
    auto field = (*column_to)[column_to->size() - 9];
    ASSERT_EQ(field, Field("str1"));
    field = (*column_to)[column_to->size() - 8];
    ASSERT_EQ(field, Field(42.42));
    field = (*column_to)[column_to->size() - 7];
    ASSERT_EQ(field, Field("str2"));
    field = (*column_to)[column_to->size() - 6];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 5];
    ASSERT_EQ(field, Field(43.43));
    field = (*column_to)[column_to->size() - 4];
    ASSERT_EQ(field, Field(Array({Field(41)})));
    field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field("str2"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field(Array({Field(42)})));
}

TEST(ColumnDynamic, SerializeDeserializeFromArena1)
{
    auto column = ColumnDynamic::create(254);
    column->insert(Field(42));
    column->insert(Field(42.42));
    column->insert(Field("str"));
    column->insert(Field(Null()));

    Arena arena;
    const char * pos = nullptr;
    auto ref1 = column->serializeValueIntoArena(0, arena, pos);
    column->serializeValueIntoArena(1, arena, pos);
    column->serializeValueIntoArena(2, arena, pos);
    column->serializeValueIntoArena(3, arena, pos);
    pos = column->deserializeAndInsertFromArena(ref1.data);
    pos = column->deserializeAndInsertFromArena(pos);
    pos = column->deserializeAndInsertFromArena(pos);
    column->deserializeAndInsertFromArena(pos);

    ASSERT_EQ((*column)[column->size() - 4], 42);
    ASSERT_EQ((*column)[column->size() - 3], 42.42);
    ASSERT_EQ((*column)[column->size() - 2], "str");
    ASSERT_EQ((*column)[column->size() - 1], Null());
}

TEST(ColumnDynamic, SerializeDeserializeFromArena2)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));
    column_from->insert(Field(Null()));

    Arena arena;
    const char * pos = nullptr;
    auto ref1 = column_from->serializeValueIntoArena(0, arena, pos);
    column_from->serializeValueIntoArena(1, arena, pos);
    column_from->serializeValueIntoArena(2, arena, pos);
    column_from->serializeValueIntoArena(3, arena, pos);

    auto column_to = ColumnDynamic::create(254);
    pos = column_to->deserializeAndInsertFromArena(ref1.data);
    pos = column_to->deserializeAndInsertFromArena(pos);
    pos = column_to->deserializeAndInsertFromArena(pos);
    column_to->deserializeAndInsertFromArena(pos);

    ASSERT_EQ((*column_to)[column_to->size() - 4], 42);
    ASSERT_EQ((*column_to)[column_to->size() - 3], 42.42);
    ASSERT_EQ((*column_to)[column_to->size() - 2], "str");
    ASSERT_EQ((*column_to)[column_to->size() - 1], Null());
    ASSERT_EQ(column_to->getVariantInfo().variant_type->getName(), "Variant(Float64, Int8, SharedVariant, String)");
    std::vector<String> expected_names = {"Float64", "Int8", "SharedVariant", "String"};
    ASSERT_EQ(column_to->getVariantInfo().variant_names, expected_names);
    std::unordered_map<String, UInt8> expected_variant_name_to_discriminator = {{"Float64", 0}, {"Int8", 1}, {"SharedVariant", 2}, {"String", 3}};
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
}

TEST(ColumnDynamic, SerializeDeserializeFromArenaOverflow1)
{
    auto column_from = ColumnDynamic::create(254);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));
    column_from->insert(Field(Null()));

    Arena arena;
    const char * pos = nullptr;
    auto ref1 = column_from->serializeValueIntoArena(0, arena, pos);
    column_from->serializeValueIntoArena(1, arena, pos);
    column_from->serializeValueIntoArena(2, arena, pos);
    column_from->serializeValueIntoArena(3, arena, pos);

    auto column_to = getDynamicWithManyVariants(253);
    pos = column_to->deserializeAndInsertFromArena(ref1.data);
    pos = column_to->deserializeAndInsertFromArena(pos);
    pos = column_to->deserializeAndInsertFromArena(pos);
    column_to->deserializeAndInsertFromArena(pos);

    ASSERT_EQ((*column_to)[column_to->size() - 4], 42);
    ASSERT_EQ((*column_to)[column_to->size() - 3], 42.42);
    ASSERT_EQ((*column_to)[column_to->size() - 2], "str");
    ASSERT_EQ((*column_to)[column_to->size() - 1], Null());
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 2);
}

TEST(ColumnDynamic, SerializeDeserializeFromArenaOverflow2)
{
    auto column_from = ColumnDynamic::create(2);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));
    column_from->insert(Field(Null()));
    column_from->insert(Field(Array({Field(42)})));

    Arena arena;
    const char * pos = nullptr;
    auto ref1 = column_from->serializeValueIntoArena(0, arena, pos);
    column_from->serializeValueIntoArena(1, arena, pos);
    column_from->serializeValueIntoArena(2, arena, pos);
    column_from->serializeValueIntoArena(3, arena, pos);
    column_from->serializeValueIntoArena(4, arena, pos);

    auto column_to = ColumnDynamic::create(2);
    column_to->insert(Field(42.42));
    pos = column_to->deserializeAndInsertFromArena(ref1.data);
    pos = column_to->deserializeAndInsertFromArena(pos);
    pos = column_to->deserializeAndInsertFromArena(pos);
    pos = column_to->deserializeAndInsertFromArena(pos);
    column_to->deserializeAndInsertFromArena(pos);

    ASSERT_EQ((*column_to)[column_to->size() - 5], 42);
    ASSERT_EQ((*column_to)[column_to->size() - 4], 42.42);
    ASSERT_EQ((*column_to)[column_to->size() - 3], "str");
    ASSERT_EQ((*column_to)[column_to->size() - 2], Null());
    ASSERT_EQ((*column_to)[column_to->size() - 1], Field(Array({Field(42)})));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Array(Int8)"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 2);
}

TEST(ColumnDynamic, skipSerializedInArena)
{
    auto column_from = ColumnDynamic::create(3);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));
    column_from->insert(Field(Null()));

    Arena arena;
    const char * pos = nullptr;
    auto ref1 = column_from->serializeValueIntoArena(0, arena, pos);
    column_from->serializeValueIntoArena(1, arena, pos);
    column_from->serializeValueIntoArena(2, arena, pos);
    auto ref4 = column_from->serializeValueIntoArena(3, arena, pos);

    const char * end = ref4.data + ref4.size;
    auto column_to = ColumnDynamic::create(254);
    pos = column_to->skipSerializedInArena(ref1.data);
    pos = column_to->skipSerializedInArena(pos);
    pos = column_to->skipSerializedInArena(pos);
    pos = column_to->skipSerializedInArena(pos);

    ASSERT_EQ(pos, end);
    ASSERT_EQ(column_to->getVariantInfo().variant_name_to_discriminator.at("SharedVariant"), 0);
    ASSERT_EQ(column_to->getVariantInfo().variant_names, Names{"SharedVariant"});
}

TEST(ColumnDynamic, compare)
{
    auto column_from = ColumnDynamic::create(3);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));
    column_from->insert(Field(Null()));
    column_from->insert(Field(Array({Field(42)})));

    ASSERT_EQ(column_from->compareAt(0, 0, *column_from, -1), 0);
    ASSERT_EQ(column_from->compareAt(0, 1, *column_from, -1), 1);
    ASSERT_EQ(column_from->compareAt(1, 1, *column_from, -1), 0);
    ASSERT_EQ(column_from->compareAt(0, 2, *column_from, -1), -1);
    ASSERT_EQ(column_from->compareAt(2, 0, *column_from, -1), 1);
    ASSERT_EQ(column_from->compareAt(2, 4, *column_from, -1), 1);
    ASSERT_EQ(column_from->compareAt(4, 2, *column_from, -1), -1);
    ASSERT_EQ(column_from->compareAt(4, 4, *column_from, -1), 0);
    ASSERT_EQ(column_from->compareAt(0, 3, *column_from, -1), 1);
    ASSERT_EQ(column_from->compareAt(1, 3, *column_from, -1), 1);
    ASSERT_EQ(column_from->compareAt(2, 3, *column_from, -1), 1);
    ASSERT_EQ(column_from->compareAt(3, 3, *column_from, -1), 0);
    ASSERT_EQ(column_from->compareAt(4, 3, *column_from, -1), 1);
    ASSERT_EQ(column_from->compareAt(3, 0, *column_from, -1), -1);
    ASSERT_EQ(column_from->compareAt(3, 1, *column_from, -1), -1);
    ASSERT_EQ(column_from->compareAt(3, 2, *column_from, -1), -1);
    ASSERT_EQ(column_from->compareAt(3, 4, *column_from, -1), -1);
}
