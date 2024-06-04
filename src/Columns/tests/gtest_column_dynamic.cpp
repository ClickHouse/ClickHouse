#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(ColumnDynamic, CreateEmpty)
{
    auto column = ColumnDynamic::create(255);
    ASSERT_TRUE(column->empty());
    ASSERT_EQ(column->getVariantInfo().variant_type->getName(), "Variant()");
    ASSERT_TRUE(column->getVariantInfo().variant_names.empty());
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator.empty());
}

TEST(ColumnDynamic, InsertDefault)
{
    auto column = ColumnDynamic::create(255);
    column->insertDefault();
    ASSERT_TRUE(column->size() == 1);
    ASSERT_EQ(column->getVariantInfo().variant_type->getName(), "Variant()");
    ASSERT_TRUE(column->getVariantInfo().variant_names.empty());
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator.empty());
    ASSERT_TRUE(column->isNullAt(0));
    ASSERT_EQ((*column)[0], Field(Null()));
}

TEST(ColumnDynamic, InsertFields)
{
    auto column = ColumnDynamic::create(255);
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

    ASSERT_EQ(column->getVariantInfo().variant_type->getName(), "Variant(Float64, Int8, String)");
    std::vector<String> expected_names = {"Float64", "Int8", "String"};
    ASSERT_EQ(column->getVariantInfo().variant_names, expected_names);
    std::unordered_map<String, UInt8> expected_variant_name_to_discriminator = {{"Float64", 0}, {"Int8", 1}, {"String", 2}};
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
}

ColumnDynamic::MutablePtr getDynamicWithManyVariants(size_t num_variants, Field tuple_element = Field(42))
{
    auto column = ColumnDynamic::create(255);
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

    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 253);

    column->insert(Field(42.42));
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 254);
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator.contains("Float64"));

    column->insert(Field(42));
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator.contains("String"));
    Field field = (*column)[column->size() - 1];
    ASSERT_EQ(field, "42");

    column->insert(Field(43));
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator.contains("String"));
    field = (*column)[column->size() - 1];
    ASSERT_EQ(field, "43");

    column->insert(Field("str1"));
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator.contains("String"));
    field = (*column)[column->size() - 1];
    ASSERT_EQ(field, "str1");

    column->insert(Field(Array({Field(42), Field(43)})));
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("Array(Int8)"));
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator.contains("String"));
    field = (*column)[column->size() - 1];
    ASSERT_EQ(field, "[42, 43]");
}

TEST(ColumnDynamic, InsertFieldsOverflow2)
{
    auto column = getDynamicWithManyVariants(254);
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 254);

    column->insert(Field("str1"));
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator.contains("String"));

    column->insert(Field(42));
    ASSERT_EQ(column->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator.contains("String"));
    Field field = (*column)[column->size() - 1];
    ASSERT_EQ(field, "42");
}

ColumnDynamic::MutablePtr getInsertFromColumn(size_t num = 1)
{
    auto column_from = ColumnDynamic::create(255);
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
    auto column_to = ColumnDynamic::create(255);
    checkInsertFrom(getInsertFromColumn(), column_to, "Variant(Float64, Int8, String)", {"Float64", "Int8", "String"}, {{"Float64", 0}, {"Int8", 1}, {"String", 2}});
}

TEST(ColumnDynamic, InsertFrom2)
{
    auto column_to = ColumnDynamic::create(255);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str"));

    checkInsertFrom(getInsertFromColumn(), column_to, "Variant(Float64, Int8, String)", {"Float64", "Int8", "String"}, {{"Float64", 0}, {"Int8", 1}, {"String", 2}});
}

TEST(ColumnDynamic, InsertFrom3)
{
    auto column_to = ColumnDynamic::create(255);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str"));
    column_to->insert(Array({Field(42)}));

    checkInsertFrom(getInsertFromColumn(), column_to, "Variant(Array(Int8), Float64, Int8, String)", {"Array(Int8)", "Float64", "Int8", "String"}, {{"Array(Int8)", 0}, {"Float64", 1}, {"Int8", 2}, {"String", 3}});
}

TEST(ColumnDynamic, InsertFromOverflow1)
{
    auto column_from = ColumnDynamic::create(255);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertFrom(*column_from, 0);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 254);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    auto field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertFrom(*column_from, 1);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "42.42");

    column_to->insertFrom(*column_from, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "str");
}

TEST(ColumnDynamic, InsertFromOverflow2)
{
    auto column_from = ColumnDynamic::create(255);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertFrom(*column_from, 0);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    auto field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertFrom(*column_from, 1);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "42.42");
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
    auto column_to = ColumnDynamic::create(255);
    checkInsertManyFrom(getInsertFromColumn(), column_to, "Variant(Float64, Int8, String)", {"Float64", "Int8", "String"}, {{"Float64", 0}, {"Int8", 1}, {"String", 2}});
}

TEST(ColumnDynamic, InsertManyFrom2)
{
    auto column_to = ColumnDynamic::create(255);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str"));

    checkInsertManyFrom(getInsertFromColumn(), column_to, "Variant(Float64, Int8, String)", {"Float64", "Int8", "String"}, {{"Float64", 0}, {"Int8", 1}, {"String", 2}});
}

TEST(ColumnDynamic, InsertManyFrom3)
{
    auto column_to = ColumnDynamic::create(255);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str"));
    column_to->insert(Array({Field(42)}));

    checkInsertManyFrom(getInsertFromColumn(), column_to, "Variant(Array(Int8), Float64, Int8, String)", {"Array(Int8)", "Float64", "Int8", "String"}, {{"Array(Int8)", 0}, {"Float64", 1}, {"Int8", 2}, {"String", 3}});
}

TEST(ColumnDynamic, InsertManyFromOverflow1)
{
    auto column_from = ColumnDynamic::create(255);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertManyFrom(*column_from, 0, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 254);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    auto field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertManyFrom(*column_from, 1, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, "42.42");
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "42.42");

    column_to->insertManyFrom(*column_from, 2, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "str");
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, "str");
}

TEST(ColumnDynamic, InsertManyFromOverflow2)
{
    auto column_from = ColumnDynamic::create(255);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertManyFrom(*column_from, 0, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 254);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    auto field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42);

    column_to->insertManyFrom(*column_from, 1, 2);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, "42.42");
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, "42.42");
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
    auto column_to = ColumnDynamic::create(255);
    checkInsertRangeFrom(getInsertFromColumn(2), column_to, "Variant(Float64, Int8, String)", {"Float64", "Int8", "String"}, {{"Float64", 0}, {"Int8", 1}, {"String", 2}});
}

TEST(ColumnDynamic, InsertRangeFrom2)
{
    auto column_to = ColumnDynamic::create(255);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str1"));

    checkInsertRangeFrom(getInsertFromColumn(2), column_to, "Variant(Float64, Int8, String)", {"Float64", "Int8", "String"}, {{"Float64", 0}, {"Int8", 1}, {"String", 2}});
}

TEST(ColumnDynamic, InsertRangeFrom3)
{
    auto column_to = ColumnDynamic::create(255);
    column_to->insert(Field(42));
    column_to->insert(Field(42.42));
    column_to->insert(Field("str1"));
    column_to->insert(Array({Field(42)}));

    checkInsertRangeFrom(getInsertFromColumn(2), column_to, "Variant(Array(Int8), Float64, Int8, String)", {"Array(Int8)", "Float64", "Int8", "String"}, {{"Array(Int8)", 0}, {"Float64", 1}, {"Int8", 2}, {"String", 3}});
}

TEST(ColumnDynamic, InsertRangeFromOverflow1)
{
    auto column_from = ColumnDynamic::create(255);
    column_from->insert(Field(42));
    column_from->insert(Field(43));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertRangeFrom(*column_from, 0, 4);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    auto field = (*column_to)[column_to->size() - 4];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field("42.42"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field("str"));
}

TEST(ColumnDynamic, InsertRangeFromOverflow2)
{
    auto column_from = ColumnDynamic::create(255);
    column_from->insert(Field(42));
    column_from->insert(Field(43));
    column_from->insert(Field(42.42));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insertRangeFrom(*column_from, 0, 3);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    auto field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field("42.42"));
}

TEST(ColumnDynamic, InsertRangeFromOverflow3)
{
    auto column_from = ColumnDynamic::create(255);
    column_from->insert(Field(42));
    column_from->insert(Field(43));
    column_from->insert(Field(42.42));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insert(Field("Str"));
    column_to->insertRangeFrom(*column_from, 0, 3);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    auto field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field("42.42"));
}

TEST(ColumnDynamic, InsertRangeFromOverflow4)
{
    auto column_from = ColumnDynamic::create(255);
    column_from->insert(Field(42));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));

    auto column_to = getDynamicWithManyVariants(254);
    column_to->insertRangeFrom(*column_from, 0, 3);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    auto field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field("42"));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field("42.42"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field("str"));
}

TEST(ColumnDynamic, InsertRangeFromOverflow5)
{
    auto column_from = ColumnDynamic::create(255);
    column_from->insert(Field(42));
    column_from->insert(Field(43));
    column_from->insert(Field(42.42));
    column_from->insert(Field("str"));

    auto column_to = getDynamicWithManyVariants(253);
    column_to->insert(Field("str"));
    column_to->insertRangeFrom(*column_from, 0, 4);
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 255);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    auto field = (*column_to)[column_to->size() - 4];
    ASSERT_EQ(field, Field(42));
    field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(43));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field("42.42"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field("str"));
}

TEST(ColumnDynamic, InsertRangeFromOverflow6)
{
    auto column_from = ColumnDynamic::create(255);
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
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Array(Int8)"));
    auto field = (*column_to)[column_to->size() - 5];

    ASSERT_EQ(field, Field("44"));
    field = (*column_to)[column_to->size() - 4];
    ASSERT_EQ(field, Field(42.42));
    field = (*column_to)[column_to->size() - 3];
    ASSERT_EQ(field, Field(43.43));
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, Field("str"));
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, Field("[42]"));
}

TEST(ColumnDynamic, SerializeDeserializeFromArena1)
{
    auto column = ColumnDynamic::create(255);
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
    auto column_from = ColumnDynamic::create(255);
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

    auto column_to = ColumnDynamic::create(255);
    pos = column_to->deserializeAndInsertFromArena(ref1.data);
    pos = column_to->deserializeAndInsertFromArena(pos);
    pos = column_to->deserializeAndInsertFromArena(pos);
    column_to->deserializeAndInsertFromArena(pos);

    ASSERT_EQ((*column_from)[column_from->size() - 4], 42);
    ASSERT_EQ((*column_from)[column_from->size() - 3], 42.42);
    ASSERT_EQ((*column_from)[column_from->size() - 2], "str");
    ASSERT_EQ((*column_from)[column_from->size() - 1], Null());
    ASSERT_EQ(column_to->getVariantInfo().variant_type->getName(), "Variant(Float64, Int8, String)");
    std::vector<String> expected_names = {"Float64", "Int8", "String"};
    ASSERT_EQ(column_to->getVariantInfo().variant_names, expected_names);
    std::unordered_map<String, UInt8> expected_variant_name_to_discriminator = {{"Float64", 0}, {"Int8", 1}, {"String", 2}};
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
}

TEST(ColumnDynamic, SerializeDeserializeFromArenaOverflow)
{
    auto column_from = ColumnDynamic::create(255);
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

    ASSERT_EQ((*column_from)[column_from->size() - 4], 42);
    ASSERT_EQ((*column_from)[column_from->size() - 3], 42.42);
    ASSERT_EQ((*column_from)[column_from->size() - 2], "str");
    ASSERT_EQ((*column_from)[column_from->size() - 1], Null());
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_FALSE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
}

TEST(ColumnDynamic, skipSerializedInArena)
{
    auto column_from = ColumnDynamic::create(255);
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
    auto column_to = ColumnDynamic::create(255);
    pos = column_to->skipSerializedInArena(ref1.data);
    pos = column_to->skipSerializedInArena(pos);
    pos = column_to->skipSerializedInArena(pos);
    pos = column_to->skipSerializedInArena(pos);

    ASSERT_EQ(pos, end);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.empty());
    ASSERT_TRUE(column_to->getVariantInfo().variant_names.empty());
}
