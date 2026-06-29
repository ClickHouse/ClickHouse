#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <gtest/gtest.h>
#include <Common/Arena.h>

#include <set>

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
    Names expected_names = {"Float64", "Int8", "SharedVariant", "String"};
    ASSERT_EQ(column->getVariantInfo().variant_names, expected_names);
    UnorderedMapWithMemoryTracking<String, UInt8> expected_variant_name_to_discriminator = {{"Float64", 0}, {"Int8", 1}, {"SharedVariant", 2}, {"String", 3}};
    ASSERT_TRUE(column->getVariantInfo().variant_name_to_discriminator == expected_variant_name_to_discriminator);
}

static ColumnDynamic::MutablePtr getDynamicWithManyVariants(size_t num_variants, Field tuple_element = Field(42))
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

static ColumnDynamic::MutablePtr getInsertFromColumn(size_t num = 1)
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

static void checkInsertFrom(const ColumnDynamic::MutablePtr & column_from, ColumnDynamic::MutablePtr & column_to, const std::string & expected_variant, const Names & expected_names, const UnorderedMapWithMemoryTracking<String, UInt8> & expected_variant_name_to_discriminator)
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

    /// column_from and column_to share the same variant layout, but column_to has room (max_dynamic_types=254),
    /// so the Float64 value coming from column_from's shared variant is promoted to a regular variant instead
    /// of being left in the shared variant. This keeps the invariant that a type lives in exactly one storage.
    column_to->insertFrom(*column_from, 1);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 0);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42.42);
}

static void checkInsertManyFrom(const ColumnDynamic::MutablePtr & column_from, ColumnDynamic::MutablePtr & column_to, const std::string & expected_variant, const Names & expected_names, const UnorderedMapWithMemoryTracking<String, UInt8> & expected_variant_name_to_discriminator)
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

    /// column_from and column_to share the same variant layout, but column_to has room (max_dynamic_types=254),
    /// so the Float64 value coming from column_from's shared variant is promoted to a regular variant instead
    /// of being left in the shared variant. This keeps the invariant that a type lives in exactly one storage.
    column_to->insertManyFrom(*column_from, 1, 2);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 0);
    field = (*column_to)[column_to->size() - 2];
    ASSERT_EQ(field, 42.42);
    field = (*column_to)[column_to->size() - 1];
    ASSERT_EQ(field, 42.42);
}

/// Helper for the regression test below: returns the set of type names that are physically present
/// in the shared variant of a ColumnDynamic (decoded from the binary-encoded shared variant rows).
static std::set<String> getTypeNamesInSharedVariant(const ColumnDynamic & column)
{
    std::set<String> names;
    const auto & shared_variant = column.getSharedVariant();
    for (size_t i = 0; i != shared_variant.size(); ++i)
    {
        auto value = shared_variant.getDataAt(i);
        ReadBufferFromMemory buf(value);
        names.insert(decodeDataType(buf)->getName());
    }
    return names;
}

/// A type must never be present both as a regular variant and inside the shared variant of the same
/// ColumnDynamic. doInsertFrom/doInsertManyFrom used to break this when a value coming from the source
/// shared variant was inserted into a column that still had room for a new regular variant while the
/// same type was already accumulated in the destination shared variant. The resulting column had the
/// type in both storages, which later crashed function execution over Dynamic with a row-count
/// LOGICAL_ERROR in checkFunctionArgumentSizes.
TEST(ColumnDynamic, InsertFromSharedVariantKeepsInvariant)
{
    auto check = [](bool many)
    {
        /// src_shared: holds String inside its shared variant (Int8 is the single regular variant,
        /// String overflowed into shared). Inserting its String row goes through the shared-variant
        /// branch of (do)insertFrom.
        auto src_shared = ColumnDynamic::create(1);
        src_shared->insert(Field(1));
        src_shared->insert(Field("from_shared"));
        ASSERT_EQ(getTypeNamesInSharedVariant(*src_shared), (std::set<String>{"String"}));
        const size_t shared_string_row = src_shared->size() - 1;

        /// src_regular: holds String as a regular variant.
        auto src_regular = ColumnDynamic::create(10);
        src_regular->insert(Field("from_regular"));
        ASSERT_TRUE(src_regular->getVariantInfo().variant_name_to_discriminator.contains("String"));

        /// Build column_to the way the join non-joined-rows gather does: a fresh Dynamic with room,
        /// filled row by row by insertFrom from sources with different internal layouts. First take the
        /// String value that lives in src_shared's shared variant (no String regular variant in
        /// column_to yet, but there is room), then take a String value that is a regular variant in
        /// src_regular.
        auto column_to = ColumnDynamic::create(10);
        if (many)
        {
            column_to->insertManyFrom(*src_shared, shared_string_row, 2);
            column_to->insertManyFrom(*src_regular, 0, 2);
        }
        else
        {
            column_to->insertFrom(*src_shared, shared_string_row);
            column_to->insertFrom(*src_regular, 0);
        }

        /// Invariant: String must not be present both as a regular variant and inside the shared variant.
        const bool string_is_regular = column_to->getVariantInfo().variant_name_to_discriminator.contains("String");
        const bool string_in_shared = getTypeNamesInSharedVariant(*column_to).contains("String");
        ASSERT_FALSE(string_is_regular && string_in_shared);

        /// And every inserted value must read back correctly regardless of which storage holds it.
        ASSERT_EQ((*column_to)[0], Field("from_shared"));
        ASSERT_EQ((*column_to)[column_to->size() - 1], Field("from_regular"));
    };

    check(/*many=*/ false);
    check(/*many=*/ true);
}

/// Same invariant as above, exercised through the public range-copy path. insertRangeFrom has its own
/// shared-variant source loops that used to copy a type from the source shared variant into the
/// destination shared variant without trying to add it as a regular variant first, even when there was
/// room. A subsequent range copy could then add the same type as a regular variant, leaving it in both
/// storages.
TEST(ColumnDynamic, InsertRangeFromSharedVariantKeepsInvariant)
{
    /// src_shared: String lives only inside its shared variant (Int8 is the single regular variant).
    auto src_shared = ColumnDynamic::create(1);
    src_shared->insert(Field(1));
    src_shared->insert(Field("from_shared"));
    ASSERT_EQ(getTypeNamesInSharedVariant(*src_shared), (std::set<String>{"String"}));

    /// src_regular: String is a regular variant.
    auto src_regular = ColumnDynamic::create(10);
    src_regular->insert(Field("from_regular"));
    ASSERT_TRUE(src_regular->getVariantInfo().variant_name_to_discriminator.contains("String"));

    /// Copy the shared-variant String row first (column_to has no String regular variant yet but has
    /// room), then copy the regular-variant String row.
    auto column_to = ColumnDynamic::create(10);
    column_to->insertRangeFrom(*src_shared, 1, 1);
    column_to->insertRangeFrom(*src_regular, 0, 1);

    /// Invariant: String must not be present both as a regular variant and inside the shared variant.
    const bool string_is_regular = column_to->getVariantInfo().variant_name_to_discriminator.contains("String");
    const bool string_in_shared = getTypeNamesInSharedVariant(*column_to).contains("String");
    ASSERT_FALSE(string_is_regular && string_in_shared);

    /// Both values must read back correctly regardless of which storage holds them.
    ASSERT_EQ((*column_to)[0], Field("from_shared"));
    ASSERT_EQ((*column_to)[1], Field("from_regular"));
}

/// Same invariant, but exercised through the same-variants fast path at the top of
/// insertFrom/insertManyFrom/insertRangeFrom. Two Dynamic columns can share the same variant_names while
/// having different max_dynamic_types: a low-limit source overflows a type into its shared variant, while
/// a high-limit destination with the same layout still has room for it as a regular variant. The fast path
/// used to copy the source shared-variant row straight into the destination shared variant, so a later
/// insert could add the same type as a regular variant and leave it in both storages.
TEST(ColumnDynamic, SameLayoutSharedVariantKeepsInvariant)
{
    enum Mode { One, Many, Range };
    auto check = [](Mode mode)
    {
        /// src_shared: Int8 is the single regular variant (max_dynamic_types=1), String overflowed into
        /// the shared variant. Its row 1 (String) lives in the shared variant.
        auto src_shared = ColumnDynamic::create(1);
        src_shared->insert(Field(1));
        src_shared->insert(Field("from_shared"));
        ASSERT_EQ(getTypeNamesInSharedVariant(*src_shared), (std::set<String>{"String"}));

        /// src_regular: String is a regular variant.
        auto src_regular = ColumnDynamic::create(10);
        src_regular->insert(Field("from_regular"));
        ASSERT_TRUE(src_regular->getVariantInfo().variant_name_to_discriminator.contains("String"));

        /// column_to has the SAME variant_names as src_shared (Int8 + SharedVariant) but a higher limit,
        /// so it still has room to add String as a regular variant.
        auto column_to = ColumnDynamic::create(10);
        column_to->insert(Field(2));
        ASSERT_EQ(column_to->getVariantInfo().variant_names, src_shared->getVariantInfo().variant_names);

        /// First copy the shared-variant String row (same-layout fast path), then copy the regular-variant
        /// String row (different layout).
        if (mode == Many)
        {
            column_to->insertManyFrom(*src_shared, 1, 2);
            column_to->insertManyFrom(*src_regular, 0, 2);
        }
        else if (mode == Range)
        {
            column_to->insertRangeFrom(*src_shared, 1, 1);
            column_to->insertRangeFrom(*src_regular, 0, 1);
        }
        else
        {
            column_to->insertFrom(*src_shared, 1);
            column_to->insertFrom(*src_regular, 0);
        }

        /// Invariant: String must not be present both as a regular variant and inside the shared variant.
        const bool string_is_regular = column_to->getVariantInfo().variant_name_to_discriminator.contains("String");
        const bool string_in_shared = getTypeNamesInSharedVariant(*column_to).contains("String");
        ASSERT_FALSE(string_is_regular && string_in_shared);

        /// Values must read back correctly regardless of which storage holds them.
        ASSERT_EQ((*column_to)[0], Field(2));
        ASSERT_EQ((*column_to)[1], Field("from_shared"));
        ASSERT_EQ((*column_to)[column_to->size() - 1], Field("from_regular"));
    };

    check(One);
    check(Many);
    check(Range);
}

static void checkInsertRangeFrom(const ColumnDynamic::MutablePtr & column_from, ColumnDynamic::MutablePtr & column_to, const std::string & expected_variant, const Names & expected_names, const UnorderedMapWithMemoryTracking<String, UInt8> & expected_variant_name_to_discriminator)
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
    /// column_to has plenty of room (max_dynamic_types=254), so every type coming from column_from's
    /// shared variant (Int8 and Array(Int8)) is promoted to a regular variant rather than left in the
    /// shared variant. This keeps the invariant that a type lives in exactly one storage and matches the
    /// row-by-row insertFrom path and deserializeAndInsertFromArena.
    ASSERT_EQ(column_to->getVariantInfo().variant_names.size(), 5);
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Float64"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("String"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Int8"));
    ASSERT_TRUE(column_to->getVariantInfo().variant_name_to_discriminator.contains("Array(Int8)"));
    ASSERT_EQ(column_to->getSharedVariant().size(), 0);
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
    auto ref1 = column->serializeValueIntoArena(0, arena, pos, nullptr);
    column->serializeValueIntoArena(1, arena, pos, nullptr);
    column->serializeValueIntoArena(2, arena, pos, nullptr);
    column->serializeValueIntoArena(3, arena, pos, nullptr);

    ReadBufferFromString in({ref1.data(), arena.usedBytes()}); /// NOLINT(bugprone-suspicious-stringview-data-usage)
    column->deserializeAndInsertFromArena(in, nullptr);
    column->deserializeAndInsertFromArena(in, nullptr);
    column->deserializeAndInsertFromArena(in, nullptr);
    column->deserializeAndInsertFromArena(in, nullptr);

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
    auto ref1 = column_from->serializeValueIntoArena(0, arena, pos, nullptr);
    column_from->serializeValueIntoArena(1, arena, pos, nullptr);
    column_from->serializeValueIntoArena(2, arena, pos, nullptr);
    column_from->serializeValueIntoArena(3, arena, pos, nullptr);

    auto column_to = ColumnDynamic::create(254);
    ReadBufferFromString in({ref1.data(), arena.usedBytes()}); /// NOLINT(bugprone-suspicious-stringview-data-usage)
    column_to->deserializeAndInsertFromArena(in, nullptr);
    column_to->deserializeAndInsertFromArena(in, nullptr);
    column_to->deserializeAndInsertFromArena(in, nullptr);
    column_to->deserializeAndInsertFromArena(in, nullptr);

    ASSERT_EQ((*column_to)[column_to->size() - 4], 42);
    ASSERT_EQ((*column_to)[column_to->size() - 3], 42.42);
    ASSERT_EQ((*column_to)[column_to->size() - 2], "str");
    ASSERT_EQ((*column_to)[column_to->size() - 1], Null());
    ASSERT_EQ(column_to->getVariantInfo().variant_type->getName(), "Variant(Float64, Int8, SharedVariant, String)");
    Names expected_names = {"Float64", "Int8", "SharedVariant", "String"};
    ASSERT_EQ(column_to->getVariantInfo().variant_names, expected_names);
    UnorderedMapWithMemoryTracking<String, UInt8> expected_variant_name_to_discriminator = {{"Float64", 0}, {"Int8", 1}, {"SharedVariant", 2}, {"String", 3}};
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
    auto ref1 = column_from->serializeValueIntoArena(0, arena, pos, nullptr);
    column_from->serializeValueIntoArena(1, arena, pos, nullptr);
    column_from->serializeValueIntoArena(2, arena, pos, nullptr);
    column_from->serializeValueIntoArena(3, arena, pos, nullptr);

    auto column_to = getDynamicWithManyVariants(253);
    ReadBufferFromString in({ref1.data(), arena.usedBytes()}); /// NOLINT(bugprone-suspicious-stringview-data-usage)
    column_to->deserializeAndInsertFromArena(in, nullptr);
    column_to->deserializeAndInsertFromArena(in, nullptr);
    column_to->deserializeAndInsertFromArena(in, nullptr);
    column_to->deserializeAndInsertFromArena(in, nullptr);

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
    auto ref1 = column_from->serializeValueIntoArena(0, arena, pos, nullptr);
    column_from->serializeValueIntoArena(1, arena, pos, nullptr);
    column_from->serializeValueIntoArena(2, arena, pos, nullptr);
    column_from->serializeValueIntoArena(3, arena, pos, nullptr);
    column_from->serializeValueIntoArena(4, arena, pos, nullptr);

    auto column_to = ColumnDynamic::create(2);
    column_to->insert(Field(42.42));
    ReadBufferFromString in({ref1.data(), arena.usedBytes()}); /// NOLINT(bugprone-suspicious-stringview-data-usage)
    column_to->deserializeAndInsertFromArena(in, nullptr);
    column_to->deserializeAndInsertFromArena(in, nullptr);
    column_to->deserializeAndInsertFromArena(in, nullptr);
    column_to->deserializeAndInsertFromArena(in, nullptr);
    column_to->deserializeAndInsertFromArena(in, nullptr);

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
    auto ref1 = column_from->serializeValueIntoArena(0, arena, pos, nullptr);
    column_from->serializeValueIntoArena(1, arena, pos, nullptr);
    column_from->serializeValueIntoArena(2, arena, pos, nullptr);
    column_from->serializeValueIntoArena(3, arena, pos, nullptr);

    auto column_to = ColumnDynamic::create(254);
    ReadBufferFromString in({ref1.data(), arena.usedBytes()}); /// NOLINT(bugprone-suspicious-stringview-data-usage)
    column_to->skipSerializedInArena(in);
    column_to->skipSerializedInArena(in);
    column_to->skipSerializedInArena(in);
    column_to->skipSerializedInArena(in);

    ASSERT_TRUE(in.eof());
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

TEST(ColumnDynamic, rollback)
{
    auto check_variant = [](const ColumnVariant & column_variant, VectorWithMemoryTracking<size_t> sizes)
    {
        ASSERT_EQ(column_variant.getNumVariants(), sizes.size());
        size_t num_rows = 0;

        for (size_t i = 0; i < sizes.size(); ++i)
        {
            ASSERT_EQ(column_variant.getVariants()[i]->size(), sizes[i]);
            num_rows += sizes[i];
        }

        ASSERT_EQ(num_rows, column_variant.size());
    };

    auto check_checkpoint = [&](const ColumnCheckpoint & cp, UnorderedMapWithMemoryTracking<String, size_t> sizes)
    {
        const auto & variants_checkpoints = assert_cast<const DynamicColumnCheckpoint &>(cp).variants_checkpoints;
        size_t num_rows = 0;

        for (const auto & [variant, checkpoint] : variants_checkpoints)
        {
            ASSERT_EQ(checkpoint->size, sizes.at(variant));
            num_rows += sizes.at(variant);
        }

        ASSERT_EQ(num_rows, cp.size);
    };

    VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> variant_checkpoints_sizes;
    VectorWithMemoryTracking<std::pair<ColumnCheckpointPtr, UnorderedMapWithMemoryTracking<String, size_t>>> dynamic_checkpoints;

    auto column = ColumnDynamic::create(2);
    auto checkpoint = column->getCheckpoint();

    column->insert(Field(42));
    column->updateCheckpoint(*checkpoint);
    column->insert(Field("str1"));
    column->rollback(*checkpoint);

    variant_checkpoints_sizes.emplace_back(VectorWithMemoryTracking<size_t>{0, 1, 0});
    dynamic_checkpoints.emplace_back(checkpoint, UnorderedMapWithMemoryTracking<String, size_t>{{"SharedVariant", 0}, {"Int8", 1}, {"String", 0}});

    check_checkpoint(*checkpoint, dynamic_checkpoints.back().second);
    check_variant(column->getVariantColumn(), variant_checkpoints_sizes.back());

    column->insert("str1");
    variant_checkpoints_sizes.emplace_back(VectorWithMemoryTracking<size_t>{0, 1, 1});
    dynamic_checkpoints.emplace_back(column->getCheckpoint(), UnorderedMapWithMemoryTracking<String, size_t>{{"SharedVariant", 0}, {"Int8", 1}, {"String", 1}});

    column->insert("str2");
    variant_checkpoints_sizes.emplace_back(VectorWithMemoryTracking<size_t>{0, 1, 2});
    dynamic_checkpoints.emplace_back(column->getCheckpoint(), UnorderedMapWithMemoryTracking<String, size_t>{{"SharedVariant", 0}, {"Int8", 1}, {"String", 2}});

    column->insert(Array({1, 2}));
    variant_checkpoints_sizes.emplace_back(VectorWithMemoryTracking<size_t>{1, 1, 2});
    dynamic_checkpoints.emplace_back(column->getCheckpoint(), UnorderedMapWithMemoryTracking<String, size_t>{{"SharedVariant", 1}, {"Int8", 1}, {"String", 2}});

    column->insert(Field(42.42));
    variant_checkpoints_sizes.emplace_back(VectorWithMemoryTracking<size_t>{2, 1, 2});
    dynamic_checkpoints.emplace_back(column->getCheckpoint(), UnorderedMapWithMemoryTracking<String, size_t>{{"SharedVariant", 2}, {"Int8", 1}, {"String", 2}});

    for (size_t i = 0; i != variant_checkpoints_sizes.size(); ++i)
    {
        auto column_copy = column->clone();
        column_copy->rollback(*dynamic_checkpoints[i].first);

        check_checkpoint(*dynamic_checkpoints[i].first, dynamic_checkpoints[i].second);
        check_variant(assert_cast<const ColumnDynamic &>(*column_copy).getVariantColumn(), variant_checkpoints_sizes[i]);
    }
}

TEST(ColumnDynamic, InsertRangeFrom4)
{
    auto column_to = ColumnDynamic::create(2);
    auto src = ColumnDynamic::create(2);
    src->insert(Field(42));
    src->insert(Field("Hello"));
    src->insert(Field(42.42));
    src->insert(Field(Array({1, 2, 3})));
    auto column_from = src->cloneEmpty();
    column_from->insertRangeFrom(*src, 2, 2);

    column_to->insertRangeFrom(*column_from, 0, 2);
    size_t total_variants_sizes = 0;
    for (const auto & variant : column_to->getVariantColumn().getVariants())
        total_variants_sizes += variant->size();

    ASSERT_EQ(total_variants_sizes, column_to->getVariantColumn().getLocalDiscriminators().size());
}
