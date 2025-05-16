#include <gtest/gtest.h>

#include <Columns/ColumnUniqueCompressed.h>
#include <Common/Arena.h>

#include <string>
#include <vector>

using namespace DB;

TEST(ColumnUniqueCompressed, EmptyFCBlockDF)
{
    const ColumnPtr strings = ColumnString::create();
    const size_t block_size = 4;

    /// not nullable, containts empty string as first element
    {
        auto unique_compressed_column = ColumnUniqueFCBlockDF::create(strings, block_size, false);
        EXPECT_EQ(unique_compressed_column->size(), 1);

        const auto default_index = unique_compressed_column->getOrFindValueIndex("");
        EXPECT_TRUE(default_index.has_value());
        EXPECT_EQ(default_index.value(), 0);

        const auto field = (*unique_compressed_column)[0];
        EXPECT_EQ(field.safeGet<String>(), "");

        const auto no_index = unique_compressed_column->getOrFindValueIndex("whatever");
        EXPECT_FALSE(no_index.has_value());
    }

    /// nullable, constains null and empty string as first two elements
    {
        auto unique_compressed_column = ColumnUniqueFCBlockDF::create(strings, block_size, true);
        EXPECT_EQ(unique_compressed_column->size(), 2);

        const auto empty_str_index = unique_compressed_column->getOrFindValueIndex("");
        EXPECT_TRUE(empty_str_index.has_value());
        EXPECT_EQ(empty_str_index.value(), 1);

        const auto field = (*unique_compressed_column)[0];
        EXPECT_TRUE(field.isNull());

        const auto no_index = unique_compressed_column->getOrFindValueIndex("whatever");
        EXPECT_FALSE(no_index.has_value());
    }
}

TEST(ColumnUniqueCompressed, SingleInsertsFCBlockDF)
{
    const std::vector<std::string> data = {
        "block",
        "blocking",
        "blockings",
        "sort",
        "sorted",
    };

    const ColumnPtr strings_column = ColumnString::create();
    const size_t block_size = 3;

    auto unique_compressed_column = ColumnUniqueFCBlockDF::create(strings_column, block_size, false);
    for (const auto & str : data)
    {
        const size_t index = unique_compressed_column->uniqueInsert(str);
        const auto field = (*unique_compressed_column)[index];
        EXPECT_EQ(str, field.safeGet<String>());
    }

    for (const auto & str : data)
    {
        const auto index = unique_compressed_column->getOrFindValueIndex(str);
        EXPECT_TRUE(index.has_value());
        EXPECT_EQ((*unique_compressed_column)[index.value()], str);
    }
}

static auto getNotEmptyColumnUniqueCompressedFCBlockDF(bool is_nullable = false)
{
    const std::vector<std::string> data
        = {"this is",
           "a",
           "list",
           "of",
           "totally",
           "random",
           "unsorted",
           "words",
           "and some",
           "words for compression",
           "randomness",
           "listing",
           "ofcourse",
           "another"};

    auto strings_column = ColumnString::create();
    const size_t block_size = 3;

    for (const auto & str : data)
    {
        strings_column->insert(str);
    }

    return ColumnUniqueFCBlockDF::create(std::move(strings_column), block_size, is_nullable);
}

TEST(ColumnUniqueCompressed, NestedColumnFCBlockDF)
{
    const auto unique_compressed_column = getNotEmptyColumnUniqueCompressedFCBlockDF();
    const auto nested_column = unique_compressed_column->getNestedColumn();

    for (size_t i = 0; i < nested_column->size(); ++i)
    {
        const Field f_unique_compressed = (*unique_compressed_column)[i];
        const Field f_nested = (*nested_column)[i];
        EXPECT_EQ(f_unique_compressed, f_nested);
    }
}

TEST(ColumnUniqueCompressed, RangeInsertFCBlockDF)
{
    const std::vector<std::string> data = {
        "block",
        "blocking",
        "blockings",
        "sort",
        "sorted",
    };

    auto strings_column = ColumnString::create();
    for (const auto & str : data)
    {
        strings_column->insert(str);
    }

    auto unique_compressed_column = getNotEmptyColumnUniqueCompressedFCBlockDF();
    auto indexes = unique_compressed_column->uniqueInsertRangeFrom(*strings_column, 0, 5);

    for (size_t i = 0; i < indexes->size(); ++i)
    {
        const size_t index = (*indexes)[i].safeGet<size_t>();
        const auto field = (*unique_compressed_column)[index];
        EXPECT_EQ(data[i], field.safeGet<String>());
    }

    for (const auto & str : data)
    {
        const auto index = unique_compressed_column->getOrFindValueIndex(str);
        EXPECT_TRUE(index.has_value());
        const auto field = (*unique_compressed_column)[index.value()];
        EXPECT_EQ(field.safeGet<String>(), str);
    }
}

TEST(ColumnUniqueCompressed, NullableConversionFCBlockDF)
{
    auto unique_compressed_column = getNotEmptyColumnUniqueCompressedFCBlockDF(false);
    
    const size_t some_index = 10;
    
    const String value_in_not_nullable = (*unique_compressed_column)[some_index].safeGet<String>();
    const size_t default_index_not_nullable = unique_compressed_column->uniqueInsert("");
    EXPECT_EQ(default_index_not_nullable, unique_compressed_column->getNestedTypeDefaultValueIndex());

    unique_compressed_column->nestedToNullable();

    const String value_in_nullable = (*unique_compressed_column)[some_index + 1].safeGet<String>();
    const size_t default_index_nullable = unique_compressed_column->uniqueInsert("");
    const size_t null_index = unique_compressed_column->uniqueInsert(Field{});
    EXPECT_EQ(value_in_not_nullable, value_in_nullable);
    EXPECT_EQ(default_index_nullable, unique_compressed_column->getNestedTypeDefaultValueIndex());
    EXPECT_EQ(null_index, unique_compressed_column->getNullValueIndex());
    EXPECT_NE(unique_compressed_column->getNullValueIndex(), unique_compressed_column->getNestedTypeDefaultValueIndex());
    EXPECT_TRUE((*unique_compressed_column)[null_index].isNull());

    unique_compressed_column->nestedRemoveNullable();

    EXPECT_EQ(value_in_not_nullable, (*unique_compressed_column)[some_index].safeGet<String>());
}

TEST(ColumnUniqueCompressed, RangeInsertWithOverflowFCBlockDF)
{
    const std::vector<std::string> data = {
        "block",
        "blocking",
        "blockings",
        "sort",
        "sorted",
    };

    auto strings_column = ColumnString::create();
    for (const auto & str : data)
    {
        strings_column->insert(str);
    }

    auto unique_compressed_column = getNotEmptyColumnUniqueCompressedFCBlockDF();
    const size_t to_add = 2;
    const size_t max_dict_size = unique_compressed_column->size() + to_add;
    const auto res_with_overflow = unique_compressed_column->uniqueInsertRangeWithOverflow(*strings_column, 0, 5, max_dict_size);

    for (size_t i = 0; i < to_add; ++i)
    {
        const auto index = unique_compressed_column->getOrFindValueIndex(data[i]);
        EXPECT_TRUE(index.has_value());
        EXPECT_EQ((*res_with_overflow.indexes)[i].safeGet<size_t>(), index.value());
    }

    EXPECT_EQ(res_with_overflow.overflowed_keys->size(), data.size() - to_add);
    for (size_t i = 0; i < res_with_overflow.overflowed_keys->size(); ++i)
    {
        const auto index = unique_compressed_column->getOrFindValueIndex(res_with_overflow.overflowed_keys->getDataAt(i));
        EXPECT_FALSE(index.has_value());
        EXPECT_EQ(res_with_overflow.overflowed_keys->getDataAt(i), data[i + to_add]); /// as data is sorted
        EXPECT_EQ(res_with_overflow.indexes->get64(i + to_add), unique_compressed_column->size() + i);
    }
}

TEST(ColumnUniqueCompressed, RangeInsertWithOverflowDuplicatesFCBlockDF)
{
    const std::vector<std::string> data = {
        "string",
        "string",
        "string",
        "string",
        "string",
        "string",
        "string",
        "string",
        "string",
        "actually real new value",
    };

    auto strings_column = ColumnString::create();
    for (const auto & str : data)
    {
        strings_column->insert(str);
    }

    auto unique_compressed_column = getNotEmptyColumnUniqueCompressedFCBlockDF();
    const size_t max_dict_size = unique_compressed_column->size();
    const auto res_with_overflow = unique_compressed_column->uniqueInsertRangeWithOverflow(*strings_column, 0, strings_column->size(), max_dict_size);

    /// duplicate value
    for (size_t i = 0; i < data.size() - 1; ++i)
    {
        const auto index = unique_compressed_column->getOrFindValueIndex(data[i]);
        EXPECT_FALSE(index.has_value());
        EXPECT_EQ((*res_with_overflow.indexes)[i].safeGet<size_t>(), unique_compressed_column->size());
    }

    /// unique value
    const size_t pos = data.size() - 1;
    const auto index = unique_compressed_column->getOrFindValueIndex(data[pos]);
    EXPECT_FALSE(index.has_value());
    EXPECT_EQ((*res_with_overflow.indexes)[pos].safeGet<size_t>(), unique_compressed_column->size() + 1);
}

TEST(ColumnUniqueCompressed, SerializationFCBlockDF)
{
    const auto column_unique_compressed = getNotEmptyColumnUniqueCompressedFCBlockDF();
    Arena arena;

    const char * pos = nullptr;
    for (size_t i = 0; i < column_unique_compressed->size(); ++i)
    {
        const auto data = column_unique_compressed->serializeValueIntoArena(i, arena, pos);
        const size_t string_size = unalignedLoad<size_t>(data.data);
        const StringRef value(data.data + sizeof(size_t), string_size);

        const auto real_value = (*column_unique_compressed)[i].safeGet<String>();
        EXPECT_EQ(string_size, real_value.size() + 1);
        EXPECT_EQ(value, real_value + '\0');
    }
}

TEST(ColumnUniqueCompressed, DeserializationFCBlockDF)
{
    const auto column_unique_compressed = getNotEmptyColumnUniqueCompressedFCBlockDF();
    const auto other_column = column_unique_compressed->cloneEmpty();
    ColumnUniqueFCBlockDF * other_column_ptr = typeid_cast<ColumnUniqueFCBlockDF *>(other_column.get());
    Arena arena;

    const char * pos = nullptr;
    for (size_t i = 0; i < column_unique_compressed->size(); ++i)
    {
        const StringRef data = column_unique_compressed->serializeValueIntoArena(i, arena, pos);
        const char * new_pos = nullptr;
        const size_t index = other_column_ptr->uniqueDeserializeAndInsertFromArena(data.data, new_pos);
        EXPECT_EQ((*other_column)[index].safeGet<String>(), (*column_unique_compressed)[i].safeGet<String>());
    }
}

TEST(ColumnUniqueCompressed, ReindexFCBlockDF)
{
    const std::vector<std::string> data = {
        "block",
        "blocking",
        "blockings",
        "sort",
        "sorted",
    };

    auto strings_column = ColumnString::create();
    for (const auto & str : data)
    {
        strings_column->insert(str);
    }

    auto unique_compressed_column = getNotEmptyColumnUniqueCompressedFCBlockDF();

    std::vector<std::string> old_values;
    for (size_t i = 0; i < unique_compressed_column->size(); ++i)
    {
        old_values.push_back((*unique_compressed_column)[i].safeGet<String>());
    }

    EXPECT_FALSE(unique_compressed_column->haveIndexesChanged());
    unique_compressed_column->uniqueInsertRangeFrom(*strings_column, 0, strings_column->size());
    EXPECT_TRUE(unique_compressed_column->haveIndexesChanged());

    auto mapping = unique_compressed_column->detachChangedIndexes();
    EXPECT_FALSE(unique_compressed_column->haveIndexesChanged());

    for (size_t i = 0; i < old_values.size(); ++i)
    {
        const UInt64 pos = unique_compressed_column->getOrFindValueIndex(old_values[i]).value();
        EXPECT_EQ(pos, mapping->get64(i));
    }
}

TEST(ColumnUniqueCompressed, ReindexNullableFCBlockDF)
{
    const std::vector<std::string> data = {
        "",
        "block",
        "blocking",
        "blockings",
        "sort",
        "sorted",
    };

    auto strings_column = ColumnString::create();
    for (const auto & str : data)
    {
        strings_column->insert(str);
    }

    auto unique_compressed_column = getNotEmptyColumnUniqueCompressedFCBlockDF(true);

    std::vector<std::string> old_values;
    for (size_t i = 1; i < unique_compressed_column->size(); ++i)
    {
        old_values.push_back((*unique_compressed_column)[i].safeGet<String>());
    }
    EXPECT_TRUE((*unique_compressed_column)[0].isNull());

    EXPECT_FALSE(unique_compressed_column->haveIndexesChanged());
    unique_compressed_column->uniqueInsertRangeFrom(*strings_column, 0, strings_column->size());
    EXPECT_TRUE(unique_compressed_column->haveIndexesChanged());

    auto mapping = unique_compressed_column->detachChangedIndexes();
    EXPECT_FALSE(unique_compressed_column->haveIndexesChanged());

    for (size_t i = 0; i < old_values.size(); ++i)
    {
        const UInt64 pos = unique_compressed_column->getOrFindValueIndex(old_values[i]).value();
        EXPECT_EQ(pos, mapping->get64(i + 1));
    }
    EXPECT_EQ(mapping->getUInt(0), 0);
}

TEST(ColumnUniqueCompressed, UsesPrefixLengthOptimizationFCBlockDF)
{
    const auto unique_compressed_column = getNotEmptyColumnUniqueCompressedFCBlockDF();
    const size_t byte_size1 = unique_compressed_column->byteSize();
    size_t byte_size2 = 0;
    for (size_t i = 0; i < unique_compressed_column->size(); ++i)
    {
        byte_size2 += unique_compressed_column->byteSizeAt(i);
    }
    

    EXPECT_EQ(byte_size1, byte_size2);
}
