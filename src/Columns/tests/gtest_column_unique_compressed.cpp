#include <gtest/gtest.h>

#include <Columns/ColumnUniqueCompressed.h>

#include <string>
#include <vector>

using namespace DB;

TEST(ColumnUniqueCompressed, EmptyFCBlockDF)
{
    const ColumnPtr strings = ColumnString::create();
    const size_t block_size = 4;

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

static auto getNotEmptyColumnUniqueCompressedFCBlockDF()
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

    return ColumnUniqueFCBlockDF::create(std::move(strings_column), block_size, false);
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
    unique_compressed_column->uniqueInsertRangeFrom(*strings_column, 0, 5);

    for (const auto & str : data)
    {
        const auto index = unique_compressed_column->getOrFindValueIndex(str);
        EXPECT_TRUE(index.has_value());
        const auto field = (*unique_compressed_column)[index.value()];
        EXPECT_EQ(field.safeGet<String>(), str);
    }
}
