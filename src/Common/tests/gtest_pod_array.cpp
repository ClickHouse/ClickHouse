#include <gtest/gtest.h>

#include <Common/PODArray.h>

using namespace DB;

TEST(Common, PODArrayInsert)
{
    std::string str = "test_string_abacaba";
    PODArray<char> chars;
    chars.insert(chars.end(), str.begin(), str.end());
    EXPECT_EQ(str, std::string(chars.data(), chars.size()));

    std::string insert_in_the_middle = "insert_in_the_middle";
    auto pos = str.size() / 2;
    str.insert(str.begin() + pos, insert_in_the_middle.begin(), insert_in_the_middle.end());
    chars.insert(chars.begin() + pos, insert_in_the_middle.begin(), insert_in_the_middle.end());
    EXPECT_EQ(str, std::string(chars.data(), chars.size()));

    std::string insert_with_resize;
    insert_with_resize.reserve(chars.capacity() * 2);
    char cur_char = 'a';
    while (insert_with_resize.size() < insert_with_resize.capacity())
    {
        insert_with_resize += cur_char;
        if (cur_char == 'z')
            cur_char = 'a';
        else
            ++cur_char;
    }
    str.insert(str.begin(), insert_with_resize.begin(), insert_with_resize.end());
    chars.insert(chars.begin(), insert_with_resize.begin(), insert_with_resize.end());
    EXPECT_EQ(str, std::string(chars.data(), chars.size()));
}

TEST(Common, PODArrayInsertFromItself)
{
    {
        PaddedPODArray<UInt64> array { 1 };

        for (size_t i = 0; i < 3; ++i)
            array.insertFromItself(array.begin(), array.end());

        PaddedPODArray<UInt64> expected {1,1,1,1,1,1,1,1};
        ASSERT_EQ(array,expected);
    }
}

TEST(Common, PODPushBackRawMany)
{
    PODArray<char> chars;
    chars.push_back_raw_many(5, "first");
    EXPECT_EQ(std::string("first"), std::string(chars.data(), chars.size()));
    EXPECT_EQ(5, chars.size());
    EXPECT_LE(chars.capacity() - chars.size(), 10);
    chars.push_back_raw_many(10, "0123456789");
    EXPECT_EQ(15, chars.size());
    EXPECT_EQ(std::string("first0123456789"), std::string(chars.data(), chars.size()));
}

TEST(Common, PODNoOverallocation)
{
    /// Check that PaddedPODArray allocates for smaller number of elements than the power of two due to padding.
    /// NOTE: It's Ok to change these numbers if you will modify initial size or padding.

    PaddedPODArray<char> chars;
    std::vector<size_t> capacities;

    size_t prev_capacity = 0;
    for (size_t i = 0; i < 1000000; ++i)
    {
        chars.emplace_back();
        if (chars.capacity() != prev_capacity)
        {
            prev_capacity = chars.capacity();
            capacities.emplace_back(prev_capacity);
        }
    }

    EXPECT_EQ(capacities, (std::vector<size_t>{4065, 8161, 16353, 32737, 65505, 131041, 262113, 524257, 1048545}));
}

template <size_t size>
struct ItemWithSize
{
    char v[size] {};
};

TEST(Common, PODInsertElementSizeNotMultipleOfLeftPadding)
{
    using ItemWith24Size = ItemWithSize<24>;
    PaddedPODArray<ItemWith24Size> arr1_initially_empty;

    size_t items_to_insert_size = 120000;

    for (size_t test = 0; test < items_to_insert_size; ++test)
        arr1_initially_empty.emplace_back();

    EXPECT_EQ(arr1_initially_empty.size(), items_to_insert_size);

    PaddedPODArray<ItemWith24Size> arr2_initially_nonempty;

    for (size_t test = 0; test < items_to_insert_size; ++test)
        arr2_initially_nonempty.emplace_back();

    EXPECT_EQ(arr1_initially_empty.size(), items_to_insert_size);
}

TEST(Common, PODErase)
{
    {
        PaddedPODArray<UInt64> items {0,1,2,3,4,5,6,7,8,9};
        PaddedPODArray<UInt64> expected;
        expected = {0,1,2,3,4,5,6,7,8,9};

        items.erase(items.begin(), items.begin());
        EXPECT_EQ(items, expected);

        items.erase(items.end(), items.end());
        EXPECT_EQ(items, expected);
    }
    {
        PaddedPODArray<UInt64> actual {0,1,2,3,4,5,6,7,8,9};
        PaddedPODArray<UInt64> expected;

        expected = {0,1,4,5,6,7,8,9};
        actual.erase(actual.begin() + 2, actual.begin() + 4);
        EXPECT_EQ(actual, expected);

        expected = {0,1,4};
        actual.erase(actual.begin() + 3, actual.end());
        EXPECT_EQ(actual, expected);

        expected = {};
        actual.erase(actual.begin(), actual.end());
        EXPECT_EQ(actual, expected);

        for (size_t i = 0; i < 10; ++i)
            actual.emplace_back(static_cast<UInt64>(i));

        expected = {0,1,4,5,6,7,8,9};
        actual.erase(actual.begin() + 2, actual.begin() + 4);
        EXPECT_EQ(actual, expected);

        expected = {0,1,4};
        actual.erase(actual.begin() + 3, actual.end());
        EXPECT_EQ(actual, expected);

        expected = {};
        actual.erase(actual.begin(), actual.end());
        EXPECT_EQ(actual, expected);
    }
    {
        PaddedPODArray<UInt64> actual {0,1,2,3,4,5,6,7,8,9};
        PaddedPODArray<UInt64> expected;

        expected = {1,2,3,4,5,6,7,8,9};
        actual.erase(actual.begin());
        EXPECT_EQ(actual, expected);
    }
}
