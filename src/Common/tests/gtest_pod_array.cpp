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

TEST(Common, PODInsertIteratorRange)
{
    size_t size = 1 << 20;
    char value = 123;

    PODArray<size_t> big;
    PODArray<char> small(size, value);

    EXPECT_EQ(big.size(), 0);
    EXPECT_EQ(small.size(), size);

    big.insert(small.begin(), small.end());

    EXPECT_EQ(big.size(), size);
    EXPECT_EQ(big.back(), value);

    big.assign(small.begin(), small.end());

    EXPECT_EQ(big.size(), size);
    EXPECT_EQ(big.back(), value);

    big.insert(big.begin(), small.begin(), small.end());

    EXPECT_EQ(big.size(), size * 2);
    EXPECT_EQ(big.front(), value);
    EXPECT_EQ(big.back(), value);
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
