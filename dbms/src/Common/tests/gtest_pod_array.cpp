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
