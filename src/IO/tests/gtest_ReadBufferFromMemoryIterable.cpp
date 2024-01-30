#include <gtest/gtest.h>
#include <IO/ReadBufferFromMemoryIterable.h>
#include <IO/ReadHelpers.h>

TEST(ReadBufferFromMemoryIterable, General)
{
    std::vector<std::string_view> rows = {
        {"foo"},
        {"bar"},
    };

    auto it = rows.cbegin();
    DB::ReadBufferFromMemoryIterable buffer(it, rows.cend());

    String data;

    DB::readString(data, buffer);
    ASSERT_EQ(it, rows.cbegin()+1);
    ASSERT_EQ(data, "foo");
    ASSERT_EQ(buffer.eof(), true);
    ASSERT_EQ(buffer.nextRow(), true);
    ASSERT_EQ(buffer.eof(), false);
    DB::readString(data, buffer);
    ASSERT_EQ(it, rows.cend());
    ASSERT_EQ(data, "bar");
    ASSERT_EQ(buffer.eof(), true);
    ASSERT_EQ(buffer.nextRow(), false);

    ASSERT_EQ(it, rows.cend()) << "Not all rows had been read";
    ASSERT_EQ(*(it-1), "bar");
    ASSERT_EQ(*(it-2), "foo");
}
