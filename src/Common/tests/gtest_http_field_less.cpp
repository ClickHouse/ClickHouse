#include <gtest/gtest.h>

#include <map>
#include <string>

#include <Common/HTTPFieldLess.h>

using DB::HTTPFieldLess;

TEST(HTTPFieldLess, LettersAreFolded)
{
    HTTPFieldLess less;
    EXPECT_FALSE(less("Content-Type", "content-type"));
    EXPECT_FALSE(less("content-type", "Content-Type"));
    EXPECT_FALSE(less("CONTENT-TYPE", "content-type"));
    EXPECT_FALSE(less("x-clickhouse-key", "X-ClickHouse-Key"));
}

TEST(HTTPFieldLess, PunctuationIsNotFolded)
{
    HTTPFieldLess less;
    // '^' (0x5E) and '~' (0x7E) must remain distinct — not collapsed by x|0x20
    EXPECT_TRUE(less("Foo^Bar", "Foo~Bar") || less("Foo~Bar", "Foo^Bar"));
    EXPECT_FALSE(less("Foo^Bar", "Foo^Bar")); // irreflexivity
}

TEST(HTTPFieldLess, MapPreservesOriginalCase)
{
    std::map<std::string, int, HTTPFieldLess> m;
    m["Content-Type"] = 42;
    // Case-insensitive lookup
    EXPECT_EQ(m.count("content-type"), 1u);
    EXPECT_EQ(m.count("CONTENT-TYPE"), 1u);
    EXPECT_EQ(m.at("content-type"), 42);
    // Original case is preserved as the stored key
    EXPECT_EQ(m.begin()->first, "Content-Type");
}
