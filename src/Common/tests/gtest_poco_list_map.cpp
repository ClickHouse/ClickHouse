#include <gtest/gtest.h>

#include <string>

#include <Poco/ListMap.h>
#include <Poco/Net/NameValueCollection.h>

namespace
{

using Map = Poco::ListMap<std::string, std::string>;

std::string dump(const Map & map)
{
    std::string res;
    for (const auto & [key, value] : map)
        res += key + "=" + value + " ";
    return res;
}

}

/// Poco::Net::NameValueCollection (HTTP headers, HTMLForm query parameters) is a
/// Poco::ListMap: insertion-ordered, case-insensitive, and all entries with an
/// equal key are kept in one contiguous block. ListMap::insert relies on that
/// invariant to append to a block without scanning the whole map.

TEST(PocoListMap, EqualKeysStayContiguousInInsertionOrder)
{
    Map map;
    map.insert({"a", "1"});
    map.insert({"b", "1"});
    map.insert({"A", "2"});   /// case-insensitive: joins the "a" block
    map.insert({"c", "1"});
    map.insert({"b", "2"});
    map.insert({"a", "3"});

    EXPECT_EQ(dump(map), "a=1 A=2 a=3 b=1 b=2 c=1 ");
    ASSERT_NE(map.find("B"), map.end());
    EXPECT_EQ(map.find("B")->second, "1");   /// find returns the first of the block
}

TEST(PocoListMap, EraseAndSubscript)
{
    Map map;
    map.insert({"a", "1"});
    map.insert({"b", "1"});
    map.insert({"a", "2"});
    map.insert({"b", "2"});

    EXPECT_EQ(map.erase("A"), 2u);
    EXPECT_EQ(dump(map), "b=1 b=2 ");

    map["b"] = "x";           /// assigns to the first entry of the block
    map["z"];                 /// inserts a new key at the end
    EXPECT_EQ(dump(map), "b=x b=2 z= ");

    map.erase(map.find("b"));
    map.insert({"B", "3"});   /// still appended to the (now shorter) block
    EXPECT_EQ(dump(map), "b=2 B=3 z= ");
}

TEST(PocoListMap, ManyValuesForOneKey)
{
    /// A request like `?role=r1&role=r2&...` produces thousands of parameters
    /// with the same name; they must all land in one block, in order, after
    /// the parameters that came before them.
    constexpr size_t n = 20000;

    Poco::Net::NameValueCollection params;
    params.add("user", "u");
    params.add("password", "p");
    for (size_t i = 0; i < n; ++i)
        params.add("role", "r" + std::to_string(i));
    params.add("query_id", "q");

    ASSERT_EQ(params.size(), n + 3);

    auto it = params.begin();
    ASSERT_EQ((it++)->first, "user");
    ASSERT_EQ((it++)->first, "password");
    ASSERT_EQ(it, params.find("role"));
    for (size_t i = 0; i < n; ++i, ++it)
    {
        ASSERT_NE(it, params.end());
        ASSERT_EQ(it->first, "role");
        ASSERT_EQ(it->second, "r" + std::to_string(i));
    }
    ASSERT_NE(it, params.end());
    EXPECT_EQ(it->first, "query_id");
    EXPECT_EQ(++it, params.end());
}
