#include <gtest/gtest.h>

#include <Interpreters/TablesStatus.h>

using namespace DB;

namespace
{
TablesStatusRequest makeRequest(std::initializer_list<QualifiedTableName> tables)
{
    TablesStatusRequest request;
    for (const auto & t : tables)
        request.tables.insert(t);
    return request;
}
}

/// The interserver auth hash folds in `getAuthDigest` so a relayed hash cannot be reused for a
/// different table set. The digest must therefore (a) not depend on insertion order, and (b) differ
/// whenever the requested set differs - that is what makes a tampered `request.tables` fail the hash.
TEST(TablesStatusAuthDigest, OrderIndependent)
{
    auto a = makeRequest({{"db", "t1"}, {"db", "t2"}, {"other", "t3"}});
    auto b = makeRequest({{"other", "t3"}, {"db", "t1"}, {"db", "t2"}});
    EXPECT_EQ(a.getAuthDigest(), b.getAuthDigest());
}

TEST(TablesStatusAuthDigest, DistinguishesTamperedTableSets)
{
    auto original = makeRequest({{"db", "t1"}});
    /// A relayed hash valid for {db.t1} would be reused with these tampered sets; each must yield a
    /// different digest so the recomputed hash no longer matches.
    EXPECT_NE(original.getAuthDigest(), makeRequest({{"db", "t2"}}).getAuthDigest());
    EXPECT_NE(original.getAuthDigest(), makeRequest({{"other", "t1"}}).getAuthDigest());
    EXPECT_NE(original.getAuthDigest(), makeRequest({{"db", "t1"}, {"db", "t2"}}).getAuthDigest());
    EXPECT_NE(original.getAuthDigest(), makeRequest({}).getAuthDigest());
}

/// The encoding must be injective even for names containing arbitrary bytes: two different splits of
/// the same characters (`db`.`t` vs `d`.`bt`), and names with embedded NUL, must not collide - a plain
/// separator would let a tampered request be crafted to reuse a hash.
TEST(TablesStatusAuthDigest, EncodingIsUnambiguous)
{
    EXPECT_NE(makeRequest({{"db", "t"}}).getAuthDigest(), makeRequest({{"d", "bt"}}).getAuthDigest());
    EXPECT_NE(makeRequest({{"a", "b"}}).getAuthDigest(), makeRequest({{"ab", ""}}).getAuthDigest());
    /// Embedded-NUL cross-component ambiguity: (db="a", tbl="b\0c") vs (db="a\0b", tbl="c").
    EXPECT_NE(
        makeRequest({{std::string("a"), std::string("b\0c", 3)}}).getAuthDigest(),
        makeRequest({{std::string("a\0b", 3), std::string("c")}}).getAuthDigest());
    /// One table with a NUL-containing name must not collide with two separate tables.
    EXPECT_NE(
        makeRequest({{std::string("a"), std::string("b\0" "2:c", 5)}}).getAuthDigest(),
        makeRequest({{"a", "b"}, {"c", ""}}).getAuthDigest());
}
