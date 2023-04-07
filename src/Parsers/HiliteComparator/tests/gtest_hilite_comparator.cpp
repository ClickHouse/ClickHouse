#include <gtest/gtest.h>
#include <Parsers/IAST.h>
#include <Parsers/HiliteComparator/HiliteComparator.h>

using namespace HiliteComparator;

TEST(HiliteComparator, ConsumeHilites)
{
    using namespace DB;
    // The order is different from the order in HILITES on purpose.
    String s;
    s += IAST::hilite_keyword;
    s += IAST::hilite_alias;
    s += IAST::hilite_identifier;
    s += IAST::hilite_none;
    s += IAST::hilite_operator;
    s += IAST::hilite_substitution;
    s += IAST::hilite_function;
    s += "test";
    s += IAST::hilite_keyword;
    const char * ptr = s.c_str();
    const char * expected_ptr = strchr(ptr, 't');
    const char * last_hilite = nullptr;
    consume_hilites(ptr, &last_hilite);
    ASSERT_EQ(expected_ptr, ptr);
    ASSERT_TRUE(last_hilite != nullptr);
    ASSERT_EQ(IAST::hilite_function, last_hilite);
}

TEST(HiliteComparator, RemoveHilites)
{
    using namespace DB;
    String s;
    s += IAST::hilite_keyword;
    s += "te";
    s += IAST::hilite_alias;
    s += IAST::hilite_identifier;
    s += "s";
    s += IAST::hilite_none;
    s += "t";
    s += IAST::hilite_operator;
    s += IAST::hilite_substitution;
    s += IAST::hilite_function;
    ASSERT_EQ("test", remove_hilites(s));
}

TEST(HiliteComparator, AreEqualWithHilites)
{
    using namespace DB;
    String s = IAST::hilite_keyword;
    ASSERT_THROW(are_equal_with_hilites(s, s, true), std::logic_error);
    ASSERT_TRUE(are_equal_with_hilites(s, s, false));
}

TEST(HiliteComparator, AreEqualWithHilitesAndEndWithoutHilite)
{
    using namespace DB;

    ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, "", "");
    ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, "", IAST::hilite_none);
    ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, IAST::hilite_none, "");
    ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, IAST::hilite_none, IAST::hilite_none);

    {
        String s;
        s += IAST::hilite_none;
        s += "select";
        s += IAST::hilite_none;
        ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, s, "select");
    }

    {
        String s;
        s += DB::IAST::hilite_none;
        s += "\n sel";
        s += DB::IAST::hilite_none;
        s += "ect";
        s += DB::IAST::hilite_none;
        ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, s, "\n select");
    }

    {
        String left;
        left += DB::IAST::hilite_keyword;
        left += "keyword long";
        left += DB::IAST::hilite_none;

        String right;
        right += DB::IAST::hilite_keyword;
        right += "keyword";
        right += DB::IAST::hilite_none;
        right += " ";
        right += DB::IAST::hilite_keyword;
        right += "long";
        right += DB::IAST::hilite_none;
        ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, left, right);
    }
}
