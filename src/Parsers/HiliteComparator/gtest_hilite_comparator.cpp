#include <gtest/gtest.h>
#include <Parsers/IAST.h>
#include <Parsers/HiliteComparator/HiliteComparator.h>


TEST(FormatHiliting, MetaTestConsumeHilites)
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
    const char * last_hilite = consume_hilites(&ptr);
    ASSERT_EQ(expected_ptr, ptr);
    ASSERT_TRUE(last_hilite != nullptr);
    ASSERT_EQ(IAST::hilite_function, last_hilite);
}

TEST(FormatHiliting, MetaTestRemoveHilites)
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

TEST(FormatHiliting, MetaTestAreEqualWithHilites)
{
    using namespace DB;
    ASSERT_PRED2(are_equal_with_hilites, "", "");

    for (const char * hilite : HILITES)
    {
        ASSERT_PRED2(are_equal_with_hilites, "", std::string_view(hilite));
        ASSERT_PRED2(are_equal_with_hilites, std::string_view(hilite), "");
    }

    {
        String s;
        s += IAST::hilite_none;
        s += "select";
        s += IAST::hilite_none;
        ASSERT_PRED2(are_equal_with_hilites, s, "select");
    }

    {
        String s;
        s += DB::IAST::hilite_none;
        s += "\n sel";
        s += DB::IAST::hilite_none;
        s += "ect";
        s += DB::IAST::hilite_none;
        ASSERT_PRED2(are_equal_with_hilites, s, "\n select");
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
        ASSERT_PRED2(are_equal_with_hilites, left, right);
    }
}