#include <gtest/gtest.h>
#include <Parsers/IAST.h>
#include <Parsers/HiliteComparator/HiliteComparator.h>

using namespace HiliteComparator;

TEST(HiliteComparator, ConsumeHilites)
{
    using namespace DB;
    // The order is different from the order in HILITES on purpose.
    String s;
    s += IAST::FormattingBuffer::hilite_keyword;
    s += IAST::FormattingBuffer::hilite_alias;
    s += IAST::FormattingBuffer::hilite_identifier;
    s += IAST::FormattingBuffer::hilite_none;
    s += IAST::FormattingBuffer::hilite_operator;
    s += IAST::FormattingBuffer::hilite_substitution;
    s += IAST::FormattingBuffer::hilite_function;
    s += "test";
    s += IAST::FormattingBuffer::hilite_keyword;
    const char * ptr = s.c_str();
    const char * expected_ptr = strchr(ptr, 't');
    const char * last_hilite = nullptr;
    consume_hilites(ptr, &last_hilite);
    ASSERT_EQ(expected_ptr, ptr);
    ASSERT_TRUE(last_hilite != nullptr);
    ASSERT_EQ(IAST::FormattingBuffer::hilite_function, last_hilite);
}

TEST(HiliteComparator, RemoveHilites)
{
    using namespace DB;
    String s;
    s += IAST::FormattingBuffer::hilite_keyword;
    s += "te";
    s += IAST::FormattingBuffer::hilite_alias;
    s += IAST::FormattingBuffer::hilite_identifier;
    s += "s";
    s += IAST::FormattingBuffer::hilite_none;
    s += "t";
    s += IAST::FormattingBuffer::hilite_operator;
    s += IAST::FormattingBuffer::hilite_substitution;
    s += IAST::FormattingBuffer::hilite_function;
    ASSERT_EQ("test", remove_hilites(s));
}

TEST(HiliteComparator, AreEqualWithHilites)
{
    using namespace DB;
    String s = IAST::FormattingBuffer::hilite_keyword;
    ASSERT_THROW(are_equal_with_hilites(s, s, true), std::logic_error);
    ASSERT_TRUE(are_equal_with_hilites(s, s, false));
}

TEST(HiliteComparator, AreEqualWithHilitesAndEndWithoutHilite)
{
    using namespace DB;

    ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, "", "");
    ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, "", IAST::FormattingBuffer::hilite_none);
    ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, IAST::FormattingBuffer::hilite_none, "");
    ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, IAST::FormattingBuffer::hilite_none, IAST::FormattingBuffer::hilite_none);

    {
        String s;
        s += IAST::FormattingBuffer::hilite_none;
        s += "select";
        s += IAST::FormattingBuffer::hilite_none;
        ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, s, "select");
    }

    {
        String s;
        s += IAST::FormattingBuffer::hilite_none;
        s += "\n sel";
        s += IAST::FormattingBuffer::hilite_none;
        s += "ect";
        s += IAST::FormattingBuffer::hilite_none;
        ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, s, "\n select");
    }

    {
        String left;
        left += IAST::FormattingBuffer::hilite_keyword;
        left += "keyword long";
        left += IAST::FormattingBuffer::hilite_none;

        String right;
        right += IAST::FormattingBuffer::hilite_keyword;
        right += "keyword";
        right += IAST::FormattingBuffer::hilite_none;
        right += " ";
        right += IAST::FormattingBuffer::hilite_keyword;
        right += "long";
        right += IAST::FormattingBuffer::hilite_none;
        ASSERT_PRED2(are_equal_with_hilites_and_end_without_hilite, left, right);
    }
}
