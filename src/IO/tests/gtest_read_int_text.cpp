#include <gtest/gtest.h>

#include <IO/readIntText.h>
#include <iostream>


namespace DB::ErrorCodes
{
    extern const int OK;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

using namespace DB;

template <int base, typename T>
int getErrorOfParseIntInBase(std::string_view text)
{
    try
    {
        parseIntInBase<base, T>(text);
        return ErrorCodes::OK;
    }
    catch (Exception & e)
    {
        return e.code();
    }
}

TEST(ReadIntTextTest, parseIntInBase)
{
    EXPECT_EQ((parseIntInBase<2, int>("1101")), 13);
    EXPECT_EQ((parseIntInBase<8, int>("1101")), 577);
    EXPECT_EQ((parseIntInBase<10, int>("1101")), 1101);
    EXPECT_EQ((parseIntInBase<16, int>("1101")), 4353);

    EXPECT_EQ((getErrorOfParseIntInBase<2, int>("125")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((parseIntInBase<8, int>("125")), 85);
    EXPECT_EQ((parseIntInBase<10, int>("125")), 125);
    EXPECT_EQ((parseIntInBase<16, int>("125")), 293);

    EXPECT_EQ((getErrorOfParseIntInBase<2, int>("1F")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((getErrorOfParseIntInBase<8, int>("1F")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((getErrorOfParseIntInBase<10, int>("1F")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((parseIntInBase<16, int>("1F")), 31);

    EXPECT_EQ((getErrorOfParseIntInBase<2, int>("Ab01")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((getErrorOfParseIntInBase<8, int>("Ab01")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((getErrorOfParseIntInBase<10, int>("Ab01")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((parseIntInBase<16, int>("Ab01")), 43777);
}
