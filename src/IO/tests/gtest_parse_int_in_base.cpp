#include <gtest/gtest.h>

#include <IO/readIntTextInBase.h>

#include <iostream>

namespace DB::ErrorCodes
{
    extern const int OK;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

using namespace DB;

template <typename T, int base>
int getErrorOfParseIntInBase(std::string_view text)
{
    try
    {
        parseIntInBase<T, base>(text);
        return ErrorCodes::OK;
    }
    catch (Exception & e)
    {
        return e.code();
    }
}

TEST(ParseIntInBaseTest, parseIntInBase)
{
    EXPECT_EQ((parseIntInBase<int, 2>("1101")), 13);
    EXPECT_EQ((parseIntInBase<int, 8>("1101")), 577);
    EXPECT_EQ((parseIntInBase<int, 10>("1101")), 1101);
    EXPECT_EQ((parseIntInBase<int, 16>("1101")), 4353);

    EXPECT_EQ((getErrorOfParseIntInBase<int, 2>("125")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((parseIntInBase<int, 8>("125")), 85);
    EXPECT_EQ((parseIntInBase<int, 10>("125")), 125);
    EXPECT_EQ((parseIntInBase<int, 16>("125")), 293);

    EXPECT_EQ((getErrorOfParseIntInBase<int, 2>("1F")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((getErrorOfParseIntInBase<int, 8>("1F")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((getErrorOfParseIntInBase<int, 10>("1F")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((parseIntInBase<int, 16>("1F")), 31);

    EXPECT_EQ((getErrorOfParseIntInBase<int, 2>("Ab01")), ErrorCodes::CANNOT_PARSE_NUMBER);
    EXPECT_EQ((getErrorOfParseIntInBase<int, 8>("Ab01")), ErrorCodes::CANNOT_PARSE_NUMBER);
    EXPECT_EQ((getErrorOfParseIntInBase<int, 10>("Ab01")), ErrorCodes::CANNOT_PARSE_NUMBER);
    EXPECT_EQ((parseIntInBase<int, 16>("Ab01")), 43777);
}
