#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <stdexcept>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int STD_EXCEPTION;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{
size_t getLocalErrorCount(int code)
{
    return ErrorCodes::values[code].get().local.count;
}
}

TEST(Exception, RecordToSystemErrorsOnlyRecordsSuppressedExceptions)
{
    const auto suppressed_count = getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT);
    Exception suppressed;
    {
        Exception::SuppressErrorCodesScope scope;
        suppressed = Exception(ErrorCodes::CANNOT_PARSE_TEXT, "suppressed");

        EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), suppressed_count);
        suppressed.recordToSystemErrors();
        EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), suppressed_count + 1);
        suppressed.recordToSystemErrors();
        EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), suppressed_count + 1);
    }

    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), suppressed_count + 1);

    const auto unrecorded_count = getLocalErrorCount(ErrorCodes::STD_EXCEPTION);
    const std::runtime_error std_exception("not recorded");
    Exception unrecorded(Exception::CreateFromSTDTag{}, std_exception);

    EXPECT_EQ(getLocalErrorCount(ErrorCodes::STD_EXCEPTION), unrecorded_count);
    unrecorded.recordToSystemErrors();
    EXPECT_EQ(getLocalErrorCount(ErrorCodes::STD_EXCEPTION), unrecorded_count);

    const auto recorded_count = getLocalErrorCount(ErrorCodes::UNSUPPORTED_METHOD);
    Exception recorded(ErrorCodes::UNSUPPORTED_METHOD, "already recorded");

    EXPECT_EQ(getLocalErrorCount(ErrorCodes::UNSUPPORTED_METHOD), recorded_count + 1);
    recorded.recordToSystemErrors();
    EXPECT_EQ(getLocalErrorCount(ErrorCodes::UNSUPPORTED_METHOD), recorded_count + 1);
}

}
