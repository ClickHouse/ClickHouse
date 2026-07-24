#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <stdexcept>
#include <thread>

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

[[noreturn]] void throwFromSuppressedHelper()
{
    try
    {
        Exception::SuppressErrorCodesScope scope;
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "helper failed");
    }
    catch (Exception & e)
    {
        e.recordToSystemErrors();
        throw;
    }
}
}

TEST(Exception, RecordToSystemErrorsRespectsSuppression)
{
    const auto suppressed_count = getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT);
    Exception suppressed;
    {
        Exception::SuppressErrorCodesScope scope;
        suppressed = Exception(ErrorCodes::CANNOT_PARSE_TEXT, "suppressed");

        EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), suppressed_count);
        suppressed.recordToSystemErrors();
        EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), suppressed_count);
    }

    suppressed.recordToSystemErrors();
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

TEST(Exception, RecordingWaitsForOutermostSuppressionScope)
{
    const auto count = getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT);
    Exception nested;

    {
        Exception::SuppressErrorCodesScope outer_scope;
        {
            Exception::SuppressErrorCodesScope inner_scope;
            nested = Exception(ErrorCodes::CANNOT_PARSE_TEXT, "nested");
        }

        nested.recordToSystemErrors();
        EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count);
    }

    nested.recordToSystemErrors();
    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count + 1);
}

TEST(Exception, OutermostCallerControlsRecording)
{
    const auto count = getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT);

    try
    {
        Exception::SuppressErrorCodesScope outer_scope;
        throwFromSuppressedHelper();
    }
    catch (const Exception &)
    {
        /// The outer caller handled the helper failure with a successful fallback.
    }

    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count);

    try
    {
        throwFromSuppressedHelper();
    }
    catch (const Exception &)
    {
        /// Without an outer suppression scope, the helper records before propagation.
    }

    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count + 1);
}

TEST(Exception, ForcedRecordingBypassesNestedSuppression)
{
    const auto count = getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT);
    Exception nested;

    {
        Exception::SuppressErrorCodesScope outer_scope;
        {
            Exception::SuppressErrorCodesScope inner_scope;
            nested = Exception(ErrorCodes::CANNOT_PARSE_TEXT, "unexpected consumed error");
        }

        nested.recordToSystemErrors(/* force */ true);
        EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count + 1);
    }

    nested.recordToSystemErrors(/* force */ true);
    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count + 1);
}

TEST(Exception, SuppressionIsThreadLocal)
{
    const auto count = getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT);

    Exception suppressed;
    {
        Exception::SuppressErrorCodesScope scope;
        suppressed = Exception(ErrorCodes::CANNOT_PARSE_TEXT, "suppressed on current thread");
        std::thread other_thread([] { Exception(ErrorCodes::CANNOT_PARSE_TEXT, "recorded on another thread"); });
        other_thread.join();
    }

    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count + 1);
    suppressed.recordToSystemErrors();
    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count + 2);
}

}
