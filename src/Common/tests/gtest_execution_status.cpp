#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <base/scope_guard.h>
#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

using namespace DB;

namespace
{

size_t getLocalErrorCount(int code)
{
    return ErrorCodes::values[code].get().local.count;
}

}

/// tryDeserializeText must be atomic: on a malformed payload it returns false and leaves the target
/// unchanged. deserializeText reads `code` before it can fail on the rest of the payload, so a
/// non-atomic implementation would leave a partially-overwritten status (e.g. "0garbage" -> code=0).
/// Callers that keep a sentinel and read it back on failure (getExecutionStatus / the Replicated DDL
/// status cross-check) rely on this.

TEST(ExecutionStatus, TryDeserializeTextKeepsTargetOnMalformedInput)
{
    for (const auto & bad :
        {std::string("0garbage"), std::string("0"), std::string("garbage"), std::string(""), std::string("+")})
    {
        ExecutionStatus status(-1, "Cannot obtain error message");
        EXPECT_FALSE(status.tryDeserializeText(bad)) << "payload: " << bad;
        EXPECT_EQ(status.code, -1) << "payload: " << bad;
        EXPECT_EQ(status.message, "Cannot obtain error message") << "payload: " << bad;
    }
}

TEST(ExecutionStatus, TryDeserializeTextRoundTrip)
{
    ExecutionStatus original(42, "boom");

    ExecutionStatus parsed(-1, "Cannot obtain error message");
    EXPECT_TRUE(parsed.tryDeserializeText(original.serializeText()));
    EXPECT_EQ(parsed.code, 42);
    EXPECT_EQ(parsed.message, "boom");
}

TEST(ExecutionStatus, TryDeserializeTextDoesNotRecordHandledError)
{
    const std::string malformed = "0";
    int error_code = 0;
    try
    {
        ExecutionStatus status;
        status.deserializeText(malformed);
        FAIL() << "Expected malformed status to throw";
    }
    catch (const Exception & e)
    {
        error_code = e.code();
    }

    const auto count = getLocalErrorCount(error_code);
    ExecutionStatus status;
    EXPECT_FALSE(status.tryDeserializeText(malformed));
    EXPECT_EQ(getLocalErrorCount(error_code), count);

    try
    {
        status.deserializeText(malformed);
        FAIL() << "Expected malformed status to throw";
    }
    catch (const Exception &) // NOLINT(bugprone-empty-catch)
    {
    }
    EXPECT_EQ(getLocalErrorCount(error_code), count + 1);
}

TEST(ExecutionStatus, TryDeserializeTextRethrowsUnexpectedError)
{
    const auto count = getLocalErrorCount(ErrorCodes::BAD_ARGUMENTS);
    auto previous_callback = std::move(Exception::callback);
    SCOPE_EXIT({ Exception::callback = std::move(previous_callback); });

    bool injecting_error = false;
    Exception::callback = [&](std::string_view, int, bool, const Exception::Trace &)
    {
        if (!injecting_error)
        {
            injecting_error = true;
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Injected unexpected deserialization error");
        }
    };

    ExecutionStatus status(-1, "Cannot obtain error message");
    try
    {
        status.tryDeserializeText("0");
        FAIL() << "Expected unexpected status deserialization error to propagate";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }

    EXPECT_EQ(getLocalErrorCount(ErrorCodes::BAD_ARGUMENTS), count + 1);
    EXPECT_EQ(status.code, -1);
    EXPECT_EQ(status.message, "Cannot obtain error message");
}
