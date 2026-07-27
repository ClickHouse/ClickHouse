#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <barrier>
#include <memory>
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
    const auto count = getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT); // NOLINT(clang-analyzer-deadcode.DeadStores)

    try
    {
        Exception::SuppressErrorCodesScope outer_scope;
        throwFromSuppressedHelper();
    }
    catch (const Exception &) // NOLINT(bugprone-empty-catch)
    {
        /// The outer caller handled the helper failure with a successful fallback.
    }

    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count);

    try
    {
        throwFromSuppressedHelper();
    }
    catch (const Exception &) // NOLINT(bugprone-empty-catch)
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

TEST(Exception, SuppressedCopiesRecordOnce)
{
    const auto count = getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT);
    Exception original;
    {
        Exception::SuppressErrorCodesScope scope;
        original = Exception(ErrorCodes::CANNOT_PARSE_TEXT, "copied suppressed exception");
    }

    Exception copy(original);
    Exception assigned;
    assigned = original;
    std::unique_ptr<Exception> cloned(original.clone());

    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count);
    try
    {
        original.rethrow();
    }
    catch (Exception & rethrown)
    {
        rethrown.recordToSystemErrors();
    }

    original.recordToSystemErrors();
    copy.recordToSystemErrors();
    assigned.recordToSystemErrors();
    cloned->recordToSystemErrors();
    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count + 1);
}

TEST(Exception, MovingSuppressedExceptionTransfersRecording)
{
    const auto count = getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT);
    Exception source;
    {
        Exception::SuppressErrorCodesScope scope;
        source = Exception(ErrorCodes::CANNOT_PARSE_TEXT, "moved suppressed exception");
    }

    Exception moved(std::move(source));
    source.recordToSystemErrors();
    moved.recordToSystemErrors();
    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count + 1);

    Exception assigned_source;
    {
        Exception::SuppressErrorCodesScope scope;
        assigned_source = Exception(ErrorCodes::CANNOT_PARSE_TEXT, "move-assigned suppressed exception");
    }

    Exception assigned;
    assigned = std::move(assigned_source);
    assigned_source.recordToSystemErrors();
    assigned.recordToSystemErrors();
    EXPECT_EQ(getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT), count + 2);
}

TEST(Exception, ConcurrentRecordingIsIdempotent)
{
    const auto count = getLocalErrorCount(ErrorCodes::CANNOT_PARSE_TEXT);
    Exception suppressed;
    {
        Exception::SuppressErrorCodesScope scope;
        suppressed = Exception(ErrorCodes::CANNOT_PARSE_TEXT, "concurrently recorded exception");
    }

    constexpr size_t num_threads = 16;
    std::barrier<> start(num_threads);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&]
        {
            start.arrive_and_wait();
            suppressed.recordToSystemErrors();
        });
    }

    for (auto & thread : threads)
        thread.join();

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
