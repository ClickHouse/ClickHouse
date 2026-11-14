#include <gtest/gtest.h>

#include <exception>
#include <string>

#include <Common/Exception.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Stopwatch.h>
#include <Common/re2.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/IAST.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int FAULT_INJECTED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{

QueryStatusPtr makeQueryStatus(const String & query_id, const Settings & settings = {}, UInt64 watch_start_ns = 0)
{
    ClientInfo client_info;
    client_info.current_query_id = query_id;
    return std::make_shared<QueryStatus>(
        getContext().context,
        "SELECT 1",
        /*normalized_query_hash_*/ 0,
        client_info,
        /*user_id_*/ std::nullopt,
        /*priority_handle_*/ QueryPriorities::Handle{},
        /*query_slot_*/ nullptr,
        /*memory_reservation_*/ nullptr,
        /*thread_group_*/ nullptr,
        IAST::QueryKind::Select,
        settings,
        watch_start_ns,
        /*is_internal*/ false);
}

/// Stands in for the reason the cancelling caller's exception carries. Every thread of the query
/// must still see it, so it doubles as the positive control of each assertion below.
constexpr std::string_view cancellation_reason = "custom cancellation reason";

std::exception_ptr makeCancellationException()
{
    return std::make_exception_ptr(Exception(ErrorCodes::FAULT_INJECTED, "{}", cancellation_reason));
}

/// Mimics the context `MergeTreeReaderCompact` appends to whatever it catches.
std::string callerSuffix(size_t caller)
{
    return fmt::format("(while reading column c{})", caller);
}

}

/// A cancellation check rethrows the exception passed to `cancelQuery`, and the
/// `catch (Exception & e) { e.addMessage(...); throw; }` idiom up the stack mutates what it catches.
/// So a caller must never be handed an object another caller can still decorate.
TEST(QueryStatusCancellationException, EachThrowIfKilledCallerGetsItsOwnException)
{
    auto query = makeQueryStatus("gtest_cancellation_exception_consumers");
    query->cancelQuery(CancelReason::CANCELLED_BY_USER, makeCancellationException());

    std::exception_ptr previous_exception;
    for (size_t caller = 1; caller <= 3; ++caller)
    {
        try
        {
            query->throwIfKilled();
            FAIL() << "caller " << caller << ": expected throwIfKilled to throw";
        }
        catch (Exception & e)
        {
            auto current_exception = std::current_exception();
            EXPECT_TRUE(query->isStoredCancellationException(current_exception));
            EXPECT_NE(current_exception, previous_exception);
            previous_exception = current_exception;
            const std::string message = e.message();

            EXPECT_EQ(e.code(), ErrorCodes::FAULT_INJECTED);
            EXPECT_NE(message.find(cancellation_reason), std::string::npos)
                << "caller " << caller << " lost the cancellation reason: " << message;

            for (size_t other = 1; other < caller; ++other)
                EXPECT_EQ(message.find(callerSuffix(other)), std::string::npos)
                    << "caller " << caller << " inherited caller " << other << "'s appended context: " << message;

            e.addMessage(callerSuffix(caller));
        }
    }
}

/// The producer of the exception keeps its own reference and decorates it too:
/// `BackupCoordinationStageSync` passes `state.hosts.at(...).exception` to `cancelQuery` and
/// rethrows that same pointer itself at three other sites.
TEST(QueryStatusCancellationException, StoredExceptionDoesNotAliasTheCallersObject)
{
    auto query = makeQueryStatus("gtest_cancellation_exception_producer");

    auto produced = makeCancellationException();
    query->cancelQuery(CancelReason::CANCELLED_BY_USER, produced);
    EXPECT_TRUE(query->isStoredCancellationException(produced));

    try
    {
        std::rethrow_exception(produced);
    }
    catch (Exception & e)
    {
        e.addMessage("(producer context)");
    }

    try
    {
        query->throwIfKilled();
        FAIL() << "expected throwIfKilled to throw";
    }
    catch (Exception & e)
    {
        EXPECT_TRUE(query->isStoredCancellationException(std::current_exception()));
        const std::string & message = e.message();

        EXPECT_NE(message.find(cancellation_reason), std::string::npos)
            << "the consumer lost the cancellation reason: " << message;
        EXPECT_EQ(message.find("(producer context)"), std::string::npos)
            << "the stored exception aliases the producer's object: " << message;
    }
}

/// The two branches that never read the stored exception must keep reporting their own error code.
TEST(QueryStatusCancellationException, BranchesWithoutAStoredExceptionAreUnchanged)
{
    auto cancelled_without_exception = makeQueryStatus("gtest_cancellation_exception_none");
    cancelled_without_exception->cancelQuery(CancelReason::CANCELLED_BY_USER);
    try
    {
        cancelled_without_exception->throwIfKilled();
        FAIL() << "expected throwIfKilled to throw";
    }
    catch (Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED);
    }

    /// A timeout takes precedence over the exception supplied by the caller.
    auto timed_out = makeQueryStatus("gtest_cancellation_exception_timeout");
    timed_out->cancelQuery(CancelReason::TIMEOUT, makeCancellationException());
    try
    {
        timed_out->throwIfKilled();
        FAIL() << "expected throwIfKilled to throw";
    }
    catch (Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TIMEOUT_EXCEEDED);
    }
}

TEST(QueryStatusCancellationException, CachedTimeoutIncludesElapsedTimeForEitherFirstCaller)
{
    Settings settings;
    settings.set("max_execution_time", 1);
    const RE2 message_pattern(R"(Timeout exceeded: elapsed ([0-9]+\.[0-9]{3}) ms, maximum: 1000\.000 ms)");

    for (bool worker_first : {true, false})
    {
        SCOPED_TRACE(worker_first ? "throwIfKilled first" : "checkTimeLimit first");
        /// Bracket the start of execution. The two seconds spent waiting for admission must be excluded.
        const Stopwatch before_admission(CLOCK_MONOTONIC);
        const auto start_ns = before_admission.getStart() - 2'000'000'000ULL;
        auto query = makeQueryStatus("gtest_cached_timeout", settings, start_ns);
        const Stopwatch after_admission(CLOCK_MONOTONIC);
        query->cancelQuery(CancelReason::TIMEOUT);
        const auto min_elapsed_ms = static_cast<double>(after_admission.elapsedMicroseconds()) / 1000;

        std::string first_message;
        for (bool use_worker_path : {worker_first, !worker_first})
        {
            try
            {
                if (use_worker_path)
                    query->throwIfKilled();
                else
                    query->checkTimeLimit();
                FAIL() << "Expected a query timeout";
            }
            catch (const Exception & e)
            {
                ASSERT_EQ(e.code(), ErrorCodes::TIMEOUT_EXCEEDED);
                double elapsed_ms = 0;
                ASSERT_TRUE(RE2::FullMatch(e.message(), message_pattern, &elapsed_ms)) << e.message();
                EXPECT_GE(elapsed_ms, min_elapsed_ms);
                EXPECT_LE(elapsed_ms, static_cast<double>(before_admission.elapsedMicroseconds() + 1) / 1000);
                if (first_message.empty())
                    first_message = e.message();
                else
                    EXPECT_EQ(e.message(), first_message);
            }
        }
    }
}
