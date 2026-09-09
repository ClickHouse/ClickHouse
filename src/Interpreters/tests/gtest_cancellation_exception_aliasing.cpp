#include <gtest/gtest.h>

#include <exception>
#include <string>

#include <Common/Exception.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/IAST.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{

QueryStatusPtr makeQueryStatus(const String & query_id)
{
    ClientInfo client_info;
    client_info.current_query_id = query_id;
    Settings settings;
    return std::make_shared<QueryStatus>(
        getContext().context,
        "SELECT 1",
        /*normalized_query_hash_*/ 0,
        client_info,
        /*priority_handle_*/ QueryPriorities::Handle{},
        /*query_slot_*/ nullptr,
        /*memory_reservation_*/ nullptr,
        /*thread_group_*/ nullptr,
        IAST::QueryKind::Select,
        settings,
        /*watch_start_nanoseconds*/ 0,
        /*is_internal*/ false);
}

/// Stands in for the reason the cancelling caller's exception carries. Every thread of the query
/// must still see it, so it doubles as the positive control of each assertion below.
constexpr std::string_view cancellation_reason = "cancelled by the client";

std::exception_ptr makeCancellationException()
{
    return std::make_exception_ptr(Exception(ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT, "{}", cancellation_reason));
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

    for (size_t caller = 1; caller <= 3; ++caller)
    {
        try
        {
            query->throwIfKilled();
            FAIL() << "caller " << caller << ": expected throwIfKilled to throw";
        }
        catch (Exception & e)
        {
            const std::string message = e.message();

            EXPECT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT);
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
        const std::string message = e.message();

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

    /// A timeout reports the elapsed time instead of the stored exception, even when one is set.
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
