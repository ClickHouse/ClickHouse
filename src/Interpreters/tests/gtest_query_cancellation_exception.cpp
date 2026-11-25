#include <gtest/gtest.h>

#include <future>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/IAST.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int FAULT_INJECTED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{
struct Query
{
    ContextMutablePtr context = Context::createCopy(getContext().context);
    QueryStatusPtr status;

    Query()
    {
        context->makeQueryContext();
        status = std::make_shared<QueryStatus>(
            context,
            "SELECT 1",
            /*normalized_query_hash_*/ 0,
            ClientInfo{},
            /*user_id_*/ std::nullopt,
            QueryPriorities::Handle{},
            nullptr,
            nullptr,
            nullptr,
            IAST::QueryKind::Select,
            Settings{},
            0,
            false);
        context->setProcessListElement(status);
    }
};

std::exception_ptr customException()
{
    return std::make_exception_ptr(Exception(ErrorCodes::FAULT_INJECTED, "Custom cancellation"));
}

std::exception_ptr thrownCancellation()
{
    try
    {
        CurrentThread::checkIfNotCancelled();
    }
    catch (...)
    {
        return std::current_exception();
    }
    return {};
}
}

TEST(QueryCancellationException, StandardCodesWithoutQuery)
{
    std::async(std::launch::async, []
    {
        ASSERT_FALSE(CurrentThread::isInitialized());
        EXPECT_FALSE(CurrentThread::isQueryCancellationException(customException()));
        for (int code : {ErrorCodes::QUERY_WAS_CANCELLED, ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT})
        {
            auto exception = std::make_exception_ptr(Exception(code, "Cancellation"));
            EXPECT_TRUE(CurrentThread::isQueryCancellationException(exception));
            ThreadStatus::QueryCancellationBlocker blocker;
            EXPECT_TRUE(CurrentThread::isQueryCancellationException(exception));
        }
    }).get();
}

TEST(QueryCancellationException, OperationTimeoutWithoutQuery)
{
    std::async(std::launch::async, []
    {
        ASSERT_FALSE(CurrentThread::isInitialized());
        auto timeout = std::make_exception_ptr(Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Operation timed out"));
        EXPECT_FALSE(CurrentThread::isQueryCancellationException(timeout));
        ThreadStatus::QueryCancellationBlocker blocker;
        EXPECT_FALSE(CurrentThread::isQueryCancellationException(timeout));
    }).get();
}

TEST(QueryCancellationException, OperationTimeoutRacingWithQueryTimeout)
{
    std::async(std::launch::async, []
    {
        ThreadStatus thread_status;
        Query query;
        auto group = std::make_shared<ThreadGroup>(query.context, 0);
        ThreadGroupSwitcher switcher(group, ThreadName::REMOTE_FS_READ_THREAD_POOL);

        try
        {
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Operation timed out");
        }
        catch (...)
        {
            auto operation_timeout = std::current_exception();
            EXPECT_FALSE(CurrentThread::isQueryCancellationException(operation_timeout));

            query.status->cancelQuery(CancelReason::TIMEOUT);
            auto query_timeout = thrownCancellation();
            ASSERT_NE(query_timeout, nullptr);
            EXPECT_EQ(getExceptionErrorCode(query_timeout), ErrorCodes::TIMEOUT_EXCEEDED);
            EXPECT_TRUE(query.status->isStoredCancellationException(query_timeout));
            EXPECT_TRUE(CurrentThread::isQueryCancellationException(query_timeout));
            EXPECT_FALSE(query.status->isStoredCancellationException(operation_timeout));
            EXPECT_FALSE(CurrentThread::isQueryCancellationException(operation_timeout));
            EXPECT_EQ(std::current_exception(), operation_timeout);

            auto next_query_timeout = thrownCancellation();
            ASSERT_NE(next_query_timeout, nullptr);
            EXPECT_NE(next_query_timeout, query_timeout);
            EXPECT_TRUE(CurrentThread::isQueryCancellationException(next_query_timeout));
            EXPECT_TRUE(CurrentThread::isQueryCancellationException(query_timeout));
        }
    }).get();
}

TEST(QueryCancellationException, IdentityBlockerAndThreadGroup)
{
    std::async(std::launch::async, []
    {
        ThreadStatus thread_status;
        Query first;
        Query second;
        auto exception = customException();
        auto same_exception = exception; // NOLINT(performance-unnecessary-copy-initialization) - intentionally testing copied exception identity
        auto other_exception = customException();
        auto first_group = std::make_shared<ThreadGroup>(first.context, 0);
        auto second_group = std::make_shared<ThreadGroup>(second.context, 0);
        EXPECT_FALSE(CurrentThread::isQueryCancellationException(exception));
        {
            ThreadGroupSwitcher first_switcher(first_group, ThreadName::REMOTE_FS_READ_THREAD_POOL);
            ASSERT_EQ(CurrentThread::getGroup(), first_group);
            EXPECT_EQ(thrownCancellation(), nullptr);
            EXPECT_FALSE(CurrentThread::isQueryCancellationException(exception));
            first.status->cancelQuery(CancelReason::CANCELLED_BY_USER, exception);
            auto propagated_exception = thrownCancellation();
            ASSERT_NE(propagated_exception, nullptr);
            EXPECT_NE(propagated_exception, exception);
            EXPECT_TRUE(CurrentThread::isQueryCancellationException(propagated_exception));
            EXPECT_TRUE(CurrentThread::isQueryCancellationException(same_exception));
            EXPECT_FALSE(CurrentThread::isQueryCancellationException(other_exception));
            {
                ThreadStatus::QueryCancellationBlocker blocker;
                EXPECT_EQ(thrownCancellation(), nullptr);
                EXPECT_FALSE(CurrentThread::isQueryCancellationException(exception));
                auto standard = std::make_exception_ptr(Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Cancellation"));
                EXPECT_TRUE(CurrentThread::isQueryCancellationException(standard));
            }
            {
                ThreadGroupSwitcher second_switcher(second_group, ThreadName::REMOTE_FS_READ_THREAD_POOL, true);
                ASSERT_EQ(CurrentThread::getGroup(), second_group);
                EXPECT_EQ(thrownCancellation(), nullptr);
                EXPECT_FALSE(CurrentThread::isQueryCancellationException(exception));
                second.status->cancelQuery(CancelReason::CANCELLED_BY_USER, other_exception);
                auto other_propagated_exception = thrownCancellation();
                ASSERT_NE(other_propagated_exception, nullptr);
                EXPECT_NE(other_propagated_exception, other_exception);
                EXPECT_TRUE(CurrentThread::isQueryCancellationException(other_propagated_exception));
                EXPECT_FALSE(CurrentThread::isQueryCancellationException(exception));
                EXPECT_TRUE(CurrentThread::isQueryCancellationException(other_exception));
            }
            ASSERT_EQ(CurrentThread::getGroup(), first_group);
            EXPECT_TRUE(CurrentThread::isQueryCancellationException(exception));
        }
        EXPECT_EQ(CurrentThread::getGroup(), nullptr);
        EXPECT_FALSE(CurrentThread::isQueryCancellationException(exception));

        auto context = Context::createCopy(getContext().context);
        context->makeQueryContext();
        auto group_without_status = std::make_shared<ThreadGroup>(context, 0);
        ThreadGroupSwitcher switcher(group_without_status, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        ASSERT_EQ(CurrentThread::getGroup(), group_without_status);
        EXPECT_FALSE(CurrentThread::isQueryCancellationException(exception));
        context.reset();
        ASSERT_EQ(CurrentThread::tryGetQueryContext(), nullptr);
        EXPECT_EQ(thrownCancellation(), nullptr);
        EXPECT_FALSE(CurrentThread::isQueryCancellationException(exception));
    }).get();
}

TEST(QueryCancellationException, TimeoutOverridesStoredException)
{
    std::async(std::launch::async, []
    {
        ThreadStatus thread_status;
        Query query;
        auto exception = customException();
        auto group = std::make_shared<ThreadGroup>(query.context, 0);
        ThreadGroupSwitcher switcher(group, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        ASSERT_EQ(CurrentThread::getGroup(), group);
        query.status->cancelQuery(CancelReason::TIMEOUT, exception);
        auto timeout = thrownCancellation();
        ASSERT_NE(timeout, nullptr);
        EXPECT_NE(timeout, exception);
        EXPECT_EQ(getExceptionErrorCode(timeout), ErrorCodes::TIMEOUT_EXCEEDED);
        EXPECT_TRUE(CurrentThread::isQueryCancellationException(timeout));
        EXPECT_FALSE(CurrentThread::isQueryCancellationException(exception));
    }).get();
}

TEST(QueryCancellationException, CopiesPreserveCancellationOrigin)
{
    std::async(std::launch::async, []
    {
        ThreadStatus thread_status;
        for (auto reason : {CancelReason::TIMEOUT, CancelReason::CANCELLED_BY_USER})
        {
            Query query;
            auto group = std::make_shared<ThreadGroup>(query.context, 0);
            ThreadGroupSwitcher switcher(group, ThreadName::REMOTE_FS_READ_THREAD_POOL);
            query.status->cancelQuery(reason, customException());
            auto original = thrownCancellation();
            ASSERT_NE(original, nullptr);

            try
            {
                std::rethrow_exception(original);
            }
            catch (const Exception & e)
            {
                auto check = [&](const std::exception_ptr & copy)
                {
                    EXPECT_NE(copy, original);
                    EXPECT_TRUE(CurrentThread::isQueryCancellationException(copy));
                };

                check(copyMutableException(original));
                check(std::make_exception_ptr(e));
                Exception assigned;
                assigned = e;
                check(std::make_exception_ptr(std::move(assigned)));
                std::unique_ptr<Poco::Exception> cloned(e.clone());
                try
                {
                    cloned->rethrow();
                }
                catch (...)
                {
                    check(std::current_exception());
                }
                auto unrelated = std::make_exception_ptr(Exception::createRuntime(e.code(), e.message()));
                EXPECT_FALSE(CurrentThread::isQueryCancellationException(unrelated));
            }
        }
    }).get();
}

TEST(QueryCancellationException, BlockerFollowsFiberAndRestoresNestedState)
{
    std::async(std::launch::async, []
    {
        EXPECT_FALSE(ThreadStatus::QueryCancellationBlocker::isActive());
        {
            ThreadStatus::QueryCancellationBlocker outer;
            {
                ThreadStatus::QueryCancellationBlocker inner;
                EXPECT_TRUE(ThreadStatus::QueryCancellationBlocker::isActive());
            }
            EXPECT_TRUE(ThreadStatus::QueryCancellationBlocker::isActive());

            auto other_fiber = FiberLocalStorage::create();
            FiberLocalStorage::swap(*other_fiber);
            EXPECT_FALSE(ThreadStatus::QueryCancellationBlocker::isActive());
            FiberLocalStorage::swap(*other_fiber);
            EXPECT_TRUE(ThreadStatus::QueryCancellationBlocker::isActive());
        }
        EXPECT_FALSE(ThreadStatus::QueryCancellationBlocker::isActive());
    }).get();
}

}
