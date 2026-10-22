#include <gtest/gtest.h>

#include <Interpreters/ProcessList.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/PartitionActionBlocker.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/threadPoolCallbackRunner.h>

#include <atomic>
#include <future>
#include <stdexcept>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
}

namespace DB
{
namespace ErrorCodes
{
extern const int ABORTED;
extern const int QUERY_WAS_CANCELLED;
}

TEST(OperationCancellation, CleanupSuppressionRestoresAfterException)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto state = std::make_shared<OperationCancellationState>();
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [state] { return state->getRequest(); });
        ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
        state->request(CancellationRequest::StopWork);
        EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
        try
        {
            ThreadStatus::QueryCancellationBlocker cleanup;
            EXPECT_EQ(CurrentThread::get().getCancellationRequest(), CancellationRequest::StopWork);
            EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            {
                ThreadStatus::QueryCancellationBlocker nested;
                EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            }
            EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            throw std::runtime_error("cleanup failed");
        }
        catch (const std::runtime_error &)
        {
            EXPECT_FALSE(ThreadStatus::QueryCancellationBlocker::isActive());
        }
        state->request(CancellationRequest::None);
        EXPECT_EQ(state->getRequest(), CancellationRequest::StopWork);
        EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
    }).get();
}

TEST(OperationCancellation, ShutdownLatchCancelsNewEntries)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        MergeList merges;
        auto part = std::make_shared<FutureMergedMutatedPart>();
        part->part_info = MergeTreePartInfo("p", 1, 2, 1);
        part->name = part->part_info.getPartNameV1();
        auto first = merges.insert(StorageID("default", "cancellation_test"), part, getContext().context);
        merges.cancelAll();
        EXPECT_EQ((*first)->cancellation_state->getRequest(), CancellationRequest::StopWork);
        auto second = merges.insert(StorageID("default", "cancellation_test"), part, getContext().context);
        ThreadGroupSwitcher attached((*second)->thread_group, ThreadName::MERGE_MUTATE);
        EXPECT_EQ(CurrentThread::get().getCancellationRequest(), CancellationRequest::StopWork);
        EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
    }).get();
}

TEST(OperationCancellation, QueuedCleanupSuppressionDoesNotClearCancellation)
{
    for (bool use_local_runner : {false, true})
    {
        std::async(std::launch::async, [use_local_runner]
        {
            ThreadStatus status;
            auto state = std::make_shared<OperationCancellationState>();
            auto group = ThreadGroup::createForMergeMutate(getContext().context, [state] { return state->getRequest(); });
            ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
            /// Capacity includes the blocked worker and both callbacks queued behind it.
            ThreadPool pool(
                CurrentMetrics::LocalThread,
                CurrentMetrics::LocalThreadActive,
                CurrentMetrics::LocalThreadScheduled,
                /*max_threads=*/ 1,
                /*max_free_threads=*/ 1,
                /*queue_size=*/ 3);
            std::promise<void> release;
            auto gate = release.get_future().share();
            bool released = false;
            pool.scheduleOrThrowOnError([gate] { gate.wait(); });
            SCOPE_EXIT_SAFE({ if (!released) release.set_value(); });
            auto unsafe = threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::MERGE_MUTATE);
            ThreadPoolCallbackRunnerLocal<void> local(pool, ThreadName::MERGE_MUTATE);
            auto enqueue = [&](std::function<void()> callback)
            {
                if (use_local_runner)
                    return std::move(local.enqueueAndGiveOwnership(std::move(callback))->future);
                return unsafe(std::move(callback), {});
            };
            auto work = enqueue([]
            {
                EXPECT_FALSE(ThreadStatus::QueryCancellationBlocker::isActive());
                EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
            });
            std::future<void> cleanup_task;
            {
                ThreadStatus::QueryCancellationBlocker cleanup;
                cleanup_task = enqueue([]
                {
                    EXPECT_TRUE(ThreadStatus::QueryCancellationBlocker::isActive());
                    EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
                    EXPECT_EQ(CurrentThread::get().getCancellationRequest(), CancellationRequest::StopWork);
                });
            }
            state->request(CancellationRequest::StopWork);
            release.set_value();
            released = true;
            work.get();
            cleanup_task.get();
            enqueue([]
            {
                EXPECT_FALSE(ThreadStatus::QueryCancellationBlocker::isActive());
                EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
            }).get();
            pool.wait();
        }).get();
    }
}

TEST(OperationCancellation, QueryCancellationExceptionHasPriority)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            auto context = Context::createCopy(getContext().context);
            context->makeQueryContext();
            auto query = std::make_shared<QueryStatus>(
                context,
                "",
                0,
                context->getClientInfo(),
                std::nullopt,
                QueryPriorities::Handle{},
                QuerySlotPtr{},
                MemoryReservationPtr{},
                ThreadGroupPtr{},
                IAST::QueryKind::Select,
                context->getSettingsRef(),
                0,
                true);
            context->setProcessListElement(query);
            auto state = std::make_shared<OperationCancellationState>();
            state->request(CancellationRequest::StopWork);
            auto parent = ThreadGroup::createForMergeMutate(context, [state] { return state->getRequest(); });
            auto part = std::make_shared<FutureMergedMutatedPart>();
            part->part_info = MergeTreePartInfo("p", 1, 2, 1);
            part->name = part->part_info.getPartNameV1();
            MergeListElement entry(StorageID("default", "priority_test"), part, context, {}, parent);
            ActionBlocker ttl_blocker;
            ThreadGroupSwitcher scope(entry.thread_group, ThreadName::MERGE_MUTATE);
            entry.enableTTLCancellation(ttl_blocker);
            ttl_blocker.cancelForever();
            query->cancelQuery(CancelReason::CANCELLED_BY_USER);
            EXPECT_EQ(CurrentThread::get().getCancellationRequest(), CancellationRequest::StopWork);
            try
            {
                CurrentThread::checkIfNotCancelled();
                FAIL() << "Both query and operation are cancelled";
            }
            catch (const Exception & exception)
            {
                EXPECT_EQ(exception.code(), ErrorCodes::QUERY_WAS_CANCELLED);
            }
            context->setProcessListElement(nullptr);
        })
        .get();
}

TEST(OperationCancellation, MergeListCancellationReachesThreadGroup)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            auto future_part = std::make_shared<FutureMergedMutatedPart>();
            future_part->part_info = MergeTreePartInfo("p", 1, 2, 1);
            future_part->name = future_part->part_info.getPartNameV1();
            MergeListElement entry(StorageID("default", "cancellation_test"), future_part, getContext().context);
            ThreadGroupSwitcher scope(entry.thread_group, ThreadName::MERGE_MUTATE);
            EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            entry.cancellation_state->request(CancellationRequest::StopWork);
            EXPECT_TRUE(CurrentThread::get().isQueryCanceled());
            EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
        })
        .get();
}

TEST(OperationCancellation, MergeListGroupOutlivesEntry)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            auto future_part = std::make_shared<FutureMergedMutatedPart>();
            future_part->part_info = MergeTreePartInfo("p", 1, 2, 1);
            future_part->name = future_part->part_info.getPartNameV1();
            auto entry = std::make_unique<MergeListElement>(StorageID("default", "cancellation_test"), future_part, getContext().context);
            auto group = entry->thread_group;
            entry->cancellation_state->request(CancellationRequest::StopWork);
            entry.reset();
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
            EXPECT_TRUE(CurrentThread::get().isQueryCanceled());
            EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
        })
        .get();
}

TEST(OperationCancellation, TTLSourcePublishedAfterAttachmentOutlivesTaskSetup)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto part = std::make_shared<FutureMergedMutatedPart>();
        part->part_info = MergeTreePartInfo("p", 1, 2, 1);
        part->name = part->part_info.getPartNameV1();
        auto entry = std::make_unique<MergeListElement>(StorageID("default", "ttl_test"), part, getContext().context);
        auto group = entry->thread_group;
        ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
        {
            ActionBlocker ttl_blocker;
            entry->enableTTLCancellation(ttl_blocker);
            EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            ttl_blocker.cancelForever();
        }
        entry.reset();
        EXPECT_EQ(CurrentThread::get().getCancellationRequest(), CancellationRequest::StopWork);
        EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
        ThreadStatus::QueryCancellationBlocker cleanup;
        EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
    }).get();
}

TEST(OperationCancellation, MergeListBlockerCancellationLatches)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            PartitionActionBlocker blocker;
            auto future_part = std::make_shared<FutureMergedMutatedPart>();
            future_part->part_info = MergeTreePartInfo("p", 1, 2, 1);
            future_part->name = future_part->part_info.getPartNameV1();
            MergeListElement entry(
                StorageID("default", "cancellation_test"), future_part, getContext().context, blocker.getCancellationRequestPredicate("p"));
            ThreadGroupSwitcher scope(entry.thread_group, ThreadName::MERGE_MUTATE);
            auto other = blocker.cancelForPartition("other");
            EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            {
                auto lock = blocker.cancelForPartition("p");
                EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
                EXPECT_EQ(entry.cancellation_state->getRequest(), CancellationRequest::StopWork);
            }
            EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
        })
        .get();
}

TEST(OperationCancellation, AttachedThreadObservesCancellation)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            auto canceled = std::make_shared<std::atomic_bool>(false);
            auto group = ThreadGroup::createForMergeMutate(
                getContext().context, [canceled]
                {
                    return canceled->load() ? CancellationRequest::StopWork : CancellationRequest::None;
                });
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);

            EXPECT_FALSE(CurrentThread::get().isQueryCanceled());
            EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            canceled->store(true);
            EXPECT_TRUE(CurrentThread::get().isQueryCanceled());
            try
            {
                CurrentThread::checkIfNotCancelled();
                FAIL() << "Operation cancellation must throw";
            }
            catch (const Exception & exception)
            {
                EXPECT_EQ(exception.code(), ErrorCodes::ABORTED);
            }
        })
        .get();
}

TEST(OperationCancellation, WorkerUsesEnqueueContextAndRestoresIt)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1, 1, 10);
            auto canceled = std::make_shared<std::atomic_bool>(false);
            auto group = ThreadGroup::createForMergeMutate(
                getContext().context, [canceled]
                {
                    return canceled->load() ? CancellationRequest::StopWork : CancellationRequest::None;
                });
            std::weak_ptr<ThreadGroup> weak_group = group;
            auto runner = threadPoolCallbackRunnerUnsafe<bool>(pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
            std::promise<void> release_worker;
            auto gate = release_worker.get_future().share();
            std::future<bool> result;
            {
                ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
                result = runner(
                    [gate]
                    {
                        gate.wait();
                        try
                        {
                            CurrentThread::checkIfNotCancelled();
                        }
                        catch (const Exception & exception)
                        {
                            return exception.code() == ErrorCodes::ABORTED;
                        }
                        return false;
                    },
                    Priority{});
            }
            group.reset();
            canceled->store(true);
            release_worker.set_value();
            EXPECT_TRUE(result.get());
            pool.wait();
            EXPECT_TRUE(weak_group.expired());

            /// Reuse the same pool thread for unrelated work; it must not retain cancellation.
            auto unrelated = ThreadGroup::createForMergeMutate(getContext().context);
            ThreadGroupSwitcher scope(unrelated, ThreadName::MERGE_MUTATE);
            EXPECT_FALSE(runner(
                             []
                             {
                                 CurrentThread::checkIfNotCancelled();
                                 return CurrentThread::get().isQueryCanceled();
                             },
                             Priority{})
                             .get());
        })
        .get();
}

}
