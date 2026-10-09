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
extern const int CANNOT_SCHEDULE_TASK;
extern const int QUERY_WAS_CANCELLED;
}

TEST(OperationCancellation, CleanupObservesEscalationWithoutClearingRequest)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto state = std::make_shared<OperationCancellationState>();
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [state] { return state->getRequest(); });
        ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
        EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
        state->request(CancellationRequest::StopWork);
        EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
        {
            ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
            EXPECT_EQ(CurrentThread::getCancellationRequest(), CancellationRequest::StopWork);
            EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            {
                ExecutionPhaseScope work(ExecutionPhase::Work);
                EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
            }
            EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            state->request(CancellationRequest::StopAll);
            EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
            {
                ThreadStatus::QueryCancellationBlocker blocker;
                EXPECT_EQ(CurrentThread::getCancellationRequest(), CancellationRequest::StopAll);
                EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            }
            EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
        }
        state->request(CancellationRequest::None);
        state->request(CancellationRequest::StopWork);
        EXPECT_EQ(state->getRequest(), CancellationRequest::StopAll);
        EXPECT_EQ(CurrentThread::getExecutionPhase(), ExecutionPhase::Work);
    }).get();
}

TEST(OperationCancellation, PhaseRestoresAcrossGroupSwitchAndException)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [] { return CancellationRequest::StopWork; });
        auto other = ThreadGroup::createForMergeMutate(getContext().context, [] { return CancellationRequest::StopWork; });
        ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
        {
            ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
            {
                ThreadGroupSwitcher same(group, ThreadName::MERGE_MUTATE);
                EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
            }
            try
            {
                ThreadGroupSwitcher switched(other, ThreadName::MERGE_MUTATE, true);
                EXPECT_EQ(CurrentThread::getExecutionPhase(), ExecutionPhase::Work);
                CurrentThread::checkIfNotCancelled();
                FAIL() << "Unrelated group inherited cleanup";
            }
            catch (const Exception &)
            {
                EXPECT_EQ(CurrentThread::getGroup(), group);
                EXPECT_EQ(CurrentThread::getExecutionPhase(), ExecutionPhase::Cleanup);
            }
            EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
        }
        EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
    }).get();
}

TEST(OperationCancellation, ShutdownLatchPreservesEscalationForNewEntries)
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
        merges.cancelAll(CancellationRequest::StopAll);
        merges.cancelAll(CancellationRequest::StopWork);
        EXPECT_EQ((*first)->cancellation_state->getRequest(), CancellationRequest::StopAll);
        auto second = merges.insert(StorageID("default", "cancellation_test"), part, getContext().context);
        ThreadGroupSwitcher attached((*second)->thread_group, ThreadName::MERGE_MUTATE);
        ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
        EXPECT_EQ(CurrentThread::getCancellationRequest(), CancellationRequest::StopAll);
        EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
    }).get();
}

TEST(OperationCancellation, QueuedPhaseIsCapturedButCancellationRemainsLive)
{
    for (bool use_local_runner : {false, true})
    {
        std::async(std::launch::async, [use_local_runner]
        {
            ThreadStatus status;
            auto state = std::make_shared<OperationCancellationState>();
            auto group = ThreadGroup::createForMergeMutate(getContext().context, [state] { return state->getRequest(); });
            auto escalated = std::make_shared<OperationCancellationState>();
            auto other = ThreadGroup::createForMergeMutate(getContext().context, [escalated] { return escalated->getRequest(); });
            ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
            ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
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
                EXPECT_EQ(CurrentThread::getExecutionPhase(), ExecutionPhase::Work);
                EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
            });
            std::future<void> cleanup_task;
            std::future<void> escalated_task;
            {
                ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
                cleanup_task = enqueue([]
                {
                    EXPECT_EQ(CurrentThread::getExecutionPhase(), ExecutionPhase::Cleanup);
                    EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
                    EXPECT_EQ(CurrentThread::getCancellationRequest(), CancellationRequest::StopWork);
                });
                ThreadGroupSwitcher switched(other, ThreadName::MERGE_MUTATE, true);
                ExecutionPhaseScope other_cleanup(ExecutionPhase::Cleanup);
                escalated_task = enqueue([]
                {
                    EXPECT_EQ(CurrentThread::getExecutionPhase(), ExecutionPhase::Cleanup);
                    EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
                    EXPECT_EQ(CurrentThread::getCancellationRequest(), CancellationRequest::StopAll);
                });
            }
            state->request(CancellationRequest::StopWork);
            escalated->request(CancellationRequest::StopAll);
            release.set_value();
            released = true;
            work.get();
            cleanup_task.get();
            escalated_task.get();
            enqueue([]
            {
                EXPECT_EQ(CurrentThread::getExecutionPhase(), ExecutionPhase::Work);
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
            state->request(CancellationRequest::StopAll);
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
            ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
            EXPECT_EQ(CurrentThread::getCancellationRequest(), CancellationRequest::StopAll);
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
        EXPECT_EQ(CurrentThread::getCancellationRequest(), CancellationRequest::StopWork);
        EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
        ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
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

TEST(OperationCancellation, CancellableAdmissionDoesNotWaitForUnrelatedWork)
{
    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
    std::promise<void> release_worker;
    auto gate = release_worker.get_future().share();
    pool.scheduleOrThrowOnError([gate] { gate.wait(); });

    std::atomic_bool canceled = false;
    std::atomic_bool job_ran = false;
    std::promise<void> checked;
    auto checked_future = checked.get_future();
    auto scheduler = std::async(
        std::launch::async,
        [&]
        {
            ThreadStatus status;
            bool first_check = true;
            auto group = ThreadGroup::createForMergeMutate(
                getContext().context, [&]
                {
                    if (first_check)
                    {
                        first_check = false;
                        checked.set_value();
                    }
                    return canceled.load() ? CancellationRequest::StopWork : CancellationRequest::None;
                });
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
            ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::ASYNC_COPY, /* check_cancellation */ true);
            try
            {
                runner.enqueueAndKeepTrack([&job_ran] { job_ran = true; });
            }
            catch (const Exception & exception)
            {
                return exception.code();
            }
            return 0;
        });

    /// Always release the unrelated worker, including when the cancellation check is missing.
    EXPECT_EQ(checked_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    canceled = true;
    const auto admission_status = scheduler.wait_for(std::chrono::seconds(5));
    release_worker.set_value();
    EXPECT_EQ(admission_status, std::future_status::ready);
    EXPECT_EQ(scheduler.get(), ErrorCodes::ABORTED);
    pool.wait();
    EXPECT_FALSE(job_ran);
}

TEST(OperationCancellation, DefaultAdmissionIgnoresCancellation)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            auto group = ThreadGroup::createForMergeMutate(getContext().context, [] { return CancellationRequest::StopWork; });
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
            ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
            ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::ASYNC_COPY);
            bool job_ran = false;
            runner.enqueueAndKeepTrack([&job_ran] { job_ran = true; });
            runner.waitForAllToFinishAndRethrowFirstError();
            EXPECT_TRUE(job_ran);
        })
        .get();
}

TEST(OperationCancellation, CancelledAdmissionDiscardsOwnQueuedWork)
{
    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1, 1, 2);
    std::promise<void> release_worker;
    auto gate = release_worker.get_future().share();
    pool.scheduleOrThrowOnError([gate] { gate.wait(); });
    std::atomic_bool canceled = false;
    std::atomic_bool job_ran = false;
    std::promise<void> queued;
    auto queued_future = queued.get_future();
    auto scheduler = std::async(
        std::launch::async,
        [&]
        {
            ThreadStatus status;
            auto group = ThreadGroup::createForMergeMutate(
                getContext().context, [&]
                {
                    return canceled.load() ? CancellationRequest::StopWork : CancellationRequest::None;
                });
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
            try
            {
                ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::ASYNC_COPY, /* check_cancellation */ true);
                runner.enqueueAndKeepTrack([&job_ran] { job_ran = true; });
                queued.set_value();
                runner.enqueueAndKeepTrack([&job_ran] { job_ran = true; });
            }
            catch (const Exception & exception)
            {
                return exception.code();
            }
            return 0;
        });

    EXPECT_EQ(queued_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    canceled = true;
    const auto admission_status = scheduler.wait_for(std::chrono::seconds(5));
    release_worker.set_value();
    EXPECT_EQ(admission_status, std::future_status::ready);
    EXPECT_EQ(scheduler.get(), ErrorCodes::ABORTED);
    pool.wait();
    EXPECT_FALSE(job_ran);
}

TEST(OperationCancellation, CancelledAdmissionRejectsWorkWithFreeCapacity)
{
    std::async(
        std::launch::async,
        []
        {
            ThreadStatus status;
            auto group = ThreadGroup::createForMergeMutate(getContext().context, [] { return CancellationRequest::StopWork; });
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
            ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
            ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::ASYNC_COPY, /* check_cancellation */ true);
            bool job_ran = false;
            EXPECT_THROW(runner.enqueueAndKeepTrack([&job_ran] { job_ran = true; }), Exception);
            runner.waitForAllToFinishAndRethrowFirstError();
            EXPECT_FALSE(job_ran);
        })
        .get();
}

TEST(OperationCancellation, CancellableAdmissionPreservesPoolErrors)
{
    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
    pool.finish();
    ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::ASYNC_COPY, /* check_cancellation */ true);
    try
    {
        runner.enqueueAndKeepTrack([] { });
        FAIL() << "A finished pool must reject scheduling";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::CANNOT_SCHEDULE_TASK);
    }
}

TEST(OperationCancellation, CancelledAdmissionDrainsRunningWork)
{
    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
    std::promise<void> release_worker;
    auto gate = release_worker.get_future().share();
    std::promise<void> started;
    auto started_future = started.get_future();
    std::promise<void> observed_cancellation;
    auto observed_future = observed_cancellation.get_future();
    std::atomic_bool cancellation_observed = false;
    std::atomic_bool canceled = false;
    std::atomic_bool finished = false;
    auto scheduler = std::async(
        std::launch::async,
        [&]
        {
            ThreadStatus status;
            auto group = ThreadGroup::createForMergeMutate(
                getContext().context, [&]
                {
                    if (!canceled.load())
                        return CancellationRequest::None;
                    if (!cancellation_observed.exchange(true))
                        observed_cancellation.set_value();
                    return CancellationRequest::StopWork;
                });
            ThreadGroupSwitcher scope(group, ThreadName::MERGE_MUTATE);
            try
            {
                ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::ASYNC_COPY, /* check_cancellation */ true);
                runner.enqueueAndKeepTrack(
                    [gate, &started, &finished]
                    {
                        started.set_value();
                        gate.wait();
                        finished = true;
                    });
                runner.enqueueAndKeepTrack([] { });
            }
            catch (const Exception & exception)
            {
                EXPECT_TRUE(finished);
                return exception.code();
            }
            return 0;
        });

    EXPECT_EQ(started_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    canceled = true;
    EXPECT_EQ(observed_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(scheduler.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    release_worker.set_value();
    EXPECT_EQ(scheduler.get(), ErrorCodes::ABORTED);
    pool.wait();
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
