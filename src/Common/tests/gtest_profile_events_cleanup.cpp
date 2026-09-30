#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/LocalDirectorySyncGuard.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>

#include <gtest/gtest.h>

#include <memory>
#include <thread>

#if defined(OS_LINUX)
#include <fcntl.h>
#include <cerrno>
#endif

namespace ProfileEvents
{
    extern const Event TextIndexLazyAdvanceCount;
    extern const Event DirectorySync;
    extern const Event MetadataTransactionRollbacks;
    extern const Event MetadataTransactionRollbacksFailed;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

TEST(ProfileEventsCleanup, PostingCursorPreservesPublishingScopeAfterThreadHandoff)
{
    const auto event = ProfileEvents::TextIndexLazyAdvanceCount;
    std::unique_ptr<PostingListCursor> cursor;
    ThreadGroupPtr creator_group;
    std::thread creator([&]
    {
        ThreadStatus status;
        creator_group = std::make_shared<ThreadGroup>(getContext().context, 0);
        CurrentThread::attachToGroupIfDetached(creator_group);
        auto values = std::make_shared<PaddedPODArray<UInt32>>();
        values->push_back(1);
        values->push_back(3);
        cursor = std::make_unique<PostingListCursor>(std::move(values));
        cursor->advance(3);
        CurrentThread::detachFromGroupIfNotDetached();
    });
    creator.join();
    EXPECT_EQ(creator_group->performance_counters[event], 0);

    std::thread publisher([&]
    {
        ThreadStatus status;
        auto publisher_group = std::make_shared<ThreadGroup>(getContext().context, 0);
        CurrentThread::attachToGroupIfDetached(publisher_group);
        {
            ProfileEventsScope scope;
            {
                DENY_ALLOCATIONS_IN_SCOPE;
                cursor.reset();
            }
            EXPECT_EQ(CurrentThread::getProfileEvents()[event], 1);
        }
        EXPECT_EQ(CurrentThread::getProfileEvents()[event], 1);
        EXPECT_EQ(publisher_group->performance_counters[event], 1);
        CurrentThread::detachFromGroupIfNotDetached();
    });
    publisher.join();
    EXPECT_EQ(creator_group->performance_counters[event], 0);
}

#if defined(OS_LINUX)
TEST(ProfileEventsCleanup, DirectorySyncPreservesPublishingScopeAndClosesDescriptor)
{
    const auto event = ProfileEvents::DirectorySync;
    std::unique_ptr<LocalDirectorySyncGuard> guard;
    ThreadGroupPtr creator_group;
    int fd = -1;
    std::thread creator([&]
    {
        ThreadStatus status;
        creator_group = std::make_shared<ThreadGroup>(getContext().context, 0);
        CurrentThread::attachToGroupIfDetached(creator_group);
        fd = ::open(".", O_RDONLY | O_DIRECTORY);
        ASSERT_GE(fd, 0);
        guard = std::make_unique<LocalDirectorySyncGuard>(fd);
        CurrentThread::detachFromGroupIfNotDetached();
    });
    creator.join();
    ASSERT_NE(guard, nullptr);

    std::thread publisher([&]
    {
        ThreadStatus status;
        auto publisher_group = std::make_shared<ThreadGroup>(getContext().context, 0);
        CurrentThread::attachToGroupIfDetached(publisher_group);
        {
            ProfileEventsScope scope;
            {
                DENY_ALLOCATIONS_IN_SCOPE;
                guard.reset();
            }
            EXPECT_EQ(CurrentThread::getProfileEvents()[event], 1);
        }
        EXPECT_EQ(CurrentThread::getProfileEvents()[event], 1);
        EXPECT_EQ(publisher_group->performance_counters[event], 1);
        errno = 0;
        EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
        CurrentThread::detachFromGroupIfNotDetached();
    });
    publisher.join();
    EXPECT_EQ(creator_group->performance_counters[event], 0);
}
#endif

TEST(ProfileEventsCleanup, MetadataRollbackPreservesOriginalFailure)
{
    struct FailingOperation : IMetadataOperation
    {
        bool fail_undo;
        bool & undo_called;

        FailingOperation(bool fail_undo_, bool & undo_called_)
            : fail_undo(fail_undo_), undo_called(undo_called_)
        {
        }

        void execute() override
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata execute failure");
        }

        void undo() override
        {
            undo_called = true;
            if (fail_undo)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata undo failure");
        }
    };

    for (bool fail_undo : {false, true})
    {
        std::thread worker([&]
        {
            ThreadStatus status;
            bool undo_called = false;
            MetadataOperationsHolder holder;
            holder.addOperation(std::make_unique<FailingOperation>(fail_undo, undo_called));
            try
            {
                holder.commit();
                FAIL() << "The failed operation must propagate its exception";
            }
            catch (const Exception & exception)
            {
                EXPECT_NE(exception.message().find("metadata execute failure"), std::string::npos);
            }
            EXPECT_TRUE(undo_called);
            EXPECT_EQ(CurrentThread::getProfileEvents()[ProfileEvents::MetadataTransactionRollbacks], 1);
            EXPECT_EQ(CurrentThread::getProfileEvents()[ProfileEvents::MetadataTransactionRollbacksFailed], fail_undo ? 1 : 0);
        });
        worker.join();
    }
}

}
