#include <gtest/gtest.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/VariableContext.h>

using namespace DB;

TEST(MemoryTrackerBlockerInThread, Default)
{
    ASSERT_FALSE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::Process));
    MemoryTrackerBlockerInThread blocker;
    ASSERT_TRUE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::User));
    ASSERT_FALSE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::Global));
    ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::User);
}

TEST(MemoryTrackerBlockerInThread, Global)
{
    ASSERT_FALSE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::Process));
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    ASSERT_TRUE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::Global));
    ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Global);
}

TEST(MemoryTrackerBlockerInThread, Nested)
{
    {
        MemoryTrackerBlockerInThread blocker_global(VariableContext::Global);
        {
            MemoryTrackerBlockerInThread blocker_user(VariableContext::User);
            ASSERT_TRUE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::User));
            ASSERT_TRUE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::Global));
            ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Global);
        }
        ASSERT_TRUE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::Global));
        ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Global);
    }
    ASSERT_FALSE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::Process));
    ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Max);
}

TEST(MemoryTrackerBlockerInThread, DeeplyNested)
{
    MemoryTrackerBlockerInThread b(VariableContext::User);
    {
        MemoryTrackerBlockerInThread b1(VariableContext::Global);
        {
            MemoryTrackerBlockerInThread b2(VariableContext::User);
            {
                MemoryTrackerBlockerInThread b3(VariableContext::Global);
                {
                    MemoryTrackerBlockerInThread b4(VariableContext::User);
                    ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Global);
                }
                ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Global);
            }
            ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Global);
        }
        ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Global);
    }
    ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::User);
}

TEST(MemoryTrackerBlockerInThread, Move)
{
    {
        MemoryTrackerBlockerInThread blocker_global(VariableContext::Global);
        MemoryTrackerBlockerInThread blocker_user(VariableContext::User);

        blocker_user = std::move(blocker_global);
        ASSERT_TRUE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::Global));
        ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Global);
    }
    ASSERT_FALSE(MemoryTrackerBlockerInThread::isBlocked(VariableContext::Process));
    ASSERT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Max);
}
