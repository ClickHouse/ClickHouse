#include <gtest/gtest.h>

#include <thread>

#include <Common/FiberLocal.h>
#include <Common/MemoryTrackerBlockerInThread.h>

/// Tests that ensure `FiberLocalStorage` can load and store values on big-endian platforms correctly.
/// Slot defaults are written as a whole `uintptr_t` word, so small types must be read from its least significant
/// bytes. They used to be read from the first bytes on all platforms, which on big-endian turned the default of
/// `MemoryTrackerBlockerInThread` into zero and disabled memory limits.
/// Most of these tests can fail only on big-endian.

namespace
{

/// Any one-byte slot works for testing the typed accessors directly. The storage is thread-local and each
/// test runs on its own thread, so writing to a slot that another component normally owns affects nothing else.
constexpr size_t test_slot = FiberLocalSlot::LOCK_MEMORY_EXCEPTION_BLOCK_FAULT_INJECTIONS;

}

/// The slot of `MemoryTrackerBlockerInThread` has a non-zero default of a one-byte type. A thread that
/// has not set it must see `Max`, otherwise every memory tracker level is considered blocked and no limit is enforced.
TEST(FiberLocal, DefaultOfSmallTypeIsNotMisread)
{
    VariableContext level = VariableContext::Global;
    std::thread([&] { level = MemoryTrackerBlockerInThread::getLevel(); }).join();
    EXPECT_EQ(level, VariableContext::Max);
}

TEST(FiberLocal, DefaultDoesNotBlockAnyLevel)
{
    bool blocked = true;
    std::thread([&] { blocked = MemoryTrackerBlockerInThread::isBlocked(VariableContext::Thread); }).join();
    EXPECT_FALSE(blocked);
}

TEST(FiberLocal, StoreLoadRoundTripOfSmallTypes)
{
    std::thread(
        []
        {
            {
                MemoryTrackerBlockerInThread blocker(VariableContext::Process);
                EXPECT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Process);
            }
            EXPECT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Max);

            FiberLocalStorage::store<bool, test_slot>(true);
            EXPECT_TRUE((FiberLocalStorage::load<bool, test_slot>()));
            FiberLocalStorage::store<bool, test_slot>(false);
            EXPECT_FALSE((FiberLocalStorage::load<bool, test_slot>()));
        }).join();
}

/// The value is kept in the least significant bytes of the slot word, whatever the byte order.
TEST(FiberLocal, SmallValueIsTheNumericValueOfTheWord)
{
    std::thread(
        []
        {
            FiberLocalStorage::store<uint8_t, test_slot>(0x5a);
            EXPECT_EQ((FiberLocalStorage::load<uint64_t, test_slot>()), 0x5aULL);
            EXPECT_EQ((FiberLocalStorage::load<uint16_t, test_slot>()), 0x5a);

            FiberLocalStorage::store<uint16_t, test_slot>(0x1234);
            EXPECT_EQ((FiberLocalStorage::load<uint64_t, test_slot>()), 0x1234ULL);
            EXPECT_EQ((FiberLocalStorage::load<uint8_t, test_slot>()), 0x34);
        }).join();
}
