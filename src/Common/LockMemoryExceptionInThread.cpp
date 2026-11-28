#include <Common/LockMemoryExceptionInThread.h>
#include <base/defines.h>

/// LockMemoryExceptionInThread
constinit FiberLocal<uint64_t, FiberLocalSlot::LOCK_MEMORY_EXCEPTION_COUNTER> LockMemoryExceptionInThread::counter;
constinit FiberLocal<VariableContext, FiberLocalSlot::LOCK_MEMORY_EXCEPTION_LEVEL> LockMemoryExceptionInThread::level;
constinit FiberLocal<bool, FiberLocalSlot::LOCK_MEMORY_EXCEPTION_BLOCK_FAULT_INJECTIONS> LockMemoryExceptionInThread::block_fault_injections;
LockMemoryExceptionInThread::LockMemoryExceptionInThread(VariableContext level_, bool block_fault_injections_)
    : previous_level(level)
    , previous_block_fault_injections(block_fault_injections)
{
    counter = counter + 1;
    level = level_;
    block_fault_injections = block_fault_injections_;
}
LockMemoryExceptionInThread::~LockMemoryExceptionInThread()
{
    counter = counter - 1;
    level = previous_level;
    block_fault_injections = previous_block_fault_injections;
}

void LockMemoryExceptionInThread::addUniqueLock(VariableContext level_, bool block_fault_injections_)
{
    chassert(counter == 0);
    counter = 1;
    level = level_;
    block_fault_injections = block_fault_injections_;
}

void LockMemoryExceptionInThread::removeUniqueLock()
{
    chassert(counter == 1);
    counter = 0;
    level = VariableContext::Global;
    block_fault_injections = false;
}
