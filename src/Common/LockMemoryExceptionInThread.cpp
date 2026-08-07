#include <Common/LockMemoryExceptionInThread.h>
#include <base/defines.h>

/// LockMemoryExceptionInThread
thread_local uint64_t LockMemoryExceptionInThread::counter = 0;
thread_local VariableContext LockMemoryExceptionInThread::level = VariableContext::Global;
thread_local bool LockMemoryExceptionInThread::block_fault_injections = false;
LockMemoryExceptionInThread::LockMemoryExceptionInThread(VariableContext level_, bool block_fault_injections_)
    : previous_level(level)
    , previous_block_fault_injections(block_fault_injections)
{
    ++counter;
    level = level_;
    block_fault_injections = block_fault_injections_;
}
LockMemoryExceptionInThread::~LockMemoryExceptionInThread()
{
    --counter;
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
