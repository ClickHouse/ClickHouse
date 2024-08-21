#include <Common/LockMemoryExceptionInThread.h>

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
