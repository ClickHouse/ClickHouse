#pragma once

namespace DB::Runtime::Wyrd
{

/// Marks a scope in which the adversary may suspend the calling thread to test lock-freedom of the others.
class LockFreeScope
{
public:
    LockFreeScope();
    ~LockFreeScope();

    LockFreeScope(const LockFreeScope &) = delete;
    LockFreeScope & operator=(const LockFreeScope &) = delete;
};

/// Reports that the calling thread completed an operation, which lets the adversary resume a suspended thread.
void progress();

}
