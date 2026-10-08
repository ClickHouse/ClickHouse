#pragma once

#include <atomic>

namespace DB
{

/// Marks signal-observed data unusable during mutation and usable again on destruction.
class SignalUnsafeMutationGuard
{
public:
    explicit SignalUnsafeMutationGuard(std::atomic<bool> & is_usable_);
    ~SignalUnsafeMutationGuard();

private:
    std::atomic<bool> & is_usable;
};

}
