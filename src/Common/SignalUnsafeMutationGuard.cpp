#include <Common/SignalUnsafeMutationGuard.h>

namespace DB
{

SignalUnsafeMutationGuard::SignalUnsafeMutationGuard(std::atomic<bool> & is_usable_)
    : is_usable(is_usable_)
{
    is_usable.store(false, std::memory_order_relaxed);
    std::atomic_signal_fence(std::memory_order_seq_cst);
}

SignalUnsafeMutationGuard::~SignalUnsafeMutationGuard()
{
    std::atomic_signal_fence(std::memory_order_seq_cst);
    is_usable.store(true, std::memory_order_relaxed);
}

}
