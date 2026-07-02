#pragma once

#include <atomic>
#include <utility>
#include <base/types.h>


namespace DB
{

/// Bounds the number of bcrypt verifications running concurrently across the whole process.
///
/// bcrypt is deliberately CPU-expensive, so an unauthenticated flood of *distinct* passwords
/// (which the per-credential bcrypt cache cannot absorb) can saturate every core. This limiter
/// caps the in-flight verification count so worst-case bcrypt CPU is bounded regardless of how
/// many distinct passwords an attacker tries.
///
/// A limit of 0 means unlimited (the default, preserving historical behavior).
/// Admission is fail-fast (never blocks): a thread that cannot get a slot is rejected immediately
/// rather than queued, so a flood cannot pile up connection threads and amplify the DoS.
class BcryptConcurrencyLimiter
{
public:
    /// RAII token returned by tryAcquire(). When acquired(), a slot is held and released on destruction.
    /// Move-only; an empty (default-constructed / moved-from) guard holds nothing.
    class Guard
    {
    public:
        Guard() = default;
        explicit Guard(BcryptConcurrencyLimiter * limiter_) : limiter(limiter_) {}

        Guard(Guard && other) noexcept : limiter(std::exchange(other.limiter, nullptr)) {}
        Guard & operator=(Guard && other) noexcept
        {
            if (this != &other)
            {
                release();
                limiter = std::exchange(other.limiter, nullptr);
            }
            return *this;
        }

        Guard(const Guard &) = delete;
        Guard & operator=(const Guard &) = delete;

        ~Guard() { release(); }

        bool acquired() const { return limiter != nullptr; }

    private:
        void release()
        {
            if (limiter)
            {
                limiter->in_flight.fetch_sub(1, std::memory_order_acq_rel);
                limiter = nullptr;
            }
        }

        BcryptConcurrencyLimiter * limiter = nullptr;
    };

    /// 0 = unlimited. Safe to call at any time (e.g. on config reload).
    void setLimit(UInt64 limit_) { limit.store(limit_, std::memory_order_relaxed); }
    UInt64 getLimit() const { return limit.load(std::memory_order_relaxed); }

    UInt64 getInFlight() const { return in_flight.load(std::memory_order_relaxed); }

    /// Reserves a slot. Returns an acquired() guard on success, or an empty guard when the limit is
    /// already reached. With limit == 0 it always succeeds. in_flight stays exactly equal to the
    /// number of held slots, so the bound holds at every instant (no transient over-count) and a
    /// concurrent flood is never under-rejected past the limit.
    Guard tryAcquire()
    {
        const UInt64 max = limit.load(std::memory_order_relaxed);
        UInt64 current = in_flight.load(std::memory_order_relaxed);
        for (;;)
        {
            if (max != 0 && current >= max)
                return Guard{};
            if (in_flight.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel, std::memory_order_relaxed))
                return Guard{this};
            /// current was reloaded by compare_exchange_weak; retry.
        }
    }

private:
    std::atomic<UInt64> limit{0};
    std::atomic<UInt64> in_flight{0};
};

}
