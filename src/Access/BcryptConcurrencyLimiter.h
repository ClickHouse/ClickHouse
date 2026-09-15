#pragma once

#include <atomic>
#include <utility>
#include <base/types.h>


namespace DB
{

/// Bounds the number of bcrypt verifications running concurrently across the whole process.
///
/// A limit of 0 means unlimited. Admission is fail-fast and never blocks: a thread that cannot get
/// a slot is rejected immediately rather than queued.
class BcryptConcurrencyLimiter
{
public:
    /// RAII token returned by `tryAcquire`. While `acquired`, a slot is held; it is released on
    /// destruction. Move-only; an empty (default-constructed / moved-from) guard holds nothing.
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

    /// 0 = unlimited. Thread-safe: a new value applies from the next `tryAcquire` onwards.
    void setLimit(UInt64 limit_) { limit.store(limit_, std::memory_order_relaxed); }
    UInt64 getLimit() const { return limit.load(std::memory_order_relaxed); }

    UInt64 getInFlight() const { return in_flight.load(std::memory_order_relaxed); }

    /// Reserves a slot. Returns an `acquired` guard on success, or an empty guard when the limit is
    /// already reached; with a limit of 0 it always succeeds. `in_flight` stays exactly equal to the
    /// number of held slots, so the bound holds at every instant.
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
