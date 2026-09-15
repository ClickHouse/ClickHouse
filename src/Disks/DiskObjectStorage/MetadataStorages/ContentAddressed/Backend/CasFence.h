#pragma once
#include <cstdint>
#include <functional>

namespace DB::Cas
{

/// The mount fence a write is admitted under, expressed as three closures so a caller (or a test)
/// can swap in a real mount's fence or a fixed, always-open one without a virtual base class.
struct Fence
{
    enum class Admit : uint8_t { Ok, LostOrRearmed, NoBudget };

    /// The fence's current generation.
    std::function<uint64_t()> generation;
    /// Whether a write admitted under `admitted_generation`, expected to still be running
    /// `needed_ms` from now, may proceed.
    std::function<Admit(uint64_t admitted_generation, uint64_t needed_ms)> admit;
    /// Throw if `admitted_generation` is no longer the fence's live generation.
    std::function<void(uint64_t admitted_generation)> check_or_throw;

    /// A fence that never trips: generation 0 forever, `admit` always `Ok`, `check_or_throw` never
    /// throws. For backends with no mount lease to enforce (in-memory, tests).
    static Fence open();
};

inline Fence Fence::open()
{
    return Fence{
        []() -> uint64_t { return 0; },
        [](uint64_t, uint64_t) { return Fence::Admit::Ok; },
        [](uint64_t) {}};
}

}
