#pragma once

#include <cstdint>


namespace DB
{

/// The outcome of a write, decided by the answers to its attempts.
enum class WriteOutcome : uint8_t
{
    /// The write is applied: it succeeded, or an earlier attempt of it is proven to be applied.
    Applied,
    /// No attempt was applied, and no attempt can be applied later.
    NotApplied,
    /// An attempt may have been applied, and nothing proves whether it was.
    Unknown,
};

}
