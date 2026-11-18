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
    /// Not applied, because the precondition of the write did not hold: another write got there first.
    ConditionFailed,
    /// An attempt may have been applied, and nothing proves whether it was.
    Unknown,
};

/// What the attempts of a write that publishes an object told. The retry loops of the writer send the request again,
/// so the record covers all calls with it. The outcome of the write follows from the record.
struct AttemptsRecord
{
    enum class Proof : uint8_t
    {
        None,
        /// A success, or the idempotency id of this write in the object at the key.
        Applied,
        /// The precondition did not hold, and no attempt of this write may have been applied before; or the object of
        /// another write is at a key that only one write can create (`If-None-Match: *`).
        ConditionFailed,
    };

    /// An attempt got no answer that proves that it was not applied. It stays across the calls.
    bool may_have_been_applied = false;
    /// What the last call proved. Each call starts without a proof.
    Proof proof = Proof::None;

    [[nodiscard]] WriteOutcome outcome() const
    {
        switch (proof)
        {
            case Proof::Applied:
                return WriteOutcome::Applied;
            case Proof::ConditionFailed:
                return WriteOutcome::ConditionFailed;
            case Proof::None:
                break;
        }
        return may_have_been_applied ? WriteOutcome::Unknown : WriteOutcome::NotApplied;
    }
};

}
