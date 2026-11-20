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
    /// What a success or the idempotency id proves.
    enum class Proof : uint8_t
    {
        None,
        /// A success, or the idempotency id of this write in the object at the key.
        Applied,
        /// The object of another write is at a key that only one write can create (`If-None-Match: *`).
        ConditionFailed,
    };

    /// An attempt got no answer that proves that it was not applied. It stays across the calls.
    bool may_have_been_applied = false;
    /// The last answer of the last call refused the precondition of the write.
    bool precondition_refused = false;
    /// What the last call proved.
    Proof proof = Proof::None;

    /// Each call proves anew, and the attempts of earlier calls with the request stay.
    void startCall()
    {
        precondition_refused = false;
        proof = Proof::None;
    }

    /// A refusal after an attempt that may have been applied can be the answer to a replay of this write.
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
        if (may_have_been_applied)
            return WriteOutcome::Unknown;
        return precondition_refused ? WriteOutcome::ConditionFailed : WriteOutcome::NotApplied;
    }
};

}
