#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Common/Exception.h>
#include <algorithm>
#include <limits>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int INVALID_STATE;
}
}

namespace DB::Cas
{

namespace
{

/// Live-lock brake, the same shape and for the same reason as `CasPlainObjects`': the deadline is the
/// real bound, and this only stops an unexpected continuous conflict from spinning until it elapses.
constexpr size_t MAX_CKPT_CAS_ATTEMPTS = 100;

/// The per-field semantic maximum for an OPTIONAL field: a present value beats an absent one (an
/// absence is "this writer knew nothing", never "this writer says none"), and two present values
/// resolve by the field's own order -- for `RefTxnId` that is writer_epoch then ref_sequence, the
/// intended timeline even across an epoch restart that resets the sequence.
template <typename T>
std::optional<T> maxKnown(const std::optional<T> & a, const std::optional<T> & b)
{
    if (!a)
        return b;
    if (!b)
        return a;
    return std::max(*a, *b);
}

std::optional<RefTxnId> mergeCommittedThrough(const RefCkpt & a, const RefCkpt & b)
{
    if (!a.committed_through)
        return b.committed_through;
    if (!b.committed_through)
        return a.committed_through;
    if (a.committed_through->writer_epoch == b.committed_through->writer_epoch)
        return std::max(*a.committed_through, *b.committed_through);

    const RefCkpt & higher = *a.committed_through < *b.committed_through ? b : a;
    const RefCkpt & lower = *a.committed_through < *b.committed_through ? a : b;
    if (lower.committed_through->writer_epoch + 1 != higher.committed_through->writer_epoch
        || !higher.last_epoch_seal
        || *higher.last_epoch_seal < *lower.committed_through
        || *higher.committed_through < *higher.last_epoch_seal)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS _ckpt: cross-epoch committed_through requires the immediately next writer epoch and "
            "a seal covering the lower frontier without exceeding the higher frontier");
    return higher.committed_through;
}

/// `life_epoch` MAY NOT DECREASE, and this is the whole of that rule. It lives HERE, at the publish
/// site, and deliberately not inside `mergeCkpt`: the merge is commutative, which is the stated reason
/// the two writers need no ordering between them, and a commutative function cannot even express this
/// rule -- it does not know which of its two arguments is the durable one. Here that distinction
/// exists, because `durable` came from this attempt's read and `contribution` is what the caller wants
/// to add.
///
/// The refusal is narrow ON PURPOSE, and the narrowness is what makes it stronger rather than weaker
/// than a rule against any disagreement. Two present-and-different values are ORDINARY: the two writers
/// that know a `life_epoch` derive it from different epochs that legitimately differ --
/// `completeCreation` from the catalog creator's `writer_epoch`, `commitRefChunk`'s birth chunk from the
/// `NamespaceBirth` record's -- so a resumed creation (`reconcileStaleCreator` handing a stalled
/// `Creating` entry to a later actor, same incarnation) and a plain restart between CREATE TABLE and the
/// first INSERT both raise the value honestly. Refusing THAT would wedge the namespace permanently:
/// `_ckpt` has no repair path and no writer may delete it outside namespace removal, so every retry
/// would re-read the old value, re-contribute the new one and re-throw.
///
/// A DECREASE is the case that cannot happen honestly, which is precisely why it is worth detecting.
/// `writer_epoch` is durable-monotone per server root (`allocateWriterEpoch` CAS-bumps
/// `<prefix>/gc/server-roots/<srid>/epoch`), and every live namespace is rooted at its own member's
/// `server_root_id`, so a namespace's creator and any actor that later reconciles it share ONE monotone
/// counter and a live actor's epoch always exceeds a terminal one's. A contribution below what is
/// durable therefore means a writer whose epoch is already superseded got its contribution through --
/// a fence violation, the class this subsystem cares most about. The semantic maximum absorbs it
/// silently, which is why the check cannot be left to the merge even setting commutativity aside.
///
/// (That argument is per-server-root. It would need revisiting if one namespace could ever be created
/// by two DIFFERENT server roots, whose epoch counters are independent and so unordered.)
///
/// It is a PREDICATE rather than a check that throws, because the caller has to consult the mount fence
/// between detecting this and classifying it: a writer the fence is about to refuse has landed nothing
/// anywhere, and reporting corruption for it would turn an expected transient control signal into a
/// permanent verdict. Only a STILL-ADMITTED writer contributing a superseded epoch is the violation this
/// detects.
bool lifeEpochWouldDecrease(const RefCkpt & durable, const RefCkpt & contribution)
{
    return durable.life_epoch && contribution.life_epoch && *contribution.life_epoch < *durable.life_epoch;
}

/// The verdict for a decrease by a writer that IS still admitted.
///
/// The message says the object cannot be repaired in place, because that is the part an operator cannot
/// work out from the numbers and cannot afford to guess: nothing rewrites `_ckpt` downwards, and no
/// writer deletes it outside namespace removal, so if the durable value is the wrong one then every
/// honest writer from here on contributes something lower and hits this same refusal forever. The
/// refusal is still right -- silently adopting a suspect genesis epoch would corrupt the epoch-seal
/// grammar for the life of the namespace -- but a fail-closed state with no in-place exit has to say so
/// where it fires, not leave the operator to discover it by retrying.
[[noreturn]] void throwLifeEpochDecrease(const RefCkpt & durable, const RefCkpt & contribution, const String & key)
{
    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "CAS {}: life_epoch may not decrease -- {} is durable and a still-admitted writer contributed {}. "
        "Writer epochs are monotone per server root, so a lower contribution means a superseded writer's "
        "work reached this object; refusing rather than taking the maximum. This object has NO in-place "
        "repair: it is never rewritten downwards and is deleted only by namespace removal, so if {} is "
        "itself the wrong value then every later writer will hit this same refusal and the namespace "
        "cannot be written again until it is recreated",
        key, *durable.life_epoch, *contribution.life_epoch, *durable.life_epoch);
}

}

RefCkpt mergeCkpt(const RefCkpt & a, const RefCkpt & b)
{
    RefCkpt merged;
    /// `life_epoch` is merged like every other field, by the same semantic maximum: taking it from
    /// either side by name is how a writer that knows nothing about it would erase it. In the steady
    /// state one side knows it and the other does not, and the max keeps the one that does. (It is not
    /// a namespace-lifetime constant -- see the field's own doc in `Formats/CasRefCkptFormat.h` -- but
    /// the values it legitimately takes only ever RISE, which is what makes the max right here and what
    /// lets `publishCkpt` refuse the fall separately.)
    merged.life_epoch = maxKnown(a.life_epoch, b.life_epoch);
    merged.committed_through = mergeCommittedThrough(a, b);
    merged.checkpoint_snapshot_id = maxKnown(a.checkpoint_snapshot_id, b.checkpoint_snapshot_id);
    merged.last_epoch_seal = maxKnown(a.last_epoch_seal, b.last_epoch_seal);
    /// Contributions may omit independent facts, but the resulting durable shape may not. In
    /// particular, if one writer supplies `life_epoch` and another supplies a later frontier, their
    /// merge must carry the chain evidence before `publishCkpt` can encode it.
    if (merged.committed_through)
        checkRefCkptInvariants(merged, "_ckpt merge");
    return merged;
}

RecoveryGrounding chooseRecoveryGrounding(const std::optional<CatalogEntry> & catalog_state,
                                          const std::optional<RefCkpt> & ckpt)
{
    if (!catalog_state || catalog_state->state == NsState::Creating)
        throw Exception(ErrorCodes::INVALID_STATE, "CAS recovery grounding: namespace is absent or Creating");
    if (catalog_state->state != NsState::Live && catalog_state->state != NsState::Removing)
        throw Exception(ErrorCodes::INVALID_STATE, "CAS recovery grounding: namespace has an unsupported lifecycle state");
    if (!ckpt || !ckpt->life_epoch)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS recovery grounding: a Live or Removing namespace requires a readable _ckpt with life_epoch");

    checkRefCkptInvariants(*ckpt, "recovery grounding");
    RecoveryGrounding result;
    result.committed_through = ckpt->committed_through;
    if (!result.committed_through)
    {
        if (ckpt->checkpoint_snapshot_id || ckpt->last_epoch_seal)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS recovery grounding: a checkpoint without committed_through cannot name a snapshot or epoch seal");
        return result;
    }

    if (ckpt->checkpoint_snapshot_id && ckpt->last_epoch_seal
        && *ckpt->checkpoint_snapshot_id == *ckpt->last_epoch_seal)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS recovery grounding: checkpoint_snapshot_id must not name last_epoch_seal");

    if (ckpt->checkpoint_snapshot_id)
        result.base = ckpt->checkpoint_snapshot_id;
    if (result.base)
    {
        if (result.base->ref_sequence == std::numeric_limits<uint64_t>::max())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS recovery grounding: checkpoint base has no representable successor");
        result.walk_from = RefTxnId{result.base->writer_epoch, result.base->ref_sequence + 1};
    }
    else if (!result.base)
    {
        result.walk_from = RefTxnId{*ckpt->life_epoch, 1};
    }
    return result;
}

std::optional<CkptSample> readCkpt(Backend & backend, const Layout & layout, const NamespaceLifeId & life)
{
    std::optional<GetResult> got = backend.get(layout.refCkptKey(life));
    if (!got)
        return std::nullopt;
    /// Materialized read, then decode: the object is MUTABLE, so the body must be fixed before it is
    /// parsed, and the token must be the one that labels exactly these bytes.
    return CkptSample{decodeRefCkpt(got->bytes), got->token};
}

CkptPublishOutcome publishCkpt(Backend & backend, const Layout & layout, const NamespaceLifeId & life,
                               const RefCkpt & contribution, uint64_t admitted_generation,
                               const std::function<void(uint64_t)> & check_fence_or_throw,
                               const CkptDeadline & deadline)
{
    const String key = layout.refCkptKey(life);
    std::optional<CkptSample> current;
    bool have_current = false;

    for (size_t attempt = 0; attempt < MAX_CKPT_CAS_ATTEMPTS; ++attempt)
    {
        if (deadline.now_ms() >= deadline.deadline_ms)
            break;

        /// Read the WHOLE body every attempt. A retry after a conflict must merge against what is
        /// there NOW: reusing the previous attempt's reading is precisely the read-modify-write with
        /// the merge left out, one round later.
        if (!have_current)
        {
            current = readCkpt(backend, layout, life);
            have_current = true;
        }

        /// The one rule the commutative merge cannot state, and it has to be decided HERE, before the
        /// merge: the semantic maximum turns a decrease into a body identical to the stored one, which
        /// the identical-skip below would return as a successful no-op. Detecting it after the merge
        /// would therefore detect nothing.
        ///
        /// The FENCE decides which of the two verdicts it gets, so it is consulted first. A writer the
        /// fence is about to refuse has landed nothing anywhere and gets `FencedOut` -- the same
        /// transient control signal every other refusal in this function returns rather than throws. A
        /// writer that is still admitted and yet contributing a superseded epoch is the fence violation
        /// this detects, and that one is corruption.
        if (current && lifeEpochWouldDecrease(current->ckpt, contribution))
        {
            try
            {
                check_fence_or_throw(admitted_generation);
            }
            catch (...)
            {
                return CkptPublishOutcome::FencedOut;
            }
            throwLifeEpochDecrease(current->ckpt, contribution, key);
        }

        /// ANY writer may create the object; none of them may invent a field. An absent `_ckpt` is
        /// created from the contribution as it stands, so a publisher that knows only the checkpoint
        /// creates one that knows only the checkpoint, and the birth transaction's `life_epoch` merges
        /// into it whenever it arrives -- in either order, because the merge is a per-field maximum.
        const RefCkpt merged = current ? mergeCkpt(current->ckpt, contribution) : contribution;

        /// Nothing new: return WITHOUT a CAS. This is not an optimization -- both writers publish on
        /// every snapshot and every seal, and most of those carry a checkpoint the object already has,
        /// so issuing the write anyway would mint a fresh token per no-op and turn every other writer's
        /// in-flight CAS into a conflict, for a body byte-identical to the one already stored.
        if (current && merged == current->ckpt)
        {
            try
            {
                check_fence_or_throw(admitted_generation);
            }
            catch (...)
            {
                return CkptPublishOutcome::FencedOut;
            }
            return CkptPublishOutcome::IdenticalSkip;
        }

        /// AFTER the read, BEFORE the CAS, on EVERY attempt (spec §3). A generation that moved since
        /// admission means this writer's lease incarnation is gone and the body it just merged is
        /// stale, so the CAS must never be sent -- and because the check precedes it, nothing was.
        try
        {
            check_fence_or_throw(admitted_generation);
        }
        catch (...)
        {
            /// Typed, not propagated: the caller asked "did this land", and "the fence moved, so
            /// nothing was sent" is an answer, not a failure of the operation. Only the fence check is
            /// wrapped, so nothing else can be mistaken for it.
            return CkptPublishOutcome::FencedOut;
        }

        const std::optional<Token> expected =
            current ? std::optional<Token>{current->token} : std::nullopt;
        /// Encode before entering the ambiguity catch. Allocation or invariant failures happen before
        /// any request is sent and must propagate as themselves, not trigger a needless resolution GET.
        const String merged_bytes = encodeRefCkpt(merged);
        try
        {
            if (backend.casPut(key, merged_bytes, expected).outcome == CasOutcome::Committed)
                return CkptPublishOutcome::Published;
        }
        catch (...)
        {
            /// A thrown CAS response does not say whether the object changed. Never retry its bytes
            /// from memory: first point-read the exact mutable object, including its fresh token. If
            /// that observation includes this contribution under the semantic join, the write is
            /// resolved durable; otherwise that exact observation is the only valid base for a retry.
            try
            {
                current = readCkpt(backend, layout, life);
                have_current = true;
            }
            catch (...)
            {
                throwCasWriteRetryLater("CAS _ckpt for namespace '" + life.ns.string()
                    + "': a CAS response was ambiguous and the mandatory exact-read resolution failed ("
                    + getCurrentExceptionMessage(/*with_stacktrace*/ false) + ")");
            }

            try
            {
                check_fence_or_throw(admitted_generation);
            }
            catch (...)
            {
                return CkptPublishOutcome::FencedOut;
            }

            if (current && lifeEpochWouldDecrease(current->ckpt, contribution))
                throwLifeEpochDecrease(current->ckpt, contribution, key);

            const RefCkpt resolved_merge = current ? mergeCkpt(current->ckpt, contribution) : contribution;
            if (current && resolved_merge == current->ckpt)
                return CkptPublishOutcome::Published;

            /// `current` is the exact observation made after the ambiguous response. The next loop
            /// iteration retries the SAME contribution against its token (or expected absence), with
            /// no blind CAS and no redundant intervening GET.
            continue;
        }
        /// `Conflict`: the incarnation we read is no longer current, so another writer's merge landed
        /// first. Nothing of ours was written; re-read and merge against the winner.
        current.reset();
        have_current = false;
    }

    /// Fail closed. Every attempt was all-or-nothing, so there is no partial state -- only an
    /// unpublished contribution, which the caller must be told about rather than left to assume.
    throwCasWriteRetryLater("CAS _ckpt for namespace '" + life.ns.string()
        + "': persistent CAS contention, the checkpoint contribution was not published");
}

MissingBaseVerdict classifyMissingSampledBase(const Token & sampled_token, const std::optional<Token> & current_token)
{
    if (current_token && !(*current_token == sampled_token))
        return MissingBaseVerdict::RestartRecovery;
    return MissingBaseVerdict::Corrupted;
}

bool snapshotDeletableUnderCkpt(const RefTxnId & snapshot_id, const std::optional<RefTxnId> & checkpoint_snapshot_id)
{
    return checkpoint_snapshot_id.has_value() && snapshot_id < *checkpoint_snapshot_id;
}

}
