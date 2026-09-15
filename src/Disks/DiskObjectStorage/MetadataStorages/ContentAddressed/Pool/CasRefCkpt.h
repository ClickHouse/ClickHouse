#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <cstdint>
#include <optional>

namespace DB::Cas
{

/// The pure, catalog-authoritative input to both recovery entry points. The checkpoint supplies every
/// recovery boundary; `LIST` cannot supply a base, life epoch, frontier, or stopping condition.
struct RecoveryGrounding
{
    std::optional<RefTxnId> base;
    std::optional<RefTxnId> walk_from;
    std::optional<RefTxnId> committed_through;
};

/// Choose recovery's finite exact range from catalog lifecycle authority and `_ckpt`. Throws
/// `INVALID_STATE` for absent/Creating names and `CORRUPTED_DATA` when a Live/Removing life lacks
/// the checkpoint or genesis fact it must have. This helper performs no I/O and never trusts LIST.
RecoveryGrounding chooseRecoveryGrounding(const std::optional<CatalogEntry> & catalog_state,
                                          const std::optional<RefCkpt> & ckpt);

/// THE update and read algorithms for one namespace's `_ckpt` object (spec INV-4). The object itself
/// and its codec live in `Formats/CasRefCkptFormat.h`; this header is what WRITES it and what a reader
/// consults when the base it sampled turns out to be gone.

/// The one semantic-maximum merge, shared by BOTH `_ckpt` writers (the snapshot publisher and the
/// sealer). Per field: the greater `life_epoch`, the greater present `checkpoint_snapshot_id`, the
/// greater present `last_epoch_seal`; an absent optional loses to a present one, and two absents stay
/// absent.
///
/// There is deliberately ONE of these rather than a surgical per-writer update. A `_ckpt` write is a
/// whole-body read-modify-write, so a writer that wrote back only the field it knows about would carry
/// its STALE reading of the other field along with it and silently regress the other writer's
/// progress. That regression is not hypothetical: it is TLC counterexample `_sab_sealclobbersbase`,
/// where a sealer writing its sampled body back verbatim drops a concurrently published base and the
/// next recovery loses an ACKED transaction.
///
/// Compatible contributions still merge commutatively, but the committed frontier is deliberately not
/// an unconstrained CRDT maximum: a cross-epoch pair must be numerically adjacent and carry its seal
/// evidence. That makes arbitrary regrouping of a corrupt historical set invalid, while the actual
/// publish protocol remains simple: each write merges one contribution with the one durable body it
/// just read.
///
/// It is therefore NOT where `life_epoch`'s may-not-decrease rule lives, and that is a placement
/// decision rather than an omission: a commutative function does not know which of its arguments is the
/// durable one, so it cannot tell a decrease from an increase. That rule belongs to `publishCkpt`, which
/// does know.
RefCkpt mergeCkpt(const RefCkpt & a, const RefCkpt & b);

/// What one `publishCkpt` call did.
enum class CkptPublishOutcome : uint8_t
{
    Published,      /// the contribution is durable, and this call sent at least one write before it
                    /// was; which write made it durable -- this one or a competitor's -- is not claimed
    IdenticalSkip,  /// the contribution added nothing to what was already there; NO write was issued
    FencedOut,      /// this actor's admission is gone. Whether its last attempt landed is UNRESOLVED:
                    /// admission can be lost before the write is sent, and equally after the write is
                    /// proven durable. Re-read before assuming either way
};

/// Merge `contribution` into `life`'s `_ckpt` and make the result durable, as ONE read-modify-write
/// on `op`.
///
/// An ABSENT object is created from `contribution` as it stands. Every writer may create it and none
/// may complete it: a publisher that knows only the checkpoint creates one that knows only the
/// checkpoint, and the field a different writer knows merges in whenever it arrives, in either order.
/// That is the whole reason each field is optional rather than defaulted.
///
/// Both DECLINE-TIME verdicts consult `op.admitted()` before they speak, because a writer the fence is
/// about to refuse has landed nothing AT THAT POINT: it is told `FencedOut` rather than `IdenticalSkip`
/// or a corruption verdict. `FencedOut` is returned rather than thrown because it is an expected,
/// transient control signal, and the snapshot publisher that calls this sits after a durable PUT where
/// an exception would be worse than a value. It does NOT promise the object is unchanged -- a lost
/// admission is also reported for a write already proven durable.
///
/// FAILS CLOSED, never open:
///   - an existing `_ckpt` that does not decode PROPAGATES `CORRUPTED_DATA` and is never overwritten.
///     It is the only record of recovery's base and of what cleanup may delete; replacing it with a
///     body derived from `contribution` alone would erase the base while leaving a well-formed object
///     behind -- corruption laundered into something a reader would trust.
///   - a contribution whose `life_epoch` is BELOW the durable one raises `CORRUPTED_DATA`, decided
///     before the merge, so no body is built and no write is sent. This is the one refusal that HAS to
///     live here rather than in `mergeCkpt`: only this function knows which side is durable.
///   - exhausting the policy under persistent conflict throws the retry-later class. No partial state
///     exists to clean up: every attempt either committed the complete merged body or changed nothing.
CkptPublishOutcome publishCkpt(CasOperation & op, const Layout & layout, const NamespaceLifeId & life,
                               const RefCkpt & contribution, const Retry & policy = Retry::standard());

/// One observation of a namespace's `_ckpt`: the decoded body and the incarnation it was read at. The
/// incarnation is what the missing-base revalidation adjudicates against, so a reader that keeps only
/// the body cannot apply the rule.
struct CkptSample
{
    RefCkpt ckpt;
    Etag etag;
};

/// Point-read of `life`'s `_ckpt`. `nullopt` means the object is absent (a namespace whose creation has
/// not published one yet); a present-but-undecodable object throws `CORRUPTED_DATA`.
std::optional<CkptSample> readCkpt(CasOperation & op, const Layout & layout, const NamespaceLifeId & life);

/// The verdict of INV-4's three-way revalidation, for the one leg that is not simply "it is there".
enum class MissingBaseVerdict : uint8_t
{
    RestartRecovery,  /// the checkpoint moved under us -- re-read `_ckpt` and start over from the new base
    Corrupted,        /// the base is gone under a checkpoint that still names it -- fail closed
};

/// Adjudicate a sampled recovery anchor that turned out to be unavailable, by comparing the `_ckpt`
/// incarnation this recovery sampled against the one a fresh re-read observes. The anchor is the
/// checkpoint-named snapshot and its retained same-id non-seal log witness; the caller supplies this
/// verdict after either exact read is absent.
///
///   - incarnation ADVANCED  -> `RestartRecovery`. Cleanup legitimately advanced the checkpoint and
///     deleted the previous anchor while we were reading. Nothing is wrong; restart from the newer
///     base (bounded by the caller's own restart budget).
///   - incarnation UNCHANGED -> `Corrupted`. The checkpoint still names an object that is not there, and
///     the deletion gate makes that unreachable in an honest run: the named snapshot and matching log
///     are both retained. Something deleted a live anchor.
///   - `_ckpt` itself ABSENT on the re-read -> `Corrupted` for the same reason, and more bluntly: the
///     namespace has a base we sampled and no checkpoint at all. `_ckpt` is deleted only as part of
///     namespace removal, which cannot be racing a live recovery of that same namespace.
///
/// Pure, so it is decided the same way at every call site; the caller raises `CORRUPTED_DATA` on
/// `Corrupted` with its own context.
MissingBaseVerdict classifyMissingSampledBase(const Etag & sampled, const std::optional<Etag> & current);

/// INV-4's snapshot-deletion gate: a snapshot is deletable only STRICTLY BELOW the checkpoint. Strict
/// rather than at-or-below because the checkpoint names the snapshot a recovery is entitled to fetch
/// by exact key; deleting THAT one is what turns a stale-but-harmless pointer into the corruption
/// `classifyMissingSampledBase` has to report (TLC counterexample `_sab_staleckptcorruption`).
///
/// Fail-closed on `nullopt`: a namespace with no checkpoint yet has NOTHING deletable, because no
/// snapshot has been established as a covering base.
bool snapshotDeletableUnderCkpt(const RefTxnId & snapshot_id, const std::optional<RefTxnId> & checkpoint_snapshot_id);

}
