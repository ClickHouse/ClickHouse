#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/types.h>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace DB::Cas
{

/// Text codec for `cas_ref_log`, the immutable object stored at `_log/<txn_id>`. Each object contains
/// exactly one committed transaction: its namespace, transaction id, and the batch of `RefOp`s applied
/// by that commit. The body has a header, a meta line `{"ns","we","rs",["!pse","!pss"]}`, one JSON
/// record per op, and a `{"n":count}` trailer. Records are emitted in the transaction's stored order
/// and contain no codec-generated timestamps, so encoding the same value is byte-identical. This
/// determinism is a property of the representation, not an adoption gate: ref commits use
/// `putIfAbsentControlled`, and the caller applies the `Always`/`.zst` storage policy by sealing the
/// returned text.
///
/// `RefOpKind::EpochSeal` closes an epoch transition in-band (spec INV-2): a seal transaction contains
/// exactly that one op, and the meta line's optional `prev_epoch_seal` (wire fields `!pse`/`!pss`,
/// CRITICAL -- an unrecognized `!`-key fails closed with `UNKNOWN_FORMAT_VERSION` rather than being
/// silently skipped, since dropping it would lose INV-2's chain evidence while still passing the
/// structural grammar) chains to the transaction id of the seal that closed the PRECEDING epoch, and
/// its own `writer_epoch` is exactly one below the referencing transaction's. `prev_epoch_seal` is
/// required on exactly sequence 1 of every epoch above the namespace's genesis (`life_epoch`, the
/// writer epoch of its `NamespaceBirth`) and forbidden elsewhere -- see
/// `validateEpochSealGrammarStructural` and `validateEpochSealGrammarContextual`.

/// One operation kind in a ref transaction log. The numeric values are part of the in-memory and
/// serialized representation; unknown values are rejected by the decoder.
enum class RefOpKind : uint8_t
{
    NamespaceBirth = 1,
    OwnerTransition = 2,
    SetPublishedAt = 3,
    RemoveNamespace = 4,
    EpochSeal = 5,
};

/// One operation inside a `RefLogTxn`. Only the fields documented next to `kind` are meaningful for
/// that kind, and the codec never reads or writes the others. `OwnerTransition` optionally removes
/// `old_binding` and/or installs `new_binding`; `SetPublishedAt` carries the expected manifest and the
/// publication timestamp. `RefOwnerBinding` is shared with the snapshot format through
/// `CasRefWireVocab.h`. `NamespaceBirth`, `RemoveNamespace`, and `EpochSeal` carry none of `RefOp`'s
/// fields -- `EpochSeal`'s only payload, `prev_epoch_seal`, lives on the containing `RefLogTxn`, not
/// here (it is a property of the transaction's position in the log, not of the op).
struct RefOp
{
    RefOpKind kind = RefOpKind::NamespaceBirth;

    std::optional<RefOwnerBinding> old_binding;    /// OwnerTransition: absent = pure add
    std::optional<RefOwnerBinding> new_binding;    /// OwnerTransition: absent = pure removal

    String ref_name;                               /// SetPublishedAt
    ManifestRef expected_manifest_ref;             /// SetPublishedAt
    uint64_t published_at_ms = 0;                  /// SetPublishedAt

    bool operator==(const RefOp &) const = default;
};

/// The complete immutable body of one ref transaction log object. `ns` and `txn_id` are repeated in
/// the body even though both are key-derived. `decodeRefLogTxn` compares them with the values supplied
/// from the object key, rejecting a valid body copied under a different key as corruption.
struct RefLogTxn
{
    String ns;
    RefTxnId txn_id;
    std::vector<RefOp> ops;
    /// The id of the `EpochSeal` transaction that closed the PRECEDING epoch (spec INV-2's genesis
    /// contract): required iff `txn_id.ref_sequence == 1` and `txn_id.writer_epoch` is above the
    /// namespace's `life_epoch` (its `NamespaceBirth` writer epoch), forbidden at every other
    /// sequence. Populated by both the recovery-minted seal that closes an empty epoch and the first
    /// ordinary transaction a writer appends after a transition -- see
    /// `validateEpochSealGrammarStructural` / `validateEpochSealGrammarContextual`. Declared LAST (not
    /// grouped with `txn_id`) to keep it a strict append to `RefLogTxn`'s field order. This project's
    /// `-Wmissing-field-initializers`/`-Wmissing-designated-field-initializers` still require every
    /// pre-existing positional `RefLogTxn{ns, txn_id, ops}` aggregate-init call site across the tree to
    /// spell out an explicit trailing `std::nullopt` -- appending is still the smaller, purely
    /// mechanical diff (one token per call site) compared to inserting a field in the middle.
    std::optional<RefTxnId> prev_epoch_seal;

    bool operator==(const RefLogTxn &) const = default;
};

/// Hard limits enforced by the codec at both encode and decode, measured over the JSON text bytes.
/// Normal transactions have an operation-count limit, a byte limit, and a per-op size limit. A
/// transaction containing `RemoveNamespace` is "removal-class": it shares the larger complete-table
/// byte budget and has neither a separate operation-count cap nor a per-op cap, because that byte
/// budget alone bounds it.
inline constexpr size_t ref_txn_max_ops = 5000;
/// Admission is ops-only (no accumulated-size estimation); this stays as a decode-side acceptance
/// bound plus a post-encode writer assert. The canonical writer cannot reach it: at most
/// `ref_txn_max_ops` ops at `ref_op_max_bytes` each is well under this, with framing headroom to
/// spare.
inline constexpr size_t ref_txn_max_bytes = 20 * 1024 * 1024;
inline constexpr size_t ref_removal_max_bytes = 64 * 1024 * 1024;
/// Per-op size cap on normal-class ops, enforced exactly per op (one op encoded alone, no
/// accumulation) both at admission and at decode. Not unreachable: `checkCanonicalRefName` imposes
/// no length limit and ref/part names grow with partition-key values. Removal-class ops are exempt
/// (they share the byte budget above and have no per-op cap).
inline constexpr size_t ref_op_max_bytes = 4096;

/// Encode the transaction to canonical, uncompressed text. The persist path seals this text according
/// to the `Always`/`.zst` policy; keeping the codec unsealed lets the byte-budget check and preview
/// callers measure the actual text, and lets the state machine validate an uncompressed round-trip.
/// Operations retain their stored order. Throws CORRUPTED_DATA on a zero `txn_id` field, a
/// non-canonical `ref_name`, an out-of-range `manifest_ref`, an unknown op kind, or an op-count/byte
/// limit violation over the encoded text.
String encodeRefLogTxn(const RefLogTxn & txn);

/// Decode canonical text after the caller has opened the stored `.zst` object. `expected_ns` and
/// `expected_txn_id` come from the object key; the decoded body must equal them, otherwise the body/key
/// binding fails with CORRUPTED_DATA. Unknown non-critical fields are skipped so additive fields can be
/// introduced without changing this reader, while a future header version is rejected with
/// UNKNOWN_FORMAT_VERSION. Truncation, an unknown op or owner kind, a non-canonical `ref_name`, a zero
/// transaction-id field, a body/key mismatch, and a limit violation are reported as CORRUPTED_DATA.
RefLogTxn decodeRefLogTxn(std::string_view data, const String & expected_ns, const RefTxnId & expected_txn_id);

/// Encoded byte size of exactly one exact-owner-removal op line: an `owner_transition` with only an old
/// binding, no new binding, as it appears in a hypothetical whole-namespace removal transaction (one
/// such op per committed ref and precommit, followed by a terminal `remove_namespace`), encoded via
/// `encodeRefLogTxn`.
size_t removalOpEncodedSize(RefOwnerKind owner_kind, const String & ref_name, const ManifestRef & manifest_ref);

/// Encoded byte size of a removal transaction's framing (header + meta + terminal remove_namespace op +
/// trailer) for `op_count` total ops, excluding the per-owner removal op lines. `removalFramingSize(...)
/// + Σ removalOpEncodedSize` equals `encodeRefLogTxn(...)`'s size, for the hypothetical whole-namespace
/// removal transaction described above, exactly.
size_t removalFramingSize(const String & ns, const RefTxnId & txn_id, uint64_t op_count);

/// Encoded byte size of exactly one op, on its own, as it appears inside a `RefLogTxn` body (the
/// record line only -- no header, meta, or trailer framing). Used to enforce the per-op size cap at
/// admission and at decode without ever accumulating a whole transaction's bytes.
size_t encodedOpSize(const RefOp & op);

/// The one canonical removal-class discriminator: a transaction (or a not-yet-encoded item's built
/// ops) is removal-class iff `ops` contains a `RemoveNamespace` op. `MutationScope::Kind::WholeShard`
/// is NOT a substitute -- the stale-precommit reclaim sweep is also `WholeShard`-scoped but is not
/// removal-class. Every site that selects the removal byte budget (`ref_removal_max_bytes` vs
/// `ref_txn_max_bytes`) or exempts an item from the normal-class op/per-op caps must call this, not
/// re-derive its own predicate.
bool refLogTxnIsRemovalClass(const std::vector<RefOp> & ops);

/// True iff `txn` is exactly a well-formed epoch-seal transaction: one operation, and that operation
/// is `EpochSeal`. Unlike `refLogTxnIsRemovalClass` (a class predicate that tolerates other ops
/// alongside `RemoveNamespace`), a seal transaction's grammar forbids any companion op (spec INV-2),
/// so this checks the exact shape rather than mere presence -- callers that decode untrusted bytes
/// and branch on "is this a seal" (Task 2's slot-occupy walk) get a precise answer.
bool refLogTxnIsEpochSeal(const RefLogTxn & txn);

/// The context-free half of the INV-2 seal grammar: a transaction carrying an `EpochSeal` op contains
/// EXACTLY that one op, and `prev_epoch_seal`, when present, is well-formed (both `RefTxnId`
/// components nonzero), appears ONLY at `txn_id.ref_sequence == 1`, and names the IMMEDIATELY
/// preceding epoch (`prev_epoch_seal->writer_epoch + 1 == txn_id.writer_epoch`) -- a self, forward,
/// or skipping pointer is rejected even though it cannot arise from `writeLogMeta`'s own writer,
/// because Tasks 2/6 walk this pointer backwards over untrusted decoded bodies. Called by both
/// `encodeRefLogTxn` and
/// `decodeRefLogTxn`, so a caller-built transaction and a decoded one are held to the identical rule.
/// Throws CORRUPTED_DATA on violation. Does NOT check the required-iff rule -- that needs
/// `life_epoch`, which the codec never sees; see `validateEpochSealGrammarContextual`.
void validateEpochSealGrammarStructural(const RefLogTxn & txn);

/// The contextual half of the INV-2 seal grammar: `prev_epoch_seal` is required on exactly
/// `txn_id.ref_sequence == 1` of every epoch with `txn_id.writer_epoch > life_epoch` (a namespace's
/// `life_epoch` is the writer epoch of its `NamespaceBirth` record -- its genesis, which may be above
/// 1 for a namespace born after earlier epoch transitions) and forbidden at
/// `txn_id.writer_epoch <= life_epoch`; a no-op for `txn_id.ref_sequence != 1` (the unconditional
/// "forbidden elsewhere" half of the rule is `validateEpochSealGrammarStructural`'s job, not this
/// function's). Callers own `life_epoch`: the apply layer and the writer-side encode call sites that
/// mint a sequence-1 transaction. Throws CORRUPTED_DATA on violation.
void validateEpochSealGrammarContextual(const RefLogTxn & txn, uint64_t life_epoch);

}
