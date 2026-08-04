#pragma once
#include <cstdint>
#include <span>
#include <string_view>

namespace DB::Cas
{

/// Shared vocabulary and policy registry for persisted content-addressed objects. The registry
/// supplies the version gate and the per-object text-file traits used by the codecs in this
/// directory; it does not own object bytes or storage lifecycle state.

/// The highest pool-format generation this build understands. A build keeps every decoder for
/// generations 1..G_BUILD (new code always reads old); an object is readable iff its
/// compatibility_version <= G_BUILD. Bump this (and append a change-point in CasFormat.cpp) when a new
/// format generation is introduced.
///
/// Generation 2 is the first generation that understands mixed-algorithm pools: the schema-3
/// source-edge settlement key includes the algorithm prefix, so a generation-1 reader can open the
/// pool but cannot decode its GC state. Pool admission CAS-raises `min_reader_generation` to this
/// build's own floor (`G_BUILD`), and a persisted floor above `G_BUILD` fails closed.
///
/// Generation 3 replaced mutable ref-shard objects with immutable `_log` and `_snap` objects.
///
/// Generation 4 makes each namespace's ref-log ids per-namespace and CONTIGUOUS within a writer epoch
/// (INV-1). The bytes of a `_log`/`_snap` object did not change, but their MEANING did: a generation-3
/// pool's ids were drawn from a pool-wide counter and are full of legitimate holes, which this build
/// reads as a truncated -- i.e. corrupt -- stream. The per-object forward gate cannot reject such a
/// pool (its version is not in the future), so pool-meta decoding applies
/// `kContiguousRefStreamsGeneration` as a backward floor. Pools below the floor must be recreated;
/// there is no migration path in the pre-release format.
///
/// Generation 5 (Stage B's own recreate-only bump, the plan's "format bump B") re-keys the ref layer
/// under `<ns>/<incarnation>/` (spec INV-3: the whole-pool namespace catalog mints the incarnation).
/// Again the bytes of `_log`/`_snap`/`_ckpt` objects did not change, but the KEY SHAPE they live under
/// did: a generation-4 key named a namespace directly (`cas/refs/<ns>/_log/<id>`), while this
/// generation's reader recognizes only the incarnation-qualified shape
/// (`cas/refs/<ns>/<incarnation>/_log/<id>`) -- `Layout::parseRefObjectKey`/`parseRefCkptKey` already
/// refuse the un-incarnated shape with `CORRUPTED_DATA` (Stage B Tasks 1/1c landed that refusal ahead
/// of this bump, deliberately: the pre-release format carries zero persisted data and zero compat
/// obligation, so the key shapes and the bump that makes them the ONLY readable shape need not land in
/// the same commit). `kNamespaceLifeKeyedGeneration` is the backward floor for this change, applied the
/// same way `kContiguousRefStreamsGeneration` is.
///
/// Generation 6 replaces that namespace-bearing grammar with opaque pool-wide life identifiers and
/// splits hot ref streams from point-read state: `cas/ns/stream/<life_id>/...` contains `_log`, `_snap`
/// while `cas/ns/state/<life_id>/...` contains `_ckpt` and `_files`. A generation-5
/// pool must be recreated; no dual parser or copy-forward path exists.
///
/// Generation 7 replaces the fold seal's independent namespace-keyed coverage and cleanup
/// collections with one opaque-life-keyed row and removes the retired terminal-marker object class. A generation-6
/// pool must be recreated; there is no dual reader for the split grammar.
///
/// Generation 8 persists the creation-time `gc_shards` authority in `_pool_meta`. Generation-7 pools
/// must be recreated because namespace admission can precede creation of `gc/state`; accepting a
/// metadata object without this field would leave different openers charging different seal bounds.
/// Generation 9 adds `_ckpt.committed_through`, the exact recovery frontier. Generation-8 pools
/// must be recreated: the absence of this field has the incompatible meaning that no transaction has
/// entered durable logical history.
constexpr uint32_t G_BUILD = 9;

/// The pool-format generation at which ref-log ids became per-namespace and contiguous. Pool metadata
/// below this value cannot be opened, because its ref streams carry holes this build reports as
/// corruption; the backward-floor check is applied by `decodePoolMeta`. Named separately from `G_BUILD`
/// so a later generation that CAN still read a generation-4 pool does not silently move the floor with
/// it.
constexpr uint32_t kContiguousRefStreamsGeneration = 4;

/// The pool-format generation at which the ref layer (and, per Stage B's Task 4b, namespace files)
/// became incarnation-scoped under `<ns>/<incarnation>/`. Pool metadata below this value cannot be
/// opened: its ref-object keys carry no incarnation segment, which this build's parsers refuse as
/// corruption rather than read as a compatibility case (see the `G_BUILD` doc above). The backward-
/// floor check is applied by `decodePoolMeta`, exactly mirroring `kContiguousRefStreamsGeneration`;
/// named separately for the same reason that one is -- so a later generation that can still read a
/// generation-5 pool does not silently move this floor with it. Pools below the floor must be
/// recreated; there is no migration path in the pre-release format.
constexpr uint32_t kNamespaceLifeKeyedGeneration = 5;

/// The recreate-only generation at which namespace text disappeared from physical life keys and hot
/// ref streams were separated from point-read namespace state.
constexpr uint32_t kOpaqueNamespaceLifeLayoutGeneration = 6;

/// The recreate-only generation at which one unified ref-life row replaced the split coverage and
/// namespace-cleanup grammar.
constexpr uint32_t kUnifiedRefLifeFoldGeneration = 7;

/// The recreate-only generation at which `_pool_meta` became the authority for `gc_shards`.
constexpr uint32_t kPoolGcShardsGeneration = 8;

/// The recreate-only generation at which `_ckpt` gained its exact committed-transaction frontier.
constexpr uint32_t kCommittedRefFrontierGeneration = 9;

/// Stable identifiers for every self-describing persisted object class. The text registry uses the
/// corresponding `type` string as the on-disk identity. Numeric values are part of the format history:
/// retired values remain unused so an old object can never be mistaken for a later class.
enum class FormatId : uint16_t
{
    Blob = 1,
    /// Values 2, 3, and 4 are retired. The former tree and GC-snapshot classes were replaced by the
    /// root-local part manifest, while the former mutable `cas_ref_shard` class was replaced by the
    /// immutable `RefSnapshot` and `RefLog` objects. Keep all three values unused.
    GcState = 5,
    /// Value 6 is retired: condemned state now rides source-edge runs and the fold-seal
    /// `condemned_summary`. Keep it unused. Value 7 is also retired: the build-watermark floor is
    /// carried by the `MountLease` beat rather than a standalone object.
    PoolMeta = 8,
    Roster = 9,
    /// Value 10 is retired: discovery authority is the pool-wide `cas/ref_catalog` object rather
    /// than a roots registry object or a physical stream listing.
    GcOutcomes = 11,
    PartManifest = 12,    /// Immutable root-local `cas_part_manifest` payload-hybrid text object.
    RunFile = 13,         /// Deterministic, uncompressed `cas_run` GC source-edge NDJSON stream.
    FoldSeal = 14,        /// Write-once `cas_fold_seal` coverage and blob-target/cleanup-run object.
    /// Value 15 is retired: the fold seal is the sole per-generation coverage record after the
    /// one-pass GC round. The following three classes are per-server-root mount-safety objects.
    Owner = 16,           /// `cas_owner` anchor from server-root ID to server UUID.
    ServerEpoch = 17,     /// `cas_epoch` writer-epoch fence carrying `next_writer_epoch`.
    MountLease = 18,      /// Live `cas_mount_lease` object.
    /// These identifiers cover objects that were added to the registry after their initial codecs:
    /// the ref transaction log, complete ref snapshot, blob freshness sidecar, and GC heartbeat.
    /// Their values are frozen and must never be reused.
    RefLog = 19,          /// cas_ref_log     — ref transaction log object
    RefSnapshot = 20,     /// cas_ref_snap    — complete per-namespace ref table
    BlobMeta = 21,        /// cas_blob_meta   — per-blob freshness sidecar
    GcHeartbeat = 22,     /// cas_gc_hb       — GC leader heartbeat
    RefCkpt = 23,         /// cas_ref_ckpt    — per-namespace checkpoint (INV-4)
    RefCatalog = 24,      /// cas_ref_catalog — the whole-pool namespace catalog (INV-3)
    GcMaintenanceState = 25, /// cas_gc_maintenance_state — leak-only namespace-janitor cursor
};

/// Returns the writer generation stamped on newly written objects. The current pre-roster writer
/// always stamps `G_BUILD`.
uint32_t currentWriterVersion();

/// Returns the compatibility generation stamped on newly written objects. Until the roster and
/// write-down-to-floor policy exist, this is always `G_BUILD`; readers reject values above `G_BUILD`.
uint32_t currentCompatibilityVersion();

/// Applies the common fail-closed reader gate. If an object's `compatibility_version` exceeds
/// `G_BUILD`, throws `UNKNOWN_FORMAT_VERSION` before the caller interprets the body; `what` identifies
/// the object in the exception message.
void checkCompatibility(uint32_t compatibility_version, std::string_view what);

/// One append-only entry in a class's format history. At `generation`, the class's ENCODING or the
/// MEANING of what it encodes changed, and a reader must understand at least `min_reader` to read an
/// object written at that generation. Additive changes retain the previous reader floor; breaking
/// changes set the floor to the change generation itself. Generation 4's ref-stream entry is the
/// worked example of the second kind: not one byte of `cas_ref_log` moved, but its ids became dense,
/// so an older stream is unreadable to this build and the floor is the change generation.
struct FormatChangePoint
{
    uint16_t generation;
    uint16_t min_reader;
};

/// Returns the append-only change-point history for `id`, oldest first. A class's history begins at
/// the generation it was BORN in, not at 1: the classes that existed from the start carry the frozen
/// `{1, 1}` baseline, while `RefCkpt` — introduced at generation 4 — begins at `{4, 4}`, because there
/// is no such thing as a generation-1 `_ckpt` and claiming one would say a generation-1 reader could
/// read it. Future changes append entries without editing old ones.
std::span<const FormatChangePoint> changePoints(FormatId id);

/// The text-format registry has one row per decodable persisted object. Each row is the single source
/// for the header-line `type`, body family, unknown-key policy, compression policy, and fail-closed
/// size caps. A format missing from this registry cannot be decoded.

/// The shape of a text object body: a materialized control object, a streamed sorted record sequence,
/// or a descriptor followed by raw payload bytes.
enum class TextFamily : uint8_t { Control = 1, RecordStream = 2, PayloadHybrid = 3 };

/// Whether a decoder skips unknown ordinary keys or rejects them. Critical keys prefixed with `!`
/// are rejected by all families because they signal a required extension.
enum class KeyStrictness : uint8_t { Tolerant = 1, Strict = 2 };

/// Deterministic storage policy. `Always` uses whole-object zstd and a `.zst` key suffix; `Never`
/// remains raw; `PinnedRaw` is raw because byte adoption compares the serialized bytes.
enum class CompressionPolicy : uint8_t { Never = 1, Always = 2, PinnedRaw = 3 };

/// Codec metadata for one registered text object. The byte caps apply to decompressed object content
/// and individual text lines; `object_cap == 0` means a streamed format has no whole-object cap.
struct FormatTraits
{
    FormatId id;
    std::string_view type;      /// header-line "type" value
    TextFamily family;
    KeyStrictness strictness;
    CompressionPolicy compression;
    uint64_t object_cap;        /// max DECOMPRESSED object bytes; 0 = uncapped (streamed)
    uint64_t line_cap;          /// max bytes of one text line
};

/// Returns the traits for `id`. Throws `LOGICAL_ERROR` for `FormatId::Roster`, which is reserved and
/// has no codec or traits row yet.
const FormatTraits & traitsFor(FormatId id);
/// Looks up a header-line `type` string. Returns nullptr for an unregistered type; it does not throw
/// because callers use this result to classify the input before decoding it.
const FormatTraits * traitsForType(std::string_view type);
/// Returns the storage-key suffix for `id`: `.zst` for `Always`, and an empty suffix otherwise.
/// Key builders use this policy directly so a point lookup never has to inspect the object body or
/// try multiple keys.
std::string_view storedSuffix(FormatId id);

}
