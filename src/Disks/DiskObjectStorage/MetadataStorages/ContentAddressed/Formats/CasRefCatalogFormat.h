#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace DB::Cas
{

class Layout;

/// The byte bound every namespace name admitted into `ref_catalog` must satisfy (spec INV-3:
/// "namespace names get a byte bound"). It keeps the catalog's operator-visible row and line grammar
/// bounded, and both directions of the codec enforce it. Logical namespace bytes do NOT enter
/// predicate (2): fold-seal `rfl` rows are keyed only by the fixed-width opaque life id.
constexpr size_t kMaxNamespaceBytes = 512;

/// One namespace's catalog lifecycle state (spec INV-3, §3). `Creating` blocks publication and
/// requires a `creator` fence identity; `Live` is the steady state and forbids `creator`;
/// `Removing` forbids new positive ownership and, like `Live`, forbids `creator` (a namespace at
/// this state was already `Live`, so no creation fence identity applies to it any longer).
///
/// THESE ARE WIRE VALUES, AND THEY ARE APPEND-ONLY, exactly like `HoldReason`: a catalog object
/// written by one build is read by another, so a renumbered or repurposed value would make an older
/// catalog name a different lifecycle than the one it recorded. Add new states at the end; never
/// renumber, never repurpose a retired word.
enum class NsState : uint8_t
{
    Creating = 1,
    Live = 2,
    Removing = 3,
};

/// The wire word one `NsState` is persisted as. Exported so any other reader of a lifecycle state
/// renders the SAME three words this codec does, rather than a second, independently drifting copy.
/// Every value it is ever called with comes from either a live `NsState` (a compile-time-closed
/// enumeration) or a value `nsStateFromWord` already validated on decode, so an unrecognized value
/// reaching it is a bug in THIS process, not corruption arriving from a store -- `LOGICAL_ERROR`.
std::string_view nsStateToWord(NsState s);
/// Inverse of `nsStateToWord`; throws `CORRUPTED_DATA` for anything but the three registered words.
NsState nsStateFromWord(std::string_view w);

/// The fence identity of the mounted writer CREATING one namespace (spec §3): the server root plus
/// the writer epoch and admission fence generation captured at the moment `Creating` was minted. It
/// is what a reconciler compares against `CasServerRoot`'s liveness/fence machinery before a stalled
/// `Creating` entry may be CAS-reconciled away (INV-3: "stalled creators occupy entries until
/// fence-terminal reconciliation").
struct CreatorFence
{
    String server_root_id;
    uint64_t writer_epoch = 0;
    uint64_t fence_generation = 0;

    bool operator==(const CreatorFence &) const = default;
};

/// One namespace's catalog row. `incarnation` is the ref-layer-scoped life identity minted once, at
/// `Creating` (spec INV-3; consumed as a `NamespaceLifeId` by every ref/namespace-file key helper --
/// see `NamespaceLifeId::fromCatalogEntry`), and never changes for the rest of this row's life: a
/// namespace dropped and recreated gets a FRESH row with a FRESH incarnation, never a reused one --
/// that is what makes rebirth structurally inert instead of an alias. `incarnation == 0` is always
/// invalid, at every state -- "0 never names a life", the same rule `NamespaceLifeId` enforces.
///
/// `creator` is a STRICT GRAMMAR pairing: REQUIRED iff `state == Creating`, FORBIDDEN otherwise.
/// `removal_started_round` is similarly REQUIRED iff `state == Removing`: it is sampled once by the
/// `Live -> Removing` catalog CAS and never changes, so diagnostics can measure removal age without
/// inventing a caller-local epoch. Both pairings are enforced in both codec directions.
struct CatalogEntry
{
    RootNamespace ns;
    NsState state = NsState::Creating;
    UInt128 incarnation = 0;
    std::optional<CreatorFence> creator = std::nullopt;
    std::optional<uint64_t> removal_started_round = std::nullopt;

    bool operator==(const CatalogEntry &) const = default;
};

/// The whole-pool namespace catalog (spec INV-3): one object, key `cas/ref_catalog`
/// (`Layout::refCatalogKey`), read on every fold round and every recovery, mutated by one token-CAS
/// write per lifecycle transition. `entries` is CANONICALLY ORDERED by namespace bytes, strictly
/// ascending -- no duplicate namespace -- and both directions of the codec enforce it, so an
/// out-of-order or duplicate-keyed catalog can never become durable.
struct RefCatalog
{
    std::vector<CatalogEntry> entries;

    bool operator==(const RefCatalog &) const = default;
};

/// Encodes `catalog` as the canonical `cas_ref_catalog` text object: a header line, one "ent" record
/// per entry in canonical (ns-sorted) order, and a record-count trailer -- the same tagged-record
/// container `encodeFoldSeal` uses. Enforces the FULL strict grammar on the way out: canonical order
/// and no duplicate namespace, a non-empty namespace within the `kMaxNamespaceBytes` bound, nonzero
/// incarnation, and the `creator`/state pairing. This is our own state about to become durable, so a
/// violation is `LOGICAL_ERROR`, not `CORRUPTED_DATA`. Also enforces the per-line `LIMIT_EXCEEDED`
/// line-cap gate (mirroring `encodeFoldSeal`'s `checkLineBytes` exactly, including its error code --
/// a caller catching `LIMIT_EXCEEDED` as a capacity refusal must not see a `LOGICAL_ERROR` bug report
/// instead) -- but deliberately does NOT enforce the whole-object cap itself: that predicate must
/// name the namespace under admission, which only a caller of `checkCatalogAdmission` knows.
///
/// These bytes go to and come from the backend DIRECTLY, exactly like `cas_ref_ckpt`: the Pool-side
/// `CasRefCatalog::read`/`casUpdateImpl` (`Pool/CasRefCatalog.cpp`) bypass `sealObject`/`openObject`,
/// which are the identity under this class's `CompressionPolicy::Never` and would add nothing. A
/// policy flip to `Always` therefore breaks this silently -- and is caught, because `storedSuffix`
/// would stop being empty and the registry test asserting `storedSuffix(FormatId::RefCatalog) == ""`
/// fails. That assertion is the tripwire for this shortcut, not an incidental check of the key shape.
/// One consequence of the bypass, stated rather than fixed here (pre-existing for `RefCkpt` too):
/// `openObject`'s own object-cap enforcement is skipped on the read path, so nothing on either the
/// plain write path or the read path enforces the 256 MiB object cap outside `checkCatalogAdmission`
/// -- the cap is load-bearing only through THAT gate, never through the codec or the backend read.
String encodeRefCatalog(const RefCatalog & catalog);

/// Decodes and validates a `cas_ref_catalog` object, re-checking every grammar rule `encodeRefCatalog`
/// enforces against bytes that may have come from anywhere: `CORRUPTED_DATA` on a duplicate namespace,
/// non-canonical order, a missing, empty, or over-bound namespace, a zero incarnation, an incomplete or
/// forbidden creator fence, an unknown state word, or trailing bytes.
RefCatalog decodeRefCatalog(std::string_view data);

/// PRE-PUT GATE, predicate (1) of INV-3's additive admission: `encoded_bytes <= catalog_object_cap`
/// (the registry's own cap for `FormatId::RefCatalog`). Equality is accepted; refuses
/// (`LIMIT_EXCEEDED`, naming `ns`) one byte over.
void checkCatalogObjectBytes(uint64_t encoded_bytes, const RootNamespace & ns);

/// Worst-case bytes for the fold-seal frame with no records: maximal generation fields plus the
/// widest possible `uint64_t` trailer count. Measured through the real encoder.
uint64_t foldSealFixedBytes();

/// The worst-case bytes ONE admitted catalog entry could ever add to a fold seal: one ref-life row
/// containing held coverage and terminal cleanup evidence at their widest legal shapes. Measured
/// through `encodeFoldSeal` itself, like `foldSealFixedBytes`.
uint64_t worstCaseEntryFoldReservationBytes();

/// Worst-case incremental bytes for one canonical blob-target run row. The serialized physical key
/// includes `layout`'s pool prefix, so layout is part of the bound.
uint64_t widestBlobTargetRunReservationBytes(const Layout & layout, uint64_t gc_shards);

/// Worst-case incremental bytes for one condemned-summary row at the greatest configured shard.
uint64_t widestCondemnedSummaryReservationBytes(uint64_t gc_shards);

/// PRE-PUT GATE, predicate (2) of INV-3's additive admission. Reserves the widest fixed frame, one
/// widest ref-life row per candidate catalog entry, and one widest blob-target plus condemned-summary
/// row per authoritative GC shard. The `btr` multiplier follows the authoritative fold-seal grammar:
/// at most one canonical sequence-0 run is legal for each shard. Equality is accepted; refuses
/// (`LIMIT_EXCEEDED`, naming `ns`) one entry over. Every multiplication and addition saturates, so an
/// unreachable-in-practice count can never wrap into something that reads as "fits".
void checkFoldSealReservation(
    uint64_t entry_count, uint64_t gc_shards, const Layout & layout, const RootNamespace & ns);

/// Runs BOTH admission predicates against `candidate` -- the catalog state as it would read
/// immediately AFTER the admission under consideration -- naming `admitting_ns` in whichever
/// predicate refuses (INV-3: "admission refuses loudly"; "TWO INDEPENDENT predicates"). `candidate`
/// is grammar-checked first (via `encodeRefCatalog`; `LOGICAL_ERROR` on our own bug), then predicate
/// (1) and predicate (2), in that order. Returns the encoded bytes on success, so a caller's `casPut`
/// writes EXACTLY what admission checked -- never a second, independently re-encoded copy.
///
/// Constraint 13 (removal is never refused): this function is for entry-ADMITTING mutations only.
/// A removal transition (`Live` -> `Removing`) must go through the catalog's plain update path
/// instead, never through here.
String checkCatalogAdmission(
    const RefCatalog & candidate, uint64_t gc_shards, const Layout & layout,
    const RootNamespace & admitting_ns);

}
