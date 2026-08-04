#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

namespace
{

/// Generation-1 baseline for every class. A future format change appends to that class's array and
/// bumps `G_BUILD`: additive changes use the previous reader floor, while breaking changes use the
/// new generation as the floor. Existing entries are immutable history.
constexpr FormatChangePoint BASELINE[] = {{1, 1}};

/// The two ref classes changed at generation 4 (INV-1, per-namespace contiguous ids) AND AGAIN at
/// generation 5 (Stage B's recreate-only "format bump B": the ref layer re-keyed under
/// `<ns>/<incarnation>/`). Both changes are BREAKING even though not one byte of the encoding moved
/// either time -- a generation-3 stream's ids came from a pool-wide counter and legitimately skip,
/// which a generation-4 reader reports as corruption, and a generation-4 key names no incarnation at
/// all, which a generation-5 reader also reports as corruption (`Layout::parseRefObjectKey`). Each
/// floor is the change generation itself.
constexpr FormatChangePoint REF_STREAM[] = {
    {1, 1},
    {kContiguousRefStreamsGeneration, kContiguousRefStreamsGeneration},
    {kNamespaceLifeKeyedGeneration, kNamespaceLifeKeyedGeneration},
    {kOpaqueNamespaceLifeLayoutGeneration, kOpaqueNamespaceLifeLayoutGeneration},
};

/// `cas_ref_ckpt` is BORN at generation 4, so it has no generation-1 baseline to inherit: there is no
/// such thing as a generation-1 `_ckpt` object, and claiming one would say a generation-1 reader could
/// read it. Generation 5 re-keys it under `<ns>/<incarnation>/` exactly like `REF_STREAM` above, for
/// the same reason and with the same floor.
constexpr FormatChangePoint REF_CKPT[] = {
    {kContiguousRefStreamsGeneration, kContiguousRefStreamsGeneration},
    {kNamespaceLifeKeyedGeneration, kNamespaceLifeKeyedGeneration},
    {kOpaqueNamespaceLifeLayoutGeneration, kOpaqueNamespaceLifeLayoutGeneration},
    {kCommittedRefFrontierGeneration, kCommittedRefFrontierGeneration},
};

/// `cas_ref_catalog` is BORN at generation 4, one generation BEFORE the bump that makes namespace
/// existence catalog-authoritative (Stage B's Task 4, "format bump B" -- `kNamespaceLifeKeyedGeneration`):
/// Task 2 introduced the catalog OBJECT while `G_BUILD` was still the value
/// `kContiguousRefStreamsGeneration` names, and Task 4 is the later change that actually wires
/// discovery to read it and bumps the floor. The catalog's own encoding is unaffected by that bump (it
/// reuses `kContiguousRefStreamsGeneration` as its birth generation, not a second constant named after
/// itself, for the same reason `REF_CKPT` originally did), so it carries no second change point here.
constexpr FormatChangePoint REF_CATALOG[] = {{kContiguousRefStreamsGeneration, kContiguousRefStreamsGeneration}};
constexpr FormatChangePoint GC_MAINTENANCE_STATE[] = {{kUnifiedRefLifeFoldGeneration, kUnifiedRefLifeFoldGeneration}};
constexpr FormatChangePoint POOL_META[] = {
    {1, 1},
    {kPoolGcShardsGeneration, kPoolGcShardsGeneration},
    {kCommittedRefFrontierGeneration, kCommittedRefFrontierGeneration},
};

}

std::span<const FormatChangePoint> changePoints(FormatId id)
{
    switch (id)
    {
        case FormatId::RefLog:
        case FormatId::RefSnapshot:
            return REF_STREAM;
        case FormatId::RefCkpt:
            return REF_CKPT;
        case FormatId::RefCatalog:
            return REF_CATALOG;
        case FormatId::GcMaintenanceState:
            return GC_MAINTENANCE_STATE;
        case FormatId::PoolMeta:
            return POOL_META;
        case FormatId::Blob:
        case FormatId::GcState:
        case FormatId::Roster:
        case FormatId::GcOutcomes:
        case FormatId::PartManifest:
        case FormatId::RunFile:
        case FormatId::FoldSeal:
        case FormatId::Owner:
        case FormatId::ServerEpoch:
        case FormatId::MountLease:
        case FormatId::BlobMeta:
        case FormatId::GcHeartbeat:
            return BASELINE;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "CasFormat: unknown FormatId {}", static_cast<int>(id));
}

uint32_t currentWriterVersion()
{
    return G_BUILD;
}

uint32_t currentCompatibilityVersion()
{
    /// Until roster-based write-down is implemented, every object carries the current build as its
    /// compatibility floor.
    return G_BUILD;
}

void checkCompatibility(uint32_t compatibility_version, std::string_view what)
{
    if (compatibility_version > G_BUILD)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "CAS {}: object requires reader generation {} but this build supports at most {}",
            what, compatibility_version, G_BUILD);
}

namespace
{
constexpr uint64_t kKiB = 1024;
constexpr uint64_t kMiB = 1024 * 1024;

/// Caps are 100-1000x above realistic sizes; hitting one indicates a corrupt object or protocol bug.
/// `RefLog` and `RefSnapshot` objects are read whole, so their 64 MiB decompressed object cap
/// accommodates the JSON-inflated removal-class transaction and full snapshot. Their codecs
/// independently enforce the existing `ref_txn_max_bytes` (20 MiB) and 64 MiB removal/snapshot budgets
/// before sealing.
///
/// Their `line_cap` intentionally equals `object_cap`. A smaller per-line limit would add no memory
/// protection to a whole-read format, while creating a write/read split: admission measures the whole
/// transaction against the object budget, so a large individual ref payload could be accepted on
/// write and rejected on decode, leaving a persisted ref object that cannot be read. The line cap is
/// instead meaningful for streaming formats, where it bounds the resident O(line) record. Matching
/// `line_cap` to `object_cap` lets any individually valid record consume the available object budget.
///
/// Compression is per type and deterministic, with no size threshold. `Always` types can grow large
/// and use a `.zst` key suffix; `PinnedRaw` types need stable bytes for adoption; `Never` types are
/// small raw singletons.
constexpr FormatTraits TRAITS[] =
{
    {FormatId::Blob,         "cas_blob",          TextFamily::PayloadHybrid, KeyStrictness::Tolerant, CompressionPolicy::Never,     256,        256},
    {FormatId::BlobMeta,     "cas_blob_meta",     TextFamily::Control,       KeyStrictness::Tolerant, CompressionPolicy::Never,     1 * kMiB,   64 * kKiB},
    {FormatId::PoolMeta,     "cas_pool_meta",     TextFamily::Control,       KeyStrictness::Tolerant, CompressionPolicy::Never,     1 * kMiB,   64 * kKiB},
    {FormatId::RefLog,       "cas_ref_log",       TextFamily::Control,       KeyStrictness::Tolerant, CompressionPolicy::Always,    64 * kMiB,  64 * kMiB},
    {FormatId::RefSnapshot,  "cas_ref_snap",      TextFamily::Control,       KeyStrictness::Tolerant, CompressionPolicy::Always,    64 * kMiB,  64 * kMiB},
    /// `cas_ref_ckpt` is a three-field mutable singleton read by a point GET on every recovery and on
    /// every cleanup decision, so its caps are deliberately TIGHT (64 KiB / 4 KiB rather than the
    /// megabyte scale its Control-family siblings use): nothing legitimate approaches them, and the cap
    /// is the first thing that fires if a foreign object ever lands at the key. STRICT for the same
    /// reason its decoder is -- every field changes what cleanup may delete, so nothing in it may be
    /// skipped. Raw (`Never`): a small singleton, and `publishCkpt` re-encodes it on every attempt.
    {FormatId::RefCkpt,      "cas_ref_ckpt",      TextFamily::Control,       KeyStrictness::Strict,   CompressionPolicy::Never,     64 * kKiB,  4 * kKiB},
    /// `cas_ref_catalog` (INV-3): one object for the whole pool, token-CAS like `gc/state`, read on
    /// every fold round and every recovery. STRICT for the same reason `cas_ref_ckpt` is -- every
    /// field decides a namespace's lifecycle, so nothing in it may be skipped. Raw (`Never`): the
    /// admission gate measures `encodeRefCatalog`'s own output directly, so a compressed size would
    /// answer the wrong question. The object cap is the fold-seal's own 256 MiB (predicate (2) of the
    /// additive admission check bounds it further via the entry count); the line cap is tight (4 KiB)
    /// because one entry's record is ordinarily small -- but not always small enough: a namespace or
    /// `server_root_id` near their own byte bounds, worst-case escaped, can push a single line past
    /// 4 KiB, and `encodeRefCatalog` REFUSES that entry (`LIMIT_EXCEEDED`, `CasRefCatalogFormat.cpp`'s
    /// `checkLineBytes`) rather than writing an object no reader could later decode.
    {FormatId::RefCatalog,   "cas_ref_catalog",   TextFamily::Control,       KeyStrictness::Strict,   CompressionPolicy::Never,     256 * kMiB, 4 * kKiB},
    {FormatId::GcMaintenanceState, "cas_gc_maintenance_state", TextFamily::Control, KeyStrictness::Strict, CompressionPolicy::Never, 512 * kKiB, 512 * kKiB},
    {FormatId::PartManifest, "cas_part_manifest", TextFamily::PayloadHybrid, KeyStrictness::Tolerant, CompressionPolicy::Always,    256 * kMiB, 64 * kKiB},
    {FormatId::RunFile,      "cas_run",           TextFamily::RecordStream,  KeyStrictness::Strict,   CompressionPolicy::PinnedRaw, 0,          4 * kKiB},
    {FormatId::FoldSeal,     "cas_fold_seal",     TextFamily::Control,       KeyStrictness::Strict,   CompressionPolicy::PinnedRaw, 256 * kMiB, 64 * kKiB},
    {FormatId::GcState,      "cas_gc_state",      TextFamily::Control,       KeyStrictness::Tolerant, CompressionPolicy::Never,     1 * kMiB,   64 * kKiB},
    {FormatId::GcHeartbeat,  "cas_gc_hb",         TextFamily::Control,       KeyStrictness::Tolerant, CompressionPolicy::Never,     1 * kMiB,   64 * kKiB},
    {FormatId::GcOutcomes,   "cas_gc_outcomes",   TextFamily::Control,       KeyStrictness::Tolerant, CompressionPolicy::Always,    256 * kMiB, 64 * kKiB},
    {FormatId::Owner,        "cas_owner",         TextFamily::Control,       KeyStrictness::Tolerant, CompressionPolicy::Never,     1 * kMiB,   64 * kKiB},
    {FormatId::ServerEpoch,  "cas_epoch",         TextFamily::Control,       KeyStrictness::Tolerant, CompressionPolicy::Never,     1 * kMiB,   64 * kKiB},
    {FormatId::MountLease,   "cas_mount_lease",   TextFamily::Control,       KeyStrictness::Tolerant, CompressionPolicy::Never,     1 * kMiB,   64 * kKiB},
};
}

const FormatTraits & traitsFor(FormatId id)
{
    for (const FormatTraits & t : TRAITS)
        if (t.id == id)
            return t;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "CasFormat: no traits for FormatId {} (reserved?)", static_cast<uint16_t>(id));
}

const FormatTraits * traitsForType(std::string_view type)
{
    for (const FormatTraits & t : TRAITS)
        if (t.type == type)
            return &t;
    return nullptr;
}

std::string_view storedSuffix(FormatId id)
{
    return traitsFor(id).compression == CompressionPolicy::Always ? ".zst" : "";
}

}
