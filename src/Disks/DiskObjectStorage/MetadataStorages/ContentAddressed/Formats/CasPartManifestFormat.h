#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace DB::Cas
{

/// Text codec for the immutable, root-local `cas_part_manifest` body. It is a `PayloadHybrid` object
/// made of JSON lines followed by a raw payload zone. The public types and helper signatures remain
/// stable for the surrounding CAS protocol.
///
///   header line                 {"type":"cas_part_manifest","v":N}
///   descriptor meta line         {"epoch","build","ord"} (the ManifestRef, shared rendering with
///                                refsnaplog, `CasWireVocab.h`) + `root_namespace` + `payload_digest`
///                                (payload digest, 32 lowercase hex)
///   one entry-record line each   {"path":path,"place":placement-word, then either the Blob's
///                                {"algo","digest","size"} or the Inline's {"size"}}, in canonical path order
///   trailer line                 {"n":entry-count}
///   PAYLOAD ZONE (raw, follows the trailer): for each Inline entry, in path order, a
///                                `head -v`-style banner line `==> "<escaped path>" size=<n> <==\n`, then
///                                exactly `n` raw bytes, then `\n`. The path uses the same writer as
///                                the entry-record line, so decode can rebuild the banner byte-wise.
///                                Blob entries carry no
///                                payload-zone bytes — their bytes live in a separately addressed
///                                CAS blob; the manifest carries only the `BlobRef` + size.
///
/// The payload zone is why this format is `PayloadHybrid` rather than a plain `RecordStream`/
/// `Control` object: `inline_bytes` is arbitrary binary, not necessarily valid UTF-8, so it cannot be
/// JSON-string-encoded safely and instead rides outside the JSON-line region entirely.

/// Where a manifest entry's file bytes live. There are no nested tree objects: a directory is a path
/// prefix, not a placement. `Inline` bytes belong to the manifest's raw payload zone; `Blob` bytes
/// are stored separately under `blobKey`.
enum class EntryPlacement : uint8_t
{
    Inline = 1,   /// bytes embedded in `inline_bytes`
    Blob = 2,     /// bytes stored as a content-addressed blob at `blobKey`
};

/// Canonical wire word for one manifest entry placement.
std::string_view entryPlacementToWireWord(EntryPlacement placement);

/// Its fail-closed inverse: an unknown word is `CORRUPTED_DATA`.
EntryPlacement entryPlacementFromWireWord(std::string_view w);

/// One file entry inside a part manifest. `ref` is meaningful only for `Blob`; `inline_bytes` only
/// for `Inline`. `blob_size` is the raw `Blob` byte count (0 for `Inline` — decode never fills it for
/// an inline entry, since the wire format carries no redundant size for inline bytes). Use `size()`
/// for the entry's logical file size regardless of placement; no consumer should branch on
/// `placement` just to answer "how big is this file". `ref` is the full blob identity: the algorithm
/// travels with the digest, per entry, so a manifest may mix hash algorithms. A bare digest is never
/// the identity.
struct ManifestEntry
{
    String path;
    EntryPlacement placement = EntryPlacement::Inline;
    BlobRef ref{};
    uint64_t blob_size = 0;
    String inline_bytes;
    bool operator==(const ManifestEntry &) const = default;

    /// The single source of truth for this entry's logical file size, independent of where its bytes
    /// live. Decoding an `Inline` entry leaves `blob_size == 0`, because the wire record has no
    /// redundant blob size; a carried-forward inline entry can therefore be non-empty even though
    /// `blob_size` is zero. Consumers that need the logical file size must use this method rather
    /// than inspecting the placement-specific fields themselves.
    uint64_t size() const { return placement == EntryPlacement::Inline ? inline_bytes.size() : blob_size; }
};

/// The immutable body of one root-local part manifest. It repeats `ref` and `root_namespace_id` so
/// readers can validate the journal reference and owning root namespace against the body; neither
/// repetition is a second identity. `payload_digest` is integrity/debug metadata only: it is never a
/// key, deduplication input, or in-degree. Mutable per-reference payload remains in the root
/// `RefRecord`. Entries carry their own digest algorithm, so one manifest may mix digest widths.
/// Entries are strictly ascending by `path` after decoding, which permits index-free binary-search
/// and prefix-range lookup without adding a directory index to the immutable body.
struct PartManifest
{
    ManifestRef ref;
    RootNamespace root_namespace_id;
    UInt128 payload_digest{};
    std::vector<ManifestEntry> entries;
    bool operator==(const PartManifest &) const = default;
};

/// Deterministic, streaming-capable encode. Entries are written in canonical path order (the encoder
/// sorts them); a duplicate path throws `CORRUPTED_DATA`. Byte output is reproducible for identical
/// input (no timestamps, no nondeterministic compression). Returns the canonical TEXT (NOT sealed);
/// the caller compresses via `sealObject(FormatId::PartManifest, …)` on the persist path
/// (`CompressionPolicy::Always`).
String encodePartManifest(const PartManifest & m);

/// Decode the canonical TEXT (the caller `openObject`s a stored `.zst` first). Throws `CORRUPTED_DATA`
/// on a malformed header/descriptor/record/trailer/payload-zone shape or an unknown placement word;
/// `UNKNOWN_FORMAT_VERSION` for a header `v` above this build.
PartManifest decodePartManifest(std::string_view data);

/// Content digest of the canonical encoded body, using the CAS content-hash primitive
/// (`CityHash_v1_0_2::CityHash128`, the same one used for blob/tree hashing, not a second hash
/// primitive). Callers set `PartManifest.payload_digest` from this. It is
/// integrity/debug ONLY - never a key, never dedup, never in-degree. Stable for identical bodies;
/// changes when any byte of the canonical encoding does, and is independent of the `payload_digest`
/// field itself (computed with it zeroed).
UInt128 computePayloadDigest(const PartManifest & m);

/// Fail-closed identity checks used when reading or folding a manifest. The journal `ManifestRef`
/// must equal the `ref` inside the decoded body.
bool refMatchesBody(const ManifestRef & journal_ref, const PartManifest & body);
/// The body `root_namespace_id` must equal the owning root namespace.
bool manifestNamespaceMatches(const RootNamespace & owning, const PartManifest & body);

/// Pure ordered-entry lookup primitives over a decoded manifest. `decodePartManifest` guarantees
/// strict ascending order by `path`; `PartFolderView` composes these lookups with wiring policy.

/// Binary search. Returns nullptr when absent. The pointer aliases `entries` — do not outlive it.
const ManifestEntry * findEntry(const std::vector<ManifestEntry> & entries, std::string_view path);

/// The contiguous [first, last) sub-span of entries whose path starts with `dir_prefix` (canonical
/// order makes matches contiguous). Empty prefix = the whole span.
std::pair<const ManifestEntry *, const ManifestEntry *>
entryRange(const std::vector<ManifestEntry> & entries, std::string_view dir_prefix);

}
