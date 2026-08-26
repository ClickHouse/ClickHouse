#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Common/CacheBase.h>
#include <memory>

namespace DB::Cas
{

/// The object key and ranged payload window for a manifest entry stored as a separate blob. The
/// offset is measured from the beginning of the blob object and skips its fixed envelope; `length`
/// is the entry's raw file size. Inline entries do not have a `BlobLocation`.
struct BlobLocation
{
    String key;
    uint64_t offset = 0;                      /// payload start within the object
    uint64_t length = 0;
};

/// Reads and validates part manifests, caches immutable decodes, and translates blob entries into
/// ranged object reads. A read first obtains the object's current backend token, then reuses a
/// decode only for the matching `(ManifestId, Token)` pair; a cache miss performs a `GET` and
/// validates both the manifest reference and owning namespace before publication into the cache.
/// Missing or changing objects and failed identity checks are surfaced as exceptions, never as an
/// empty or partially trusted manifest.
///
/// The reader receives its backend, immutable layout and pool metadata, and event sink by reference;
/// it has no `Pool` back-reference and owns no `Pool`-level mutex. The decode cache is a
/// byte-weighted `CacheBase` LRU whose synchronization is internal to `CacheBase`; a null cache
/// means caching is disabled (`manifest_decode_cache_bytes == 0`).
class CasManifestReader
{
public:
    /// Binds the reader to the pool environment. A positive cache budget creates the byte-weighted
    /// LRU; zero disables caching while leaving the mandatory `HEAD` and validation sequence intact.
    CasManifestReader(
        Backend & backend_, const Layout & layout_, const PoolMeta & meta_,
        const CasEventSink & event_sink_, size_t manifest_decode_cache_bytes);

    /// Reads a manifest by value using the fail-closed sequence described above. A missing body,
    /// disappearance between `HEAD` and `GET`, decode failure, or either identity mismatch throws;
    /// only a fully validated decode can enter the cache.
    PartManifest readManifest(const ManifestId & id);

    /// Reads a manifest like `readManifest` but returns the immutable shared decode. This preserves
    /// the cache's value on the part-folder path and avoids copying all manifest entries on success.
    std::shared_ptr<const PartManifest> readManifestShared(const ManifestId & id);

    /// Computes the object key and payload window for a `Blob` entry without performing I/O. An
    /// `Inline` entry, or any unsupported placement value, throws `BAD_ARGUMENTS` because it has no
    /// standalone object to read.
    BlobLocation locate(const ManifestEntry & entry) const;

    /// Test seam: retained bytes of the manifest decode cache (0 when disabled).
    size_t manifestDecodeCacheBytes() const { return manifest_cache ? manifest_cache->sizeInBytes() : 0; }

private:
    /// The cache must include the backend token: a reused manifest identifier can refer to a new
    /// object incarnation, and its immutable decoded bytes must not be reused across incarnations.
    struct ManifestCacheKey
    {
        ManifestId manifest_id;
        Token token;
        bool operator==(const ManifestCacheKey &) const = default;
    };

    /// Hashes both identity components and the token type so cache lookup uses the same complete
    /// identity as `ManifestCacheKey::operator==`.
    struct ManifestCacheKeyHash
    {
        size_t operator()(const ManifestCacheKey & k) const;
    };

    /// Estimates retained decode memory from fixed object overhead plus entry path and inline-byte
    /// storage. Weighting by bytes gives a server reading many parts an honest memory ceiling instead
    /// of a count-only bound; the cache key still provides the fail-closed token semantics.
    struct PartManifestWeight
    {
        /// Returns the approximate bytes retained for one decoded manifest by the cache.
        size_t operator()(const PartManifest & m) const
        {
            size_t bytes = 256;
            for (const auto & e : m.entries)
                bytes += e.path.size() + e.inline_bytes.size() + 96;
            return bytes;
        }
    };
    using ManifestDecodeCache = CacheBase<ManifestCacheKey, PartManifest, ManifestCacheKeyHash, PartManifestWeight>;

    Backend & backend;
    const Layout & layout;
    const PoolMeta & meta;
    const CasEventSink & event_sink;
    std::unique_ptr<ManifestDecodeCache> manifest_cache;
};

}
