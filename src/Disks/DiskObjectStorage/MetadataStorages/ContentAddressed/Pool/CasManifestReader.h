#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
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

/// Reads and validates part manifests, caches immutable decodes by `ManifestId`, and translates blob
/// entries into ranged object reads. A manifest id is minted once and its body is written once, so
/// one id names one content forever: a cache hit is served without any request, and a miss performs
/// one `GET` and validates both the manifest reference and the owning namespace before publication
/// into the cache. A missing body, a decode failure or a failed identity check is surfaced as an
/// exception, never as an empty or partially trusted manifest.
///
/// The reader receives its `CasRequests`, immutable layout and pool metadata, and event sink by
/// reference; it has no `Pool` back-reference and owns no `Pool`-level mutex. Each cache-miss read
/// admits its own operation, so a lost mount lease refuses a miss the same way any other read does.
/// The decode cache is a byte-weighted `CacheBase` LRU whose synchronization is internal to
/// `CacheBase`; a null cache means caching is disabled (`manifest_decode_cache_bytes == 0`).
class CasManifestReader
{
public:
    /// Binds the reader to the pool environment. A positive cache budget creates the byte-weighted
    /// LRU; zero disables caching while leaving the one-`GET`-and-validate sequence intact.
    CasManifestReader(
        CasRequests & requests_, const Layout & layout_, const PoolMeta & meta_,
        const CasEventSink & event_sink_, size_t manifest_decode_cache_bytes);

    /// Reads a manifest by value using the fail-closed sequence described above. A missing body,
    /// decode failure, or either identity mismatch throws; only a fully validated decode can enter
    /// the cache.
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
    /// Estimates retained decode memory from fixed object overhead plus entry path and inline-byte
    /// storage. Weighting by bytes gives a server reading many parts an honest memory ceiling
    /// instead of a count-only bound.
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
    using ManifestDecodeCache = CacheBase<ManifestId, PartManifest, std::hash<ManifestId>, PartManifestWeight>;

    CasRequests & requests;
    const Layout & layout;
    const PoolMeta & meta;
    const CasEventSink & event_sink;
    std::unique_ptr<ManifestDecodeCache> manifest_cache;
};

}
