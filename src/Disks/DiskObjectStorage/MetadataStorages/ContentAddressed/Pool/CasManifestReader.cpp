#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasManifestReader.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <optional>

namespace ProfileEvents
{
    extern const Event CASPartFolderManifestGets;
}

namespace CurrentMetrics
{
    extern const Metric CASManifestDecodeCacheBytes;
    extern const Metric CASManifestDecodeCacheEntries;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CORRUPTED_DATA;
    extern const int BAD_ARGUMENTS;
}
}

namespace DB::Cas
{

CasManifestReader::CasManifestReader(
    CasRequests & requests_, const Layout & layout_, const PoolMeta & meta_,
    const CasEventSink & event_sink_, size_t manifest_decode_cache_bytes)
    : requests(requests_), layout(layout_), meta(meta_), event_sink(event_sink_)
{
    if (manifest_decode_cache_bytes > 0)
        manifest_cache = std::make_unique<ManifestDecodeCache>(
            "LRU", CurrentMetrics::CASManifestDecodeCacheBytes, CurrentMetrics::CASManifestDecodeCacheEntries,
            manifest_decode_cache_bytes, /*max_count=*/16384, ManifestDecodeCache::DEFAULT_SIZE_RATIO);
}

std::shared_ptr<const PartManifest> CasManifestReader::readManifestShared(const ManifestId & id)
{
    /// One id names one content forever (minted once, written once, only ever deleted), so a cached
    /// decode is served without any request.
    if (manifest_cache)
        if (auto cached = manifest_cache->get(id))
            return cached;

    /// A live reference naming a missing manifest body is a dangling-reference violation
    /// (`INV-NO-DANGLE`). Never substitute an empty manifest: callers must observe the missing object
    /// as an exception. The `GET` alone carries the absence signal, so no `HEAD` precedes it.
    const String key = layout.manifestKey(id);
    CasOperation op = requests.admit();
    std::optional<Object> object = op.read(key, Retry::standard());
    if (!object)
    {
        if (event_sink)
        {
            CasEvent _ev1;
            _ev1.type = CasEventType::ReadMissing;
            _ev1.object_kind = CasEventObjectKind::Manifest;
            _ev1.object_hash = manifestRefDebugString(id.ref);
            _ev1.outcome = "missing";
            _ev1.reason = "live ref names manifest but its object is missing (INV-NO-DANGLE)";
            _ev1.detail = {{"code", "FILE_DOESNT_EXIST"}, {"site", "readManifest"}};
            event_sink(std::move(_ev1));
        }
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
            "live ref names manifest at {} but its object is missing — INV-NO-DANGLE", key);
    }
    ProfileEvents::increment(ProfileEvents::CASPartFolderManifestGets);

    PartManifest body = decodePartManifest(openObject(FormatId::PartManifest, object->bytes));

    /// The journal reference must equal the body's self-described `ref`; otherwise the reference
    /// addresses a different object and the decoded bytes cannot be trusted.
    if (!refMatchesBody(id.ref, body))
    {
        if (event_sink)
        {
            CasEvent _ev2;
            _ev2.type = CasEventType::CorruptDecode;
            _ev2.object_kind = CasEventObjectKind::Manifest;
            _ev2.object_hash = manifestRefDebugString(id.ref);
            _ev2.outcome = "corrupt";
            _ev2.reason = "manifest body `ref` does not match the journal ManifestRef (refMatchesBody)";
            _ev2.detail = {{"code", "CORRUPTED_DATA"}, {"site", "readManifest"}};
            event_sink(std::move(_ev2));
        }
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS manifest at {} body ref does not match the journal ManifestRef — refMatchesBody", key);
    }

    /// The body's `root_namespace_id` must equal the owning namespace. A mismatch is a
    /// cross-namespace dangling reference and would give cleanup the wrong ownership authority.
    if (!manifestNamespaceMatches(id.root_namespace, body))
    {
        if (event_sink)
        {
            CasEvent _ev3;
            _ev3.type = CasEventType::CorruptDecode;
            _ev3.object_kind = CasEventObjectKind::Manifest;
            _ev3.object_hash = manifestRefDebugString(id.ref);
            _ev3.outcome = "corrupt";
            _ev3.reason = "manifest body root_namespace_id does not match the owning namespace (manifestNamespaceMatches)";
            _ev3.detail = {{"code", "CORRUPTED_DATA"}, {"site", "readManifest"}};
            event_sink(std::move(_ev3));
        }
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS manifest at {} body root_namespace_id does not match the owning namespace — manifestNamespaceMatches", key);
    }

    auto decoded = std::make_shared<PartManifest>(std::move(body));
    if (manifest_cache)
        manifest_cache->set(id, decoded);
    return decoded;
}

PartManifest CasManifestReader::readManifest(const ManifestId & id)
{
    return *readManifestShared(id);
}

BlobLocation CasManifestReader::locate(const ManifestEntry & entry) const
{
    /// A ranged read into the content object: the payload starts at a constant offset for blobs
    /// (the pool's fixed blob_header_len — no per-object header read). Inline carries no standalone
    /// object location (there is no Subtree placement on a part manifest).
    switch (entry.placement)
    {
        case EntryPlacement::Blob:
        {
            /// The entry carries the complete blob reference (algorithm and digest), so the key is
            /// derived directly from it. The payload starts at the pool's fixed envelope length;
            /// no object-specific header read is needed before the ranged payload read.
            return BlobLocation{
                .key = layout.blobKey(entry.ref),
                .offset = meta.blob_header_len,
                .length = entry.blob_size,
            };
        }
        case EntryPlacement::Inline:
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "entry placement {} has no blob location", static_cast<int>(entry.placement));
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "entry placement {} has no blob location", static_cast<int>(entry.placement));
}

}
