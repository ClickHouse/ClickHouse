#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>

#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event CASMetaPut;
    extern const Event CASMetaCompareSwap;
    extern const Event CASMetaDelete;
}

namespace DB::Cas
{

std::optional<LoadedMeta> loadMeta(Backend & backend, const Layout & layout, const BlobRef & ref)
{
    const String key = layout.blobMetaKey(ref);
    auto got = backend.get(key);
    if (!got)
        return std::nullopt;
    return LoadedMeta{.meta = decodeBlobMeta(got->bytes), .etag = got->token};
}

CasOverwriteResult putMetaIfAbsent(Pool & pool, const BlobRef & ref, const BlobMeta & meta)
{
    ProfileEvents::increment(ProfileEvents::CASMetaPut);
    const String key = pool.layout().blobMetaKey(ref);
    return pool.stagingPutIfAbsentMutable(key, encodeBlobMeta(meta));
}

CasOverwriteResult casMeta(Pool & pool, const BlobRef & ref, const Token & expected, const BlobMeta & meta)
{
    ProfileEvents::increment(ProfileEvents::CASMetaCompareSwap);
    const String key = pool.layout().blobMetaKey(ref);
    return pool.stagingConditionalOverwrite(key, encodeBlobMeta(meta), expected);
}

DeleteOutcome deleteMetaExact(Backend & backend, const Layout & layout, const BlobRef & ref, const Token & expected)
{
    ProfileEvents::increment(ProfileEvents::CASMetaDelete);
    const String key = layout.blobMetaKey(ref);
    return backend.deleteExact(key, expected);
}

}
