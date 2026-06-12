#include <IO/ICacheProvider.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Default bodies for the per-range buffer API. They throw until a provider
/// overrides them; every real provider and test mock does.

CacheViewPtr ICacheProvider::planResidencyView(const StoredObject &, size_t, ByteRange)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "planResidencyView is not implemented by cache provider {}", name());
}

VectorWithMemoryTracking<MissEntry> ICacheProvider::openWriteBuffers(
    const StoredObject &, size_t, const VectorWithMemoryTracking<ByteRange> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "openWriteBuffers is not implemented by cache provider {}", name());
}

}
