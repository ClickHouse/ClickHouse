#include <IO/ICacheProvider.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Default bodies for the new per-range buffer API. They throw until a provider
/// overrides them, so the old `ICacheHandle`-based providers and the test mocks
/// still compile during the migration without implementing the new methods.

CacheViewPtr ICacheProvider::lookupView(const StoredObject &, size_t, ByteRange)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "lookupView is not implemented by cache provider {}", name());
}

CacheViewPtr ICacheProvider::planResidencyView(
    const StoredObject & object, size_t object_file_offset, ByteRange range_in_file)
{
    return lookupView(object, object_file_offset, range_in_file);
}

std::vector<MissEntry> ICacheProvider::openWriteBuffers(
    const StoredObject &, size_t, const std::vector<ByteRange> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "openWriteBuffers is not implemented by cache provider {}", name());
}

}
