#include <Common/CacheBase.h>
#include <Common/ColumnsHashing.h>

namespace DB
{
template class CacheBase<
    ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKey,
    ColumnsHashing::LowCardinalityDictionaryCache::CachedValues,
    ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKeyHash>;
}
