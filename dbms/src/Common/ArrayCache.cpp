#include "ArrayCache.h"

template <typename Key, typename Payload>
constexpr size_t ArrayCache<Key, Payload>::min_chunk_size;

template class ArrayCache<int, int>;
