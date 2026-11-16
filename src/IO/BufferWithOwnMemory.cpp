#include "config.h"

#include <IO/BufferWithOwnMemoryImpl.h>

#include <Common/JemallocCacheAllocator.h>


namespace DB
{

template void Memory<Allocator<false>>::alloc(size_t);
template void Memory<Allocator<false>>::resize(size_t, bool);

template void Memory<Allocator<true>>::alloc(size_t);
template void Memory<Allocator<true>>::resize(size_t, bool);

/// When jemalloc is disabled JemallocCacheAllocator is an alias for Allocator<false>, already instantiated above.
#if USE_JEMALLOC
template void Memory<JemallocCacheAllocator>::alloc(size_t);
template void Memory<JemallocCacheAllocator>::resize(size_t, bool);
#endif

}
