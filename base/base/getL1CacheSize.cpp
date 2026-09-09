#include <base/getL1CacheSize.h>

#include <base/getCPUCacheSize.h>

/// Function-local static: computed once, and LTO-safe - a namespace-scope dynamic initializer can
/// be dead-code-eliminated.
size_t getL1CacheSize()
{
    static const size_t probed = getCPUDataCacheSize(1);
    return probed ? probed : 32 * 1024;
}
