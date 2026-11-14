#include <base/getL1CacheSize.h>

#include <base/getCPUCacheSize.h>

size_t getL1CacheSize()
{
    static const size_t probed = getCPUDataCacheSize(1);
    return probed ? probed : 32 * 1024;
}
