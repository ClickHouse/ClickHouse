#include <base/getL2CacheSize.h>

#include <base/getCPUCacheSize.h>

size_t getL2CacheSize()
{
    static const size_t probed = getCPUDataCacheSize(2);
    return probed ? probed : 256 * 1024;
}
