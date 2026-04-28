#include <Storages/MergeTree/ANNIndex/PartRowIdMap.h>

#include <xxhash.h>

namespace DB::PartRowIdMapFormat
{

UInt64 hashBody(const void * data, size_t size)
{
    return XXH64(data, size, /*seed*/ 0);
}

}
