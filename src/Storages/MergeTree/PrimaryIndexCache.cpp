#include <Storages/MergeTree/PrimaryIndexCache.h>

namespace DB
{

template class CacheBase<UInt128, PrimaryIndex, UInt128TrivialHash, PrimaryIndexWeightFunction>;

}
