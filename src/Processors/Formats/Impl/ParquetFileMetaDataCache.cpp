#include <Processors/Formats/Impl/ParquetFileMetaDataCache.h>

#if USE_PARQUET

namespace DB
{

ParquetFileMetaDataCache::ParquetFileMetaDataCache()
    : CacheBase<String, parquet::FileMetaData>(CurrentMetrics::end(), CurrentMetrics::end(), 0)
{}

ParquetFileMetaDataCache * ParquetFileMetaDataCache::instance()
{
    static ParquetFileMetaDataCache instance;
    return &instance;
}

}

#endif
