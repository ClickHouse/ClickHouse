#include <Processors/Formats/Impl/ParquetMetadataCache.h>

#if USE_PARQUET

namespace ProfileEvents
{
    extern const Event ParquetMetadataCacheMisses;
    extern const Event ParquetMetadataCacheHits;
    extern const Event ParquetMetadataCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric ParquetMetadataCacheBytes;
    extern const Metric ParquetMetadataCacheFiles;
}

namespace DB
{
    // Implementation is header-only, but we need this file for ProfileEvents and CurrentMetrics registration
}

#endif
