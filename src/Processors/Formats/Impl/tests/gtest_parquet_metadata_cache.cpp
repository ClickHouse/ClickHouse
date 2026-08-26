#include <gtest/gtest.h>

#include "config.h"

#if USE_PARQUET

#include <Processors/Formats/Impl/ParquetMetadataCache.h>

namespace DB
{

TEST(ParquetMetadataCache, SharesImmutableMetadataAndKeepsItAliveAfterEviction)
{
    ParquetMetadataCache cache("SLRU", 1 << 20, 100, 0.5);
    const auto key = ParquetMetadataCache::createKey("file.parquet", "strong-etag");
    size_t loads = 0;
    auto load = [&]
    {
        ++loads;
        parquet::format::FileMetaData metadata;
        metadata.num_rows = 42;
        return metadata;
    };

    auto first = cache.getOrSetMetadataPtr(key, load);
    auto second = cache.getOrSetMetadataPtr(key, load);
    EXPECT_EQ(loads, 1);
    EXPECT_EQ(first.get(), second.get());
    EXPECT_EQ(first->num_rows, 42);

    cache.clear();
    EXPECT_EQ(first->num_rows, 42);
}

}

#endif
