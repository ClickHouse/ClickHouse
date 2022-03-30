#include "config_core.h"

#if USE_ROCKSDB
#include <gtest/gtest.h>
#include <rocksdb/table.h>
#include <rocksdb/db.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeMetadataCache.h>

using namespace DB;

class MergeTreeMetadataCacheTest : public ::testing::Test
{
public:
    void SetUp() override
    {
        cache = MergeTreeMetadataCache::create("./db/", 268435456);
    }

    void TearDown() override
    {
        cache->shutdown();
        cache.reset();
    }

    MergeTreeMetadataCachePtr cache;
};

TEST_F(MergeTreeMetadataCacheTest, testCommon)
{
    std::vector<String> files
        = {"columns.txt", "checksums.txt", "primary.idx", "count.txt", "partition.dat", "minmax_p.idx", "default_compression_codec.txt"};
    String prefix = "data/test_metadata_cache/check_part_metadata_cache/201806_1_1_0_4/";

    for (const auto & file : files)
    {
        auto status = cache->put(prefix + file, prefix + file);
        ASSERT_EQ(status.code(), rocksdb::Status::Code::kOk);
    }

    for (const auto & file : files)
    {
        String value;
        auto status = cache->get(prefix + file, value);
        ASSERT_EQ(status.code(), rocksdb::Status::Code::kOk);
        ASSERT_EQ(value, prefix + file);
    }

    {
        Strings keys;
        Strings values;
        cache->getByPrefix(prefix, keys, values);
        ASSERT_EQ(keys.size(), files.size());
        ASSERT_EQ(values.size(), files.size());
        for (size_t i = 0; i < files.size(); ++i)
        {
            ASSERT_EQ(values[i], keys[i]);
        }
    }

    for (const auto & file : files)
    {
        auto status = cache->del(prefix + file);
        ASSERT_EQ(status.code(), rocksdb::Status::Code::kOk);
    }

    for (const auto & file : files)
    {
        String value;
        auto status = cache->get(prefix + file, value);
        ASSERT_EQ(status.code(), rocksdb::Status::Code::kNotFound);
    }

    {
        Strings keys;
        Strings values;
        cache->getByPrefix(prefix, keys, values);
        ASSERT_EQ(keys.size(), 0);
        ASSERT_EQ(values.size(), 0);
    }
}

#endif
