#include <gtest/gtest.h>
#include <Storages/Cache/ObjectStorageListObjectsCache.h>
#include <memory>
#include <thread>

namespace DB
{

class ObjectStorageListObjectsCacheTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        cache = std::unique_ptr<ObjectStorageListObjectsCache>(new ObjectStorageListObjectsCache());
        cache->setTTL(3);
        cache->setMaxCount(100);
        cache->setMaxSizeInBytes(1000000);
    }

    std::unique_ptr<ObjectStorageListObjectsCache> cache;
    static ObjectStorageListObjectsCache::Key default_key;

    static std::shared_ptr<ObjectStorageListObjectsCache::Value> createTestValue(const std::vector<std::string>& paths)
    {
        auto value = std::make_shared<ObjectStorageListObjectsCache::Value>();
        for (const auto & path : paths)
        {
            value->push_back(std::make_shared<ObjectInfo>(path));
        }
        return value;
    }
};

ObjectStorageListObjectsCache::Key ObjectStorageListObjectsCacheTest::default_key {"default", "test-bucket", "test-prefix/", false};

TEST_F(ObjectStorageListObjectsCacheTest, BasicSetAndGet)
{
    cache->clear();
    auto value = createTestValue({"test-prefix/file1.txt", "test-prefix/file2.txt"});

    cache->set(default_key, value);
    
    auto result = cache->get(default_key).value();

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0]->getPath(), "test-prefix/file1.txt");
    EXPECT_EQ(result[1]->getPath(), "test-prefix/file2.txt");
}

TEST_F(ObjectStorageListObjectsCacheTest, CacheMiss)
{
    cache->clear();

    EXPECT_FALSE(cache->get(default_key));
}

TEST_F(ObjectStorageListObjectsCacheTest, ClearCache)
{
    cache->clear();
    auto value = createTestValue({"test-prefix/file1.txt", "test-prefix/file2.txt"});

    cache->set(default_key, value);
    cache->clear();

    EXPECT_FALSE(cache->get(default_key));
}

TEST_F(ObjectStorageListObjectsCacheTest, PrefixMatching)
{
    cache->clear();

    auto short_prefix_key = default_key;
    short_prefix_key.prefix = "parent/";

    auto mid_prefix_key = default_key;
    mid_prefix_key.prefix = "parent/child/";

    auto long_prefix_key = default_key;
    long_prefix_key.prefix = "parent/child/grandchild/";

    auto value = createTestValue(
    {
        "parent/child/grandchild/file1.txt",
        "parent/child/grandchild/file2.txt"});

    cache->set(mid_prefix_key, value);

    auto result1 = cache->get(mid_prefix_key).value();
    EXPECT_EQ(result1.size(), 2);

    auto result2 = cache->get(long_prefix_key).value();
    EXPECT_EQ(result2.size(), 2);

    EXPECT_FALSE(cache->get(short_prefix_key));
}

TEST_F(ObjectStorageListObjectsCacheTest, PrefixFiltering)
{
    cache->clear();

    auto key_with_short_prefix = default_key;
    key_with_short_prefix.prefix = "parent/";

    auto key_with_mid_prefix = default_key;
    key_with_mid_prefix.prefix = "parent/child1/";

    auto value = createTestValue({
        "parent/file1.txt",
        "parent/child1/file2.txt",
        "parent/child2/file3.txt"
    });

    cache->set(key_with_short_prefix, value);

    auto result = cache->get(key_with_mid_prefix, true).value();
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0]->getPath(), "parent/child1/file2.txt");
}

TEST_F(ObjectStorageListObjectsCacheTest, TTLExpiration)
{
    cache->clear();
    auto value = createTestValue({"test-prefix/file1.txt"});

    cache->set(default_key, value);

    // Verify we can get it immediately
    auto result1 = cache->get(default_key).value();
    EXPECT_EQ(result1.size(), 1);

    std::this_thread::sleep_for(std::chrono::seconds(4));

    EXPECT_FALSE(cache->get(default_key));
}

TEST_F(ObjectStorageListObjectsCacheTest, TTLUnlimited)
{
    cache->clear();
    cache->setTTL(0); // 0 means unlimited
    auto value = createTestValue({"test-prefix/file1.txt"});

    cache->set(default_key, value);

    // Verify we can get it immediately
    auto result1 = cache->get(default_key).value();
    EXPECT_EQ(result1.size(), 1);

    // Sleep for a reasonable amount (longer than the default 3 second TTL from SetUp)
    std::this_thread::sleep_for(std::chrono::seconds(5));

    // Should still be available since TTL is unlimited
    auto result2 = cache->get(default_key).value();
    EXPECT_EQ(result2.size(), 1);
    EXPECT_EQ(result2[0]->getPath(), "test-prefix/file1.txt");
}

TEST_F(ObjectStorageListObjectsCacheTest, TTLSwitchFromUnlimitedToFinite)
{
    cache->clear();
    cache->setTTL(0); // Start with unlimited
    auto value1 = createTestValue({"test-prefix/file1.txt"});
    auto key1 = default_key;
    key1.prefix = "unlimited/";

    cache->set(key1, value1);

    // Switch to finite TTL and add another entry
    cache->setTTL(1);
    auto value2 = createTestValue({"test-prefix/file2.txt"});
    auto key2 = default_key;
    key2.prefix = "finite/";

    cache->set(key2, value2);

    // Verify both are available immediately
    EXPECT_TRUE(cache->get(key1).has_value());
    EXPECT_TRUE(cache->get(key2).has_value());

    // Wait for finite TTL entry to expire
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Unlimited entry should still be there, finite should be gone
    EXPECT_TRUE(cache->get(key1).has_value());
    EXPECT_FALSE(cache->get(key2).has_value());
}

TEST_F(ObjectStorageListObjectsCacheTest, BestPrefixMatch)
{
    cache->clear();

    auto short_prefix_key = default_key;
    short_prefix_key.prefix = "a/b/";

    auto mid_prefix_key = default_key;
    mid_prefix_key.prefix = "a/b/c/";

    auto long_prefix_key = default_key;
    long_prefix_key.prefix = "a/b/c/d/";

    auto short_prefix = createTestValue({"a/b/c/d/file1.txt", "a/b/c/file1.txt", "a/b/file2.txt"});
    auto mid_prefix = createTestValue({"a/b/c/d/file1.txt", "a/b/c/file1.txt"});

    cache->set(short_prefix_key, short_prefix);
    cache->set(mid_prefix_key, mid_prefix);

    // should pick mid_prefix, which has size 2. filter_by_prefix=false so we can assert by size
    auto result = cache->get(long_prefix_key, false).value();
    EXPECT_EQ(result.size(), 2u);
}

TEST_F(ObjectStorageListObjectsCacheTest, WithTags)
{
    cache->clear();

    auto key_with_tags = default_key;
    key_with_tags.with_tags = true;

    auto value_with_tags = createTestValue({"test.txt"});

    cache->set(key_with_tags, value_with_tags);

    /// we have set with tags, we should be able to retrieve it
    auto result_with_tags = cache->get(key_with_tags).value();
    EXPECT_EQ(result_with_tags.size(), 1u);
    EXPECT_EQ(result_with_tags[0]->getPath(), "test.txt");

    /// querying by a key without tags should return nothing
    auto result_without_tags = cache->get(default_key);
    EXPECT_FALSE(result_without_tags.has_value());

    cache->clear();

    cache->set(default_key, value_with_tags);

    /// querying by a key with tags should return nothing
    EXPECT_FALSE(cache->get(key_with_tags).has_value());

    /// querying by a key without tags should return the value
    auto result_without_tags_2 = cache->get(default_key).value();
    EXPECT_EQ(result_without_tags_2.size(), 1u);
    EXPECT_EQ(result_without_tags_2[0]->getPath(), "test.txt");
}

}
