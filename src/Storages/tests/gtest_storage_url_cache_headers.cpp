#include <gtest/gtest.h>

#include <Storages/Cache/SchemaCache.h>
#include <Storages/StorageURL.h>

using namespace DB;

TEST(StorageURLCacheHeaders, EmptyHeaders)
{
    HTTPHeaderEntries entries;
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries), "");
}

TEST(StorageURLCacheHeaders, CaseInsensitiveAndTrim)
{
    HTTPHeaderEntries entries1{{"Accept-Encoding", "gzip"}};
    HTTPHeaderEntries entries2{{"  accept-encoding  ", "  gzip  "}};
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), "accept-encoding:gzip");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries2), "accept-encoding:gzip");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), canonicalizeHTTPHeadersForSchemaCache(entries2));
}

TEST(StorageURLCacheHeaders, DeterministicOrdering)
{
    HTTPHeaderEntries entries1{{"Accept-Encoding", "gzip"}, {"X-Custom", "value1"}};
    HTTPHeaderEntries entries2{{"X-Custom", "value1"}, {"Accept-Encoding", "gzip"}};
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), canonicalizeHTTPHeadersForSchemaCache(entries2));
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), "accept-encoding:gzip;x-custom:value1");
}

TEST(StorageURLCacheHeaders, SensitiveHeaderRedaction)
{
    HTTPHeaderEntries entries1{{"Authorization", "Bearer secret_token_1"}};
    HTTPHeaderEntries entries2{{"Authorization", "Bearer secret_token_2"}};
    HTTPHeaderEntries entries3{{"Authorization", "Bearer secret_token_1"}};

    String canon1 = canonicalizeHTTPHeadersForSchemaCache(entries1);
    String canon2 = canonicalizeHTTPHeadersForSchemaCache(entries2);
    String canon3 = canonicalizeHTTPHeadersForSchemaCache(entries3);

    /// Ensure plaintext secret is NOT present in canonical representation
    EXPECT_EQ(canon1.find("secret_token_1"), String::npos);
    EXPECT_EQ(canon1.find("Bearer"), String::npos);
    EXPECT_TRUE(canon1.starts_with("authorization:hash:"));

    /// Distinct tokens produce distinct hash keys
    EXPECT_NE(canon1, canon2);

    /// Identical tokens produce matching hash keys
    EXPECT_EQ(canon1, canon3);
}

TEST(StorageURLCacheHeaders, SchemaCacheKeyComparison)
{
    SchemaCache::Key key1{"http://example.com/data.csv", "CSV", "", "", "none", "accept-encoding:gzip"};
    SchemaCache::Key key2{"http://example.com/data.csv", "CSV", "", "", "none", "accept-encoding:identity"};
    SchemaCache::Key key3{"http://example.com/data.csv", "CSV", "", "", "none", "accept-encoding:gzip"};
    SchemaCache::Key key_empty{"http://example.com/data.csv", "CSV", "", "", "none", ""};

    EXPECT_EQ(key1, key3);
    EXPECT_NE(key1, key2);
    EXPECT_NE(key1, key_empty);

    SchemaCache::KeyHash hasher;
    EXPECT_EQ(hasher(key1), hasher(key3));
    EXPECT_NE(hasher(key1), hasher(key2));
    EXPECT_NE(hasher(key1), hasher(key_empty));
}

TEST(StorageURLCacheHeaders, TelemetryHeadersIgnored)
{
    HTTPHeaderEntries entries1{{"Accept-Encoding", "gzip"}, {"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}};
    HTTPHeaderEntries entries2{{"Accept-Encoding", "gzip"}, {"tracestate", "rojo=1"}};
    HTTPHeaderEntries entries3{{"Accept-Encoding", "gzip"}, {"X-Request-ID", "req-12345"}};
    HTTPHeaderEntries entries_clean{{"Accept-Encoding", "gzip"}};

    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), "accept-encoding:gzip");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries2), "accept-encoding:gzip");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries3), "accept-encoding:gzip");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), canonicalizeHTTPHeadersForSchemaCache(entries_clean));
}

TEST(StorageURLCacheHeaders, KeyHashBoundaryCollisionResistance)
{
    /// Adjacent fields with shared boundary:
    /// compression_method="gzip", headers="foo" vs compression_method="gz", headers="ipfoo"
    SchemaCache::Key key_gzip{"http://example.com/data.csv", "CSV", "", "DEFAULT", "gzip", "foo"};
    SchemaCache::Key key_gz{"http://example.com/data.csv", "CSV", "", "DEFAULT", "gz", "ipfoo"};

    EXPECT_NE(key_gzip, key_gz);

    SchemaCache::KeyHash hasher;
    EXPECT_NE(hasher(key_gzip), hasher(key_gz));
}
