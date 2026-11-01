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
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), "15:accept-encoding=4:gzip;");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries2), "15:accept-encoding=4:gzip;");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), canonicalizeHTTPHeadersForSchemaCache(entries2));
}

TEST(StorageURLCacheHeaders, DeterministicOrdering)
{
    HTTPHeaderEntries entries1{{"Accept-Encoding", "gzip"}, {"X-Custom", "value1"}};
    HTTPHeaderEntries entries2{{"X-Custom", "value1"}, {"Accept-Encoding", "gzip"}};
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), canonicalizeHTTPHeadersForSchemaCache(entries2));
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), "15:accept-encoding=4:gzip;8:x-custom=6:value1;");
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
    EXPECT_TRUE(canon1.starts_with("13:authorization=21:hash:"));

    /// Distinct tokens produce distinct hash keys
    EXPECT_NE(canon1, canon2);

    /// Identical tokens produce matching hash keys
    EXPECT_EQ(canon1, canon3);
}

TEST(StorageURLCacheHeaders, AmbiguousDelimiterBoundaryCollisionResistance)
{
    /// Delimiter collision: X-A: v;x-b:w vs X-A: v and X-B: w
    HTTPHeaderEntries entries_embedded{{"X-A", "v;x-b:w"}};
    HTTPHeaderEntries entries_separate{{"X-A", "v"}, {"X-B", "w"}};

    String canon_embedded = canonicalizeHTTPHeadersForSchemaCache(entries_embedded);
    String canon_separate = canonicalizeHTTPHeadersForSchemaCache(entries_separate);

    EXPECT_NE(canon_embedded, canon_separate);
    EXPECT_EQ(canon_embedded, "3:x-a=7:v;x-b:w;");
    EXPECT_EQ(canon_separate, "3:x-a=1:v;3:x-b=1:w;");
}

TEST(StorageURLCacheHeaders, DuplicateHeaderLastValueWins)
{
    /// In HTTP transport, duplicate headers resolve with last-value-wins
    HTTPHeaderEntries entries_plain_then_gzip{{"Accept-Encoding", "identity"}, {"Accept-Encoding", "gzip"}};
    HTTPHeaderEntries entries_gzip_then_plain{{"Accept-Encoding", "gzip"}, {"Accept-Encoding", "identity"}};

    String canon_plain_then_gzip = canonicalizeHTTPHeadersForSchemaCache(entries_plain_then_gzip);
    String canon_gzip_then_plain = canonicalizeHTTPHeadersForSchemaCache(entries_gzip_then_plain);

    EXPECT_NE(canon_plain_then_gzip, canon_gzip_then_plain);
    EXPECT_EQ(canon_plain_then_gzip, "15:accept-encoding=4:gzip;");
    EXPECT_EQ(canon_gzip_then_plain, "15:accept-encoding=8:identity;");
}

TEST(StorageURLCacheHeaders, BroadSensitiveHeaderRedaction)
{
    /// Verify X-Password, X-Auth, Authorization, Cookie, X-API-Key, and custom credential headers
    HTTPHeaderEntries sensitive_headers{
        {"X-Password", "super_secret_pw"},
        {"X-Auth", "auth_token_value"},
        {"Authorization", "Bearer jwt_secret"},
        {"Cookie", "session_id=12345"},
        {"X-API-Key", "api_key_value"},
        {"X-Secret-Token", "token_val"},
        {"Credential-Data", "creds_999"},
    };

    String canon = canonicalizeHTTPHeadersForSchemaCache(sensitive_headers);

    /// None of the plaintext secrets should appear anywhere in the output
    EXPECT_EQ(canon.find("super_secret_pw"), String::npos);
    EXPECT_EQ(canon.find("auth_token_value"), String::npos);
    EXPECT_EQ(canon.find("Bearer"), String::npos);
    EXPECT_EQ(canon.find("jwt_secret"), String::npos);
    EXPECT_EQ(canon.find("session_id"), String::npos);
    EXPECT_EQ(canon.find("api_key_value"), String::npos);
    EXPECT_EQ(canon.find("token_val"), String::npos);
    EXPECT_EQ(canon.find("creds_999"), String::npos);

    /// All should be present with hashed values
    EXPECT_TRUE(canon.contains("authorization=21:hash:"));
    EXPECT_TRUE(canon.contains("cookie=21:hash:"));
    EXPECT_TRUE(canon.contains("credential-data=21:hash:"));
    EXPECT_TRUE(canon.contains("x-api-key=21:hash:"));
    EXPECT_TRUE(canon.contains("x-auth=21:hash:"));
    EXPECT_TRUE(canon.contains("x-password=21:hash:"));
    EXPECT_TRUE(canon.contains("x-secret-token=21:hash:"));
}

TEST(StorageURLCacheHeaders, SchemaCacheKeyComparison)
{
    SchemaCache::Key key1{"http://example.com/data.csv", "CSV", "", "", "none", "15:accept-encoding=4:gzip;"};
    SchemaCache::Key key2{"http://example.com/data.csv", "CSV", "", "", "none", "15:accept-encoding=8:identity;"};
    SchemaCache::Key key3{"http://example.com/data.csv", "CSV", "", "", "none", "15:accept-encoding=4:gzip;"};
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

    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), "15:accept-encoding=4:gzip;");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries2), "15:accept-encoding=4:gzip;");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries3), "15:accept-encoding=4:gzip;");
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
