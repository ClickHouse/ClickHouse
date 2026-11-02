#include <gtest/gtest.h>

#include <Storages/Cache/SchemaCache.h>
#include <Storages/StorageURL.h>
#include <Common/SipHash.h>
#include <Common/Hex.h>

using namespace DB;

static String computeExpectedHeaderEntry(const String & lower_name, const String & val)
{
    SipHash h;
    h.update(val.data(), val.size());
    String hash_str = "hash:" + getHexUIntLowercase(h.get64());
    return std::to_string(lower_name.size()) + ":" + lower_name + "=" + std::to_string(hash_str.size()) + ":" + hash_str + ";";
}

TEST(StorageURLCacheHeaders, EmptyHeaders)
{
    HTTPHeaderEntries entries;
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries), "");
}

TEST(StorageURLCacheHeaders, CaseInsensitiveAndTrim)
{
    HTTPHeaderEntries entries1{{"Accept-Encoding", "gzip"}};
    HTTPHeaderEntries entries2{{"  accept-encoding  ", "  gzip  "}};

    String expected = computeExpectedHeaderEntry("accept-encoding", "gzip");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), expected);
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries2), expected);
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), canonicalizeHTTPHeadersForSchemaCache(entries2));
}

TEST(StorageURLCacheHeaders, DeterministicOrdering)
{
    HTTPHeaderEntries entries1{{"Accept-Encoding", "gzip"}, {"X-Custom", "value1"}};
    HTTPHeaderEntries entries2{{"X-Custom", "value1"}, {"Accept-Encoding", "gzip"}};

    String expected = computeExpectedHeaderEntry("accept-encoding", "gzip") + computeExpectedHeaderEntry("x-custom", "value1");
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), canonicalizeHTTPHeadersForSchemaCache(entries2));
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), expected);
}

TEST(StorageURLCacheHeaders, UnknownHeaderValueConfidentiality)
{
    /// P1 Security: Arbitrary header names outside any heuristic (e.g. X-Private-Data)
    /// must NEVER expose their raw plaintext values.
    HTTPHeaderEntries entries{{"X-Private-Data", "confidential-secret-12345"}};
    String canon = canonicalizeHTTPHeadersForSchemaCache(entries);

    EXPECT_EQ(canon.find("confidential-secret-12345"), String::npos);
    EXPECT_TRUE(canon.starts_with("14:x-private-data=21:hash:"));
}

TEST(StorageURLCacheHeaders, BroadSensitiveCredentialsHashed)
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

TEST(StorageURLCacheHeaders, DistinctValuesYieldDistinctIdentities)
{
    HTTPHeaderEntries entries1{{"Authorization", "Bearer secret_token_1"}};
    HTTPHeaderEntries entries2{{"Authorization", "Bearer secret_token_2"}};
    HTTPHeaderEntries entries3{{"Authorization", "Bearer secret_token_1"}};

    String canon1 = canonicalizeHTTPHeadersForSchemaCache(entries1);
    String canon2 = canonicalizeHTTPHeadersForSchemaCache(entries2);
    String canon3 = canonicalizeHTTPHeadersForSchemaCache(entries3);

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
}

TEST(StorageURLCacheHeaders, DuplicateHeaderLastValueWins)
{
    /// In HTTP transport, duplicate headers resolve with last-value-wins
    HTTPHeaderEntries entries_plain_then_gzip{{"Accept-Encoding", "identity"}, {"Accept-Encoding", "gzip"}};
    HTTPHeaderEntries entries_gzip_then_plain{{"Accept-Encoding", "gzip"}, {"Accept-Encoding", "identity"}};

    String canon_plain_then_gzip = canonicalizeHTTPHeadersForSchemaCache(entries_plain_then_gzip);
    String canon_gzip_then_plain = canonicalizeHTTPHeadersForSchemaCache(entries_gzip_then_plain);

    EXPECT_NE(canon_plain_then_gzip, canon_gzip_then_plain);

    String expected_gzip = computeExpectedHeaderEntry("accept-encoding", "gzip");
    String expected_plain = computeExpectedHeaderEntry("accept-encoding", "identity");

    EXPECT_EQ(canon_plain_then_gzip, expected_gzip);
    EXPECT_EQ(canon_gzip_then_plain, expected_plain);
}

TEST(StorageURLCacheHeaders, CaseInsensitiveDuplicateNames)
{
    HTTPHeaderEntries entries{{"Accept-Encoding", "identity"}, {"accept-encoding", "gzip"}};
    String canon = canonicalizeHTTPHeadersForSchemaCache(entries);

    String expected_gzip = computeExpectedHeaderEntry("accept-encoding", "gzip");
    EXPECT_EQ(canon, expected_gzip);
}

TEST(StorageURLCacheHeaders, SchemaCacheKeyComparison)
{
    String headers_gzip = computeExpectedHeaderEntry("accept-encoding", "gzip");
    String headers_identity = computeExpectedHeaderEntry("accept-encoding", "identity");

    SchemaCache::Key key1{"http://example.com/data.csv", "CSV", "", "", "none", headers_gzip};
    SchemaCache::Key key2{"http://example.com/data.csv", "CSV", "", "", "none", headers_identity};
    SchemaCache::Key key3{"http://example.com/data.csv", "CSV", "", "", "none", headers_gzip};
    SchemaCache::Key key_empty{"http://example.com/data.csv", "CSV", "", "", "none", ""};

    EXPECT_EQ(key1, key3);
    EXPECT_NE(key1, key2);
    EXPECT_NE(key1, key_empty);

    SchemaCache::KeyHash hasher;
    EXPECT_EQ(hasher(key1), hasher(key3));
    EXPECT_NE(hasher(key1), hasher(key2));
    EXPECT_NE(hasher(key1), hasher(key_empty));
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

TEST(StorageURLCacheHeaders, ExplicitTraceStateHeadersPartitionCache)
{
    HTTPHeaderEntries entries_vendor_one{{"tracestate", "vendor=one"}};
    HTTPHeaderEntries entries_vendor_two{{"tracestate", "vendor=two"}};

    EXPECT_NE(canonicalizeHTTPHeadersForSchemaCache(entries_vendor_one), canonicalizeHTTPHeadersForSchemaCache(entries_vendor_two));
    EXPECT_NE(canonicalizeHTTPHeadersForSchemaCache(entries_vendor_one), "");
    EXPECT_NE(canonicalizeHTTPHeadersForSchemaCache(entries_vendor_two), "");

    HTTPHeaderEntries entries_base{{"Accept-Encoding", "gzip"}};
    HTTPHeaderEntries entries_with_trace{
        {"Accept-Encoding", "gzip"},
        {"tracestate", "vendor=one"}
    };
    EXPECT_NE(canonicalizeHTTPHeadersForSchemaCache(entries_base), canonicalizeHTTPHeadersForSchemaCache(entries_with_trace));
}

TEST(StorageURLCacheHeaders, ExplicitTraceParentHeadersPartitionCache)
{
    HTTPHeaderEntries entries_trace1{{"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}};
    HTTPHeaderEntries entries_trace2{{"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02"}};

    EXPECT_NE(canonicalizeHTTPHeadersForSchemaCache(entries_trace1), canonicalizeHTTPHeadersForSchemaCache(entries_trace2));
    EXPECT_NE(canonicalizeHTTPHeadersForSchemaCache(entries_trace1), "");
    EXPECT_NE(canonicalizeHTTPHeadersForSchemaCache(entries_trace2), "");
}

TEST(StorageURLCacheHeaders, SnappyWireModeDistinction)
{
    EXPECT_EQ(toContentEncodingName(CompressionMethod::Snappy, SnappyMode::Basic), "snappy:basic");
    EXPECT_EQ(toContentEncodingName(CompressionMethod::Snappy, SnappyMode::Framed), "snappy:framed");
    EXPECT_EQ(toContentEncodingName(CompressionMethod::Snappy), "snappy");
    EXPECT_EQ(toContentEncodingName(CompressionMethod::Gzip, SnappyMode::Basic), "gzip");

    SchemaCache::Key key_basic{"http://example.com/data.snappy", "CSV", "", "DEFAULT", "snappy:basic", ""};
    SchemaCache::Key key_framed{"http://example.com/data.snappy", "CSV", "", "DEFAULT", "snappy:framed", ""};

    EXPECT_NE(key_basic, key_framed);

    SchemaCache::KeyHash hasher;
    EXPECT_NE(hasher(key_basic), hasher(key_framed));
}
