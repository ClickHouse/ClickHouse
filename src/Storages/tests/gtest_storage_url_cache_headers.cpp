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
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), canonicalizeHTTPHeadersForSchemaCache(entries2));
    EXPECT_TRUE(canonicalizeHTTPHeadersForSchemaCache(entries1).starts_with("15:accept-encoding=21:hash:"));
}

TEST(StorageURLCacheHeaders, DeterministicOrdering)
{
    HTTPHeaderEntries entries1{{"Accept-Encoding", "gzip"}, {"X-Custom", "value1"}};
    HTTPHeaderEntries entries2{{"X-Custom", "value1"}, {"Accept-Encoding", "gzip"}};
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), canonicalizeHTTPHeadersForSchemaCache(entries2));
    EXPECT_TRUE(canonicalizeHTTPHeadersForSchemaCache(entries1).starts_with("15:accept-encoding=21:hash:"));
    EXPECT_TRUE(canonicalizeHTTPHeadersForSchemaCache(entries1).contains(";8:x-custom=21:hash:"));
}

TEST(StorageURLCacheHeaders, UniversalSecretMasking)
{
    std::vector<String> test_header_names
        = {"Authorization",
           "Cookie",
           "X-API-Key",
           "X-Password",
           "X-Auth",
           "X-Internal-Secret",
           "X-Custom-Credential",
           "Customer-Token",
           "AnyArbitraryHeader"};

    for (const auto & hname : test_header_names)
    {
        String secret = "SuperSecretValue_XYZ_12345";
        HTTPHeaderEntries entries{{hname, secret}};
        String canon = canonicalizeHTTPHeadersForSchemaCache(entries);

        /// Plaintext secret MUST NOT appear anywhere in the serialized representation
        EXPECT_EQ(canon.find("SuperSecretValue_XYZ_12345"), String::npos);
        EXPECT_TRUE(canon.contains("=21:hash:"));
    }
}

TEST(StorageURLCacheHeaders, DistinctAndMatchingSecretsPartitionCorrectly)
{
    String url = "http://127.0.0.1:8123/data.csv";
    HTTPHeaderEntries e1{{"X-Password", "secret1"}};
    HTTPHeaderEntries e2{{"X-Password", "secret2"}};
    HTTPHeaderEntries e3{{"X-Password", "secret1"}};

    String c1 = canonicalizeHTTPHeadersForSchemaCache(e1);
    String c2 = canonicalizeHTTPHeadersForSchemaCache(e2);
    String c3 = canonicalizeHTTPHeadersForSchemaCache(e3);

    /// Distinct secrets produce distinct representations; identical produce matching
    EXPECT_NE(c1, c2);
    EXPECT_EQ(c1, c3);

    SchemaCache::Key k1{url, "CSV", "", "DEFAULT", "none", c1};
    SchemaCache::Key k2{url, "CSV", "", "DEFAULT", "none", c2};
    SchemaCache::Key k3{url, "CSV", "", "DEFAULT", "none", c3};

    EXPECT_NE(k1, k2);
    EXPECT_EQ(k1, k3);

    SchemaCache::KeyHash hasher;
    EXPECT_NE(hasher(k1), hasher(k2));
    EXPECT_EQ(hasher(k1), hasher(k3));
}

TEST(StorageURLCacheHeaders, AmbiguousDelimiterBoundaryCollisionResistance)
{
    /// Reviewer finding P2 counterexample:
    /// Collection A: headers('X-A' = 'v;x-b:w')
    /// Collection B: headers('X-A' = 'v', 'X-B' = 'w')
    HTTPHeaderEntries entries_embedded{{"X-A", "v;x-b:w"}};
    HTTPHeaderEntries entries_separate{{"X-A", "v"}, {"X-B", "w"}};

    String canon_embedded = canonicalizeHTTPHeadersForSchemaCache(entries_embedded);
    String canon_separate = canonicalizeHTTPHeadersForSchemaCache(entries_separate);

    EXPECT_NE(canon_embedded, canon_separate);
    EXPECT_TRUE(canon_embedded.starts_with("3:x-a=21:hash:"));
    EXPECT_FALSE(canon_embedded.contains("3:x-b="));
    EXPECT_TRUE(canon_separate.starts_with("3:x-a=21:hash:"));
    EXPECT_TRUE(canon_separate.contains(";3:x-b=21:hash:"));
}

TEST(StorageURLCacheHeaders, DuplicateHeaderLastValueWins)
{
    /// In HTTP transport (Poco::Net::HTTPRequest::set), duplicate headers resolve with last-value-wins.
    HTTPHeaderEntries entries_plain_then_gzip{{"Accept-Encoding", "identity"}, {"Accept-Encoding", "gzip"}};
    HTTPHeaderEntries entries_gzip_then_plain{{"Accept-Encoding", "gzip"}, {"Accept-Encoding", "identity"}};

    String canon_plain_then_gzip = canonicalizeHTTPHeadersForSchemaCache(entries_plain_then_gzip);
    String canon_gzip_then_plain = canonicalizeHTTPHeadersForSchemaCache(entries_gzip_then_plain);

    EXPECT_NE(canon_plain_then_gzip, canon_gzip_then_plain);

    HTTPHeaderEntries expected_gzip{{"Accept-Encoding", "gzip"}};
    HTTPHeaderEntries expected_identity{{"Accept-Encoding", "identity"}};

    EXPECT_EQ(canon_plain_then_gzip, canonicalizeHTTPHeadersForSchemaCache(expected_gzip));
    EXPECT_EQ(canon_gzip_then_plain, canonicalizeHTTPHeadersForSchemaCache(expected_identity));
}

TEST(StorageURLCacheHeaders, MixedCaseDuplicateHeaderNames)
{
    HTTPHeaderEntries entries{{"Accept-Encoding", "identity"}, {"accept-encoding", "gzip"}};
    HTTPHeaderEntries expected{{"accept-encoding", "gzip"}};
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries), canonicalizeHTTPHeadersForSchemaCache(expected));
}

TEST(StorageURLCacheHeaders, ThreeRepeatedValuesLastWins)
{
    HTTPHeaderEntries entries{{"X-Val", "1"}, {"X-Val", "2"}, {"X-Val", "3"}};
    HTTPHeaderEntries expected{{"x-val", "3"}};
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries), canonicalizeHTTPHeadersForSchemaCache(expected));
}

TEST(StorageURLCacheHeaders, DuplicateNamesSeparatedByOtherHeaders)
{
    HTTPHeaderEntries entries{{"Accept-Encoding", "gzip"}, {"X-Custom", "foo"}, {"Accept-Encoding", "identity"}};
    HTTPHeaderEntries expected{{"X-Custom", "foo"}, {"Accept-Encoding", "identity"}};
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries), canonicalizeHTTPHeadersForSchemaCache(expected));
}

TEST(StorageURLCacheHeaders, DelimitersAndSpecialCharsInValue)
{
    HTTPHeaderEntries entries{{"Cookie", "sess=abc; role=admin:super; path=/"}};
    String canon = canonicalizeHTTPHeadersForSchemaCache(entries);

    EXPECT_EQ(canon.find("sess="), String::npos);
    EXPECT_EQ(canon.find("role="), String::npos);
    EXPECT_TRUE(canon.starts_with("6:cookie=21:hash:"));
}

TEST(StorageURLCacheHeaders, EmptyValueVsMissingHeader)
{
    HTTPHeaderEntries empty_val{{"X-Empty", ""}};
    HTTPHeaderEntries no_headers{};

    String c_empty = canonicalizeHTTPHeadersForSchemaCache(empty_val);
    String c_none = canonicalizeHTTPHeadersForSchemaCache(no_headers);

    EXPECT_NE(c_empty, c_none);
    EXPECT_EQ(c_none, "");
    EXPECT_TRUE(c_empty.starts_with("7:x-empty=21:hash:"));
}

TEST(StorageURLCacheHeaders, TelemetryHeadersIgnored)
{
    /// OpenTelemetry auto-injected trace context headers are ignored so tracing does not bust cache hits.
    HTTPHeaderEntries entries1{{"Accept-Encoding", "gzip"}, {"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}};
    HTTPHeaderEntries entries2{{"Accept-Encoding", "gzip"}, {"tracestate", "rojo=1"}};
    HTTPHeaderEntries entries_clean{{"Accept-Encoding", "gzip"}};

    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries1), canonicalizeHTTPHeadersForSchemaCache(entries_clean));
    EXPECT_EQ(canonicalizeHTTPHeadersForSchemaCache(entries2), canonicalizeHTTPHeadersForSchemaCache(entries_clean));

    /// Arbitrary user-specified headers like X-Request-ID are NOT ignored and conservatively partition cache.
    HTTPHeaderEntries entries_req_id{{"Accept-Encoding", "gzip"}, {"X-Request-ID", "req-12345"}};
    EXPECT_NE(canonicalizeHTTPHeadersForSchemaCache(entries_req_id), canonicalizeHTTPHeadersForSchemaCache(entries_clean));
}

TEST(StorageURLCacheHeaders, DelimitersInHeaderNamesLengthFramed)
{
    HTTPHeaderEntries tricky{{"x-a;x-b:1234", "val"}};
    String canon = canonicalizeHTTPHeadersForSchemaCache(tricky);
    EXPECT_TRUE(canon.starts_with("12:x-a;x-b:1234=21:hash:"));
}

TEST(StorageURLCacheHeaders, SchemaCacheKeyComparison)
{
    HTTPHeaderEntries entries_gzip{{"Accept-Encoding", "gzip"}};
    HTTPHeaderEntries entries_identity{{"Accept-Encoding", "identity"}};

    String h_gzip = canonicalizeHTTPHeadersForSchemaCache(entries_gzip);
    String h_identity = canonicalizeHTTPHeadersForSchemaCache(entries_identity);

    SchemaCache::Key key1{"http://example.com/data.csv", "CSV", "", "", "none", h_gzip};
    SchemaCache::Key key2{"http://example.com/data.csv", "CSV", "", "", "none", h_identity};
    SchemaCache::Key key3{"http://example.com/data.csv", "CSV", "", "", "none", h_gzip};
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
