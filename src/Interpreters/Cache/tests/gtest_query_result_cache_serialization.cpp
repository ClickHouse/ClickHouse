#include <gtest/gtest.h>

#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Cache/QueryResultCacheFactory.h>
#include <Interpreters/Cache/QueryResultCacheRedisKey.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Core/Settings.h>
#include <Core/UUID.h>

#include <Poco/Util/MapConfiguration.h>

using namespace DB;

namespace
{

/// Build a Key via the DeserializeTag-free constructor that still requires an AST.
/// We parse a simple SELECT to get a valid ASTPtr.
QueryResultCache::Key makeKey(
    const String & query,
    SharedHeader header,
    std::optional<UUID> user_id,
    std::vector<UUID> roles,
    bool is_shared,
    bool is_compressed,
    const String & tag,
    int ttl_sec = 60)
{
    const char * end = query.data() + query.size();
    ParserQuery parser(end, false);
    ASTPtr ast = parseQuery(parser, query, DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    Settings settings;
    if (!tag.empty())
        settings.set("query_cache_tag", tag);

    auto now = std::chrono::system_clock::now();
    auto expires = now + std::chrono::seconds(ttl_sec);

    return QueryResultCache::Key(
        ast,
        "default", /// current_database
        settings,
        header,
        "test_query_id_123",
        user_id,
        roles,
        is_shared,
        now,
        expires,
        is_compressed,
        /*is_subquery_=*/ false);
}

/// Create a SharedHeader (Block wrapped in shared_ptr) with two columns: UInt64 + String.
SharedHeader makeTwoColumnHeader()
{
    Block block;
    block.insert({std::make_shared<DataTypeUInt64>()->createColumn(), std::make_shared<DataTypeUInt64>(), "number"});
    block.insert({std::make_shared<DataTypeString>()->createColumn(), std::make_shared<DataTypeString>(), "text"});
    return std::make_shared<const Block>(std::move(block));
}

/// Build a Chunk with `num_rows` rows of (UInt64, String).
Chunk makeTwoColumnChunk(size_t num_rows, UInt64 start = 0)
{
    auto col_uint = ColumnUInt64::create();
    auto col_str = ColumnString::create();
    for (size_t i = 0; i < num_rows; ++i)
    {
        col_uint->insert(start + i);
        col_str->insert("row_" + std::to_string(start + i));
    }
    Columns cols;
    cols.push_back(std::move(col_uint));
    cols.push_back(std::move(col_str));
    return Chunk(std::move(cols), num_rows);
}

/// Compare two Chunks for equality: same number of rows, identical column data.
void assertChunksEqual(const Chunk & a, const Chunk & b)
{
    ASSERT_EQ(a.getNumRows(), b.getNumRows());
    ASSERT_EQ(a.getNumColumns(), b.getNumColumns());
    for (size_t col = 0; col < a.getNumColumns(); ++col)
    {
        const auto & ca = a.getColumns()[col];
        const auto & cb = b.getColumns()[col];
        for (size_t row = 0; row < a.getNumRows(); ++row)
        {
            ASSERT_EQ(ca->compareAt(row, row, *cb, 0), 0)
                << "Mismatch at col=" << col << " row=" << row;
        }
    }
}

} // namespace

/// ==========================================================================
/// A1. Key serialization roundtrip
/// ==========================================================================

TEST(QueryResultCacheSerialization, KeyRoundtripWithUserId)
{
    auto header = makeTwoColumnHeader();
    UUID uid = UUIDHelpers::generateV4();
    std::vector<UUID> roles = {UUIDHelpers::generateV4(), UUIDHelpers::generateV4()};

    auto key = makeKey("SELECT 1", header, uid, roles, true, false, "test_tag", 120);

    WriteBufferFromOwnString wbuf;
    key.serializeTo(wbuf);

    ReadBufferFromString rbuf(wbuf.str());
    auto restored = QueryResultCache::Key::deserializeFrom(rbuf);

    EXPECT_EQ(restored.ast_hash.high64, key.ast_hash.high64);
    EXPECT_EQ(restored.ast_hash.low64, key.ast_hash.low64);
    EXPECT_EQ(restored.user_id.has_value(), true);
    EXPECT_EQ(*restored.user_id, uid);
    EXPECT_EQ(restored.current_user_roles.size(), 2u);
    EXPECT_EQ(restored.current_user_roles[0], roles[0]);
    EXPECT_EQ(restored.current_user_roles[1], roles[1]);
    EXPECT_EQ(restored.is_shared, true);
    EXPECT_EQ(restored.is_compressed, false);
    EXPECT_EQ(restored.tag, "test_tag");
    EXPECT_EQ(restored.query_id, "test_query_id_123");
    EXPECT_FALSE(restored.query_string.empty());
}

TEST(QueryResultCacheSerialization, KeyRoundtripNoUserId)
{
    auto header = makeTwoColumnHeader();
    auto key = makeKey("SELECT 2", header, std::nullopt, {}, false, true, "");

    WriteBufferFromOwnString wbuf;
    key.serializeTo(wbuf);

    ReadBufferFromString rbuf(wbuf.str());
    auto restored = QueryResultCache::Key::deserializeFrom(rbuf);

    EXPECT_EQ(restored.ast_hash.high64, key.ast_hash.high64);
    EXPECT_EQ(restored.ast_hash.low64, key.ast_hash.low64);
    EXPECT_FALSE(restored.user_id.has_value());
    EXPECT_TRUE(restored.current_user_roles.empty());
    EXPECT_EQ(restored.is_shared, false);
    EXPECT_EQ(restored.is_compressed, true);
    EXPECT_EQ(restored.tag, "");
}

TEST(QueryResultCacheSerialization, KeyRoundtripEmptyRoles)
{
    auto header = makeTwoColumnHeader();
    UUID uid = UUIDHelpers::generateV4();

    auto key = makeKey("SELECT 3", header, uid, {}, true, false, "ns");

    WriteBufferFromOwnString wbuf;
    key.serializeTo(wbuf);

    ReadBufferFromString rbuf(wbuf.str());
    auto restored = QueryResultCache::Key::deserializeFrom(rbuf);

    EXPECT_TRUE(restored.user_id.has_value());
    EXPECT_EQ(*restored.user_id, uid);
    EXPECT_TRUE(restored.current_user_roles.empty());
}

TEST(QueryResultCacheSerialization, KeyRoundtripMultipleRoles)
{
    auto header = makeTwoColumnHeader();
    UUID uid = UUIDHelpers::generateV4();
    std::vector<UUID> roles;
    for (int i = 0; i < 5; ++i)
        roles.push_back(UUIDHelpers::generateV4());

    auto key = makeKey("SELECT 4", header, uid, roles, false, false, "multi");

    WriteBufferFromOwnString wbuf;
    key.serializeTo(wbuf);

    ReadBufferFromString rbuf(wbuf.str());
    auto restored = QueryResultCache::Key::deserializeFrom(rbuf);

    ASSERT_EQ(restored.current_user_roles.size(), 5u);
    for (int i = 0; i < 5; ++i)
        EXPECT_EQ(restored.current_user_roles[i], roles[i]);
}

/// ==========================================================================
/// A2. Key::encodeToRedisKey format validation
/// ==========================================================================

TEST(QueryResultCacheSerialization, EncodeToRedisKeyEmptyTag)
{
    auto header = makeTwoColumnHeader();
    auto key = makeKey("SELECT 10", header, std::nullopt, {}, false, false, "");

    String redis_key = key.encodeToRedisKey();

    constexpr std::string_view prefix = "ch:qcache:v0:t0::private:";
    EXPECT_TRUE(redis_key.starts_with(prefix)) << "Unexpected Redis key prefix for empty tag";

    const String suffix = redis_key.substr(prefix.size());
    const size_t separator_pos = suffix.find(':');
    ASSERT_NE(separator_pos, String::npos) << "Expected private scope hash separator in Redis key: " << redis_key;

    const String scope_hash = suffix.substr(0, separator_pos);
    const String ast_hash = suffix.substr(separator_pos + 1);
    ASSERT_EQ(scope_hash.size(), 32u) << "Expected 32 hex chars for private scope hash, got: " << redis_key;
    ASSERT_EQ(ast_hash.size(), 32u) << "Expected 32 hex chars for AST hash, got: " << redis_key;

    /// Verify both hash components are valid hex digits.
    for (const char c : scope_hash)
        EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))
            << "Invalid hex char in Redis key scope hash: " << c;

    for (const char c : ast_hash)
        EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))
            << "Invalid hex char in Redis key AST hash: " << c;
}

TEST(QueryResultCacheSerialization, EncodeToRedisKeyWithTag)
{
    auto header = makeTwoColumnHeader();
    auto key = makeKey("SELECT 11", header, std::nullopt, {}, true, false, "mytag");

    String redis_key = key.encodeToRedisKey();

    constexpr std::string_view prefix = "ch:qcache:v0:t0:mytag:shared:";
    ASSERT_EQ(redis_key.size(), prefix.size() + 32) << "Expected generation+scope prefix plus 32 hex chars, got: " << redis_key;
    EXPECT_TRUE(redis_key.starts_with(prefix)) << "Redis key should start with generation+scope prefix";

    /// The hex part after the prefix.
    String hex_part = redis_key.substr(prefix.size());
    for (char c : hex_part)
        EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))
            << "Invalid hex char in Redis key hex part: " << c;
}

TEST(QueryResultCacheSerialization, EncodeToRedisKeyDeterministic)
{
    auto header = makeTwoColumnHeader();
    auto key1 = makeKey("SELECT 12", header, std::nullopt, {}, false, false, "t");
    auto key2 = makeKey("SELECT 12", header, std::nullopt, {}, false, false, "t");

    /// Same query + same tag → same Redis key.
    EXPECT_EQ(key1.encodeToRedisKey(), key2.encodeToRedisKey());
}

TEST(QueryResultCacheSerialization, EncodeToRedisKeySeparatesPrivateScopes)
{
    auto header = makeTwoColumnHeader();
    auto key1 = makeKey("SELECT 13", header, UUIDHelpers::generateV4(), {}, false, false, "t");
    auto key2 = makeKey("SELECT 13", header, UUIDHelpers::generateV4(), {}, false, false, "t");

    EXPECT_NE(key1.encodeToRedisKey(), key2.encodeToRedisKey());
}

TEST(QueryResultCacheSerialization, EncodeToRedisKeyIncludesGeneration)
{
    auto header = makeTwoColumnHeader();
    auto key = makeKey("SELECT 14", header, std::nullopt, {}, true, false, "t");

    QueryResultCache::WriteContext gen1{.global_generation = 1, .tag_generation = 2};
    QueryResultCache::WriteContext gen2{.global_generation = 1, .tag_generation = 3};

    EXPECT_NE(key.encodeToRedisKey(gen1), key.encodeToRedisKey(gen2));
}

TEST(QueryResultCacheSerialization, EncodeToRedisKeyUsesStandalonePrefix)
{
    auto header = makeTwoColumnHeader();
    auto key = makeKey("SELECT 15", header, std::nullopt, {}, true, false, "cluster");

    const String redis_key = key.encodeToRedisKey();
    const String expected_prefix = std::string(QUERY_RESULT_CACHE_REDIS_KEY_PREFIX);

    EXPECT_TRUE(redis_key.starts_with(expected_prefix));
    EXPECT_EQ(redis_key.find('{'), String::npos);
    EXPECT_EQ(redis_key.find('}'), String::npos);
}

TEST(QueryResultCacheSerialization, RedisKeyTagParsingMatchesExactTag)
{
    auto header = makeTwoColumnHeader();
    auto key = makeKey("SELECT 16", header, std::nullopt, {}, true, false, "alpha:private:beta:shared:gamma");

    const String redis_key = key.encodeToRedisKey();
    const auto parsed_tag = QueryResultCacheRedisKeyUtils::tryGetTagFromRedisKey(redis_key);

    ASSERT_TRUE(parsed_tag.has_value());
    EXPECT_EQ(*parsed_tag, "alpha:private:beta:shared:gamma");
    EXPECT_TRUE(QueryResultCacheRedisKeyUtils::hasTag(redis_key, "alpha:private:beta:shared:gamma"));
    EXPECT_FALSE(QueryResultCacheRedisKeyUtils::hasTag(redis_key, "private"));
    EXPECT_FALSE(QueryResultCacheRedisKeyUtils::hasTag(redis_key, "shared"));
}

TEST(QueryResultCacheSerialization, RedisKeyTagParsingMatchesPrivateScopeAndLockKeys)
{
    auto header = makeTwoColumnHeader();
    auto key = makeKey("SELECT 17", header, UUIDHelpers::generateV4(), {UUIDHelpers::generateV4()}, false, false, "tag:with:lock");

    const String redis_key = key.encodeToRedisKey();
    const String lock_key = redis_key + ":lock";

    const auto parsed_tag = QueryResultCacheRedisKeyUtils::tryGetTagFromRedisKey(lock_key);
    ASSERT_TRUE(parsed_tag.has_value());
    EXPECT_EQ(*parsed_tag, "tag:with:lock");
    EXPECT_TRUE(QueryResultCacheRedisKeyUtils::hasTag(lock_key, "tag:with:lock"));
    EXPECT_FALSE(QueryResultCacheRedisKeyUtils::hasTag(lock_key, "with"));
}

TEST(QueryResultCacheSerialization, RedisKeyTagParsingRejectsMalformedKeys)
{
    EXPECT_FALSE(QueryResultCacheRedisKeyUtils::tryGetTagFromRedisKey("not-a-query-cache-key").has_value());
    EXPECT_FALSE(QueryResultCacheRedisKeyUtils::tryGetTagFromRedisKey("ch:qcache:v0:t0:tag:shared").has_value());
}

TEST(QueryResultCacheSerialization, RedisKeyTagParsingDistinguishesPrefixOverlap)
{
    /// `clearByTag("foo")` must not see a key whose tag starts with `foo:`.
    /// `SCAN MATCH ch:qcache:v*:t*:foo:*:?{32}` over-matches because Redis `*`
    /// also crosses `:`, so the exact-tag check in `hasTag` is what makes the
    /// clear-by-tag path safe for tags containing `:`.
    auto header = makeTwoColumnHeader();
    auto key_foo = makeKey("SELECT 18", header, std::nullopt, {}, true, false, "foo");
    auto key_foo_bar = makeKey("SELECT 19", header, std::nullopt, {}, true, false, "foo:bar");

    const String redis_key_foo = key_foo.encodeToRedisKey();
    const String redis_key_foo_bar = key_foo_bar.encodeToRedisKey();

    EXPECT_TRUE(QueryResultCacheRedisKeyUtils::hasTag(redis_key_foo, "foo"));
    EXPECT_FALSE(QueryResultCacheRedisKeyUtils::hasTag(redis_key_foo, "foo:bar"));

    EXPECT_TRUE(QueryResultCacheRedisKeyUtils::hasTag(redis_key_foo_bar, "foo:bar"));
    EXPECT_FALSE(QueryResultCacheRedisKeyUtils::hasTag(redis_key_foo_bar, "foo"));
}

/// ==========================================================================
/// A3. Entry serialization roundtrip
/// ==========================================================================

TEST(QueryResultCacheSerialization, EntryRoundtripSingleChunk)
{
    auto header = makeTwoColumnHeader();
    QueryResultCache::Entry entry;
    entry.chunks.push_back(makeTwoColumnChunk(10));

    WriteBufferFromOwnString wbuf;
    entry.serializeTo(wbuf, *header);

    ReadBufferFromString rbuf(wbuf.str());
    auto restored = QueryResultCache::Entry::deserializeFrom(rbuf, *header);

    ASSERT_EQ(restored.chunks.size(), 1u);
    assertChunksEqual(entry.chunks[0], restored.chunks[0]);
    EXPECT_FALSE(restored.totals.has_value());
    EXPECT_FALSE(restored.extremes.has_value());
}

TEST(QueryResultCacheSerialization, EntryRoundtripMultipleChunks)
{
    auto header = makeTwoColumnHeader();
    QueryResultCache::Entry entry;
    entry.chunks.push_back(makeTwoColumnChunk(5, 0));
    entry.chunks.push_back(makeTwoColumnChunk(3, 100));
    entry.chunks.push_back(makeTwoColumnChunk(7, 200));

    WriteBufferFromOwnString wbuf;
    entry.serializeTo(wbuf, *header);

    ReadBufferFromString rbuf(wbuf.str());
    auto restored = QueryResultCache::Entry::deserializeFrom(rbuf, *header);

    ASSERT_EQ(restored.chunks.size(), 3u);
    assertChunksEqual(entry.chunks[0], restored.chunks[0]);
    assertChunksEqual(entry.chunks[1], restored.chunks[1]);
    assertChunksEqual(entry.chunks[2], restored.chunks[2]);
}

TEST(QueryResultCacheSerialization, EntryRoundtripEmptyChunks)
{
    auto header = makeTwoColumnHeader();
    QueryResultCache::Entry entry;
    /// No chunks at all — should roundtrip as empty.

    WriteBufferFromOwnString wbuf;
    entry.serializeTo(wbuf, *header);

    ReadBufferFromString rbuf(wbuf.str());
    auto restored = QueryResultCache::Entry::deserializeFrom(rbuf, *header);

    EXPECT_TRUE(restored.chunks.empty());
    EXPECT_FALSE(restored.totals.has_value());
    EXPECT_FALSE(restored.extremes.has_value());
}

TEST(QueryResultCacheSerialization, EntryRoundtripWithTotals)
{
    auto header = makeTwoColumnHeader();
    QueryResultCache::Entry entry;
    entry.chunks.push_back(makeTwoColumnChunk(3));
    entry.totals = makeTwoColumnChunk(1, 999);

    WriteBufferFromOwnString wbuf;
    entry.serializeTo(wbuf, *header);

    ReadBufferFromString rbuf(wbuf.str());
    auto restored = QueryResultCache::Entry::deserializeFrom(rbuf, *header);

    ASSERT_EQ(restored.chunks.size(), 1u);
    ASSERT_TRUE(restored.totals.has_value());
    assertChunksEqual(*entry.totals, *restored.totals);
    EXPECT_FALSE(restored.extremes.has_value());
}

TEST(QueryResultCacheSerialization, EntryRoundtripWithExtremes)
{
    auto header = makeTwoColumnHeader();
    QueryResultCache::Entry entry;
    entry.chunks.push_back(makeTwoColumnChunk(3));
    entry.extremes = makeTwoColumnChunk(1, 888);

    WriteBufferFromOwnString wbuf;
    entry.serializeTo(wbuf, *header);

    ReadBufferFromString rbuf(wbuf.str());
    auto restored = QueryResultCache::Entry::deserializeFrom(rbuf, *header);

    ASSERT_EQ(restored.chunks.size(), 1u);
    EXPECT_FALSE(restored.totals.has_value());
    ASSERT_TRUE(restored.extremes.has_value());
    assertChunksEqual(*entry.extremes, *restored.extremes);
}

TEST(QueryResultCacheSerialization, EntryRoundtripWithTotalsAndExtremes)
{
    auto header = makeTwoColumnHeader();
    QueryResultCache::Entry entry;
    entry.chunks.push_back(makeTwoColumnChunk(5));
    entry.totals = makeTwoColumnChunk(1, 500);
    entry.extremes = makeTwoColumnChunk(1, 600);

    WriteBufferFromOwnString wbuf;
    entry.serializeTo(wbuf, *header);

    ReadBufferFromString rbuf(wbuf.str());
    auto restored = QueryResultCache::Entry::deserializeFrom(rbuf, *header);

    ASSERT_EQ(restored.chunks.size(), 1u);
    ASSERT_TRUE(restored.totals.has_value());
    ASSERT_TRUE(restored.extremes.has_value());
    assertChunksEqual(*entry.totals, *restored.totals);
    assertChunksEqual(*entry.extremes, *restored.extremes);
}

/// ==========================================================================
/// A4. serializeValue → deserializeValue full roundtrip
/// ==========================================================================
/// RedisRemoteCacheBackend::serializeValue/deserializeValue are private
/// static, but they simply concatenate Key::serializeTo +
/// Entry::serializeTo into one buffer. We replicate this exact logic
/// to test the full roundtrip.

TEST(QueryResultCacheSerialization, FullValueRoundtrip)
{
    auto header = makeTwoColumnHeader();
    UUID uid = UUIDHelpers::generateV4();
    std::vector<UUID> roles = {UUIDHelpers::generateV4()};

    auto key = makeKey("SELECT 100", header, uid, roles, true, false, "full_rt", 300);

    QueryResultCache::Entry entry;
    entry.chunks.push_back(makeTwoColumnChunk(10, 0));
    entry.chunks.push_back(makeTwoColumnChunk(5, 100));
    entry.totals = makeTwoColumnChunk(1, 999);

    /// Serialize: key + entry into one buffer (same as serializeValue).
    WriteBufferFromOwnString wbuf;
    key.serializeTo(wbuf);
    entry.serializeTo(wbuf, *key.header);
    std::string serialized = wbuf.str();

    /// Deserialize (same as deserializeValue).
    ReadBufferFromString rbuf(serialized);
    auto restored_key = QueryResultCache::Key::deserializeFrom(rbuf);
    auto restored_entry = QueryResultCache::Entry::deserializeFrom(rbuf, *restored_key.header);

    /// Verify key fields.
    EXPECT_EQ(restored_key.ast_hash.high64, key.ast_hash.high64);
    EXPECT_EQ(restored_key.ast_hash.low64, key.ast_hash.low64);
    EXPECT_TRUE(restored_key.user_id.has_value());
    EXPECT_EQ(*restored_key.user_id, uid);
    ASSERT_EQ(restored_key.current_user_roles.size(), 1u);
    EXPECT_EQ(restored_key.current_user_roles[0], roles[0]);
    EXPECT_EQ(restored_key.is_shared, true);
    EXPECT_EQ(restored_key.tag, "full_rt");

    /// Verify entry data.
    ASSERT_EQ(restored_entry.chunks.size(), 2u);
    assertChunksEqual(entry.chunks[0], restored_entry.chunks[0]);
    assertChunksEqual(entry.chunks[1], restored_entry.chunks[1]);
    ASSERT_TRUE(restored_entry.totals.has_value());
    assertChunksEqual(*entry.totals, *restored_entry.totals);
    EXPECT_FALSE(restored_entry.extremes.has_value());
}

TEST(QueryResultCacheSerialization, FullValueRoundtripEmptyEntry)
{
    auto header = makeTwoColumnHeader();
    auto key = makeKey("SELECT 101", header, std::nullopt, {}, false, false, "");

    QueryResultCache::Entry entry; /// empty: no chunks, no totals, no extremes

    WriteBufferFromOwnString wbuf;
    key.serializeTo(wbuf);
    entry.serializeTo(wbuf, *key.header);
    std::string serialized = wbuf.str();

    ReadBufferFromString rbuf(serialized);
    auto restored_key = QueryResultCache::Key::deserializeFrom(rbuf);
    auto restored_entry = QueryResultCache::Entry::deserializeFrom(rbuf, *restored_key.header);

    EXPECT_EQ(restored_key.ast_hash.high64, key.ast_hash.high64);
    EXPECT_FALSE(restored_key.user_id.has_value());
    EXPECT_TRUE(restored_entry.chunks.empty());
    EXPECT_FALSE(restored_entry.totals.has_value());
    EXPECT_FALSE(restored_entry.extremes.has_value());
}

/// ==========================================================================
/// A5. QueryResultCacheFactory register / create / duplicate / unknown
/// ==========================================================================

TEST(QueryResultCacheFactory, RegisterAndCreate)
{
    auto & factory = QueryResultCacheFactory::instance();

    /// Creating an unknown type should throw.
    Poco::AutoPtr<Poco::Util::MapConfiguration> cfg(new Poco::Util::MapConfiguration);
    EXPECT_THROW(factory.create("nonexistent_cache_type", *cfg), Exception);
}

TEST(QueryResultCacheFactory, DuplicateRegistrationThrows)
{
    auto & factory = QueryResultCacheFactory::instance();

    /// Register a unique test name, then re-register it — the second call should throw.
    auto dummy_creator = [](const Poco::Util::AbstractConfiguration &) -> std::shared_ptr<IQueryResultCache> {
        return nullptr;
    };

    /// First registration should succeed.
    EXPECT_NO_THROW(factory.registerCache("__gtest_duplicate_check__", dummy_creator));

    /// Second registration with the same name should throw.
    EXPECT_THROW(factory.registerCache("__gtest_duplicate_check__", dummy_creator), Exception);
}

TEST(QueryResultCacheLocal, ClearBlocksInflightWrite)
{
    LocalQueryResultCache cache(1024 * 1024, 16, 1024 * 1024, 1024);
    auto header = makeTwoColumnHeader();
    auto key = makeKey("SELECT 1000", header, std::nullopt, {}, false, false, "clear_me");

    auto writer = cache.createWriter(key, std::chrono::milliseconds(0), false, 1, 0, 0);
    writer.buffer(makeTwoColumnChunk(3), QueryResultCacheWriter::ChunkType::Result);

    cache.clear(std::nullopt);
    writer.finalizeWrite();

    EXPECT_EQ(cache.count(), 0u);
}

TEST(QueryResultCacheLocal, ClearTagInvalidatesOnlyMatchingTag)
{
    LocalQueryResultCache cache(1024 * 1024, 16, 1024 * 1024, 1024);
    auto header = makeTwoColumnHeader();
    auto key_drop = makeKey("SELECT 1001", header, std::nullopt, {}, false, false, "drop");
    auto key_keep = makeKey("SELECT 1002", header, std::nullopt, {}, false, false, "keep");

    auto writer_drop = cache.createWriter(key_drop, std::chrono::milliseconds(0), false, 1, 0, 0);
    auto writer_keep = cache.createWriter(key_keep, std::chrono::milliseconds(0), false, 1, 0, 0);

    writer_drop.buffer(makeTwoColumnChunk(1, 10), QueryResultCacheWriter::ChunkType::Result);
    writer_keep.buffer(makeTwoColumnChunk(1, 20), QueryResultCacheWriter::ChunkType::Result);

    cache.clear(String{"drop"});

    writer_drop.finalizeWrite();
    writer_keep.finalizeWrite();

    EXPECT_EQ(cache.count(), 1u);

    auto keep_reader = cache.createReader(key_keep);
    EXPECT_TRUE(keep_reader.hasCacheEntryForKey(false));

    auto drop_reader = cache.createReader(key_drop);
    EXPECT_FALSE(drop_reader.hasCacheEntryForKey(false));
}
