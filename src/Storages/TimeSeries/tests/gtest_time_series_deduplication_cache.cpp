#include <Storages/TimeSeries/TimeSeriesDeduplicationCache.h>

#include <Columns/ColumnString.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>

#include <gtest/gtest.h>

#include <chrono>
#include <vector>

using namespace DB;

namespace CurrentMetrics
{
    extern const Metric TimeSeriesTagsDeduplicationCacheEntries;
    extern const Metric TimeSeriesTagsDeduplicationCacheBytes;
}

namespace ProfileEvents
{
    extern const Event TimeSeriesTagsDeduplicationCacheHits;
    extern const Event TimeSeriesTagsDeduplicationCacheMisses;
}

namespace
{
    Block makeBlock(const std::vector<String> & keys)
    {
        auto column = ColumnString::create();
        for (const auto & key : keys)
            column->insert(key);
        return Block{ColumnWithTypeAndName{std::move(column), std::make_shared<DataTypeString>(), "key"}};
    }

    std::vector<String> keysOf(const Block & block)
    {
        std::vector<String> keys;
        const auto & column = *block.getByPosition(0).column;
        for (size_t i = 0; i != column.size(); ++i)
            keys.emplace_back(column.getDataAt(i));
        return keys;
    }

    struct Fixture
    {
        static constexpr UInt64 EXPIRATION_SECONDS = 3600;
        static constexpr UInt64 MAX_OFFSET_SECONDS = EXPIRATION_SECONDS / 4;

        TimeSeriesDeduplicationCache::TimePoint now{std::chrono::steady_clock::time_point{} + std::chrono::hours(1000)};
        UInt64 random_value = 0;
        TimeSeriesDeduplicationCache cache;

        Fixture()
            : cache(
                  1024 * 1024,
                  EXPIRATION_SECONDS,
                  CurrentMetrics::TimeSeriesTagsDeduplicationCacheEntries,
                  CurrentMetrics::TimeSeriesTagsDeduplicationCacheBytes,
                  ProfileEvents::TimeSeriesTagsDeduplicationCacheHits,
                  ProfileEvents::TimeSeriesTagsDeduplicationCacheMisses,
                  [this] { return now; },
                  [this] { return random_value; })
        {
        }

        /// Writes `keys` with the current random value and marks them as written.
        void write(const std::vector<String> & keys)
        {
            TimeSeriesDeduplicationCache::PendingRows pending_rows;
            auto written = cache.filterOutWrittenRows(makeBlock(keys), 0, pending_rows);
            ASSERT_EQ(keysOf(written), keys);
            cache.markRowsAsWritten(std::move(pending_rows));
        }

        /// Returns the keys an insert of `keys` would write now, without marking them as written.
        std::vector<String> toWrite(const std::vector<String> & keys)
        {
            TimeSeriesDeduplicationCache::PendingRows pending_rows;
            return keysOf(cache.filterOutWrittenRows(makeBlock(keys), 0, pending_rows));
        }
    };
}

TEST(TimeSeriesDeduplicationCache, ExpirationOffsetIsZeroForShortPeriods)
{
    for (UInt64 expiration_seconds = 0; expiration_seconds < 4; ++expiration_seconds)
    {
        for (UInt64 random_value : {UInt64{0}, UInt64{1}, UInt64{7}, UInt64{123456789}})
            EXPECT_EQ(TimeSeriesDeduplicationCache::expirationOffsetSeconds(expiration_seconds, random_value), 0u);
    }
}

TEST(TimeSeriesDeduplicationCache, ExpirationOffsetIsBoundedByAQuarterOfThePeriod)
{
    const UInt64 expiration_seconds = 3600;
    const UInt64 max_offset = expiration_seconds / 4;

    for (UInt64 random_value = 0; random_value < 5000; ++random_value)
        EXPECT_LE(TimeSeriesDeduplicationCache::expirationOffsetSeconds(expiration_seconds, random_value), max_offset);

    EXPECT_EQ(TimeSeriesDeduplicationCache::expirationOffsetSeconds(expiration_seconds, 0), 0u);
    EXPECT_EQ(TimeSeriesDeduplicationCache::expirationOffsetSeconds(expiration_seconds, max_offset), max_offset);
    EXPECT_EQ(TimeSeriesDeduplicationCache::expirationOffsetSeconds(expiration_seconds, max_offset + 1), 0u);
    EXPECT_EQ(TimeSeriesDeduplicationCache::expirationOffsetSeconds(4, 1), 1u);
    EXPECT_EQ(TimeSeriesDeduplicationCache::expirationOffsetSeconds(4, 2), 0u);
}

TEST(TimeSeriesDeduplicationCache, RowsWrittenTogetherExpireAcrossTheLastQuarter)
{
    Fixture f;

    f.random_value = 0;
    f.write({"a"});
    f.random_value = Fixture::MAX_OFFSET_SECONDS;
    f.write({"b"});

    const auto written_at = f.now;
    const std::vector<String> both{"a", "b"};
    const std::vector<String> only_b{"b"};
    const std::vector<String> none;

    f.now = written_at;
    EXPECT_EQ(f.toWrite(both), none);

    f.now = written_at + std::chrono::seconds(Fixture::EXPIRATION_SECONDS - Fixture::MAX_OFFSET_SECONDS - 1);
    EXPECT_EQ(f.toWrite(both), none);

    f.now = written_at + std::chrono::seconds(Fixture::EXPIRATION_SECONDS - Fixture::MAX_OFFSET_SECONDS);
    EXPECT_EQ(f.toWrite(both), only_b);

    f.now = written_at + std::chrono::seconds(Fixture::EXPIRATION_SECONDS - 1);
    EXPECT_EQ(f.toWrite(both), only_b);

    f.now = written_at + std::chrono::seconds(Fixture::EXPIRATION_SECONDS);
    EXPECT_EQ(f.toWrite(both), both);
}

TEST(TimeSeriesDeduplicationCache, RewriteRestartsThePeriodWithANewOffset)
{
    Fixture f;

    f.random_value = Fixture::MAX_OFFSET_SECONDS;
    f.write({"a"});

    const auto first_written_at = f.now;
    f.now = first_written_at + std::chrono::seconds(Fixture::EXPIRATION_SECONDS - Fixture::MAX_OFFSET_SECONDS);
    f.random_value = 0;
    f.write({"a"});

    const auto second_written_at = f.now;
    f.now = second_written_at + std::chrono::seconds(Fixture::EXPIRATION_SECONDS - 1);
    EXPECT_EQ(f.toWrite({"a"}), std::vector<String>{});

    f.now = second_written_at + std::chrono::seconds(Fixture::EXPIRATION_SECONDS);
    EXPECT_EQ(f.toWrite({"a"}), std::vector<String>{"a"});
}
