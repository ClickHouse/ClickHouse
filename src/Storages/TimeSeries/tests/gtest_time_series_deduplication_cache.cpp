#include <Storages/TimeSeries/TimeSeriesDeduplicationCache.h>

#include <Columns/ColumnString.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>

#include <gtest/gtest.h>

#include <chrono>
#include <limits>
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

    std::vector<String> makeKeys(size_t num_keys)
    {
        std::vector<String> keys;
        for (size_t i = 0; i != num_keys; ++i)
            keys.push_back("k" + std::to_string(i));
        return keys;
    }

    struct Fixture
    {
        static constexpr UInt64 EXPIRATION_SECONDS = 3600;
        /// Mirrors `MIN_LIFETIME_RATIO` in TimeSeriesDeduplicationCache.cpp.
        static constexpr UInt64 MIN_LIFETIME_SECONDS = EXPIRATION_SECONDS * 4 / 5;

        TimeSeriesDeduplicationCache::TimePoint now{std::chrono::steady_clock::time_point{} + std::chrono::hours(1000)};

        TimeSeriesDeduplicationCache cache;

        explicit Fixture(UInt64 expiration_seconds = EXPIRATION_SECONDS)
            : cache(
                  1024 * 1024,
                  expiration_seconds,
                  CurrentMetrics::TimeSeriesTagsDeduplicationCacheEntries,
                  CurrentMetrics::TimeSeriesTagsDeduplicationCacheBytes,
                  ProfileEvents::TimeSeriesTagsDeduplicationCacheHits,
                  ProfileEvents::TimeSeriesTagsDeduplicationCacheMisses,
                  [this] { return now; })
        {
        }

        /// Writes `keys` and marks them as written.
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

TEST(TimeSeriesDeduplicationCache, RowsWrittenTogetherExpireAcrossTheLastFifth)
{
    Fixture f;

    const auto keys = makeKeys(100);
    f.write(keys);

    const auto written_at = f.now;

    f.now = written_at;
    EXPECT_EQ(f.toWrite(keys).size(), 0u);

    f.now = written_at + std::chrono::seconds(Fixture::MIN_LIFETIME_SECONDS - 1);
    EXPECT_EQ(f.toWrite(keys).size(), 0u);

    /// Each row's lifetime is uniform over the last fifth, so the chance that all 100 land on one side of the midpoint is 2 * 2^-100.
    f.now = written_at + std::chrono::seconds((Fixture::MIN_LIFETIME_SECONDS + Fixture::EXPIRATION_SECONDS) / 2);
    size_t num_expired_at_midpoint = f.toWrite(keys).size();
    EXPECT_GT(num_expired_at_midpoint, 0u);
    EXPECT_LT(num_expired_at_midpoint, 100u);

    f.now = written_at + std::chrono::seconds(Fixture::EXPIRATION_SECONDS);
    EXPECT_EQ(f.toWrite(keys).size(), 100u);
}

TEST(TimeSeriesDeduplicationCache, RewriteRestartsThePeriod)
{
    Fixture f;

    f.write({"a"});

    const auto first_written_at = f.now;
    f.now = first_written_at + std::chrono::seconds(Fixture::EXPIRATION_SECONDS);
    EXPECT_EQ(f.toWrite({"a"}), std::vector<String>{"a"});
    f.write({"a"});

    const auto second_written_at = f.now;
    f.now = second_written_at + std::chrono::seconds(Fixture::MIN_LIFETIME_SECONDS - 1);
    EXPECT_EQ(f.toWrite({"a"}), std::vector<String>{});

    f.now = second_written_at + std::chrono::seconds(Fixture::EXPIRATION_SECONDS);
    EXPECT_EQ(f.toWrite({"a"}), std::vector<String>{"a"});
}

TEST(TimeSeriesDeduplicationCache, MaximumExpirationNeverExpires)
{
    Fixture f(std::numeric_limits<UInt64>::max());

    f.write({"a"});

    EXPECT_EQ(f.toWrite({"a"}), std::vector<String>{});

    f.now += std::chrono::seconds(1);
    EXPECT_EQ(f.toWrite({"a"}), std::vector<String>{});

    f.now += std::chrono::hours(24 * 365 * 100);
    EXPECT_EQ(f.toWrite({"a"}), std::vector<String>{});
}

TEST(TimeSeriesDeduplicationCache, OneSecondPeriodExpiresWithinASecond)
{
    Fixture f(1);

    f.write({"a"});

    EXPECT_EQ(f.toWrite({"a"}), std::vector<String>{});

    f.now += std::chrono::seconds(1);
    EXPECT_EQ(f.toWrite({"a"}), std::vector<String>{"a"});
}
