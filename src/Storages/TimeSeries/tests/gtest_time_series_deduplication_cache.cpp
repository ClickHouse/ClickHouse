#include <Storages/TimeSeries/TimeSeriesDeduplicationCache.h>

#include <Columns/ColumnString.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>

#include <gtest/gtest.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <utility>
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

namespace
{
    /// The key of a row and whether its values differ from the first row of the key.
    struct ScheduledRow
    {
        size_t key;
        bool changed;
    };

    constexpr size_t NUM_SCHEDULED_ROWS = 200;
    constexpr size_t NUM_KEYS = 150;

    /// The first `NUM_KEYS` rows have different keys. Each other row repeats one of those keys: every other one with the same
    /// values (it isn't written), the rest with changed values (they are written and replace the pending row).
    constexpr size_t NUM_WRITTEN_ROWS = NUM_KEYS + (NUM_SCHEDULED_ROWS - NUM_KEYS) / 2;

    std::vector<ScheduledRow> makeSchedule()
    {
        std::vector<ScheduledRow> rows;
        for (size_t i = 0; i != NUM_SCHEDULED_ROWS; ++i)
        {
            if (i < NUM_KEYS)
                rows.push_back({.key = i, .changed = false});
            else
                rows.push_back({.key = (i - NUM_KEYS) * 7 % NUM_KEYS, .changed = (i % 2 == 1)});
        }
        return rows;
    }

    /// Deterministic bits of a key, spread over the whole word.
    UInt64 keyBits(size_t key)
    {
        UInt64 x = (key + 1) * 0x9E3779B97F4A7C15ULL;
        return x ^ (x >> 31);
    }

    UUID keyUUID(size_t key)
    {
        return UUID((UInt128(keyBits(key)) << 64) | UInt128(keyBits(key + NUM_KEYS)));
    }

    /// The layout of the tags block made by `TimeSeriesSink::makeTagsBlock`: `id` first, then `metric_name`, the columns of
    /// `tags_to_columns` (none by default), `tags`, and `min_time` and `max_time` for the tables of earlier versions storing them.
    struct TagsBlockShape
    {
        std::string_view name;
        std::string_view id_type;
        Field (*make_id)(size_t key);
        bool with_min_and_max_time;
    };

    std::vector<TagsBlockShape> getTagsBlockShapes()
    {
        return {
            /// The default `id` of the current version.
            {"tags_id_tuple",
             "Tuple(UInt64, LowCardinality(UUID))",
             [](size_t key) -> Field { return Tuple{Field(keyBits(key)), Field(keyUUID(key))}; },
             false},
            {"tags_id_uuid", "UUID", [](size_t key) -> Field { return keyUUID(key); }, false},
            {"tags_id_uint64", "UInt64", [](size_t key) -> Field { return keyBits(key); }, false},
            {"tags_id_uuid_min_max_time", "UUID", [](size_t key) -> Field { return keyUUID(key); }, true},
        };
    }

    Block makeTagsBlock(const TagsBlockShape & shape, const std::vector<ScheduledRow> & rows)
    {
        const auto & factory = DataTypeFactory::instance();
        auto id_type = factory.get(String(shape.id_type));
        auto metric_name_type = factory.get("LowCardinality(String)");
        auto tags_type = factory.get("Map(LowCardinality(String), String)");
        auto time_type = factory.get("Nullable(DateTime64(3))");

        auto id_column = id_type->createColumn();
        auto metric_name_column = metric_name_type->createColumn();
        auto tags_column = tags_type->createColumn();
        auto min_time_column = time_type->createColumn();
        auto max_time_column = time_type->createColumn();

        for (const auto & [key, changed] : rows)
        {
            id_column->insert(shape.make_id(key));
            metric_name_column->insert("metric_" + std::to_string(key % 20));

            Map tags;
            tags.push_back(Tuple{Field("instance"), Field("host_" + std::to_string(key % 10))});
            tags.push_back(Tuple{Field("job"), Field("job_" + std::to_string(key % 3))});
            for (size_t i = 0; i != key % 4; ++i)
                tags.push_back(Tuple{Field("label_" + std::to_string(i)), Field("value_" + std::to_string(key) + "_" + std::to_string(i))});
            if (changed)
                tags.push_back(Tuple{Field("version"), Field("2")});
            tags_column->insert(tags);

            /// A time series without samples has no time range.
            if (key % 4 == 0)
            {
                min_time_column->insert(Field());
                max_time_column->insert(Field());
            }
            else
            {
                auto min_time = static_cast<Int64>(1700000000000 + key * 1000);
                min_time_column->insert(DecimalField<DateTime64>(DateTime64(min_time), 3));
                max_time_column->insert(DecimalField<DateTime64>(DateTime64(min_time + (changed ? 60000 : 1000)), 3));
            }
        }

        Block block;
        block.insert(ColumnWithTypeAndName{std::move(id_column), id_type, TimeSeriesColumnNames::ID});
        block.insert(ColumnWithTypeAndName{std::move(metric_name_column), metric_name_type, TimeSeriesColumnNames::MetricName});
        block.insert(ColumnWithTypeAndName{std::move(tags_column), tags_type, TimeSeriesColumnNames::Tags});
        if (shape.with_min_and_max_time)
        {
            block.insert(ColumnWithTypeAndName{std::move(min_time_column), time_type, TimeSeriesColumnNames::MinTime});
            block.insert(ColumnWithTypeAndName{std::move(max_time_column), time_type, TimeSeriesColumnNames::MaxTime});
        }
        return block;
    }

    /// The metric families block made by `makeMetricFamiliesBlock` in TimeSeriesSink.cpp from the outer columns, all `String`.
    /// A row without a metric family precedes every 20th row, and `filter` drops it, as `findRowsWithMetricFamily` does.
    Block makeMetricFamiliesBlock(const std::vector<ScheduledRow> & rows, PaddedPODArray<UInt8> & filter)
    {
        const std::array<String, 4> types = {"counter", "gauge", "histogram", "summary"};
        const std::array<String, 3> units = {"", "seconds", "bytes"};

        auto metric_family_column = ColumnString::create();
        auto type_column = ColumnString::create();
        auto unit_column = ColumnString::create();
        auto help_column = ColumnString::create();

        for (size_t i = 0; i != rows.size(); ++i)
        {
            if (i % 20 == 0)
            {
                metric_family_column->insertDefault();
                type_column->insertDefault();
                unit_column->insertDefault();
                help_column->insertDefault();
                filter.push_back(false);
            }

            const auto & [key, changed] = rows[i];
            metric_family_column->insert("family_" + std::to_string(key));
            type_column->insert(types[key % types.size()]);
            unit_column->insert(units[key % units.size()]);
            help_column->insert("Help of family " + std::to_string(key) + (changed ? " (changed)" : ""));
            filter.push_back(true);
        }

        auto type = std::make_shared<DataTypeString>();
        Block block;
        block.insert(ColumnWithTypeAndName{std::move(metric_family_column), type, TimeSeriesColumnNames::MetricFamily});
        block.insert(ColumnWithTypeAndName{std::move(type_column), type, TimeSeriesColumnNames::Type});
        block.insert(ColumnWithTypeAndName{std::move(unit_column), type, TimeSeriesColumnNames::Unit});
        block.insert(ColumnWithTypeAndName{std::move(help_column), type, TimeSeriesColumnNames::Help});
        return block;
    }

    /// The pinned `PendingRows` of a block: the number of keys and the checksum of the (key hash, row hash) pairs.
    struct PinnedPendingRows
    {
        std::string_view name;
        size_t size;
        UInt64 checksum;

        bool operator==(const PinnedPendingRows &) const = default;
    };

    /// `SipHash` of the (key hash, row hash) pairs in the order of key hashes.
    UInt64 checksumOfPendingRows(const TimeSeriesDeduplicationCache::PendingRows & pending_rows)
    {
        std::vector<std::pair<UInt128, UInt128>> sorted(pending_rows.begin(), pending_rows.end());
        std::sort(sorted.begin(), sorted.end());

        SipHash hash;
        for (const auto & [key_hash, row_hash] : sorted)
        {
            hash.update(key_hash);
            hash.update(row_hash);
        }
        return hash.get64();
    }
}

/// The hashes of the rows of the blocks of both callers in TimeSeriesSink.cpp: the tags block, whose key is `id`,
/// and the metric families block, whose key is the metric family.
TEST(TimeSeriesDeduplicationCache, HashesOfRowsArePinned)
{
    /// The constants were computed by master's per-row hashing (`updateHashWithValue` of the key column and of all columns
    /// into a `SipHash` per row) and must not change.
    const std::vector<PinnedPendingRows> expected = {
        {"tags_id_tuple", 150, 0x7fcf76c56d5ba6e5},
        {"tags_id_uuid", 150, 0x7890f497ffd2934c},
        {"tags_id_uint64", 150, 0x64f615a307a970bd},
        {"tags_id_uuid_min_max_time", 150, 0x174993fb9144ac18},
        {"metric_families", 150, 0x7bb26ecf955862b7},
    };

    const auto rows = makeSchedule();
    std::vector<PinnedPendingRows> actual;

    for (const auto & shape : getTagsBlockShapes())
    {
        SCOPED_TRACE(shape.name);
        Fixture f;
        TimeSeriesDeduplicationCache::PendingRows pending_rows;
        auto block = makeTagsBlock(shape, rows);
        auto written = f.cache.filterOutWrittenRows(block, block.getPositionByName(TimeSeriesColumnNames::ID), pending_rows);
        EXPECT_EQ(written.rows(), NUM_WRITTEN_ROWS);
        actual.push_back({shape.name, pending_rows.size(), checksumOfPendingRows(pending_rows)});
    }

    {
        SCOPED_TRACE("metric_families");
        Fixture f;
        TimeSeriesDeduplicationCache::PendingRows pending_rows;
        PaddedPODArray<UInt8> filter;
        auto block = makeMetricFamiliesBlock(rows, filter);
        auto written = f.cache.filterOutWrittenRows(
            block, block.getPositionByName(TimeSeriesColumnNames::MetricFamily), pending_rows, std::move(filter));
        EXPECT_EQ(written.rows(), NUM_WRITTEN_ROWS);
        actual.push_back({"metric_families", pending_rows.size(), checksumOfPendingRows(pending_rows)});
    }

    if (actual != expected)
    {
        String computed;
        for (const auto & pinned : actual)
            computed += fmt::format("        {{\"{}\", {}, 0x{:016x}}},\n", pinned.name, pinned.size, pinned.checksum);
        ADD_FAILURE() << "The computed values, in the form of `expected`:\n" << computed;
    }
}
