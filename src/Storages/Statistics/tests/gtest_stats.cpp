#include <gtest/gtest.h>

#include <fmt/format.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsMinMax.h>
#include <Storages/Statistics/StatisticsNullCount.h>
#include <Storages/StatisticsDescription.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Statistics/StatisticsTDigest.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Core/Range.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>

using namespace DB;

TEST(Statistics, TDigestLessThan)
{
    /// this is the simplest data which is continuous integeters.
    /// so the estimated errors should be low.

    std::vector<Int64> data;
    data.reserve(100000);
    for (int i = 0; i < 100000; i++)
        data.push_back(i);

    auto test_less_than = [](const std::vector<Int64> & data1,
                             const std::vector<double> & v,
                             const std::vector<double> & answers,
                             const std::vector<double> & eps)
    {

        DB::QuantileTDigest<Int64> t_digest;

        for (Int64 i : data1)
            t_digest.add(i);

        t_digest.compress();

        for (int i = 0; i < v.size(); i ++)
        {
            auto value = v[i];
            auto result = t_digest.getCountLessThan(value);
            auto answer = answers[i];
            auto error = eps[i];
            ASSERT_LE(result, answer * (1 + error));
            ASSERT_GE(result, answer * (1 - error));
        }
    };
    test_less_than(data, {-1, 1e9, 50000.0, 3000.0, 30.0}, {0, 100000, 50000, 3000, 30}, {0, 0, 0.001, 0.001, 0.001});

    std::reverse(data.begin(), data.end());
    test_less_than(data, {-1, 1e9, 50000.0, 3000.0, 30.0}, {0, 100000, 50000, 3000, 30}, {0, 0, 0.001, 0.001, 0.001});
}

TEST(Statistics, Estimator)
{
    /// MinMax::estimateLess fallback uses toFloat64 for type mismatches; register functions so it works.
    tryRegisterFunctions();
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();
    /// column a, distribution 1,2...,10000
    /// column b, distribution 500,600,500,600...
    /// column c, distribution -10000, -1000, -100, -10, -1, 1, 10, 100, 1008, 1009, 1010, ...
    MutableColumnPtr a = DataTypeInt32().createColumn();
    MutableColumnPtr b = DataTypeInt32().createColumn();
    MutableColumnPtr c = DataTypeInt32().createColumn();
    Int32 c_value[] = {-100000, -1000, -100, -10, -1, 1, 10, 100};
    for (Int32 i = 0; i < 10000; i++)
    {
        a->insert(i+1);
        b->insert(i % 2 == 0 ? 500 : 600);
        c->insert(i < 8 ? c_value[i]: 1000+i);
    }

    auto mock_statistics = [&](const String & column_name)
    {
        ColumnStatisticsDescription mock_description;
        mock_description.data_type = data_type;
        std::vector<StatisticsType> stats_type_to_create({StatisticsType::TDigest, /*StatisticsType::Uniq,*/ StatisticsType::CountMinSketch});
        for (auto stats_type : stats_type_to_create)
        {
            mock_description.types_to_desc.emplace(stats_type, SingleStatisticsDescription(stats_type, nullptr, false));
        }
        ColumnDescription column_desc;
        column_desc.name = column_name;
        column_desc.type = data_type;
        column_desc.statistics = mock_description;
        return MergeTreeStatisticsFactory::instance().get(column_desc);
    };
    ColumnStatisticsPtr stats_a = mock_statistics("a");
    stats_a->build(std::move(a));
    ColumnStatisticsPtr stats_b = mock_statistics("b");
    stats_b->build(std::move(b));
    ColumnStatisticsPtr stats_c = mock_statistics("c");
    stats_c->build(std::move(c));

    ConditionSelectivityEstimatorBuilder estimator_builder(getContext().context);
    estimator_builder.addStatistics("a", stats_a);
    estimator_builder.addStatistics("b", stats_b);
    estimator_builder.addStatistics("c", stats_c);
    estimator_builder.incrementRowCount(10000);

    auto estimator = estimator_builder.getEstimator();

    auto test_impl = [&](const String & expression, Int64 real_result, Float64 eps)
    {
        ParserExpressionWithOptionalAlias exp_parser(false);
        ContextPtr context = getContext().context;
        RPNBuilderTreeContext tree_context(context, Block{{ DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy" }}, {});
        ASTPtr ast = parseQuery(exp_parser, expression, 10000, 10000, 10000);
        RPNBuilderTreeNode node(ast.get(), tree_context);
        auto estimate_result = estimator->estimateRelationProfile(nullptr, node);
        std::cout << expression << " " << real_result << " "<< estimate_result.rows << std::endl;
        EXPECT_LT(std::abs(real_result - static_cast<Int64>(estimate_result.rows)), 10000 * eps);
    };

    auto test_f = [&](const String & expression, Int64 real_result, Float64 eps = 0.001)
    {
        test_impl(expression, real_result, eps);
        /// Let's test 'not expression'
        test_impl("not(" + expression + ")", 10000-real_result, eps);
    };
    ///
    test_f("a in (1,2,3,4,5)", 5);
    test_f("a not in (1,2,3,4,5)", 10000-5);
    test_f("b in (2, 500, 500)", 5000);
    test_f("a < 3 and b = 500", 1);
    test_f("a < 3 and b = 500 and a < b", 1); /// unknown condition 'a < b' assumes 100% selectivity
    test_f("a < 3 or b = 600", 5001);
    test_f("not (a < 3 and b = 500)", 10000-1);
    test_f("c between -1000 and -10", 3);
    test_f("b != 500 and b != 600", 0);
    test_f("not (b != 500 and b != 600)", 10000);
    test_f("b != 500 or b != 600", 10000);
    test_f("not (b != 500 or b != 600)", 0);
    test_f("a < 3 and b != 600", 1);
    test_f("a > 3 and b != 600", 4998);
    test_f("(a > 3 or a < 10) and b != 600", 5000);
    test_f("(a > 3 and a < 10) and b != 600", 3);
    test_f("(a > 3 and a < 10) or (b != 600 and b != 500)", 6);
    test_f("(a > 3 and a < 10) or not (b != 600 and b != 500)", 10000);
    test_f("((a > 3 and a < 10) or (a > 900 and a < 1000) or (a > 9050 and a < 9060))", 114);
    test_f("(a > 3 and a < 1000) or (a > 3 and a < 1011) or (a > 3 and a < 2012)", 2008);
    test_f("(a > 3 and a < 1000) or (a > 3 and a < 1011) or (b = 500)", 5503);
    test_f("(a > 3 and a < 1000) or ((a > 3 and a < 1011) and (b = 500))", 1001, 0.05); /// 5% error
    test_f("((a > 3 and a < 1000) or (a > 3 and a < 1011)) and (b = 500)", 503);
    test_f("a = 5 and a != 6", 1);
}

TEST(Statistics, MinMaxEstimateLess)
{
    auto test_minmax = [](Field min_val, Field max_val, UInt64 row_count, Field val, Float64 expected)
    {
        StatisticsMinMax stats(min_val, max_val, row_count);
        auto result = stats.estimateLess(val);
        ASSERT_TRUE(result.has_value()) << "estimateLess returned nullopt";
        EXPECT_DOUBLE_EQ(*result, expected);
    };

    /// UInt64: interpolation over [0, 9] with 10 rows
    test_minmax(UInt64(0), UInt64(9), 10, UInt64(0),  0.0);           /// at min    → (0/9)*10 = 0
    test_minmax(UInt64(0), UInt64(9), 10, UInt64(9),  10.0);          /// at max    → (9/9)*10 = 10
    test_minmax(UInt64(0), UInt64(9), 10, UInt64(10), 10.0);          /// above max → all rows
    test_minmax(UInt64(0), UInt64(9), 10, UInt64(5),  5.0/9.0*10.0); /// midpoint

    /// Int64: negative range [-100, 100] with 201 rows
    test_minmax(Int64(-100), Int64(100), 201, Int64(-200), 0.0);               /// below min
    test_minmax(Int64(-100), Int64(100), 201, Int64(200),  201.0);             /// above max
    test_minmax(Int64(-100), Int64(100), 201, Int64(0),    100.0/200.0*201.0); /// midpoint

    /// All rows have the same value: min == max
    test_minmax(UInt64(42), UInt64(42), 50, UInt64(42), 50.0); /// v == min == max → all rows
    test_minmax(UInt64(42), UInt64(42), 50, UInt64(43), 50.0); /// v > max         → all rows
    test_minmax(UInt64(42), UInt64(42), 50, UInt64(41), 0.0);  /// v < min         → 0 rows

    /// Precision: UInt64 values near 2^53 where Float64 loses consecutive integers.
    /// Float64(2^53 + 1) rounds to Float64(2^53), so naive conversion gives numerator = 0.
    /// interpolateLinear must use UInt128 internally to recover the correct result.
    const UInt64 base = (1ULL << 53); /// = 9007199254740992
    test_minmax(UInt64(base), UInt64(base + 2), 3, UInt64(base + 1), 1.5); /// (1/2)*3 = 1.5

    /// estimateLess returns nullopt when row_count = 0
    StatisticsMinMax empty(Field{}, Field{}, 0);
    EXPECT_FALSE(empty.estimateLess(Field(UInt64(42))).has_value());
}

namespace
{
/// Helper to create ColumnStatistics with specified types for merge testing
ColumnStatisticsPtr createTestStats(
    const std::vector<StatisticsType> & types,
    const DataTypePtr & data_type = std::make_shared<DataTypeInt32>())
{
    ColumnStatisticsDescription desc;
    desc.data_type = data_type;
    for (auto type : types)
        desc.types_to_desc.emplace(type, SingleStatisticsDescription(type, nullptr, false));

    return MergeTreeStatisticsFactory::instance().get(desc);
}

/// Build a Nullable(Int32) column with `total` rows where every `null_every`-th row is NULL,
/// then emit value `i` for the rest. Returns built ColumnStatistics with the requested types.
ColumnStatisticsPtr buildNullableInt32Stats(
    const std::vector<StatisticsType> & types,
    size_t total,
    size_t null_every)
{
    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    MutableColumnPtr col = data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(col.get());
    for (size_t i = 0; i < total; ++i)
    {
        if (i % null_every == 0)
            nullable_col->insertDefault();
        else
            nullable_col->insert(static_cast<Int32>(i));
    }
    auto stats = createTestStats(types, data_type);
    stats->build(std::move(col));
    return stats;
}

/// Build an estimator over a single column "x" with the given stats and row count.
auto makeEstimator(const ColumnStatisticsPtr & stats_x, UInt64 row_count)
{
    ConditionSelectivityEstimatorBuilder builder(getContext().context);
    builder.addStatistics("x", stats_x);
    builder.incrementRowCount(row_count);
    return builder.getEstimator();
}

/// Estimate the number of rows for a SQL boolean expression over column "x".
template <class Estimator>
Float64 estimateRows(Estimator & estimator, const String & expression)
{
    ParserExpressionWithOptionalAlias exp_parser(false);
    ContextPtr context = getContext().context;
    RPNBuilderTreeContext tree_context(
        context,
        Block{{ DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy" }},
        {});
    ASTPtr ast = parseQuery(exp_parser, expression, 10000, 10000, 10000);
    RPNBuilderTreeNode node(ast.get(), tree_context);
    return static_cast<Float64>(estimator->estimateRelationProfile(nullptr, node).rows);
}

/// Round-trip a ColumnStatistics through serialize/deserialize using `read_type` for deserialization.
ColumnStatisticsPtr roundTripStats(const ColumnStatisticsPtr & stats, const DataTypePtr & read_type)
{
    String serialized;
    WriteBufferFromString write_buf(serialized);
    stats->serialize(write_buf);
    write_buf.finalize();
    ReadBufferFromString read_buf(serialized);
    return ColumnStatistics::deserialize(read_buf, read_type);
}
}

TEST(Statistics, UniqRoundTripAcrossNullable)
{
    /// Verify Uniq stats survive serialize/deserialize across {non-nullable, nullable}
    /// type combinations. NULL values are not counted in cardinality.
    tryRegisterAggregateFunctions();

    auto string_type = std::make_shared<DataTypeString>();
    auto nullable_string_type = std::make_shared<DataTypeNullable>(string_type);

    auto build_with_two_unique = [](const DataTypePtr & dt, bool include_nulls)
    {
        MutableColumnPtr col = dt->createColumn();
        auto append = [&](const String & v) { col->insert(Field(v)); };
        append("alpha");
        if (include_nulls) col->insertDefault();
        append("beta");
        append("alpha");
        if (include_nulls) col->insertDefault();
        auto stats = createTestStats({StatisticsType::Uniq}, dt);
        stats->build(std::move(col));
        return stats;
    };

    /// Three round-trip combinations sharing identical assertions.
    struct Case { DataTypePtr write_type; DataTypePtr read_type; bool include_nulls; UInt64 expected_rows; };
    std::vector<Case> cases{
        {nullable_string_type, nullable_string_type, true,  5},
        {string_type,          nullable_string_type, false, 3},
        {nullable_string_type, string_type,          true,  5},
    };

    for (const auto & c : cases)
    {
        SCOPED_TRACE(fmt::format("write={} read={} rows={}", c.write_type->getName(), c.read_type->getName(), c.expected_rows));
        auto stats = build_with_two_unique(c.write_type, c.include_nulls);
        EXPECT_EQ(stats->getNumRows(), c.expected_rows);
        EXPECT_EQ(stats->estimateCardinality(), 2u);
        auto deserialized = roundTripStats(stats, c.read_type);
        EXPECT_EQ(deserialized->getNumRows(), c.expected_rows);
        EXPECT_EQ(deserialized->estimateCardinality(), 2u);
    }
}

TEST(Statistics, NonNullRowCountSemantics)
{
    /// Cover the contract that all column-level estimates derive from getNonNullRowCount(),
    /// not from total rows (which include NULLs). This combines what used to be four
    /// separate tests (estimateGreater with/without NullCount, LowCardinality(Nullable),
    /// and infinite range without NULL).

    /// Case 1 — NullCount + MinMax: 100 rows, 50 NULL + 50 non-NULL values 1..50.
    {
        SCOPED_TRACE("NullCount + MinMax");
        auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        MutableColumnPtr col = data_type->createColumn();
        auto * nullable_col = assert_cast<ColumnNullable *>(col.get());
        for (Int32 i = 0; i < 50; ++i)
        {
            nullable_col->insertDefault();
            nullable_col->insert(i + 1);
        }
        auto stats = createTestStats({StatisticsType::NullCount, StatisticsType::MinMax}, data_type);
        stats->build(std::move(col));
        ASSERT_EQ(stats->getNumRows(), 100u);
        ASSERT_EQ(stats->getNonNullRowCount(), 50u);
        EXPECT_NEAR(*stats->estimateGreater(Field(Int32(0))), 50.0, 1.0);
        EXPECT_NEAR(*stats->estimateGreater(Field(Int32(50))), 0.0, 1.0);
        EXPECT_NEAR(*stats->estimateGreater(Field(Int32(25))), 25.0, 2.0);
    }

    /// Case 2 — MinMax only (no NullCount): the same data must still report
    /// non-NULL row count from MinMax::row_count, otherwise estimateGreater
    /// would treat NULL rows as matching `> val`.
    {
        SCOPED_TRACE("MinMax only — fallback to MinMax::row_count");
        auto stats = buildNullableInt32Stats({StatisticsType::MinMax}, /*total=*/100, /*null_every=*/2);
        /// Values for non-NULL rows are i where i%2 != 0, so 1,3,...,99 → 50 rows in [1, 99].
        ASSERT_EQ(stats->getNumRows(), 100u);
        ASSERT_EQ(stats->getNonNullRowCount(), 50u);
        EXPECT_NEAR(*stats->estimateGreater(Field(Int32(0))), 50.0, 1.0);
    }

    /// Case 3 — LowCardinality(Nullable): MinMax::build must subtract NULLs for LC too.
    {
        SCOPED_TRACE("LowCardinality(Nullable) MinMax");
        auto data_type = std::make_shared<DataTypeLowCardinality>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()));
        MutableColumnPtr col = data_type->createColumn();
        for (Int32 i = 0; i < 100; ++i)
        {
            if (i % 10 == 0)
                col->insert(Field()); /// NULL
            else
                col->insert(i);
        }
        auto stats = createTestStats({StatisticsType::NullCount, StatisticsType::MinMax}, data_type);
        stats->build(std::move(col));
        ASSERT_EQ(stats->getNumRows(), 100u);
        ASSERT_EQ(stats->getNonNullRowCount(), 90u);
        EXPECT_NEAR(*stats->estimateLess(Field(Int32(0))),     0.0, 1.0);
        EXPECT_NEAR(*stats->estimateLess(Field(Int32(100))),  90.0, 1.0);
        EXPECT_NEAR(*stats->estimateGreater(Field(Int32(0))), 90.0, 1.0);
    }

    /// Case 4 — estimateRange over a NULL-excluding infinite range must be non-NULL row count.
    {
        SCOPED_TRACE("estimateRange(WholeUniverseWithoutNull)");
        auto stats = buildNullableInt32Stats({StatisticsType::NullCount}, /*total=*/100, /*null_every=*/10);
        ASSERT_EQ(stats->getNumRows(), 100u);
        ASSERT_EQ(stats->getNonNullRowCount(), 90u);
        auto estimate = stats->estimateRange(Range::createWholeUniverseWithoutNull());
        ASSERT_TRUE(estimate.has_value());
        EXPECT_DOUBLE_EQ(*estimate, 90.0);
    }
}

TEST(Statistics, NullPredicateEstimates)
{
    /// All NULL-predicate selectivity logic shares one estimator setup, so we
    /// cover contradiction/tautology and range combinations in a single test.
    /// Column x: 100 rows, 50% NULL, non-NULL values 1, 3, 5, ..., 99.
    tryRegisterFunctions(); /// MinMax::estimateLess fallback uses toFloat64 for type mismatches.
    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    MutableColumnPtr x = data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(x.get());
    for (Int32 i = 0; i < 100; ++i)
    {
        if (i % 2 == 0)
            nullable_col->insertDefault(); /// NULL
        else
            nullable_col->insert(i);
    }

    auto stats_x = createTestStats({StatisticsType::NullCount, StatisticsType::MinMax}, data_type);
    stats_x->build(std::move(x));

    auto estimator = makeEstimator(stats_x, /*row_count=*/100);

    auto check = [&](const String & expression, Float64 expected_rows, Float64 eps)
    {
        Float64 actual = estimateRows(estimator, expression);
        EXPECT_NEAR(actual, expected_rows, eps)
            << "Expression: " << expression
            << " expected=" << expected_rows << " actual=" << actual;
    };

    /// Contradiction (`x IS NULL AND x IS NOT NULL`) and tautology (`x IS NULL OR x IS NOT NULL`).
    check("x IS NULL AND x IS NOT NULL",       0.0, 1e-6);
    check("x IS NOT NULL AND x IS NULL",       0.0, 1e-6); /// commutative
    check("NOT x IS NULL AND x IS NULL",       0.0, 1e-6); /// NOT (IS NULL) ≡ IS NOT NULL
    check("x IS NULL OR x IS NOT NULL",      100.0, 1e-6);
    check("x IS NOT NULL OR x IS NULL",      100.0, 1e-6);
    check("NOT (x IS NOT NULL) OR x IS NOT NULL", 100.0, 1e-6);

    /// IS NULL combined with a range — NULL is disjoint from any concrete range.
    check("x IS NULL AND x > 50",              0.0, 1e-6);
    check("x IS NULL AND x < 50",              0.0, 1e-6);
    check("x IS NULL AND x BETWEEN 10 AND 20", 0.0, 1e-6);

    /// IS NOT NULL combined with a range — equals the range estimate over non-NULL rows.
    /// 50 non-NULL values in [1, 99]; estimateLess(50) ≈ 50 * 49 / 98 = 25.
    check("x IS NOT NULL AND x > 50",         25.0, 2.0);
    check("x IS NOT NULL AND x < 50",         25.0, 2.0);
    check("x IS NULL OR x < 50",         75.0, 2.0);
    check("x IS NOT NULL",         50.0, 2.0);
    check("x IS NOT NULL OR x < 50",         50.0, 2.0);
    check("not x < 50",         25.0, 2.0);
    check("x IS NULL OR not x < 50",         75.0, 2.0);

    /// Plain range — must not include NULL rows.
    check("x > 50",                           25.0, 2.0);
    check("x < 50",                           25.0, 2.0);
}


TEST(Statistics, SerializeDeserializeRoundTrip)
{
    /// Test that ColumnStatistics survives a serialize-deserialize round-trip
    /// with V3 format (per-type size prefix).

    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    MutableColumnPtr col = data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(col.get());
    for (Int32 i = 0; i < 100; ++i)
    {
        if (i % 10 == 0)
            nullable_col->insertDefault(); /// NULL
        else
            nullable_col->insert(i);
    }

    auto stats = createTestStats({StatisticsType::MinMax, StatisticsType::NullCount, StatisticsType::TDigest}, data_type);
    stats->build(std::move(col));

    UInt64 original_rows = stats->getNumRows();
    auto original_null_count = stats->getNullCount();

    /// Serialize
    String serialized;
    WriteBufferFromString write_buf(serialized);
    stats->serialize(write_buf);
    write_buf.finalize();

    /// Deserialize
    ReadBufferFromString read_buf(serialized);
    auto deserialized = ColumnStatistics::deserialize(read_buf, data_type);

    /// Verify row count
    EXPECT_EQ(deserialized->getNumRows(), original_rows);

    /// Verify NullCount
    EXPECT_EQ(deserialized->getNullCount(), original_null_count);

    /// Verify all stat types present
    const auto & deserialized_stats = deserialized->getStats();
    EXPECT_TRUE(deserialized_stats.contains(StatisticsType::MinMax));
    EXPECT_TRUE(deserialized_stats.contains(StatisticsType::NullCount));
    EXPECT_TRUE(deserialized_stats.contains(StatisticsType::TDigest));

    /// Verify buffer fully consumed
    EXPECT_TRUE(read_buf.eof());
}

TEST(Statistics, DeserializeV3CloneEmptyPreservesStatsDescription)
{
    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto stats = buildNullableInt32Stats({StatisticsType::MinMax, StatisticsType::NullCount}, 100, 5);
    auto deserialized = roundTripStats(stats, data_type);
    auto clone = deserialized->cloneEmpty();

    EXPECT_EQ(clone->getNumRows(), 0);
    EXPECT_TRUE(clone->structureEquals(*deserialized));
    EXPECT_TRUE(clone->getStats().contains(StatisticsType::MinMax));
    EXPECT_TRUE(clone->getStats().contains(StatisticsType::NullCount));
}

TEST(Statistics, DeserializeV3SkipsTrailingBytes)
{
    /// Test that V3 deserialization correctly skips trailing bytes when a known
    /// stat type's deserialize consumes fewer bytes than stat_size.
    /// This simulates forward compatibility: a newer version may write extra
    /// bytes after a known stat, and an older version should skip them.

    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    /// Build a stats object and serialize it
    MutableColumnPtr col = data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(col.get());
    for (Int32 i = 0; i < 50; ++i)
    {
        if (i % 5 == 0)
            nullable_col->insertDefault();
        else
            nullable_col->insert(i);
    }

    auto stats = createTestStats({StatisticsType::NullCount}, data_type);
    stats->build(std::move(col));

    String serialized;
    WriteBufferFromString write_buf(serialized);
    stats->serialize(write_buf);
    write_buf.finalize();

    /// Now manually inject trailing bytes after the NullCount stat payload.
    /// V3 format: version(2) + mask(8) + rows(8) + [stat_size(8) + stat_data(stat_size)]*
    /// We need to find where the NullCount stat_data ends and insert extra bytes,
    /// adjusting stat_size accordingly.

    /// Parse the serialized V3 format to locate stat_size and stat_data boundaries.
    /// Layout:
    ///   offset 0: UInt16 version
    ///   offset 2: UInt64 stat_types_mask
    ///   offset 10: UInt64 rows
    ///   offset 18: UInt64 stat_size (for the first stat)
    ///   offset 26: stat_data (stat_size bytes)
    ///   ...next stat or end

    ReadBufferFromString orig_buf(serialized);

    UInt16 version_raw;
    readIntBinary(version_raw, orig_buf);
    ASSERT_EQ(version_raw, static_cast<UInt16>(StatisticsFileVersion::V3));

    UInt64 stat_types_mask = 0;
    readIntBinary(stat_types_mask, orig_buf);

    UInt64 rows_value = 0;
    readIntBinary(rows_value, orig_buf);

    /// Read the stat_size for the first (and only) stat
    UInt64 stat_size = 0;
    readIntBinary(stat_size, orig_buf);

    /// Record the start of stat_data
    const char * stat_data_start = orig_buf.position();
    String stat_data(stat_data_start, stat_size);

    /// Now build a new serialized buffer with extra trailing bytes
    UInt64 new_stat_size = stat_size + 7; /// 7 extra trailing bytes
    String modified;
    WriteBufferFromString mod_buf(modified);
    writeIntBinary(version_raw, mod_buf);
    writeIntBinary(stat_types_mask, mod_buf);
    writeIntBinary(rows_value, mod_buf);
    writeIntBinary(new_stat_size, mod_buf);
    mod_buf.write(stat_data.data(), stat_size);
    /// Write 7 trailing bytes (simulating forward-compatible extra data)
    const char trailing[7] = {static_cast<char>(0xAA), static_cast<char>(0xBB), static_cast<char>(0xCC), static_cast<char>(0xDD), static_cast<char>(0xEE), static_cast<char>(0xFF), 0x00};
    mod_buf.write(trailing, 7);
    mod_buf.finalize();

    /// Deserialize from modified buffer — should successfully skip the 7 trailing bytes
    ReadBufferFromString mod_read_buf(modified);
    auto deserialized = ColumnStatistics::deserialize(mod_read_buf, data_type);

    EXPECT_EQ(deserialized->getNumRows(), rows_value);
    EXPECT_TRUE(deserialized->getStats().contains(StatisticsType::NullCount));
    EXPECT_EQ(deserialized->getNullCount(), stats->getNullCount());

    /// Buffer should be fully consumed (no leftover)
    EXPECT_TRUE(mod_read_buf.eof());
}

TEST(Statistics, DeserializeV3ThrowsOnOversizedStat)
{
    /// Test that V3 deserialization throws when a known stat type consumes
    /// more bytes than stat_size indicates (corrupted data).

    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    /// Build and serialize a stats object with MinMax (which has a non-trivial payload)
    MutableColumnPtr col = data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(col.get());
    for (Int32 i = 0; i < 50; ++i)
        nullable_col->insert(i);

    auto stats = createTestStats({StatisticsType::MinMax}, data_type);
    stats->build(std::move(col));

    String serialized;
    WriteBufferFromString write_buf(serialized);
    stats->serialize(write_buf);
    write_buf.finalize();

    /// Parse the V3 header to find stat_size and stat_data
    ReadBufferFromString orig_buf(serialized);

    UInt16 version_raw;
    readIntBinary(version_raw, orig_buf);

    UInt64 stat_types_mask = 0;
    readIntBinary(stat_types_mask, orig_buf);

    UInt64 rows_value = 0;
    readIntBinary(rows_value, orig_buf);

    UInt64 stat_size = 0;
    readIntBinary(stat_size, orig_buf);

    String stat_data(orig_buf.position(), stat_size);

    /// Build a modified buffer with stat_size smaller than actual payload
    UInt64 shrunk_stat_size = stat_size > 2 ? stat_size - 2 : 1;
    String modified;
    WriteBufferFromString mod_buf(modified);
    writeIntBinary(version_raw, mod_buf);
    writeIntBinary(stat_types_mask, mod_buf);
    writeIntBinary(rows_value, mod_buf);
    writeIntBinary(shrunk_stat_size, mod_buf);
    /// Write the full stat_data (more than shrunk_stat_size)
    mod_buf.write(stat_data.data(), stat_size);
    mod_buf.finalize();

    /// Deserialization should throw ILLEGAL_STATISTICS because consumed > stat_size
    ReadBufferFromString mod_read_buf(modified);
    EXPECT_THROW(ColumnStatistics::deserialize(mod_read_buf, data_type), DB::Exception);
}

TEST(Statistics, LikeSelectivity)
{
    /// Build a simple estimator to test LIKE / NOT LIKE / ILIKE / NOT ILIKE
    /// selectivity defaults and their complement behavior under NOT.
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    MutableColumnPtr col = DataTypeInt32().createColumn();
    for (Int32 i = 0; i < 10000; i++)
        col->insert(i + 1);

    ColumnStatisticsDescription mock_description;
    mock_description.data_type = data_type;
    mock_description.types_to_desc.emplace(StatisticsType::TDigest, SingleStatisticsDescription(StatisticsType::TDigest, nullptr, false));

    ColumnDescription column_desc;
    column_desc.name = "a";
    column_desc.type = data_type;
    column_desc.statistics = mock_description;
    auto stats = MergeTreeStatisticsFactory::instance().get(column_desc);
    stats->build(std::move(col));

    ConditionSelectivityEstimatorBuilder estimator_builder(getContext().context);
    estimator_builder.addStatistics("a", stats);
    estimator_builder.incrementRowCount(10000);
    auto estimator = estimator_builder.getEstimator();

    /// Helper: estimate rows for a condition string.
    auto estimate = [&](const String & expression) -> UInt64
    {
        ParserExpressionWithOptionalAlias exp_parser(false);
        ContextPtr context = getContext().context;
        RPNBuilderTreeContext tree_context(context, Block{{DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy"}}, {});
        ASTPtr ast = parseQuery(exp_parser, expression, 10000, 10000, 10000);
        RPNBuilderTreeNode node(ast.get(), tree_context);
        return estimator->estimateRelationProfile(nullptr, node).rows;
    };

    /// default_like_factor = 0.1, total_rows = 10000.
    /// LIKE: 0.1 * 10000 = 1000 rows.
    UInt64 like_rows = estimate("a like '%pattern%'");
    EXPECT_EQ(like_rows, 1000u);

    /// NOT LIKE: (1 - 0.1) * 10000 = 9000 rows.
    UInt64 not_like_rows = estimate("not(a like '%pattern%')");
    EXPECT_EQ(not_like_rows, 9000u);

    /// Complement: LIKE + NOT LIKE = total rows.
    EXPECT_EQ(like_rows + not_like_rows, 10000u);

    /// ILIKE: same as LIKE.
    UInt64 ilike_rows = estimate("a ilike '%pattern%'");
    EXPECT_EQ(ilike_rows, 1000u);

    /// NOT ILIKE: same as NOT LIKE.
    UInt64 not_ilike_rows = estimate("not(a ilike '%pattern%')");
    EXPECT_EQ(not_ilike_rows, 9000u);

    /// notLike function directly: 0.9 * 10000 = 9000 rows.
    UInt64 notlike_direct_rows = estimate("a not like '%pattern%'");
    EXPECT_EQ(notlike_direct_rows, 9000u);

    /// notILike function directly: 0.9 * 10000 = 9000 rows.
    UInt64 notilike_direct_rows = estimate("a not ilike '%pattern%'");
    EXPECT_EQ(notilike_direct_rows, 9000u);
}
