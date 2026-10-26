#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsBasic.h>
#include <Storages/Statistics/StatisticsMinMax.h>
#include <Storages/StatisticsDescription.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/Statistics/StatisticsTDigest.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>

#include <limits>
#include "config.h"

using namespace DB;

namespace DB::ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
extern const int LOGICAL_ERROR;
}

namespace
{

/// Estimates the row count of a boolean `expression` over `columns` the way the planner feeds the
/// estimator: the expression is built into an `ActionsDAG` and its output node is analysed.
template <class Estimator>
UInt64 estimateRowsFor(const Estimator & estimator, const String & expression, const NamesAndTypesList & columns)
{
    ContextPtr context = getContext().context;
    ParserExpressionWithOptionalAlias parser(false);
    ASTPtr ast = parseQuery(parser, expression, 10000, 10000, 10000);
    auto syntax_result = TreeRewriter(context).analyze(ast, columns);
    ActionsDAG dag = ExpressionAnalyzer(ast, syntax_result, context).getActionsDAG(/*add_aliases=*/ false, /*remove_unused_result=*/ false);
    const auto * node = &dag.findInOutputs(ast->getColumnName());
    return estimator->estimateRelationProfile(nullptr, node).rows;
}

}

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

TEST(Statistics, TryConvertToFloat64)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();

    auto converted_int = StatisticsUtils::tryConvertToFloat64(Field(Int64(-42)), data_type);
    ASSERT_TRUE(converted_int.has_value());
    EXPECT_DOUBLE_EQ(*converted_int, -42.0);

    auto converted_string = StatisticsUtils::tryConvertToFloat64(Field(String("1.25")), data_type);
    ASSERT_TRUE(converted_string.has_value());
    EXPECT_DOUBLE_EQ(*converted_string, 1.25);

    const auto decimal_type = std::make_shared<DataTypeDecimal64>(18, 2);
    auto converted_decimal = StatisticsUtils::tryConvertToFloat64(
        Field(DecimalField<Decimal64>(Decimal64(12345), 2)), decimal_type);
    ASSERT_TRUE(converted_decimal.has_value());
    EXPECT_DOUBLE_EQ(*converted_decimal, 123.45);

    const auto ipv4_type = std::make_shared<DataTypeIPv4>();
    auto converted_ipv4 = StatisticsUtils::tryConvertToFloat64(Field(IPv4(0x7f000001)), ipv4_type);
    ASSERT_TRUE(converted_ipv4.has_value());
    EXPECT_DOUBLE_EQ(*converted_ipv4, 2130706433.0);

    EXPECT_FALSE(StatisticsUtils::tryConvertToFloat64(Field(String("1.25 trailing")), data_type).has_value());
    EXPECT_FALSE(StatisticsUtils::tryConvertToFloat64(Field(Array{}), data_type).has_value());
    EXPECT_FALSE(StatisticsUtils::tryConvertToFloat64(Field(Float64(1.0)), std::make_shared<DataTypeArray>(data_type)).has_value());
}

TEST(Statistics, Estimator)
{
    /// Register scalar functions used while interpreting estimator expressions so this
    /// test does not depend on earlier tests in the binary having registered them.
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
    NamesAndTypesList columns{{"a", data_type}, {"b", data_type}, {"c", data_type}};

    auto test_impl = [&](const String & expression, Int64 real_result, Float64 eps)
    {
        UInt64 estimated_rows = estimateRowsFor(estimator, expression, columns);
        std::cout << expression << " " << real_result << " " << estimated_rows << std::endl;
        EXPECT_LT(std::abs(real_result - static_cast<Int64>(estimated_rows)), 10000 * eps);
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
    test_f("a < '3'", 2); /// Quoted numeric literal reaches statistics as a String Field.
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

/// Build a `ColumnStatistics` carrying the requested types over `data_type`.
ColumnStatisticsPtr createTestStats(
    const std::vector<StatisticsType> & types,
    const DataTypePtr & data_type)
{
    ColumnStatisticsDescription desc;
    desc.data_type = data_type;
    for (auto type : types)
        desc.types_to_desc.emplace(type, SingleStatisticsDescription(type, nullptr, false));
    return MergeTreeStatisticsFactory::instance().get(desc);
}

/// Build a `Nullable(Int32)` column with `total` rows where every `null_every`-th row is NULL.
/// Non-NULL row `i` carries value `static_cast<Int32>(i)`. Returns built statistics.
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

}

TEST(Statistics, NullableEstimatorWithBasic)
{
    /// Two Nullable(Int32) columns with three-valued logic exercised across ranges and IS [NOT] NULL.
    ///
    /// column a: Nullable(Int32), 1000 rows, every 5th NULL  → 200 NULLs, 800 non-NULLs in [1, 999]
    /// column b: Nullable(Int32), 1000 rows, every 10th NULL → 100 NULLs, 900 non-NULLs in [1, 999]
    ///
    /// `basic` populates numeric min/max (1 .. 999) plus null_count, so:
    ///   estimateLess(500) = (500-1)/(999-1) * non_null = 0.5 * non_null
    ///   → 400 for column a, 450 for column b
    tryRegisterFunctions();

    auto stats_a = buildNullableInt32Stats({StatisticsType::Basic}, /*total=*/1000, /*null_every=*/5);
    auto stats_b = buildNullableInt32Stats({StatisticsType::Basic}, /*total=*/1000, /*null_every=*/10);
    ASSERT_EQ(stats_a->getNonNullRowCount(), 800u);
    ASSERT_EQ(stats_b->getNonNullRowCount(), 900u);

    ConditionSelectivityEstimatorBuilder builder(getContext().context);
    builder.addStatistics("a", stats_a);
    builder.addStatistics("b", stats_b);
    builder.incrementRowCount(1000);
    auto estimator = builder.getEstimator();

    DataTypePtr nullable_int32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    NamesAndTypesList columns{{"a", nullable_int32}, {"b", nullable_int32}};

    auto check = [&](const String & expression, Float64 expected, Float64 eps)
    {
        Float64 actual = static_cast<Float64>(estimateRowsFor(estimator, expression, columns));
        EXPECT_NEAR(actual, expected, eps) << "Expression: " << expression;
    };

    /// Single column — plain ranges (NULL rows are excluded).
    check("a > 500",       400.0, 1.0);
    check("a < 500",       400.0, 1.0);
    check("b > 500",       450.0, 1.0);
    check("b < 500",       450.0, 1.0);

    /// Single column — IS NULL / IS NOT NULL.
    check("a IS NULL",     200.0, 1e-6);
    check("a IS NOT NULL", 800.0, 1e-6);
    check("b IS NULL",     100.0, 1e-6);
    check("b IS NOT NULL", 900.0, 1e-6);

    /// Single column — IS NULL AND range → contradiction (the range is FALSE on NULL rows).
    check("a IS NULL AND a > 500", 0.0, 1e-6);
    check("b IS NULL AND b < 500", 0.0, 1e-6);

    /// Single column — IS NULL OR range → null rows ∪ matching range rows.
    check("a IS NULL OR a > 500", 600.0, 1.0);   /// 200 + 400
    check("b IS NULL OR b < 500", 550.0, 1.0);   /// 100 + 450

    /// Single column — IS NOT NULL AND range → equals the range (NULL filtering is implicit).
    check("a IS NOT NULL AND a > 500", 400.0, 1.0);
    check("b IS NOT NULL AND b < 500", 450.0, 1.0);

    /// Single column — IS NOT NULL OR range → IS NOT NULL dominates.
    check("a IS NOT NULL OR a > 500", 800.0, 1.0);
    check("b IS NOT NULL OR b < 500", 900.0, 1.0);

    /// Cross-column — range AND range, independent: 0.4 * 0.45 = 0.18.
    check("a > 500 AND b > 500", 180.0, 2.0);

    /// Cross-column — range OR range: 1 - (1-0.4)*(1-0.45) = 0.67.
    check("a > 500 OR b > 500", 670.0, 2.0);

    /// Cross-column — IS NULL AND range, independent columns: 0.2 * 0.45 = 0.09.
    check("a IS NULL AND b > 500", 90.0, 2.0);

    /// Cross-column — IS NULL OR range: 1 - P(a IS NOT NULL) * P(b <= 500) = 1 - 0.8 * 0.55 = 0.56.
    check("a IS NULL OR b > 500", 560.0, 2.0);

    /// Cross-column — IS NULL AND IS NULL: 0.2 * 0.1 = 0.02.
    check("a IS NULL AND b IS NULL", 20.0, 2.0);

    /// Cross-column — IS NULL OR IS NULL: 1 - 0.8 * 0.9 = 0.28.
    check("a IS NULL OR b IS NULL", 280.0, 2.0);

    /// Cross-column — IS NOT NULL AND IS NOT NULL: 0.8 * 0.9 = 0.72.
    check("a IS NOT NULL AND b IS NOT NULL", 720.0, 2.0);

    /// Cross-column — IS NOT NULL AND IS NULL (different columns): 0.8 * 0.1 = 0.08.
    check("a IS NOT NULL AND b IS NULL", 80.0, 2.0);

    /// Cross-column — range AND IS NULL (different columns): 0.4 * 0.1 = 0.04.
    check("a > 500 AND b IS NULL", 40.0, 2.0);

    /// `a IS NOT NULL AND a > 500` collapses to `a > 500` (P = 0.4); then AND `b IS NULL` (P = 0.1).
    check("a IS NOT NULL AND a > 500 AND b IS NULL", 40.0, 2.0);

    /// Contradictions spanning two columns.
    check("a > 500 AND b > 500 AND b IS NULL", 0.0, 1e-6);  /// b > 500 contradicts b IS NULL
    check("a IS NULL AND a > 500 AND b IS NULL", 0.0, 1e-6); /// a IS NULL contradicts a > 500
}

TEST(Statistics, LikeSelectivity)
{
    /// Build a simple estimator to test LIKE / NOT LIKE / ILIKE / NOT ILIKE
    /// selectivity defaults and their complement behavior under NOT.
    /// LIKE selectivity is a fixed factor of the total row count, but the builder yields an estimator
    /// only when it holds some statistics, so the String column gets `basic` ones.
    tryRegisterFunctions();

    DataTypePtr data_type = std::make_shared<DataTypeString>();

    MutableColumnPtr col = DataTypeString().createColumn();
    for (Int32 i = 0; i < 10000; i++)
        col->insert(toString(i + 1));

    ColumnStatisticsDescription mock_description;
    mock_description.data_type = data_type;
    mock_description.types_to_desc.emplace(StatisticsType::Basic, SingleStatisticsDescription(StatisticsType::Basic, nullptr, false));

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
    ASSERT_NE(estimator, nullptr);

    NamesAndTypesList columns{{"a", data_type}};

    /// Helper: estimate rows for a condition string.
    auto estimate = [&](const String & expression) -> UInt64
    {
        return estimateRowsFor(estimator, expression, columns);
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

/// STID 3524-3a4b (nullability) and STID 2404-35eb (value type): a statistics collector is declared
/// on one column type, then the block column reaching `build` has a different type (a pending MODIFY
/// COLUMN mutation, or an asymmetric merge where `structureEquals` only compares statistics types and
/// misses a type-only change). Feeding the mismatched column to the collector previously mis-cast
/// inside the aggregate function and aborted: `Bad cast ... ColumnNullable` for `uniq` on a Nullable
/// type (3524-3a4b), and `Bad cast ColumnDecimal<Decimal256> to ColumnVector<long>` for `uniq` whose
/// `<long>` (Int64) specialization was fed a Decimal256 block during mutation statistics rebuild
/// (2404-35eb). The central `ColumnsStatistics::build` / `buildIfExists` now detects the mismatch via
/// `column_type->equals(stats_data_type)` and throws a diagnostic LOGICAL_ERROR naming the column, the
/// expected type and the actual type, instead of silently adapting the column. The `equals` check
/// covers both the nullability dimension and the value-type dimension, and protects all statistics
/// types, not just `uniq`.
TEST(Statistics, BuildTypeMismatchThrows)
{
    tryRegisterAggregateFunctions();

    auto make_stats = [](const String & column_name, const DataTypePtr & declared_type)
    {
        ColumnStatisticsDescription desc;
        desc.data_type = declared_type;
        desc.types_to_desc.emplace(StatisticsType::Uniq, SingleStatisticsDescription(StatisticsType::Uniq, nullptr, false));
        ColumnsStatistics result;
        result.emplace(column_name, MergeTreeStatisticsFactory::instance().get(desc));
        return result;
    };

    auto int_block = [](const String & column_name)
    {
        MutableColumnPtr col = DataTypeInt32().createColumn();
        for (Int32 i = 0; i < 100; ++i)
            col->insert(i);
        return Block{ColumnWithTypeAndName(std::move(col), std::make_shared<DataTypeInt32>(), column_name)};
    };

    /// A Decimal256 block, to reproduce STID 2404-35eb: an `Int64`-declared `uniq` collector
    /// (`AggregateFunctionUniq<long>`, column type `ColumnVector<long>`) fed a `ColumnDecimal<Decimal256>`.
    auto decimal256_type = std::make_shared<DataTypeDecimal256>(20, 0);
    auto decimal256_block = [&](const String & column_name)
    {
        MutableColumnPtr col = decimal256_type->createColumn();
        for (Int32 i = 0; i < 100; ++i)
            col->insert(DecimalField<Decimal256>(Decimal256(static_cast<Int256>(i)), 0));
        return Block{ColumnWithTypeAndName(std::move(col), decimal256_type, column_name)};
    };

    /// In debug and sanitizer builds constructing a LOGICAL_ERROR aborts the process (it is treated
    /// as a failed assertion), so the throw cannot be caught here. Assert the throw only in release
    /// builds; the positive-path checks below run everywhere. This mirrors gtest_memory_resize.cpp.
#ifndef DEBUG_OR_SANITIZER_BUILD
    /// Statistics declared Nullable(Int32); block column is plain Int32 -> mismatch -> throws.
    {
        auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        auto stats = make_stats("a", nullable_type);
        try
        {
            stats.build(int_block("a"));
            FAIL() << "expected LOGICAL_ERROR on nullability mismatch";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
            EXPECT_NE(e.message().find("Type mismatch when building statistics for column 'a'"), std::string::npos);
        }
    }

    /// Same mismatch via `buildIfExists` (the mutation-rebuild entry point).
    {
        auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        auto stats = make_stats("a", nullable_type);
        try
        {
            stats.buildIfExists(int_block("a"));
            FAIL() << "expected LOGICAL_ERROR on nullability mismatch";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
            EXPECT_NE(e.message().find("Type mismatch when building statistics for column 'a'"), std::string::npos);
        }
    }

    /// STID 2404-35eb (value-type dimension): statistics declared Int64; block column is Decimal256.
    /// The `uniq` collector built for Int64 is `AggregateFunctionUniq<long>`, whose column type is
    /// `ColumnVector<long>`; feeding a `ColumnDecimal<Decimal256>` previously aborted with
    /// `Bad cast ... ColumnDecimal<Decimal256> to ColumnVector<long>` inside `addBatchSinglePlaceNotNull`.
    /// `equals` rejects the type difference, so the guard throws the diagnostic before the cast.
    {
        auto int64_type = std::make_shared<DataTypeInt64>();
        auto stats = make_stats("a", int64_type);
        try
        {
            stats.buildIfExists(decimal256_block("a"));
            FAIL() << "expected LOGICAL_ERROR on Int64/Decimal256 value-type mismatch";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
            EXPECT_NE(e.message().find("Type mismatch when building statistics for column 'a'"), std::string::npos);
        }
    }

    /// And via `build` (the merge / full-recalc entry point) for the same value-type mismatch.
    {
        auto int64_type = std::make_shared<DataTypeInt64>();
        auto stats = make_stats("a", int64_type);
        try
        {
            stats.build(decimal256_block("a"));
            FAIL() << "expected LOGICAL_ERROR on Int64/Decimal256 value-type mismatch";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
            EXPECT_NE(e.message().find("Type mismatch when building statistics for column 'a'"), std::string::npos);
        }
    }
#endif

    /// Matching Decimal256 type builds normally (positive path, runs in all build types):
    /// a `uniq` collector declared on Decimal256 is `AggregateFunctionUniq<Decimal256>` and its column
    /// type matches, so no cast error and the cardinality is computed.
    {
        auto stats = make_stats("a", decimal256_type);
        EXPECT_NO_THROW(stats.build(decimal256_block("a")));
        EXPECT_EQ(stats.at("a")->estimateCardinality(), 100u);
    }

    /// Matching type still builds normally (100 distinct values).
    {
        auto plain_type = std::make_shared<DataTypeInt32>();
        auto stats = make_stats("a", plain_type);
        EXPECT_NO_THROW(stats.build(int_block("a")));
        EXPECT_EQ(stats.at("a")->estimateCardinality(), 100u);
    }

    /// `buildIfExists` ignores columns absent from the block (no throw).
    {
        auto plain_type = std::make_shared<DataTypeInt32>();
        auto stats = make_stats("missing", plain_type);
        EXPECT_NO_THROW(stats.buildIfExists(int_block("a")));
    }
}

/// The build-time guard above only fires when statistics are rebuilt from a block. The merge path in
/// MergeTask takes a different route: when `ColumnStatistics::structureEquals` returns true it merges an
/// already-loaded part statistic into the result collector instead of rebuilding it, so the mismatched
/// loaded statistic never reaches the build guard. `structureEquals` must therefore also reject a
/// different declared type, so a nullability-only change forces a rebuild rather than merging
/// incompatible aggregate-state layouts.
TEST(Statistics, StructureEqualsConsidersDataType)
{
    tryRegisterAggregateFunctions();

    auto make_stat = [](const DataTypePtr & declared_type)
    {
        ColumnStatisticsDescription desc;
        desc.data_type = declared_type;
        desc.types_to_desc.emplace(StatisticsType::Uniq, SingleStatisticsDescription(StatisticsType::Uniq, nullptr, false));
        return MergeTreeStatisticsFactory::instance().get(desc);
    };

    auto plain_type = std::make_shared<DataTypeInt32>();
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    /// Same kinds and same declared type -> equal structure (the common case keeps merging, no rebuild).
    EXPECT_TRUE(make_stat(plain_type)->structureEquals(*make_stat(plain_type)));
    EXPECT_TRUE(make_stat(nullable_type)->structureEquals(*make_stat(nullable_type)));

    /// Same kinds but different declared type (nullability flip) -> not equal, both directions.
    EXPECT_FALSE(make_stat(plain_type)->structureEquals(*make_stat(nullable_type)));
    EXPECT_FALSE(make_stat(nullable_type)->structureEquals(*make_stat(plain_type)));

    /// Custom-named types must be told apart by name, not by equals(): Bool is stored as UInt8 and shares
    /// its typeid, so Bool->equals(UInt8) is true even though the serialized statistics layouts differ.
    /// Comparing getName() keeps Bool and UInt8 distinct so a Bool<->UInt8 change forces a rebuild.
    auto bool_type = DataTypeFactory::instance().get("Bool");
    auto uint8_type = std::make_shared<DataTypeUInt8>();
    EXPECT_TRUE(make_stat(bool_type)->structureEquals(*make_stat(bool_type)));
    EXPECT_FALSE(make_stat(bool_type)->structureEquals(*make_stat(uint8_type)));
    EXPECT_FALSE(make_stat(uint8_type)->structureEquals(*make_stat(bool_type)));
}

TEST(Statistics, BasicDefaultCountNonNullable)
{
    auto data_type = std::make_shared<DataTypeInt32>();
    MutableColumnPtr col = data_type->createColumn();
    /// 10 rows, 4 of them equal to the default (0).
    const Int64 values[10] = {0, 1, 0, 2, 0, 3, 0, 4, 5, 6};
    for (Int64 v : values)
        col->insert(Field(v));
    auto stats = createTestStats({StatisticsType::Basic}, data_type);
    stats->build(std::move(col));

    /// A non-Nullable column has no NULL count.
    EXPECT_FALSE(stats->hasNullCount());
    EXPECT_EQ(stats->getNullCount(), 0u);
    EXPECT_EQ(stats->estimateDefaults(), 4u);

    /// Exact equality-to-default estimate.
    auto eq0 = stats->estimateEqual(Field(Int64(0)));
    ASSERT_TRUE(eq0.has_value());
    EXPECT_DOUBLE_EQ(*eq0, 4.0);

    /// A non-default value has no exact answer from `basic` alone.
    EXPECT_FALSE(stats->estimateEqual(Field(Int64(3))).has_value());
}

TEST(Statistics, BasicDefaultCountFixedString)
{
    auto data_type = std::make_shared<DataTypeFixedString>(4);
    MutableColumnPtr col = data_type->createColumn();
    /// 6 rows: 3 all-zero (default), 3 non-zero.
    col->insertDefault();                           /// "\0\0\0\0"
    col->insertDefault();                           /// "\0\0\0\0"
    col->insertDefault();                           /// "\0\0\0\0"
    col->insert(Field(String("abc\0", 4)));         /// non-default
    col->insert(Field(String("xyz\0", 4)));         /// non-default
    col->insert(Field(String("hi\0\0", 4)));        /// non-default
    auto stats = createTestStats({StatisticsType::Basic}, data_type);
    stats->build(std::move(col));

    EXPECT_EQ(stats->estimateDefaults(), 3u);

    /// col = '' should match the 3 zero rows ('' gets padded to N zero bytes by tryConvertFieldToType).
    auto eq_empty = stats->estimateEqual(Field(String("")));
    ASSERT_TRUE(eq_empty.has_value());
    EXPECT_DOUBLE_EQ(*eq_empty, 3.0);

    /// A non-default value should fall through.
    EXPECT_FALSE(stats->estimateEqual(Field(String("abc\0", 4))).has_value());
}

TEST(Statistics, BasicDefaultCountEnum)
{
    DataTypeEnum8::Values values_zero_default{{"a", 0}, {"b", 1}};
    auto enum_zero = std::make_shared<DataTypeEnum8>(values_zero_default);

    /// 5 rows: 3 'a' (raw 0, the column default) and 2 'b' (raw 1).
    /// Insert via raw integer values — ColumnVector<Int8> does not accept String fields directly.
    {
        MutableColumnPtr col = enum_zero->createColumn();
        col->insert(Field(Int64(0)));  /// 'a'
        col->insert(Field(Int64(1)));  /// 'b'
        col->insert(Field(Int64(0)));  /// 'a'
        col->insert(Field(Int64(0)));  /// 'a'
        col->insert(Field(Int64(1)));  /// 'b'
        auto stats = createTestStats({StatisticsType::Basic}, enum_zero);
        stats->build(std::move(col));

        EXPECT_EQ(stats->estimateDefaults(), 3u);  /// 3 rows with raw value 0 ('a')

        /// col = 'a' — 'a' maps to raw 0, which is the column default → exact count.
        auto eq_a = stats->estimateEqual(Field(String("a")));
        ASSERT_TRUE(eq_a.has_value());
        EXPECT_DOUBLE_EQ(*eq_a, 3.0);

        /// col = 'b' — 'b' maps to raw 1, not the column default → fall through.
        EXPECT_FALSE(stats->estimateEqual(Field(String("b"))).has_value());
    }

    /// Enum where the first enumerator has a non-zero raw value: raw 0 is not a valid enumerator,
    /// so default_count = 0 and estimateEqual must return nullopt for any enumerator (not 0).
    DataTypeEnum8::Values values_nonzero_default{{"a", 1}, {"b", 2}};
    auto enum_nonzero = std::make_shared<DataTypeEnum8>(values_nonzero_default);
    {
        MutableColumnPtr col = enum_nonzero->createColumn();
        for (int i = 0; i < 100; ++i)
            col->insert(Field(Int64(1)));  /// 100 rows of 'a' (raw 1)
        auto stats = createTestStats({StatisticsType::Basic}, enum_nonzero);
        stats->build(std::move(col));

        EXPECT_EQ(stats->estimateDefaults(), 0u);  /// no rows with raw value 0

        /// col = 'a' — 'a' is the first enumerator but maps to raw 1, not the column default.
        /// Must return nullopt, not 0 (which would suppress `col = 'a'` predicates entirely).
        EXPECT_FALSE(stats->estimateEqual(Field(String("a"))).has_value());
    }
}

TEST(Statistics, BasicDefaultCountNullableIsNullCount)
{
    /// 100 rows, every 5th NULL -> 20 NULLs.
    auto stats = buildNullableInt32Stats({StatisticsType::Basic}, /*total=*/100, /*null_every=*/5);
    EXPECT_TRUE(stats->hasNullCount());
    EXPECT_EQ(stats->getNullCount(), 20u);
    EXPECT_EQ(stats->estimateDefaults(), 20u);
    EXPECT_FALSE(stats->estimateEqual(Field(Int64(0))).has_value());
}

TEST(Statistics, BasicDefaultCountRoundTrip)
{
    auto data_type = std::make_shared<DataTypeInt32>();
    MutableColumnPtr col = data_type->createColumn();
    for (Int64 i = 0; i < 8; ++i)
        col->insert(Field(i % 2 == 0 ? Int64(0) : i)); /// 4 zeros out of 8
    auto stats = createTestStats({StatisticsType::Basic}, data_type);
    stats->build(std::move(col));
    ASSERT_EQ(stats->estimateDefaults(), 4u);

    WriteBufferFromOwnString wb;
    stats->serialize(wb);
    ReadBufferFromString rb(wb.str());
    auto restored = ColumnStatistics::deserialize(rb, data_type);

    EXPECT_EQ(restored->estimateDefaults(), 4u);
    auto eq0 = restored->estimateEqual(Field(Int64(0)));
    ASSERT_TRUE(eq0.has_value());
    EXPECT_DOUBLE_EQ(*eq0, 4.0);

    /// `basic` statistics written by 26.6 and 26.7 store the default-value count only for `Nullable` columns, as
    /// the NULL count (bit 2 of the feature mask, then called `NullCount`). For other columns it is absent, so it
    /// is unknown, and it must stay unknown when the loaded statistics are written again, as a mutation does for
    /// the columns it does not change. Check each layout these versions wrote.
    constexpr UInt8 numeric_min_max = 1u << 0;
    constexpr UInt8 string_length_sum = 1u << 1;
    constexpr UInt8 null_count = 1u << 2;

    auto load_legacy_and_rewrite = [](const DataTypePtr & type, UInt8 feature_mask, std::function<void(WriteBuffer &)> write_features)
    {
        /// `StatisticsBasic::serialize` of 26.7.
        String payload;
        {
            WriteBufferFromString buf(payload);
            writeIntBinary(static_cast<UInt64>(1000), buf); /// row_count
            writeIntBinary(feature_mask, buf);
            write_features(buf);
            buf.finalize();
        }

        /// `ColumnStatistics::serialize` of 26.7 (`V4`) with the single `Basic` statistic.
        String file;
        {
            WriteBufferFromString buf(file);
            writeIntBinary(static_cast<UInt16>(4), buf); /// StatisticsFileVersion::V4
            writeIntBinary(static_cast<UInt64>(1ULL << static_cast<UInt8>(StatisticsType::Basic)), buf);
            writeStringBinary(type->getName(), buf);
            writeIntBinary(static_cast<UInt64>(1000), buf); /// rows
            writeIntBinary(static_cast<UInt64>(payload.size()), buf);
            buf.write(payload.data(), payload.size());
            buf.finalize();
        }

        ReadBufferFromString file_rb(file);
        auto loaded = ColumnStatistics::deserialize(file_rb, type);
        WriteBufferFromOwnString rewritten_wb;
        loaded->serialize(rewritten_wb);
        ReadBufferFromString rewritten_rb(rewritten_wb.str());
        return ColumnStatistics::deserialize(rewritten_rb, type);
    };

    /// A numeric column: min and max.
    {
        auto rewritten = load_legacy_and_rewrite(data_type, numeric_min_max, [](WriteBuffer & buf)
        {
            writeFieldBinary(Field(Int64(0)), buf);
            writeFieldBinary(Field(Int64(999)), buf);
        });
        auto estimate = rewritten->getEstimate();
        EXPECT_FALSE(estimate.estimated_default_count.has_value());
        /// Without the count there is no estimate for `x = 0`, rather than an estimate of zero rows.
        EXPECT_FALSE(rewritten->estimateEqual(Field(Int64(0))).has_value());
        ASSERT_TRUE(estimate.estimated_min.has_value());
        ASSERT_TRUE(estimate.estimated_max.has_value());
        EXPECT_EQ(*estimate.estimated_min, Field(Int64(0)));
        EXPECT_EQ(*estimate.estimated_max, Field(Int64(999)));
    }

    /// A `String` column: the total byte length of the values.
    {
        auto string_type = DataTypeFactory::instance().get("String");
        auto rewritten = load_legacy_and_rewrite(string_type, string_length_sum, [](WriteBuffer & buf)
        {
            writeIntBinary(static_cast<UInt64>(2890), buf);
        });
        EXPECT_FALSE(rewritten->getEstimate().estimated_default_count.has_value());
        EXPECT_FALSE(rewritten->estimateEqual(Field(String(""))).has_value());
        const auto & basic = assert_cast<const StatisticsBasic &>(*rewritten->getStats().at(StatisticsType::Basic));
        EXPECT_EQ(basic.getStringTotalBytes(), 2890u);
    }

    /// A `Nullable` column: min, max and the NULL count, which is the default-value count of a `Nullable` column.
    {
        auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        auto rewritten = load_legacy_and_rewrite(nullable_type, numeric_min_max | null_count, [](WriteBuffer & buf)
        {
            writeFieldBinary(Field(Int64(1)), buf);
            writeFieldBinary(Field(Int64(999)), buf);
            writeIntBinary(static_cast<UInt64>(250), buf);
        });
        EXPECT_TRUE(rewritten->hasNullCount());
        EXPECT_EQ(rewritten->getNullCount(), 250u);
        EXPECT_EQ(rewritten->estimateDefaults(), 250u);
    }
}

/// The selectivity estimator accumulates the statistics of the parts into a copy made by `cloneEmpty`. The copy
/// must keep the structure of `basic` statistics deserialized from an older format without a default-value count:
/// otherwise it gets a fabricated exact count of zero defaults, and the following parts with the same old statistics
/// are discarded as incompatible.
TEST(Statistics, BasicWithoutDefaultCountCloneEmpty)
{
    tryRegisterFunctions();

    auto data_type = std::make_shared<DataTypeInt32>();
    constexpr UInt8 numeric_min_max = 1u << 0;

    /// `basic` statistics of 1000 rows with the given min and max, as written by 26.7 (`V4`) for a non-`Nullable` column.
    auto load_legacy = [&](Int64 min, Int64 max)
    {
        String payload;
        {
            WriteBufferFromString buf(payload);
            writeIntBinary(static_cast<UInt64>(1000), buf); /// row_count
            writeIntBinary(numeric_min_max, buf);
            writeFieldBinary(Field(min), buf);
            writeFieldBinary(Field(max), buf);
            buf.finalize();
        }

        String file;
        {
            WriteBufferFromString buf(file);
            writeIntBinary(static_cast<UInt16>(4), buf); /// StatisticsFileVersion::V4
            writeIntBinary(static_cast<UInt64>(1ULL << static_cast<UInt8>(StatisticsType::Basic)), buf);
            writeStringBinary(data_type->getName(), buf);
            writeIntBinary(static_cast<UInt64>(1000), buf); /// rows
            writeIntBinary(static_cast<UInt64>(payload.size()), buf);
            buf.write(payload.data(), payload.size());
            buf.finalize();
        }

        ReadBufferFromString rb(file);
        return ColumnStatistics::deserialize(rb, data_type);
    };

    auto part1 = load_legacy(0, 999);
    auto part2 = load_legacy(1000, 1999);

    auto accumulated = part1->cloneEmpty();
    EXPECT_TRUE(accumulated->structureEquals(*part1));
    accumulated->merge(part1);
    EXPECT_FALSE(accumulated->getEstimate().estimated_default_count.has_value());
    EXPECT_FALSE(accumulated->estimateEqual(Field(Int64(0))).has_value());

    EXPECT_TRUE(accumulated->structureEquals(*part2));
    accumulated->merge(part2);
    auto estimate = accumulated->getEstimate();
    ASSERT_TRUE(estimate.estimated_min.has_value());
    ASSERT_TRUE(estimate.estimated_max.has_value());
    EXPECT_EQ(*estimate.estimated_min, Field(Int64(0)));
    EXPECT_EQ(*estimate.estimated_max, Field(Int64(1999)));

    /// The same through the estimator: `x = 0` is not estimated as matching no rows, and both parts are used.
    ConditionSelectivityEstimatorBuilder builder(getContext().context);
    builder.addStatistics("x", part1);
    builder.incrementRowCount(1000);
    builder.addStatistics("x", part2);
    builder.incrementRowCount(1000);
    auto estimator = builder.getEstimator();
    NamesAndTypesList columns{{"x", data_type}};
    EXPECT_GT(estimateRowsFor(estimator, "x = 0", columns), 0u);
    EXPECT_NEAR(static_cast<double>(estimateRowsFor(estimator, "x < 1500", columns)), 1500.0, 20.0);
}

TEST(Statistics, BasicDefaultCountArray)
{
    auto data_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
    EXPECT_TRUE(basicStatisticsValidator(SingleStatisticsDescription(StatisticsType::Basic, nullptr, false), data_type));

    MutableColumnPtr col = data_type->createColumn();
    col->insert(Field(Array{}));                                     /// empty (default)
    col->insert(Field(Array{Field(UInt64(1))}));                     /// non-empty
    col->insert(Field(Array{}));                                     /// empty (default)
    col->insert(Field(Array{Field(UInt64(2)), Field(UInt64(3))}));   /// non-empty
    auto stats = createTestStats({StatisticsType::Basic}, data_type);
    stats->build(std::move(col));

    EXPECT_EQ(stats->estimateDefaults(), 2u);
    auto eq_empty = stats->estimateEqual(Field(Array{}));
    ASSERT_TRUE(eq_empty.has_value());
    EXPECT_DOUBLE_EQ(*eq_empty, 2.0);
}


/// Statistics files with version `V3` were produced by builds of `master` between PR #102356 (which
/// added a `NullCount` statistic) and its revert. They must stay readable: a part written by such a
/// build is otherwise unqueryable, and on a readonly disk it cannot be rewritten by
/// `ALTER TABLE ... MATERIALIZE STATISTICS`. The layout is `V4` without `stored_type_name`, and bit 4
/// of the type mask -- the reverted `NullCount` -- must be skipped rather than parsed as `Basic`.
TEST(Statistics, DeserializeV3SkipsRevertedNullCount)
{
    auto data_type = std::make_shared<DataTypeInt32>();

    auto lengthPrefixed = [](WriteBuffer & out, const String & stat_payload)
    {
        writeIntBinary(static_cast<UInt64>(stat_payload.size()), out);
        out.write(stat_payload.data(), stat_payload.size());
    };

    String minmax_payload;
    {
        WriteBufferFromString buf(minmax_payload);
        writeIntBinary(static_cast<UInt64>(100), buf); /// row_count
        writeStringBinary(data_type->getName(), buf);
        writeFieldBinary(Field(Int64(-5)), buf);
        writeFieldBinary(Field(Int64(42)), buf);
        buf.finalize();
    }

    /// `StatisticsNullCount::serialize` wrote a single `UInt64`.
    String null_count_payload;
    {
        WriteBufferFromString buf(null_count_payload);
        writeIntBinary(static_cast<UInt64>(7), buf);
        buf.finalize();
    }

    String file;
    {
        WriteBufferFromString buf(file);
        writeIntBinary(static_cast<UInt16>(3), buf); /// StatisticsFileVersion::V3
        /// bit 3 = `MinMax`, bit 4 = the reverted `NullCount`, which is today's `Basic` slot
        writeIntBinary(static_cast<UInt64>((1ULL << 3) | (1ULL << 4)), buf);
        writeIntBinary(static_cast<UInt64>(100), buf); /// rows
        lengthPrefixed(buf, minmax_payload);
        lengthPrefixed(buf, null_count_payload);
        buf.finalize();
    }

    ReadBufferFromString rb(file);
    auto restored = ColumnStatistics::deserialize(rb, data_type);

    ASSERT_TRUE(restored != nullptr);
    EXPECT_EQ(restored->getNumRows(), 100u);

    /// `MinMax` was read...
    ASSERT_TRUE(restored->hasMinMax());
    auto estimate = restored->getEstimate();
    ASSERT_TRUE(estimate.estimated_min.has_value());
    ASSERT_TRUE(estimate.estimated_max.has_value());
    EXPECT_EQ(*estimate.estimated_min, Field(Int64(-5)));
    EXPECT_EQ(*estimate.estimated_max, Field(Int64(42)));

    /// ...and the reverted `NullCount` payload was skipped, not misread as a `Basic` payload.
    EXPECT_FALSE(restored->getStats().contains(StatisticsType::Basic));
    EXPECT_FALSE(restored->hasNullCount());
}

namespace
{

/// The statistics declared for a column, implicit ones are marked with `(auto)`, e.g. `basic(auto),uniq_v2`.
String declaredStatistics(const ColumnsDescription & columns, const String & column_name)
{
    return columns.get(column_name).statistics.getNameForLogs();
}

/// Columns of every type the Float rule distinguishes, plus a Float64 column with an explicit `uniq_v2`.
ColumnsDescription createColumnsForImplicitStatistics()
{
    const std::vector<std::pair<String, String>> name_and_types{
        {"f32", "Float32"},
        {"f64", "Float64"},
        {"bf16", "BFloat16"},
        {"nf64", "Nullable(Float64)"},
        {"lcf64", "LowCardinality(Float64)"},
        {"u64", "UInt64"},
        {"s", "String"},
    };

    ColumnsDescription columns;
    for (const auto & [name, type] : name_and_types)
        columns.add(ColumnDescription(name, DataTypeFactory::instance().get(type)));

    ColumnDescription explicit_uniq("f64_explicit", DataTypeFactory::instance().get("Float64"));
    explicit_uniq.statistics.data_type = explicit_uniq.type;
    explicit_uniq.statistics.types_to_desc.emplace(
        StatisticsType::UniqV2,
        SingleStatisticsDescription(StatisticsType::UniqV2, make_intrusive<ASTIdentifier>("uniq_v2"), /*is_implicit_=*/false));
    columns.add(std::move(explicit_uniq));

    return columns;
}

/// Builds statistics of `types` over a column of `type_name` with `values`, converted to the column type
/// (a NULL Field inserts NULL).
ColumnStatisticsPtr buildStats(const std::vector<StatisticsType> & types, const String & type_name, const std::vector<Field> & values)
{
    auto data_type = DataTypeFactory::instance().get(type_name);
    MutableColumnPtr column = data_type->createColumn();
    for (const auto & value : values)
        column->insert(convertFieldToType(value, *data_type));
    auto stats = createTestStats(types, data_type);
    stats->build(std::move(column));
    return stats;
}

/// `count` distinct non-zero values `1, 2, ..., count`.
std::vector<Field> distinctFloats(size_t count)
{
    std::vector<Field> values;
    for (size_t i = 0; i < count; ++i)
        values.emplace_back(static_cast<Float64>(i + 1));
    return values;
}

/// The assumed cardinality derived from the estimate of `stats`, as `system.parts_columns` shows it.
std::optional<UInt64> assumedCardinalityFromEstimate(const ColumnStatistics & stats)
{
    return stats.getEstimate().estimateAssumedDistinctCardinality(stats.getDataType());
}

/// The estimate (kept by every part) derives the same assumed cardinality as the statistics it was taken from.
void expectEstimateAgrees(const ColumnStatistics & stats)
{
    EXPECT_EQ(assumedCardinalityFromEstimate(stats), stats.estimateAssumedDistinctCardinality()) << stats.getNameForLogs();
}

}

TEST(Statistics, CanAssumeAllDistinctByType)
{
    auto can_assume = [](const String & type_name) { return canAssumeAllDistinctByType(DataTypeFactory::instance().get(type_name)); };

    EXPECT_TRUE(can_assume("Float32"));
    EXPECT_TRUE(can_assume("Float64"));
    EXPECT_TRUE(can_assume("Nullable(Float32)"));
    EXPECT_TRUE(can_assume("Nullable(Float64)"));

    EXPECT_FALSE(can_assume("BFloat16"));
    EXPECT_FALSE(can_assume("Nullable(BFloat16)"));
    EXPECT_FALSE(can_assume("LowCardinality(Float64)"));
    EXPECT_FALSE(can_assume("LowCardinality(Nullable(Float64))"));
    EXPECT_FALSE(can_assume("UInt64"));
    EXPECT_FALSE(can_assume("Decimal64(4)"));
    EXPECT_FALSE(can_assume("String"));
    EXPECT_FALSE(can_assume("Array(Float64)"));
    EXPECT_FALSE(canAssumeAllDistinctByType(nullptr));
}

TEST(Statistics, AddImplicitStatisticsAssumeFloatsDistinct)
{
    /// `basic, uniq_v2`: the implicit `uniq_v2` is left out for Float32 / Float64 (also Nullable) only.
    {
        auto columns = createColumnsForImplicitStatistics();
        addImplicitStatistics(columns, "basic, uniq_v2", /*assume_floats_distinct=*/true);

        EXPECT_EQ(declaredStatistics(columns, "f32"), "basic(auto)");
        EXPECT_EQ(declaredStatistics(columns, "f64"), "basic(auto)");
        EXPECT_EQ(declaredStatistics(columns, "nf64"), "basic(auto)");
        EXPECT_EQ(declaredStatistics(columns, "bf16"), "basic(auto),uniq_v2(auto)");
        EXPECT_EQ(declaredStatistics(columns, "lcf64"), "basic(auto),uniq_v2(auto)");
        EXPECT_EQ(declaredStatistics(columns, "u64"), "basic(auto),uniq_v2(auto)");
        EXPECT_EQ(declaredStatistics(columns, "s"), "basic(auto),uniq_v2(auto)");
        /// The explicit statistic is untouched.
        EXPECT_EQ(declaredStatistics(columns, "f64_explicit"), "basic(auto),uniq_v2");
    }

    /// `uniq`: same for the older sketch. Float columns are left without implicit statistics.
    {
        auto columns = createColumnsForImplicitStatistics();
        addImplicitStatistics(columns, "uniq", /*assume_floats_distinct=*/true);

        EXPECT_EQ(declaredStatistics(columns, "f32"), "");
        EXPECT_EQ(declaredStatistics(columns, "f64"), "");
        EXPECT_EQ(declaredStatistics(columns, "nf64"), "");
        EXPECT_EQ(declaredStatistics(columns, "bf16"), "Uniq(auto)");
        EXPECT_EQ(declaredStatistics(columns, "lcf64"), "Uniq(auto)");
        EXPECT_EQ(declaredStatistics(columns, "u64"), "Uniq(auto)");
        EXPECT_EQ(declaredStatistics(columns, "s"), "Uniq(auto)");
        EXPECT_EQ(declaredStatistics(columns, "f64_explicit"), "uniq_v2");
    }

    /// The setting is off: nothing is left out.
    for (const String & types : {"basic, uniq_v2", "uniq"})
    {
        auto columns = createColumnsForImplicitStatistics();
        addImplicitStatistics(columns, types, /*assume_floats_distinct=*/false);

        const String expected = types == "uniq" ? "Uniq(auto)" : "basic(auto),uniq_v2(auto)";
        for (const String & column_name : {"f32", "f64", "nf64", "bf16", "lcf64", "u64", "s"})
            EXPECT_EQ(declaredStatistics(columns, column_name), expected) << column_name;
        EXPECT_EQ(declaredStatistics(columns, "f64_explicit"), types == "uniq" ? "Uniq(auto),uniq_v2" : "basic(auto),uniq_v2");
    }

    /// Statistics other than uniq are not affected.
    {
        auto columns = createColumnsForImplicitStatistics();
        addImplicitStatistics(columns, "basic, tdigest, uniq", /*assume_floats_distinct=*/true);
        EXPECT_EQ(declaredStatistics(columns, "f64"), "TDigest(auto),basic(auto)");
        EXPECT_EQ(declaredStatistics(columns, "u64"), "TDigest(auto),Uniq(auto),basic(auto)");
    }
}

TEST(Statistics, ShouldAssumeAllDistinct)
{
    auto columns = createColumnsForImplicitStatistics();
    addImplicitStatistics(columns, "basic, uniq_v2", /*assume_floats_distinct=*/true);

    EXPECT_TRUE(shouldAssumeAllDistinct(columns.get("f32"), true));
    EXPECT_TRUE(shouldAssumeAllDistinct(columns.get("f64"), true));
    EXPECT_TRUE(shouldAssumeAllDistinct(columns.get("nf64"), true));

    /// The setting is off.
    EXPECT_FALSE(shouldAssumeAllDistinct(columns.get("f64"), false));
    /// The type doesn't qualify.
    EXPECT_FALSE(shouldAssumeAllDistinct(columns.get("bf16"), true));
    EXPECT_FALSE(shouldAssumeAllDistinct(columns.get("lcf64"), true));
    EXPECT_FALSE(shouldAssumeAllDistinct(columns.get("u64"), true));
    EXPECT_FALSE(shouldAssumeAllDistinct(columns.get("s"), true));
    /// A uniq is declared, e.g. explicit and not materialized yet.
    EXPECT_FALSE(shouldAssumeAllDistinct(columns.get("f64_explicit"), true));

    /// With the setting off, the implicit uniq is declared, so the rule doesn't apply whatever the setting
    /// the estimator is given.
    auto columns_setting_off = createColumnsForImplicitStatistics();
    addImplicitStatistics(columns_setting_off, "basic, uniq_v2", /*assume_floats_distinct=*/false);
    EXPECT_FALSE(shouldAssumeAllDistinct(columns_setting_off.get("f64"), true));

    /// No uniq is configured at all: the rule applies.
    auto columns_without_uniq = createColumnsForImplicitStatistics();
    addImplicitStatistics(columns_without_uniq, "basic", /*assume_floats_distinct=*/true);
    EXPECT_TRUE(shouldAssumeAllDistinct(columns_without_uniq.get("f64"), true));
}

TEST(Statistics, AssumedDistinctCardinality)
{
    using Source = ColumnStatistics::CardinalitySource;
    const Float64 placeholder_ratio = 0.1; /// `ConditionSelectivityEstimator::default_cardinality_ratio`

    /// 1000 distinct non-zero values.
    {
        auto stats = buildStats({StatisticsType::Basic}, "Float64", distinctFloats(1000));

        /// Without the flag, nothing is assumed: the placeholder.
        EXPECT_EQ(stats->getCardinalitySource(), Source::Unknown);
        EXPECT_FALSE(stats->tryEstimateCardinality().has_value());
        EXPECT_EQ(stats->estimateCardinality(), static_cast<UInt64>(1000 * placeholder_ratio));
        EXPECT_FALSE(stats->estimateEqual(Field(Float64(10))).has_value());
        /// The assumed value itself only depends on the type.
        EXPECT_EQ(stats->estimateAssumedDistinctCardinality(), 1000u);

        stats->setAssumeAllDistinct(true);
        EXPECT_EQ(stats->getCardinalitySource(), Source::AssumedDistinct);
        EXPECT_EQ(stats->tryEstimateCardinality(), 1000u);
        EXPECT_EQ(stats->estimateCardinality(), 1000u);
        /// An assumption is not a measurement.
        EXPECT_FALSE(stats->hasCardinality());

        /// The uniform equality estimate: every value has one row.
        auto equal = stats->estimateEqual(Field(Float64(10)));
        ASSERT_TRUE(equal.has_value());
        EXPECT_NEAR(*equal, 1.0, 1e-9);
        /// `basic` still answers for the default value.
        EXPECT_EQ(stats->estimateEqual(Field(Float64(0))), 0.0);

        auto estimate = stats->getEstimate();
        EXPECT_FALSE(estimate.estimated_cardinality.has_value());
        EXPECT_EQ(estimate.estimateAssumedDistinctCardinality(stats->getDataType()), 1000u);
        expectEstimateAgrees(*stats);
    }

    /// Nullable with NULLs: the non-NULL rows.
    {
        std::vector<Field> values;
        for (size_t i = 0; i < 1000; ++i)
            values.push_back(i % 5 == 0 ? Field() : Field(static_cast<Float64>(i)));
        auto stats = buildStats({StatisticsType::Basic}, "Nullable(Float64)", values);
        stats->setAssumeAllDistinct(true);
        EXPECT_EQ(stats->getCardinalitySource(), Source::AssumedDistinct);
        EXPECT_EQ(stats->estimateCardinality(), 800u);
        EXPECT_EQ(assumedCardinalityFromEstimate(*stats), 800u);
        expectEstimateAgrees(*stats);

        auto equal = stats->estimateEqual(Field(Float64(7)));
        ASSERT_TRUE(equal.has_value());
        EXPECT_NEAR(*equal, 1.0, 1e-9);
    }

    /// Nullable(Float32) with zeros: for a Nullable column the default is NULL, so zeros are not capped.
    {
        std::vector<Field> values;
        for (size_t i = 0; i < 100; ++i)
            values.push_back(i < 60 ? Field(Float64(0)) : Field(static_cast<Float64>(i)));
        auto stats = buildStats({StatisticsType::Basic}, "Nullable(Float32)", values);
        stats->setAssumeAllDistinct(true);
        EXPECT_EQ(stats->estimateCardinality(), 100u);
        expectEstimateAgrees(*stats);
    }

    /// Zeros in a non-Nullable column count as one value.
    {
        std::vector<Field> values(600, Field(Float64(0)));
        for (const auto & value : distinctFloats(400))
            values.push_back(value);
        auto stats = buildStats({StatisticsType::Basic}, "Float32", values);
        stats->setAssumeAllDistinct(true);
        EXPECT_EQ(stats->estimateCardinality(), 401u);
        expectEstimateAgrees(*stats);
        EXPECT_EQ(stats->estimateEqual(Field(Float64(0))), 600.0);

        auto only_zeros = buildStats({StatisticsType::Basic}, "Float64", std::vector<Field>(50, Field(Float64(0))));
        only_zeros->setAssumeAllDistinct(true);
        EXPECT_EQ(only_zeros->estimateCardinality(), 1u);
        expectEstimateAgrees(*only_zeros);
    }

    /// `min == max`: one value.
    {
        auto stats = buildStats({StatisticsType::Basic}, "Float64", std::vector<Field>(100, Field(Float64(7.5))));
        stats->setAssumeAllDistinct(true);
        EXPECT_EQ(stats->estimateCardinality(), 1u);
        expectEstimateAgrees(*stats);

        std::vector<Field> values(100, Field(Float64(7.5)));
        values.insert(values.end(), 100, Field());
        auto nullable_stats = buildStats({StatisticsType::Basic}, "Nullable(Float64)", values);
        nullable_stats->setAssumeAllDistinct(true);
        EXPECT_EQ(nullable_stats->estimateCardinality(), 1u);
        expectEstimateAgrees(*nullable_stats);
    }

    /// NaN: `min == max` says nothing.
    {
        auto stats = buildStats({StatisticsType::Basic}, "Float64", std::vector<Field>(100, Field(std::numeric_limits<Float64>::quiet_NaN())));
        stats->setAssumeAllDistinct(true);
        EXPECT_EQ(stats->estimateCardinality(), 100u);
        expectEstimateAgrees(*stats);
    }

    /// No rows.
    {
        auto stats = buildStats({StatisticsType::Basic}, "Float64", {});
        stats->setAssumeAllDistinct(true);
        EXPECT_EQ(stats->estimateCardinality(), 0u);
        EXPECT_EQ(assumedCardinalityFromEstimate(*stats), 0u);
        expectEstimateAgrees(*stats);
    }

    /// Without `basic` there is no cap and no NULL count: the total rows.
    {
        tryRegisterAggregateFunctions();
        std::vector<Field> values(100, Field(Float64(1)));
        values.insert(values.end(), 100, Field());
        auto stats = buildStats({StatisticsType::TDigest}, "Nullable(Float64)", values);
        stats->setAssumeAllDistinct(true);
        EXPECT_EQ(stats->getCardinalitySource(), Source::AssumedDistinct);
        EXPECT_EQ(stats->estimateCardinality(), 200u);
        expectEstimateAgrees(*stats);
    }

    /// The deprecated `minmax` without `basic`: its minimum and maximum end up in the estimate, but only
    /// those of `basic` cap the assumption.
    {
        auto stats = buildStats({StatisticsType::MinMax}, "Float64", std::vector<Field>(100, Field(Float64(7.5))));
        stats->setAssumeAllDistinct(true);
        EXPECT_EQ(stats->estimateCardinality(), 100u);
        EXPECT_EQ(stats->getEstimate().estimated_min, Field(Float64(7.5)));
        EXPECT_EQ(assumedCardinalityFromEstimate(*stats), 100u);
        expectEstimateAgrees(*stats);
    }
}

TEST(Statistics, AssumedDistinctCardinalityUnchangedCases)
{
    tryRegisterAggregateFunctions();
    using Source = ColumnStatistics::CardinalitySource;

    /// A sketch wins over the assumption.
    {
        std::vector<Field> values;
        for (size_t i = 0; i < 1000; ++i)
            values.emplace_back(static_cast<Float64>(i % 10 + 1));

        for (auto uniq_type : {StatisticsType::Uniq, StatisticsType::UniqV2})
        {
            auto stats = buildStats({StatisticsType::Basic, uniq_type}, "Float64", values);
            const UInt64 sketch_cardinality = stats->estimateCardinality();
            EXPECT_NEAR(static_cast<Float64>(sketch_cardinality), 10.0, 1.0);

            stats->setAssumeAllDistinct(true);
            EXPECT_EQ(stats->getCardinalitySource(), Source::Sketch);
            EXPECT_TRUE(stats->hasCardinality());
            EXPECT_EQ(stats->estimateCardinality(), sketch_cardinality);
            EXPECT_FALSE(stats->estimateAssumedDistinctCardinality().has_value());

            auto estimate = stats->getEstimate();
            EXPECT_EQ(estimate.estimated_cardinality, sketch_cardinality);
            EXPECT_FALSE(estimate.estimateAssumedDistinctCardinality(stats->getDataType()).has_value());
        }
    }

    /// The type doesn't qualify: the placeholder, even with the flag set.
    for (const String & type_name : {"LowCardinality(Float64)", "BFloat16", "UInt64"})
    {
        auto stats = buildStats({StatisticsType::Basic}, type_name, distinctFloats(100));
        stats->setAssumeAllDistinct(true);
        EXPECT_EQ(stats->getCardinalitySource(), Source::Unknown) << type_name;
        EXPECT_FALSE(stats->estimateAssumedDistinctCardinality().has_value()) << type_name;
        EXPECT_EQ(stats->estimateCardinality(), 10u) << type_name;
        EXPECT_FALSE(stats->hasCardinality()) << type_name;
        EXPECT_FALSE(assumedCardinalityFromEstimate(*stats).has_value()) << type_name;
    }
}

TEST(Statistics, EstimatorCardinalityPolicy)
{
    tryRegisterFunctions();
    using Source = ColumnStatistics::CardinalitySource;

    auto columns = createColumnsForImplicitStatistics();
    addImplicitStatistics(columns, "basic, uniq_v2", /*assume_floats_distinct=*/true);
    auto metadata = std::make_shared<StorageInMemoryMetadata>();
    metadata->setColumns(columns);

    /// Every column gets `basic` only, as if no part had a sketch (e.g. `f64_explicit` is not materialized yet).
    auto build_estimator = [&](StorageMetadataPtr policy_metadata, bool assume_floats_distinct, bool set_policy)
    {
        ConditionSelectivityEstimatorBuilder builder(getContext().context);
        for (const String & column_name : {"f64", "nf64", "f64_explicit", "missing"})
        {
            const String type_name = column_name == "nf64" ? "Nullable(Float64)" : "Float64";
            builder.addStatistics(column_name, buildStats({StatisticsType::Basic}, type_name, distinctFloats(1000)));
        }
        builder.incrementRowCount(1000);
        if (set_policy)
            builder.setCardinalityPolicy(policy_metadata, assume_floats_distinct);
        return builder.getEstimator();
    };

    /// The number of distinct values the estimator reports to the planner, e.g. for join reordering.
    auto ndv_of = [](const ConditionSelectivityEstimatorPtr & estimator, const String & column_name) -> UInt64
    {
        const auto profile = estimator->estimateRelationProfile();
        auto it = profile.column_stats.find(column_name);
        return it == profile.column_stats.end() ? 0 : it->second.num_distinct_values;
    };

    {
        auto estimator = build_estimator(metadata, /*assume_floats_distinct=*/true, /*set_policy=*/true);
        /// The policy applies to the Float columns without a declared uniq.
        EXPECT_EQ(ndv_of(estimator, "f64"), 1000u);
        EXPECT_EQ(ndv_of(estimator, "nf64"), 1000u);
        /// A uniq is declared but not materialized: the placeholder.
        EXPECT_EQ(ndv_of(estimator, "f64_explicit"), 100u);
        /// Not in the metadata: nothing is assumed.
        EXPECT_EQ(ndv_of(estimator, "missing"), 100u);

        /// The equality estimate uses the assumed cardinality.
        NamesAndTypesList names_and_types{{"f64", std::make_shared<DataTypeFloat64>()}, {"f64_explicit", std::make_shared<DataTypeFloat64>()}};
        EXPECT_EQ(estimateRowsFor(estimator, "f64 = 10", names_and_types), 1u);
        /// `default_cond_equal_factor` (0.01) of the rows without an estimate.
        EXPECT_EQ(estimateRowsFor(estimator, "f64_explicit = 10", names_and_types), 10u);
    }

    /// The setting is off, or there is no policy at all: nothing is assumed.
    for (auto [assume_floats_distinct, set_policy] : {std::pair{false, true}, std::pair{true, false}})
    {
        auto estimator = build_estimator(metadata, assume_floats_distinct, set_policy);
        for (const String & column_name : {"f64", "nf64", "f64_explicit", "missing"})
            EXPECT_EQ(ndv_of(estimator, column_name), 100u) << column_name;
    }

    /// No metadata: nothing is assumed.
    {
        auto estimator = build_estimator(nullptr, /*assume_floats_distinct=*/true, /*set_policy=*/true);
        EXPECT_EQ(ndv_of(estimator, "f64"), 100u);
    }

    /// The metadata declares the implicit `uniq_v2` (the setting was off when it was derived).
    {
        auto columns_with_uniq = createColumnsForImplicitStatistics();
        addImplicitStatistics(columns_with_uniq, "basic, uniq_v2", /*assume_floats_distinct=*/false);
        auto metadata_with_uniq = std::make_shared<StorageInMemoryMetadata>();
        metadata_with_uniq->setColumns(columns_with_uniq);

        auto estimator = build_estimator(metadata_with_uniq, /*assume_floats_distinct=*/true, /*set_policy=*/true);
        EXPECT_EQ(ndv_of(estimator, "f64"), 100u);
    }

    /// The statistics passed in may come from the statistics cache: the flag is set only on the estimator's own copy.
    {
        ConditionSelectivityEstimatorBuilder builder(getContext().context);
        auto stats = buildStats({StatisticsType::Basic}, "Float64", distinctFloats(10));
        builder.addStatistics("f64", stats);
        builder.setCardinalityPolicy(metadata, /*assume_floats_distinct=*/true);
        auto estimator = builder.getEstimator();
        ASSERT_TRUE(estimator != nullptr);
        EXPECT_EQ(ndv_of(estimator, "f64"), 10u);
        EXPECT_EQ(stats->getCardinalitySource(), Source::Unknown);
    }
}

#if USE_DATASKETCHES

TEST(Statistics, CountMinSketchRoundTrip)
{
    auto data_type = std::make_shared<DataTypeUInt64>();

    auto round_trip = [&](const ColumnStatisticsPtr & stats)
    {
        WriteBufferFromOwnString wb;
        stats->serialize(wb);
        ReadBufferFromString rb(wb.str());
        auto restored = ColumnStatistics::deserialize(rb, data_type);
        EXPECT_TRUE(rb.eof());
        return restored;
    };

    auto estimate_equal = [](const ColumnStatisticsPtr & stats, UInt64 value)
    {
        auto estimate = stats->getStats().at(StatisticsType::CountMinSketch)->estimateEqual(Field(value));
        EXPECT_TRUE(estimate.has_value());
        return estimate.value_or(-1.0);
    };

    /// An empty sketch.
    {
        auto restored = round_trip(createTestStats({StatisticsType::CountMinSketch}, data_type));
        ASSERT_TRUE(restored != nullptr);
        EXPECT_DOUBLE_EQ(estimate_equal(restored, 5), 0.0);
    }

    /// 1000 rows with the values 0 .. 9, 100 rows each.
    {
        MutableColumnPtr col = data_type->createColumn();
        for (UInt64 i = 0; i < 1000; ++i)
            col->insert(i % 10);
        auto stats = createTestStats({StatisticsType::CountMinSketch}, data_type);
        stats->build(std::move(col));

        auto restored = round_trip(stats);
        ASSERT_TRUE(restored != nullptr);
        EXPECT_EQ(restored->getNumRows(), 1000u);
        for (UInt64 value : {0, 5, 9, 10})
            EXPECT_DOUBLE_EQ(estimate_equal(restored, value), estimate_equal(stats, value)) << "value " << value;
        EXPECT_GE(estimate_equal(restored, 5), 100.0);
    }
}

/// A malformed sketch makes `datasketches` throw `std::invalid_argument` or `std::out_of_range`, or parse into a sketch
/// that is read out of bounds later. Each case must be rejected with `ILLEGAL_STATISTICS`: a `std::logic_error`
/// escaping from `deserialize` lost the part and column context and aborted debug and sanitizer builds.
TEST(Statistics, CountMinSketchDeserializeCorrupted)
{
    auto data_type = std::make_shared<DataTypeUInt64>();

    /// `StatisticsCountMinSketch` itself, without the `ColumnStatistics` wrapper.
    auto create = [&]
    {
        auto stats = MergeTreeStatisticsFactory::instance().tryCreateSingle(StatisticsType::CountMinSketch, data_type);
        EXPECT_TRUE(stats != nullptr);
        return stats;
    };

    /// `StatisticsCountMinSketch::serialize` writes the `UInt64` size of the sketch, then the sketch.
    auto serialize = [&](const ColumnPtr & column)
    {
        auto stats = create();
        stats->build(column);
        WriteBufferFromOwnString wb;
        stats->serialize(wb);
        ReadBufferFromString rb(wb.str());
        UInt64 size = 0;
        readIntBinary(size, rb);
        String bytes = wb.str().substr(sizeof(UInt64));
        EXPECT_EQ(size, bytes.size());
        return bytes;
    };

    MutableColumnPtr col = data_type->createColumn();
    for (UInt64 i = 0; i < 1000; ++i)
        col->insert(i % 10);
    const String sketch_bytes = serialize(std::move(col));
    const String empty_sketch_bytes = serialize(data_type->createColumn());

    /// The sizes of 7 hashes x 2718 buckets: two preamble longs, then the total weight and the counters.
    ASSERT_EQ(empty_sketch_bytes.size(), 16u);
    ASSERT_EQ(sketch_bytes.size(), 16u + 8 * (1 + 7 * 2718));

    auto with_size = [](UInt64 size, const String & bytes)
    {
        WriteBufferFromOwnString wb;
        writeIntBinary(size, wb);
        wb.write(bytes.data(), bytes.size());
        return wb.str();
    };

    /// In the second preamble long: [8, 12) the number of buckets, 12 the number of hashes, [13, 15) the seed hash.
    auto with_dimensions = [&](UInt32 buckets, UInt8 hashes)
    {
        String bytes = sketch_bytes;
        memcpy(bytes.data() + 8, &buckets, sizeof(buckets));
        bytes[12] = static_cast<char>(hashes);
        return with_size(bytes.size(), bytes);
    };

    auto expect_illegal_statistics = [&](const String & case_name, const String & payload)
    {
        SCOPED_TRACE(case_name);
        auto stats = create();
        const size_t allocated_bytes = stats->allocatedBytes();
        ReadBufferFromString rb(payload);
        try
        {
            stats->deserialize(rb, StatisticsFileVersion::V4);
            ADD_FAILURE() << "expected ILLEGAL_STATISTICS";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::ILLEGAL_STATISTICS) << e.message();
            EXPECT_NE(e.message().find("MATERIALIZE STATISTICS"), std::string::npos) << e.message();
        }
        catch (const std::exception & e)
        {
            ADD_FAILURE() << "expected DB::Exception, got std::exception: " << e.what();
        }
        /// The rejected sketch did not replace the existing one.
        EXPECT_EQ(stats->allocatedBytes(), allocated_bytes);
    };

    /// The valid payloads are accepted.
    for (const String & bytes : {sketch_bytes, empty_sketch_bytes})
    {
        auto stats = create();
        const String payload = with_size(bytes.size(), bytes);
        ReadBufferFromString rb(payload);
        EXPECT_NO_THROW(stats->deserialize(rb, StatisticsFileVersion::V4));
        EXPECT_TRUE(rb.eof());
    }

    {
        String bytes = sketch_bytes;
        bytes[13] = static_cast<char>(bytes[13] ^ 0xFF);
        expect_illegal_statistics("seed hash", with_size(bytes.size(), bytes));
    }

    /// A valid sketch of other dimensions.
    expect_illegal_statistics("1 hash x 3 buckets", with_dimensions(3, 1));

    /// The number of counters overflows 32 bits, so `datasketches` 5.2.0 parses a sketch without counters.
    expect_illegal_statistics("2 hashes x 2^31 buckets", with_dimensions(1u << 31, 2));

    /// Truncated payloads with a consistent size. `datasketches` 5.2.0 does not detect a sketch that is
    /// at most 16 bytes too short.
    expect_illegal_statistics("truncated by 8 bytes", with_size(sketch_bytes.size() - 8, sketch_bytes.substr(0, sketch_bytes.size() - 8)));
    expect_illegal_statistics("truncated to the preamble", with_size(16, sketch_bytes.substr(0, 16)));

    /// A size that cannot be allocated (#91206).
    expect_illegal_statistics("size UINT64_MAX", with_size(std::numeric_limits<UInt64>::max(), sketch_bytes));
}

#endif
