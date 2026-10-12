#include <gtest/gtest.h>

#include <barrier>
#include <cstring>
#include <functional>
#include <map>
#include <set>
#include <thread>

#include <config.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Field.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsBasic.h>
#include <Storages/Statistics/StatisticsHistogram.h>
#include <Storages/Statistics/StatisticsMinMax.h>
#include <Storages/StatisticsDescription.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Statistics/StatisticsTDigest.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
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

/// Estimate the row count for a SQL boolean expression evaluated against `estimator`.
template <class Estimator>
Float64 estimateRowsFor(Estimator & estimator, const String & expression)
{
    ParserExpressionWithOptionalAlias exp_parser(false);
    ContextPtr context = getContext().context;
    RPNBuilderTreeContext tree_context(
        context,
        Block{{DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy"}},
        {});
    ASTPtr ast = parseQuery(exp_parser, expression, 10000, 10000, 10000);
    RPNBuilderTreeNode node(ast.get(), tree_context);
    return static_cast<Float64>(estimator->estimateRelationProfile(nullptr, node).rows);
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

    auto check = [&](const String & expression, Float64 expected, Float64 eps)
    {
        Float64 actual = estimateRowsFor(estimator, expression);
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


#if USE_DATASKETCHES

namespace DB::ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
}

namespace
{

ASTPtr makeHistogramAST(UInt64 buckets, UInt64 mcv_values)
{
    return makeASTFunction("histogram", make_intrusive<ASTLiteral>(buckets), make_intrusive<ASTLiteral>(mcv_values));
}

SingleStatisticsDescription makeHistogramDescription(UInt64 buckets, UInt64 mcv_values)
{
    return SingleStatisticsDescription(StatisticsType::Histogram, makeHistogramAST(buckets, mcv_values), false);
}

ColumnStatisticsPtr createHistogramStats(const DataTypePtr & data_type, UInt64 buckets, UInt64 mcv_values = 64, UInt64 random_seed = 0)
{
    ColumnStatisticsDescription desc;
    desc.data_type = data_type;
    desc.types_to_desc.emplace(StatisticsType::Histogram, makeHistogramDescription(buckets, mcv_values));
    return MergeTreeStatisticsFactory::instance().get(desc, random_seed);
}

String serializeStatistics(const ColumnStatisticsPtr & stats)
{
    WriteBufferFromOwnString buf;
    stats->serialize(buf);
    return buf.str();
}

ColumnStatisticsPtr roundTrip(const ColumnStatisticsPtr & stats, const DataTypePtr & data_type)
{
    const String serialized = serializeStatistics(stats);
    ReadBufferFromString rb(serialized);
    auto restored = ColumnStatistics::deserialize(rb, data_type);
    if (!restored)
        throw std::logic_error("statistics were not restored");
    return restored;
}

const StatisticsHistogram & getHistogram(const ColumnStatisticsPtr & stats)
{
    return assert_cast<const StatisticsHistogram &>(*stats->getStats().at(StatisticsType::Histogram));
}

ColumnPtr makeFloat64Column(const std::vector<Float64> & values, size_t begin = 0, size_t end = std::numeric_limits<size_t>::max())
{
    end = std::min(end, values.size());
    auto column = ColumnFloat64::create();
    for (size_t i = begin; i < end; ++i)
        column->insertValue(values[i]);
    return column;
}

/// Deterministic, platform-independent pseudo-random numbers (splitmix64).
class TestRandom
{
public:
    explicit TestRandom(UInt64 seed) : state(seed) {}

    UInt64 next()
    {
        state += 0x9e3779b97f4a7c15ULL;
        UInt64 value = state;
        value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
        value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
        return value ^ (value >> 31);
    }

    /// Uniform in [0, bound).
    UInt64 below(UInt64 bound) { return next() % bound; }
    /// Uniform in [0, 1).
    Float64 unit() { return static_cast<Float64>(next() >> 11) * 0x1.0p-53; }

private:
    UInt64 state;
};

/// Zipf-distributed ranks in [0, ranks) with exponent `s`, mapped to values spread over the domain.
std::vector<Float64> generateZipf(size_t rows, UInt64 ranks, Float64 s, UInt64 seed)
{
    std::vector<Float64> cumulative(ranks);
    Float64 total = 0;
    for (UInt64 rank = 0; rank < ranks; ++rank)
    {
        total += 1.0 / std::pow(static_cast<Float64>(rank + 1), s);
        cumulative[rank] = total;
    }

    TestRandom random(seed);
    std::vector<Float64> result;
    result.reserve(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        const Float64 target = random.unit() * total;
        const auto rank = static_cast<UInt64>(std::upper_bound(cumulative.begin(), cumulative.end(), target) - cumulative.begin());
        /// Frequent values are spread over the domain instead of being the smallest ones.
        result.push_back(static_cast<Float64>((std::min(rank, ranks - 1) * 7919) % 100003));
    }
    return result;
}

enum class StreamKind
{
    Uniform,
    Zipf,
    Mixed,
};

std::vector<Float64> generateStream(StreamKind kind, size_t rows, UInt64 seed)
{
    TestRandom random(seed);
    switch (kind)
    {
        case StreamKind::Uniform: {
            std::vector<Float64> result;
            for (size_t i = 0; i < rows; ++i)
                result.push_back(static_cast<Float64>(random.below(3000)));
            return result;
        }
        case StreamKind::Zipf:
            return generateZipf(rows, 5000, 1.1, seed);
        case StreamKind::Mixed: {
            auto result = generateZipf(rows, 300, 1.3, seed);
            for (auto & value : result)
            {
                const UInt64 kind_of_row = random.below(100);
                if (kind_of_row < 40)
                    value = random.unit() * 1000.0 - 500.0;
                else if (kind_of_row == 40)
                    value = std::numeric_limits<Float64>::quiet_NaN();
                else if (kind_of_row == 41)
                    value = std::numeric_limits<Float64>::infinity();
                else if (kind_of_row == 42)
                    value = -std::numeric_limits<Float64>::infinity();
                else if (kind_of_row == 43)
                    value = -0.0;
            }
            return result;
        }
    }
    return {};
}

/// Builds statistics over `values` in blocks of random sizes.
ColumnStatisticsPtr buildInRandomBlocks(
    const std::vector<Float64> & values, UInt64 buckets, UInt64 mcv_values, UInt64 random_seed, TestRandom & block_random)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    auto stats = createHistogramStats(data_type, buckets, mcv_values, random_seed);
    size_t begin = 0;
    while (begin < values.size())
    {
        const size_t size = 1 + block_random.below(block_random.below(2) ? 7 : 5000);
        stats->build(makeFloat64Column(values, begin, begin + size));
        begin += size;
    }
    return stats;
}

/// Exact counts of a stream with canonical zero, the ground truth for estimates.
struct BruteForce
{
    std::map<Float64, UInt64> finite;
    UInt64 nan = 0;
    UInt64 negative_inf = 0;
    UInt64 positive_inf = 0;

    explicit BruteForce(const std::vector<Float64> & values)
    {
        for (Float64 value : values)
        {
            if (std::isnan(value))
                ++nan;
            else if (value == -std::numeric_limits<Float64>::infinity())
                ++negative_inf;
            else if (value == std::numeric_limits<Float64>::infinity())
                ++positive_inf;
            else
                ++finite[value == 0 ? 0.0 : value];
        }
    }

    UInt64 count(Float64 value) const
    {
        auto it = finite.find(value == 0 ? 0.0 : value);
        return it == finite.end() ? 0 : it->second;
    }

    UInt64 less(Float64 value, bool inclusive) const
    {
        UInt64 result = negative_inf;
        for (const auto & [v, c] : finite)
            if (v < value || (inclusive && v == value))
                result += c;
        return result;
    }

    UInt64 finiteRows() const
    {
        UInt64 result = 0;
        for (const auto & [_, c] : finite)
            result += c;
        return result;
    }
};

std::vector<Float64> makeProbes(const BruteForce & truth)
{
    std::vector<Float64> probes;
    Float64 previous = -1e9;
    size_t index = 0;
    for (const auto & [value, _] : truth.finite)
    {
        /// Every distinct value is probed for small streams, a sample for larger ones.
        if (truth.finite.size() <= 400 || index++ % 17 == 0)
        {
            probes.push_back(value);
            probes.push_back((previous + value) / 2);
        }
        previous = value;
    }
    probes.push_back(previous + 1);
    probes.push_back(-1e9 - 1);
    std::sort(probes.begin(), probes.end());
    return probes;
}

/// Which kinds of checks `checkHistogram` could apply.
struct CheckCoverage
{
    size_t checks = 0;
    /// The summary evicted rows, and the residual sketch was exact, so equality estimates were compared exactly.
    size_t exact_equality_with_evictions = 0;
    /// The residual sketch was in estimation mode.
    size_t estimation_mode = 0;
    /// Nothing was evicted, so all estimates were compared exactly.
    size_t exact_ranges = 0;
};

/// Checks the partition invariants of the MCV / residual split and compares estimates with exact counts.
void checkHistogram(const ColumnStatisticsPtr & stats, const std::vector<Float64> & values, CheckCoverage * coverage = nullptr)
{
    const auto & histogram = getHistogram(stats);
    const BruteForce truth(values);

    const UInt64 finite_rows = histogram.getFiniteCount();
    ASSERT_EQ(finite_rows, truth.finiteRows());
    ASSERT_EQ(histogram.getNonNullCount(), values.size());

    const auto frequent_values = histogram.getFrequentValues();
    const UInt64 delta = histogram.getMaximumError();
    const UInt64 residual_rows = histogram.getResidualCount();
    UInt64 retained = 0;
    for (const auto & item : frequent_values)
        retained += item.count;

    /// F = sum c(m) + kll.n, kll.n >= delta, delta = 0 <=> kll.n = 0.
    ASSERT_EQ(finite_rows, retained + residual_rows);
    ASSERT_GE(residual_rows, delta);
    ASSERT_EQ(delta == 0, residual_rows == 0);
    ASSERT_LE(frequent_values.size(), HistogramFrequentValues(histogram.getMcvValues()).getCapacity());

    /// Misra-Gries guarantees: c(m) <= true count <= c(m) + delta, untracked values occur at most delta times.
    std::set<Float64> tracked;
    for (const auto & item : frequent_values)
    {
        tracked.insert(item.value);
        const UInt64 true_count = truth.count(item.value);
        ASSERT_LE(item.count, true_count);
        ASSERT_LE(true_count, item.count + delta);
        const auto estimate = histogram.estimateEqual(Field(item.value));
        ASSERT_TRUE(estimate.has_value());
        ASSERT_GE(*estimate, static_cast<Float64>(item.count));
        ASSERT_LE(*estimate, static_cast<Float64>(item.count + delta));
    }
    for (const auto & [value, count] : truth.finite)
        if (!tracked.contains(value))
            ASSERT_LE(count, delta);

    const Float64 comparable = static_cast<Float64>(values.size() - truth.nan);
    const bool exact_ranges = residual_rows == 0;
    const bool exact_equality = histogram.isResidualExact();
    if (coverage)
    {
        ++coverage->checks;
        coverage->exact_equality_with_evictions += exact_equality && delta > 0;
        coverage->estimation_mode += !exact_equality;
        coverage->exact_ranges += exact_ranges;
    }
    Float64 previous_less = 0;
    for (Float64 probe : makeProbes(truth))
    {
        SCOPED_TRACE(fmt::format("probe {}", probe));
        const auto equal = histogram.estimateEqual(Field(probe));
        const auto less = histogram.estimateLess(Field(probe));
        const auto less_or_equal = histogram.estimateLessOrEqual(Field(probe));
        const auto greater = histogram.estimateGreater(Field(probe));
        const auto greater_or_equal = histogram.estimateGreaterOrEqual(Field(probe));
        ASSERT_TRUE(less && less_or_equal && greater && greater_or_equal);

        ASSERT_GE(*less, previous_less - 1e-6);
        ASSERT_LE(*less, *less_or_equal + 1e-6);
        ASSERT_NEAR(*less + *greater_or_equal, comparable, 1e-6 * comparable + 1e-6);
        ASSERT_NEAR(*less_or_equal + *greater, comparable, 1e-6 * comparable + 1e-6);
        previous_less = *less;

        if (exact_equality || exact_ranges)
        {
            ASSERT_TRUE(equal.has_value());
            ASSERT_DOUBLE_EQ(*equal, static_cast<Float64>(truth.count(probe)));
        }
        if (equal && !tracked.contains(probe))
            ASSERT_LE(*equal, static_cast<Float64>(delta));
        if (exact_ranges)
        {
            ASSERT_DOUBLE_EQ(*less, static_cast<Float64>(truth.less(probe, false)));
            ASSERT_DOUBLE_EQ(*less_or_equal, static_cast<Float64>(truth.less(probe, true)));
        }
    }
}

}

TEST(Statistics, HistogramParametersAndTypes)
{
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto & factory = MergeTreeStatisticsFactory::instance();

    auto make_description = [&](ASTPtr ast, const DataTypePtr & data_type)
    {
        ColumnStatisticsDescription desc;
        desc.data_type = data_type;
        desc.types_to_desc.emplace(
            StatisticsType::Histogram, SingleStatisticsDescription(StatisticsType::Histogram, std::move(ast), false));
        return desc;
    };
    auto literal = [](auto value) { return make_intrusive<ASTLiteral>(value); };

    EXPECT_NO_THROW(factory.validate(make_description(makeHistogramAST(2, 1), int_type), int_type));
    EXPECT_NO_THROW(factory.validate(make_description(makeHistogramAST(1024, 768), int_type), int_type));
    EXPECT_NO_THROW(factory.validate(make_description(makeHistogramAST(8, 64), int_type), int_type));

    /// Exactly two parameters are required.
    EXPECT_THROW(factory.validate(make_description(makeASTFunction("histogram"), int_type), int_type), Exception);
    EXPECT_THROW(factory.validate(make_description(makeASTFunction("histogram", literal(UInt64(8))), int_type), int_type), Exception);
    EXPECT_THROW(
        factory.validate(
            make_description(makeASTFunction("histogram", literal(UInt64(8)), literal(UInt64(16)), literal(UInt64(4))), int_type),
            int_type),
        Exception);

    /// Bucket count range.
    EXPECT_THROW(factory.validate(make_description(makeHistogramAST(1, 64), int_type), int_type), Exception);
    EXPECT_THROW(factory.validate(make_description(makeHistogramAST(1025, 64), int_type), int_type), Exception);
    /// Number of most common values range.
    EXPECT_THROW(factory.validate(make_description(makeHistogramAST(8, 0), int_type), int_type), Exception);
    EXPECT_THROW(factory.validate(make_description(makeHistogramAST(8, 769), int_type), int_type), Exception);

    /// Only unsigned integer literals.
    EXPECT_THROW(
        factory.validate(make_description(makeASTFunction("histogram", literal(Float64(8.0)), literal(UInt64(16))), int_type), int_type),
        Exception);
    EXPECT_THROW(
        factory.validate(make_description(makeASTFunction("histogram", literal(UInt64(8)), literal(Int64(-16))), int_type), int_type),
        Exception);
    EXPECT_THROW(
        factory.validate(make_description(makeASTFunction("histogram", literal(UInt64(8)), literal(String("16"))), int_type), int_type),
        Exception);

    const auto string_type = std::make_shared<DataTypeString>();
    EXPECT_FALSE(histogramStatisticsValidator(makeHistogramDescription(8, 64), string_type));
    EXPECT_THROW(factory.validate(make_description(makeHistogramAST(8, 64), string_type), string_type), Exception);

    /// Internal placeholders yield the defaults, which the payload then overwrites.
    const SingleStatisticsDescription placeholder(StatisticsType::Histogram, make_intrusive<ASTIdentifier>("histogram"), false);
    const auto defaults = StatisticsHistogram::getParametersFromDescription(placeholder, false);
    EXPECT_EQ(defaults.bucket_count, StatisticsHistogram::DEFAULT_BUCKETS_FOR_DESERIALIZATION);
    EXPECT_EQ(defaults.mcv_values, StatisticsHistogram::DEFAULT_MCV_VALUES_FOR_DESERIALIZATION);
    EXPECT_THROW(StatisticsHistogram::getParametersFromDescription(placeholder, true), Exception);
    const SingleStatisticsDescription null_placeholder(StatisticsType::Histogram, nullptr, false);
    EXPECT_EQ(StatisticsHistogram::getParametersFromDescription(null_placeholder, false), defaults);

    const auto eight = makeHistogramDescription(8, 64);
    EXPECT_EQ(eight, makeHistogramDescription(8, 64));
    EXPECT_FALSE(eight == makeHistogramDescription(16, 64));
    EXPECT_FALSE(eight == makeHistogramDescription(8, 16));
    EXPECT_EQ(eight.getTypeName(), "histogram(8, 64)");
    const auto parameters = StatisticsHistogram::getParametersFromDescription(makeHistogramDescription(8, 16), true);
    EXPECT_EQ(parameters.bucket_count, 8u);
    EXPECT_EQ(parameters.mcv_values, 16u);

    const auto ipv4_type = std::make_shared<DataTypeIPv4>();
    MutableColumnPtr ipv4_column = ipv4_type->createColumn();
    ipv4_column->insert(Field(IPv4(1)));
    ipv4_column->insert(Field(IPv4(2)));
    ipv4_column->insert(Field(IPv4(3)));
    auto ipv4_stats = createHistogramStats(ipv4_type, 8);
    ipv4_stats->build(std::move(ipv4_column));
    auto ipv4_less = ipv4_stats->estimateLess(Field(IPv4(3)));
    ASSERT_TRUE(ipv4_less.has_value());
    EXPECT_DOUBLE_EQ(*ipv4_less, 2.0);
    auto ipv4_equal = ipv4_stats->estimateEqual(Field(IPv4(2)));
    ASSERT_TRUE(ipv4_equal.has_value());
    EXPECT_DOUBLE_EQ(*ipv4_equal, 1.0);
}

TEST(Statistics, HistogramMCVCapacityRounding)
{
    const std::vector<std::pair<UInt64, UInt64>> expected
        = {{1, 6}, {6, 6}, {7, 12}, {12, 12}, {13, 24}, {25, 48}, {49, 96}, {97, 192}, {193, 384}, {384, 384}, {385, 768}, {768, 768}};
    for (const auto & [requested, capacity] : expected)
        EXPECT_EQ(HistogramFrequentValues(requested).getCapacity(), capacity) << requested;
}

TEST(Statistics, HistogramRejectsCorruptPayload)
{
    const auto data_type = std::make_shared<DataTypeInt32>();
    StatisticsHistogram histogram(makeHistogramDescription(8, 64), data_type);

    /// k = 100 is below the resolution of any histogram, k is at least 200.
    datasketches::kll_sketch<Float64> kll(100);
    kll.update(0.0);

    String payload;
    {
        WriteBufferFromString buf(payload);
        writeBinary(UInt8(3), buf); /// payload version
        writeVarUInt(UInt64(8), buf); /// buckets
        writeVarUInt(UInt64(64), buf); /// most common values
        writeVarUInt(UInt64(2), buf); /// non-null rows
        writeVarUInt(UInt64(0), buf); /// NaN rows
        writeVarUInt(UInt64(0), buf); /// -Inf rows
        writeVarUInt(UInt64(0), buf); /// +Inf rows
        writeBinary(UInt8(1), buf); /// has finite bounds
        writeBinary(Float64(0), buf); /// finite min
        writeBinary(Float64(1), buf); /// finite max
        writeBinary(UInt64(0), buf); /// random state
        writeVarUInt(UInt64(1), buf); /// maximum error
        writeVarUInt(UInt64(1), buf); /// frequent values
        writeBinary(Float64(1), buf);
        writeVarUInt(UInt64(1), buf);
        const auto bytes = kll.serialize();
        writeVarUInt(bytes.size(), buf);
        buf.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
        buf.finalize();
    }

    ReadBufferFromString rb(payload);
    EXPECT_THROW(histogram.deserialize(rb, StatisticsFileVersion::V4), Exception);
}

TEST(Statistics, HistogramRejectsOversizedRowCount)
{
    const auto data_type = std::make_shared<DataTypeInt32>();
    StatisticsHistogram histogram(makeHistogramDescription(8, 64), data_type);

    String payload;
    {
        WriteBufferFromString buf(payload);
        writeBinary(UInt8(3), buf); /// payload version
        writeVarUInt(std::numeric_limits<UInt64>::max(), buf); /// buckets: rejected before KLL sizing
        buf.finalize();
    }

    ReadBufferFromString invalid_buckets(payload);
    EXPECT_THROW(histogram.deserialize(invalid_buckets, StatisticsFileVersion::V4), Exception);

    payload.clear();
    {
        WriteBufferFromString buf(payload);
        writeBinary(UInt8(3), buf); /// payload version
        writeVarUInt(UInt64(8), buf); /// buckets
        writeVarUInt(std::numeric_limits<UInt64>::max(), buf); /// most common values: rejected before sizing the summary
        buf.finalize();
    }

    ReadBufferFromString invalid_mcv_values(payload);
    EXPECT_THROW(histogram.deserialize(invalid_mcv_values, StatisticsFileVersion::V4), Exception);

    payload.clear();
    {
        WriteBufferFromString buf(payload);
        writeBinary(UInt8(3), buf); /// payload version
        writeVarUInt(UInt64(8), buf); /// buckets
        writeVarUInt(UInt64(64), buf); /// most common values
        writeVarUInt(std::numeric_limits<UInt64>::max(), buf); /// non-null rows
        writeVarUInt(UInt64(0), buf); /// NaN rows
        writeVarUInt(UInt64(0), buf); /// -Inf rows
        writeVarUInt(UInt64(0), buf); /// +Inf rows
        writeBinary(UInt8(1), buf); /// has finite bounds
        writeBinary(Float64(0), buf); /// finite min
        writeBinary(Float64(1), buf); /// finite max
        buf.finalize();
    }

    ReadBufferFromString oversized_count(payload);
    EXPECT_THROW(histogram.deserialize(oversized_count, StatisticsFileVersion::V4), Exception);
}

TEST(Statistics, HistogramSkipsUnsupportedPayloadVersions)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();

    /// A single statistic reports that it was not loaded after reading only its version byte.
    for (UInt8 version : {UInt8(0), UInt8(1), UInt8(2), UInt8(4), UInt8(255)})
    {
        StatisticsHistogram histogram(makeHistogramDescription(8, 64), data_type);
        String payload(1, static_cast<char>(version));
        payload += "rest of an unknown payload";
        ReadBufferFromString rb(payload);
        histogram.deserialize(rb, StatisticsFileVersion::V4);
        EXPECT_FALSE(histogram.isLoaded());
        EXPECT_EQ(rb.count(), 1u);
    }

    /// `ColumnStatistics::deserialize` drops it and keeps the other statistics of the column.
    ColumnStatisticsDescription desc;
    desc.data_type = data_type;
    desc.types_to_desc.emplace(StatisticsType::Basic, SingleStatisticsDescription(StatisticsType::Basic, nullptr, false));
    desc.types_to_desc.emplace(StatisticsType::Histogram, makeHistogramDescription(8, 64));
    auto stats = MergeTreeStatisticsFactory::instance().get(desc);
    stats->build(makeFloat64Column({1, 2, 3, 3}));
    String serialized = serializeStatistics(stats);

    /// V4 layout: version, type mask, type name, rows, then (size, payload) per statistic in type order.
    size_t offset = sizeof(UInt16) + sizeof(UInt64) + 1 + data_type->getName().size() + sizeof(UInt64);
    UInt64 basic_size = 0;
    std::memcpy(&basic_size, serialized.data() + offset, sizeof(basic_size));
    offset += sizeof(UInt64) + basic_size + sizeof(UInt64);
    ASSERT_EQ(static_cast<UInt8>(serialized[offset]), UInt8(3));
    serialized[offset] = 2;

    ReadBufferFromString rb(serialized);
    auto restored = ColumnStatistics::deserialize(rb, data_type);
    ASSERT_TRUE(restored != nullptr);
    EXPECT_TRUE(rb.eof());
    EXPECT_EQ(restored->getNumRows(), 4u);
    EXPECT_TRUE(restored->getStats().contains(StatisticsType::Basic));
    EXPECT_FALSE(restored->getStats().contains(StatisticsType::Histogram));
    EXPECT_FALSE(restored->structureEquals(*stats));
}

TEST(Statistics, HistogramDeterministicRandomness)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    auto build_range = [&](UInt64 seed, UInt64 begin, UInt64 end)
    {
        auto stats = createHistogramStats(data_type, 128, 64, seed);
        MutableColumnPtr column = data_type->createColumn();
        for (UInt64 value = begin; value < end; ++value)
            column->insert(Field(static_cast<Float64>(value)));
        stats->build(std::move(column));
        return stats;
    };

    auto first = build_range(11, 0, 20'000);
    auto second = build_range(11, 0, 20'000);
    auto different_seed = build_range(12, 0, 20'000);
    EXPECT_EQ(serializeStatistics(first), serializeStatistics(second));
    EXPECT_NE(serializeStatistics(first), serializeStatistics(different_seed));

    auto left = build_range(21, 0, 10'000);
    auto right = build_range(22, 10'000, 20'000);
    auto first_merge = createHistogramStats(data_type, 128, 64, 31);
    auto second_merge = createHistogramStats(data_type, 128, 64, 31);
    first_merge->merge(left);
    first_merge->merge(right);
    second_merge->merge(left);
    second_merge->merge(right);
    EXPECT_EQ(serializeStatistics(first_merge), serializeStatistics(second_merge));
}

TEST(Statistics, HistogramConcurrentFirstAccess)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    auto stats = createHistogramStats(data_type, 128, 64, 42);
    MutableColumnPtr column = data_type->createColumn();
    for (UInt64 value = 0; value < 20'000; ++value)
        column->insert(Field(static_cast<Float64>(value)));
    stats->build(std::move(column));

    constexpr size_t thread_count = 16;
    std::barrier<> start(static_cast<std::ptrdiff_t>(thread_count));
    std::vector<std::thread> threads;
    std::vector<Float64> results(thread_count);
    std::vector<std::exception_ptr> errors(thread_count);
    for (size_t thread = 0; thread < thread_count; ++thread)
    {
        threads.emplace_back(
            [&, thread]
            {
                start.arrive_and_wait();
                try
                {
                    for (size_t iteration = 0; iteration < 100; ++iteration)
                    {
                        const auto estimate = stats->estimateLess(Field(Float64(10'000)));
                        if (!estimate)
                            throw std::logic_error("histogram estimate is unavailable");
                        results[thread] = *estimate;
                        if (getHistogram(stats).getBucketBounds().empty())
                            throw std::logic_error("histogram bounds are empty");
                    }
                }
                catch (...)
                {
                    errors[thread] = std::current_exception();
                }
            });
    }
    for (auto & thread : threads)
        thread.join();

    for (size_t thread = 0; thread < thread_count; ++thread)
    {
        EXPECT_FALSE(errors[thread]);
        EXPECT_DOUBLE_EQ(results[thread], results[0]);
    }
}

TEST(Statistics, HistogramIsEquiDepth)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    MutableColumnPtr column = data_type->createColumn();
    for (UInt64 i = 0; i < 10000; ++i)
    {
        const Float64 value = static_cast<Float64>(i);
        column->insert(Field(value * value));
    }

    /// All values are distinct, so every purge evicts all counters and only the last few rows stay tracked.
    auto stats = createHistogramStats(data_type, 4, 1);
    stats->build(std::move(column));
    ASSERT_LE(getHistogram(stats).getFrequentValues().size(), 6u);

    const auto & bounds = getHistogram(stats).getBucketBounds();
    ASSERT_EQ(bounds.size(), 5u);
    EXPECT_DOUBLE_EQ(bounds.front(), 0.0);
    EXPECT_GT(bounds.back(), 99'000'000.0);

    /// Equal-width quartiles would be near 25%, 50%, and 75% of the value span.
    /// Equi-depth quartiles of x^2 are near 6.25%, 25%, and 56.25% instead.
    EXPECT_LT(bounds[1], bounds.back() * 0.15);
    EXPECT_GT(bounds[2], bounds.back() * 0.15);
    EXPECT_LT(bounds[2], bounds.back() * 0.40);
    EXPECT_GT(bounds[3], bounds.back() * 0.45);
    EXPECT_LT(bounds[3], bounds.back() * 0.70);

    for (size_t i = 1; i < bounds.size(); ++i)
    {
        auto estimate = stats->estimateLess(Field(bounds[i]));
        ASSERT_TRUE(estimate.has_value());
        EXPECT_NEAR(*estimate, static_cast<Float64>(i) * 2500.0, 600.0);
    }

    /// In estimation mode, an ordinary retained item is below KLL's rank
    /// error and must not override the existing equality fallbacks.
    EXPECT_FALSE(getHistogram(stats).estimateEqual(Field(Float64(1234 * 1234))).has_value());
}

TEST(Statistics, HistogramExtremeInterpolationIsMonotonic)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    MutableColumnPtr column = data_type->createColumn();
    column->insert(Field(-std::numeric_limits<Float64>::max()));
    column->insert(Field(std::numeric_limits<Float64>::max()));
    for (Int64 i = 0; i < 1000; ++i)
        column->insert(Field(static_cast<Float64>(i) * 1e305 - 5e307));

    auto stats = createHistogramStats(data_type, 2, 1);
    stats->build(std::move(column));

    const auto at_zero = stats->estimateLess(Field(Float64(0.0)));
    const auto near_max = stats->estimateLess(Field(Float64(1e308)));
    const auto at_max = stats->estimateLess(Field(std::numeric_limits<Float64>::max()));
    ASSERT_TRUE(at_zero && near_max && at_max);
    EXPECT_TRUE(std::isfinite(*at_zero));
    EXPECT_TRUE(std::isfinite(*near_max));
    EXPECT_LE(*at_zero, *near_max);
    EXPECT_LE(*near_max, *at_max);
}

TEST(Statistics, HistogramRangeEndpointsAndDuplicates)
{
    const auto data_type = std::make_shared<DataTypeInt32>();
    MutableColumnPtr column = data_type->createColumn();
    for (size_t i = 0; i < 3; ++i)
        column->insert(Field(Int64(0)));
    for (size_t i = 0; i < 4; ++i)
        column->insert(Field(Int64(1)));
    for (size_t i = 0; i < 5; ++i)
        column->insert(Field(Int64(2)));

    auto stats = createHistogramStats(data_type, 8);
    stats->build(std::move(column));

    auto expect_estimate = [](std::string_view label, const std::optional<Float64> & estimate, Float64 expected)
    {
        SCOPED_TRACE(label);
        ASSERT_TRUE(estimate.has_value());
        EXPECT_DOUBLE_EQ(*estimate, expected);
    };

    expect_estimate("equal", stats->estimateEqual(Field(Int64(1))), 4.0);
    expect_estimate("less", stats->estimateLess(Field(Int64(1))), 3.0);
    expect_estimate("less_or_equal", stats->estimateLessOrEqual(Field(Int64(1))), 7.0);
    expect_estimate("greater", stats->estimateGreater(Field(Int64(1))), 5.0);
    expect_estimate("greater_or_equal", stats->estimateGreaterOrEqual(Field(Int64(1))), 9.0);

    expect_estimate("closed", stats->estimateRange(Range(Int64(0), true, Int64(2), true)), 12.0);
    expect_estimate("right_open", stats->estimateRange(Range(Int64(0), true, Int64(2), false)), 7.0);
    expect_estimate("left_open", stats->estimateRange(Range(Int64(0), false, Int64(2), true)), 9.0);
    expect_estimate("open", stats->estimateRange(Range(Int64(0), false, Int64(2), false)), 4.0);
}

TEST(Statistics, HistogramMergeAndRoundTrip)
{
    const auto data_type = std::make_shared<DataTypeInt32>();
    /// Merge in the order that keeps the larger configured k in the accumulator;
    /// serialization must still persist the smaller effective resolution.
    auto left = createHistogramStats(data_type, 512, 32);
    auto right = createHistogramStats(data_type, 256, 128);

    MutableColumnPtr left_column = data_type->createColumn();
    MutableColumnPtr right_column = data_type->createColumn();
    for (Int64 i = 0; i < 5000; ++i)
        left_column->insert(Field(i));
    for (Int64 i = 5000; i < 10000; ++i)
        right_column->insert(Field(i));
    left->build(std::move(left_column));
    right->build(std::move(right_column));

    /// Different parameters are compatible. The merged statistic keeps the minimum bucket count
    /// and the size of its own most-common-values summary.
    EXPECT_TRUE(left->structureEquals(*right));
    left->merge(right);
    EXPECT_EQ(getHistogram(left).getBucketCount(), 256u);
    EXPECT_EQ(getHistogram(left).getMcvValues(), 32u);
    auto midpoint = left->estimateLess(Field(Int64(5000)));
    ASSERT_TRUE(midpoint.has_value());
    EXPECT_NEAR(*midpoint, 5000.0, 500.0);
    EXPECT_DOUBLE_EQ(*left->estimateLess(Field(Int64(0))), 0.0);
    EXPECT_DOUBLE_EQ(*left->estimateLessOrEqual(Field(Int64(9999))), 10000.0);

    WriteBufferFromOwnString wb;
    left->serialize(wb);
    ReadBufferFromString rb(wb.str());
    auto restored = ColumnStatistics::deserialize(rb, data_type);
    ASSERT_TRUE(restored != nullptr);
    EXPECT_EQ(restored->getNumRows(), 10000u);
    EXPECT_EQ(getHistogram(restored).getBucketCount(), 256u);
    EXPECT_EQ(getHistogram(restored).getMcvValues(), 32u);
    EXPECT_EQ(restored->getStats().at(StatisticsType::Histogram)->getNameForLogs(), "Histogram(256, 32)");

    /// The bounds describe the residual values only, so the top values can be MCVs instead.
    const std::vector<Float64> restored_bounds = getHistogram(restored).getBucketBounds();
    ASSERT_FALSE(restored_bounds.empty());
    EXPECT_DOUBLE_EQ(restored_bounds.front(), 0.0);
    EXPECT_LE(restored_bounds.back(), 9999.0);
    EXPECT_GT(restored_bounds.back(), 9000.0);

    auto restored_midpoint = restored->estimateLess(Field(Int64(5000)));
    ASSERT_TRUE(restored_midpoint.has_value());
    EXPECT_NEAR(*restored_midpoint, *midpoint, 200.0);

    /// V3 stores the native KLL state, the frequent values and the random-generator state, so
    /// querying and repeated load/store cycles must preserve the payload byte-for-byte.
    String previous_serialized = serializeStatistics(restored);
    for (size_t round = 0; round < 3; ++round)
    {
        ReadBufferFromString repeated_rb(previous_serialized);
        restored = ColumnStatistics::deserialize(repeated_rb, data_type);
        ASSERT_TRUE(restored != nullptr);
        const auto & repeated_bounds = getHistogram(restored).getBucketBounds();
        ASSERT_FALSE(repeated_bounds.empty());
        EXPECT_EQ(repeated_bounds, restored_bounds);
        auto repeated_midpoint = restored->estimateLess(Field(Int64(5000)));
        ASSERT_TRUE(repeated_midpoint.has_value());
        EXPECT_DOUBLE_EQ(*repeated_midpoint, *restored_midpoint);
        EXPECT_EQ(serializeStatistics(restored), previous_serialized);
    }

    /// The description is re-created from the payload.
    auto empty_clone = restored->cloneEmpty();
    EXPECT_EQ(getHistogram(empty_clone).getBucketCount(), 256u);
    EXPECT_EQ(getHistogram(empty_clone).getMcvValues(), 32u);
    EXPECT_EQ(empty_clone->getNumRows(), 0u);
    EXPECT_EQ(empty_clone->getStats().at(StatisticsType::Histogram)->getNameForLogs(), "Histogram(256, 32)");
}

TEST(Statistics, HistogramNullableAndSpecialFloats)
{
    const auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());
    MutableColumnPtr column = data_type->createColumn();
    column->insert(Field(-std::numeric_limits<Float64>::infinity()));
    column->insert(Field(Float64(-1.0)));
    column->insert(Field(Float64(0.0)));
    column->insert(Field(Float64(1.0)));
    column->insert(Field(std::numeric_limits<Float64>::infinity()));
    column->insert(Field(std::numeric_limits<Float64>::quiet_NaN()));
    column->insertDefault();

    auto stats = createHistogramStats(data_type, 8);
    stats->build(std::move(column));

    EXPECT_TRUE(stats->hasNullCount());
    EXPECT_EQ(stats->getNullCount(), 1u);
    EXPECT_EQ(stats->getNonNullRowCount(), 6u);
    EXPECT_EQ(getHistogram(stats).getNonNullCount(), 6u);

    auto less = stats->estimateLess(Field(Float64(0.0)));
    auto less_or_equal = stats->estimateLessOrEqual(Field(Float64(0.0)));
    auto greater = stats->estimateGreater(Field(Float64(0.0)));
    auto greater_or_equal = stats->estimateGreaterOrEqual(Field(Float64(0.0)));
    ASSERT_TRUE(less && less_or_equal && greater && greater_or_equal);
    EXPECT_DOUBLE_EQ(*less, 2.0);
    EXPECT_DOUBLE_EQ(*less_or_equal, 3.0);
    EXPECT_DOUBLE_EQ(*greater, 2.0);
    EXPECT_DOUBLE_EQ(*greater_or_equal, 3.0);

    EXPECT_DOUBLE_EQ(*stats->estimateLess(Field(std::numeric_limits<Float64>::infinity())), 4.0);
    EXPECT_DOUBLE_EQ(*stats->estimateLessOrEqual(Field(std::numeric_limits<Float64>::infinity())), 5.0);
    EXPECT_DOUBLE_EQ(*stats->estimateGreater(Field(-std::numeric_limits<Float64>::infinity())), 4.0);
    EXPECT_DOUBLE_EQ(*stats->estimateGreaterOrEqual(Field(-std::numeric_limits<Float64>::infinity())), 5.0);
    EXPECT_DOUBLE_EQ(*stats->estimateLess(Field(std::numeric_limits<Float64>::quiet_NaN())), 0.0);
    EXPECT_DOUBLE_EQ(*stats->estimateEqual(Field(std::numeric_limits<Float64>::infinity())), 1.0);
    EXPECT_DOUBLE_EQ(*stats->estimateEqual(Field(Float64(0.0))), 1.0);
}

TEST(Statistics, HistogramLowCardinality)
{
    const auto data_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeInt32>());
    MutableColumnPtr column = data_type->createColumn();
    for (Int64 i = 0; i < 100; ++i)
        column->insert(Field(i % 10));

    auto stats = createHistogramStats(data_type, 8);
    stats->build(std::move(column));
    auto estimate = stats->estimateLess(Field(Int64(5)));
    ASSERT_TRUE(estimate.has_value());
    EXPECT_DOUBLE_EQ(*estimate, 50.0);
}

TEST(Statistics, HistogramMCVExactLowCardinality)
{
    /// At most capacity(M) = 24 distinct values across all parts: nothing is ever evicted.
    const auto data_type = std::make_shared<DataTypeFloat64>();
    std::vector<std::vector<Float64>> parts(3);
    std::vector<Float64> all;
    TestRandom random(5);
    for (auto & part : parts)
    {
        for (size_t i = 0; i < 5000; ++i)
        {
            /// Skewed: value v has weight ~ 1 / (v + 1).
            const UInt64 rank = random.below(random.below(24) + 1);
            part.push_back(static_cast<Float64>(rank) * 2.5 - 10.0);
        }
        all.insert(all.end(), part.begin(), part.end());
    }

    auto merged = createHistogramStats(data_type, 8, 24);
    for (const auto & part_values : parts)
    {
        TestRandom block_random(part_values.size());
        auto part_stats = buildInRandomBlocks(part_values, 8, 24, 1, block_random);
        checkHistogram(part_stats, part_values);
        merged->merge(roundTrip(part_stats, data_type));
    }

    const auto & histogram = getHistogram(merged);
    EXPECT_EQ(histogram.getMaximumError(), 0u);
    EXPECT_EQ(histogram.getResidualCount(), 0u);
    EXPECT_TRUE(histogram.getBucketBounds().empty());
    EXPECT_EQ(histogram.getFrequentValues().size(), 24u);
    checkHistogram(merged, all);

    /// Untracked values are known to be absent.
    EXPECT_DOUBLE_EQ(*histogram.estimateEqual(Field(Float64(-8.75))), 0.0);
    EXPECT_DOUBLE_EQ(*histogram.estimateEqual(Field(Float64(1000))), 0.0);
    auto restored = roundTrip(merged, data_type);
    checkHistogram(restored, all);
}

TEST(Statistics, HistogramMCVPartitionInvariant)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    CheckCoverage coverage;
    for (auto kind : {StreamKind::Uniform, StreamKind::Zipf, StreamKind::Mixed})
    {
        for (UInt64 seed = 1; seed <= 3; ++seed)
        {
            SCOPED_TRACE(fmt::format("kind {}, seed {}", static_cast<int>(kind), seed));
            TestRandom random(seed * 1000 + static_cast<UInt64>(kind));
            const UInt64 mcv_values = seed == 1 ? 1 : (seed == 2 ? 16 : 100);
            const UInt64 buckets = seed == 3 ? 4 : 64;
            /// Small streams keep the residual exact, larger ones put it into estimation mode.
            const size_t rows = kind == StreamKind::Uniform && seed == 1 ? 300 : 20000 + 5000 * seed;
            const auto values = generateStream(kind, rows, seed);

            /// Random parts, each built in random blocks.
            struct Node
            {
                ColumnStatisticsPtr stats;
                std::vector<Float64> values;
            };
            std::vector<Node> nodes;
            size_t begin = 0;
            while (begin < values.size())
            {
                const size_t size = std::min(values.size() - begin, 1 + random.below(values.size() / 3));
                std::vector<Float64> part(values.begin() + begin, values.begin() + begin + size);
                auto stats = buildInRandomBlocks(part, buckets, mcv_values, random.next(), random);
                checkHistogram(stats, part, &coverage);
                nodes.push_back({std::move(stats), std::move(part)});
                begin += size;
            }

            /// Random merge tree with random serialization round trips in between.
            while (nodes.size() > 1)
            {
                const size_t first = random.below(nodes.size());
                Node left = std::move(nodes[first]);
                nodes.erase(nodes.begin() + first);
                const size_t second = random.below(nodes.size());
                Node right = std::move(nodes[second]);
                nodes.erase(nodes.begin() + second);

                if (random.below(2))
                    left.stats = roundTrip(left.stats, data_type);
                if (random.below(2))
                    right.stats = roundTrip(right.stats, data_type);

                ColumnStatisticsPtr merged;
                if (random.below(2))
                {
                    merged = left.stats;
                    merged->merge(right.stats);
                }
                else
                {
                    /// Like `MergeTask`: an empty accumulator built from the metadata.
                    merged = createHistogramStats(data_type, buckets, mcv_values, random.next());
                    merged->merge(left.stats);
                    merged->merge(right.stats);
                }

                std::vector<Float64> merged_values = std::move(left.values);
                merged_values.insert(merged_values.end(), right.values.begin(), right.values.end());
                checkHistogram(merged, merged_values, &coverage);
                nodes.push_back({std::move(merged), std::move(merged_values)});
            }
            checkHistogram(roundTrip(nodes.front().stats, data_type), nodes.front().values, &coverage);
        }
    }

    /// Both the exact and the estimating regime of the residual sketch are exercised.
    EXPECT_GT(coverage.exact_equality_with_evictions, 0u);
    EXPECT_GT(coverage.estimation_mode, 0u);
    EXPECT_GT(coverage.checks, 50u);
}

TEST(Statistics, HistogramMCVBlockBoundaryIndependence)
{
    const auto values = generateStream(StreamKind::Mixed, 30000, 77);
    for (UInt64 mcv_values : {UInt64(1), UInt64(16), UInt64(768)})
    {
        SCOPED_TRACE(mcv_values);
        const auto data_type = std::make_shared<DataTypeFloat64>();
        auto single_block = createHistogramStats(data_type, 32, mcv_values, 9);
        single_block->build(makeFloat64Column(values));
        const String expected = serializeStatistics(single_block);

        for (UInt64 split_seed = 1; split_seed <= 3; ++split_seed)
        {
            TestRandom block_random(split_seed);
            auto split = buildInRandomBlocks(values, 32, mcv_values, 9, block_random);
            EXPECT_EQ(serializeStatistics(split), expected);
        }
    }
}

TEST(Statistics, HistogramMCVDeterminism)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    auto build_pipeline = [&]
    {
        std::vector<ColumnStatisticsPtr> parts;
        for (UInt64 part = 0; part < 4; ++part)
        {
            const auto values = generateStream(part % 2 ? StreamKind::Zipf : StreamKind::Mixed, 15000, part + 1);
            auto stats = createHistogramStats(data_type, 64, 48, 100 + part);
            stats->build(makeFloat64Column(values));
            parts.push_back(stats);
        }

        /// Same as the merge of persisted parts: every input is deserialized from canonical bytes.
        auto merged = createHistogramStats(data_type, 64, 48, 7);
        for (const auto & part : parts)
            merged->merge(roundTrip(part, data_type));
        String merged_bytes = serializeStatistics(merged);

        /// A query-time merge into the first part.
        auto estimator_accumulator = roundTrip(parts.front(), data_type);
        for (size_t i = 1; i < parts.size(); ++i)
            estimator_accumulator->merge(roundTrip(parts[i], data_type));

        std::vector<String> result;
        for (const auto & part : parts)
            result.push_back(serializeStatistics(part));
        result.push_back(merged_bytes);
        result.push_back(serializeStatistics(estimator_accumulator));
        return result;
    };

    const auto first = build_pipeline();
    const auto second = build_pipeline();
    EXPECT_EQ(first, second);

    /// Queries do not change the persisted state.
    const auto values = generateStream(StreamKind::Zipf, 15000, 2);
    auto stats = createHistogramStats(data_type, 64, 48, 101);
    stats->build(makeFloat64Column(values));
    ASSERT_TRUE(stats->estimateLess(Field(Float64(5000))).has_value());
    ASSERT_FALSE(getHistogram(stats).getBucketBounds().empty());
    EXPECT_EQ(serializeStatistics(stats), first[1]);
}

TEST(Statistics, HistogramMCVRoundTripCanonical)
{
    auto check_type = [](const DataTypePtr & data_type)
    {
        SCOPED_TRACE(data_type->getName());
        auto values = generateStream(StreamKind::Mixed, 20000, 3);
        for (size_t i = 0; i < 300; ++i)
            values.push_back(i % 2 ? -0.0 : 0.0);

        MutableColumnPtr column = data_type->createColumn();
        size_t nulls = 0;
        for (size_t i = 0; i < values.size(); ++i)
        {
            if (data_type->isNullable() && i % 50 == 0)
            {
                column->insertDefault();
                ++nulls;
            }
            column->insert(Field(values[i]));
        }

        auto stats = createHistogramStats(data_type, 16, 32, 5);
        stats->build(std::move(column));
        EXPECT_EQ(getHistogram(stats).getNonNullCount(), values.size());
        EXPECT_EQ(stats->getNullCount(), nulls);

        /// -0.0 and +0.0 are one value.
        const BruteForce truth(values);
        const auto negative_zero = stats->estimateEqual(Field(-0.0));
        const auto positive_zero = stats->estimateEqual(Field(0.0));
        ASSERT_TRUE(negative_zero && positive_zero);
        EXPECT_DOUBLE_EQ(*negative_zero, *positive_zero);
        EXPECT_GE(*positive_zero, static_cast<Float64>(truth.count(0.0)) - static_cast<Float64>(getHistogram(stats).getMaximumError()));
        EXPECT_DOUBLE_EQ(*stats->estimateEqual(Field(std::numeric_limits<Float64>::infinity())), static_cast<Float64>(truth.positive_inf));
        EXPECT_DOUBLE_EQ(*stats->estimateEqual(Field(-std::numeric_limits<Float64>::infinity())), static_cast<Float64>(truth.negative_inf));
        for (const auto & item : getHistogram(stats).getFrequentValues())
            EXPECT_FALSE(std::signbit(item.value) && item.value == 0);

        String previous = serializeStatistics(stats);
        for (size_t cycle = 0; cycle < 3; ++cycle)
        {
            ReadBufferFromString rb(previous);
            auto restored = ColumnStatistics::deserialize(rb, data_type);
            ASSERT_TRUE(restored != nullptr);
            EXPECT_TRUE(rb.eof());
            EXPECT_EQ(getHistogram(restored).getFrequentValues().size(), getHistogram(stats).getFrequentValues().size());
            for (Float64 probe : {-400.0, -0.0, 0.0, 7919.0, 15838.0, 50000.0})
            {
                EXPECT_EQ(restored->estimateEqual(Field(probe)), stats->estimateEqual(Field(probe)));
                EXPECT_EQ(restored->estimateLess(Field(probe)), stats->estimateLess(Field(probe)));
            }
            const String current = serializeStatistics(restored);
            EXPECT_EQ(current, previous);
            previous = current;
        }
    };

    check_type(std::make_shared<DataTypeFloat64>());
    check_type(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()));
    check_type(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeFloat64>()));
    check_type(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())));
}

namespace
{

/// A valid V3 payload whose fields can be corrupted one at a time.
struct HistogramPayload
{
    UInt8 version = 3;
    UInt64 buckets = 8;
    UInt64 mcv_values = 4; /// capacity 6
    UInt64 non_null = 14;
    UInt64 nan = 1;
    UInt64 negative_inf = 1;
    UInt64 positive_inf = 1;
    UInt8 has_finite_values = 1;
    Float64 finite_min = 0;
    Float64 finite_max = 4;
    UInt64 random_state = 123;
    UInt64 maximum_error = 1;
    std::vector<std::pair<Float64, UInt64>> items = {{1.0, 5}, {2.0, 3}};
    /// The residual KLL sketch.
    UInt16 kll_k = 200;
    std::vector<Float64> residual = {0.0, 3.0, 4.0};
    std::optional<String> kll_bytes_override;
    std::optional<UInt64> kll_size_override;

    String serialize() const
    {
        String result;
        WriteBufferFromString buf(result);
        writeBinary(version, buf);
        writeVarUInt(buckets, buf);
        writeVarUInt(mcv_values, buf);
        writeVarUInt(non_null, buf);
        writeVarUInt(nan, buf);
        writeVarUInt(negative_inf, buf);
        writeVarUInt(positive_inf, buf);
        writeBinary(has_finite_values, buf);
        if (has_finite_values)
        {
            writeBinary(finite_min, buf);
            writeBinary(finite_max, buf);
        }
        writeBinary(random_state, buf);
        writeVarUInt(maximum_error, buf);
        writeVarUInt(items.size(), buf);
        for (const auto & [value, count] : items)
        {
            writeBinary(value, buf);
            writeVarUInt(count, buf);
        }

        String kll_bytes;
        if (kll_bytes_override)
            kll_bytes = *kll_bytes_override;
        else
        {
            datasketches::kll_sketch<Float64> kll(kll_k);
            for (Float64 value : residual)
                kll.update(value);
            [[maybe_unused]] const auto view = kll.get_sorted_view();
            const auto bytes = kll.serialize();
            kll_bytes.assign(reinterpret_cast<const char *>(bytes.data()), bytes.size());
        }
        writeVarUInt(kll_size_override.value_or(kll_bytes.size()), buf);
        buf.write(kll_bytes.data(), kll_bytes.size());
        buf.finalize();
        return result;
    }
};

}

TEST(Statistics, HistogramMCVRejectsCorruptPayload)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    auto deserialize = [&](const HistogramPayload & payload)
    {
        StatisticsHistogram histogram(makeHistogramDescription(16, 64), data_type);
        const String bytes = payload.serialize();
        ReadBufferFromString rb(bytes);
        histogram.deserialize(rb, StatisticsFileVersion::V4);
        if (!rb.eof())
            throw std::logic_error("payload was not consumed completely");
        return histogram.getBucketCount();
    };

    /// The base payload is valid, and it is canonical.
    {
        HistogramPayload payload;
        EXPECT_EQ(deserialize(payload), 8u);
        StatisticsHistogram histogram(makeHistogramDescription(16, 64), data_type);
        const String bytes = payload.serialize();
        ReadBufferFromString rb(bytes);
        histogram.deserialize(rb, StatisticsFileVersion::V4);
        EXPECT_EQ(histogram.getMcvValues(), 4u);
        EXPECT_DOUBLE_EQ(*histogram.estimateEqual(Field(1.0)), 5.0);
        WriteBufferFromOwnString out;
        histogram.serialize(out);
        EXPECT_EQ(out.str(), bytes);
    }

    String kll_with_large_min;
    {
        datasketches::kll_sketch<Float64> kll(200);
        kll.update(-1.0);
        kll.update(3.0);
        kll.update(4.0);
        const auto bytes = kll.serialize();
        kll_with_large_min.assign(reinterpret_cast<const char *>(bytes.data()), bytes.size());
    }

    const std::vector<std::pair<String, std::function<void(HistogramPayload &)>>> cases = {
        /// Parameters.
        {"too few buckets", [](auto & p) { p.buckets = 1; }},
        {"too many buckets", [](auto & p) { p.buckets = 1025; }},
        {"no most common values", [](auto & p) { p.mcv_values = 0; }},
        {"too many most common values", [](auto & p) { p.mcv_values = 769; }},
        /// Counters.
        {"NaN count above non-null count", [](auto & p) { p.nan = 15; }},
        {"-Inf count above remaining rows", [](auto & p) { p.negative_inf = 14; }},
        {"+Inf count above remaining rows", [](auto & p) { p.positive_inf = 13; }},
        {"finite rows above KLL limit", [](auto & p) { p.non_null = std::numeric_limits<UInt64>::max(); p.nan = 0; }},
        /// Finite flag and bounds.
        {"invalid finite flag", [](auto & p) { p.has_finite_values = 2; }},
        {"finite flag without finite rows", [](auto & p) { p.non_null = 3; p.items.clear(); p.residual.clear(); p.maximum_error = 0; }},
        {"finite rows without finite flag", [](auto & p) { p.has_finite_values = 0; }},
        {"non-finite min", [](auto & p) { p.finite_min = -std::numeric_limits<Float64>::infinity(); }},
        {"NaN max", [](auto & p) { p.finite_max = std::numeric_limits<Float64>::quiet_NaN(); }},
        {"min above max", [](auto & p) { p.finite_min = 5; }},
        {"non-canonical zero bound", [](auto & p) { p.finite_min = -0.0; }},
        /// Frequent values.
        {"more items than capacity",
         [](auto & p)
         {
             p.items = {{0.5, 1}, {1.0, 1}, {1.5, 1}, {2.0, 1}, {2.5, 1}, {3.0, 1}, {3.5, 2}};
             p.residual = {0.0, 4.0};
             p.non_null = 13;
         }},
        {"non-finite value", [](auto & p) { p.items[0].first = std::numeric_limits<Float64>::infinity(); }},
        {"NaN value", [](auto & p) { p.items[0].first = std::numeric_limits<Float64>::quiet_NaN(); }},
        {"non-canonical zero value", [](auto & p) { p.items = {{-0.0, 5}, {2.0, 3}}; p.residual = {1.0, 3.0, 4.0}; p.finite_min = -1; }},
        {"duplicate values", [](auto & p) { p.items[1].first = 1.0; }},
        {"decreasing values", [](auto & p) { p.items = {{2.0, 3}, {1.0, 5}}; }},
        {"value below min", [](auto & p) { p.items[0].first = -1.0; }},
        {"value above max", [](auto & p) { p.items[1].first = 5.0; }},
        {"zero count", [](auto & p) { p.items[0].second = 0; }},
        {"counts above finite rows", [](auto & p) { p.items[0].second = 12; }},
        {"overflowing counts", [](auto & p) { p.items = {{1.0, std::numeric_limits<UInt64>::max()}, {2.0, 3}}; }},
        /// Residual KLL.
        {"KLL too large", [](auto & p) { p.kll_size_override = (1ULL << 20) + 1; }},
        {"KLL garbage", [](auto & p) { p.kll_bytes_override = String(16, '\x5a'); }},
        {"KLL k below histogram resolution", [](auto & p) { p.kll_k = 100; }},
        {"KLL k above maximum resolution", [](auto & p) { p.kll_k = 2000; }},
        {"KLL k below bucket count", [](auto & p) { p.buckets = 512; p.kll_k = 300; }},
        {"KLL row count below residual rows", [](auto & p) { p.residual = {0.0, 3.0}; }},
        {"KLL row count above residual rows", [](auto & p) { p.residual = {0.0, 3.0, 4.0, 4.0}; }},
        {"KLL item outside finite bounds", [&](auto & p) { p.kll_bytes_override = kll_with_large_min; }},
        /// Maximum error.
        {"maximum error above residual rows", [](auto & p) { p.maximum_error = 4; }},
        {"zero maximum error with residual rows", [](auto & p) { p.maximum_error = 0; }},
        {"maximum error without residual rows",
         [](auto & p)
         {
             p.items = {{0.0, 1}, {1.0, 5}, {2.0, 3}, {3.0, 1}, {4.0, 1}};
             p.residual.clear();
         }},
    };

    for (const auto & [name, corrupt] : cases)
    {
        SCOPED_TRACE(name);
        HistogramPayload payload;
        corrupt(payload);
        try
        {
            deserialize(payload);
            ADD_FAILURE() << "corrupt payload was accepted";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::ILLEGAL_STATISTICS) << e.message();
        }
    }

    /// Truncated payloads are rejected at every length.
    const String bytes = HistogramPayload{}.serialize();
    for (size_t size = 1; size < bytes.size(); ++size)
    {
        StatisticsHistogram histogram(makeHistogramDescription(16, 64), data_type);
        ReadBufferFromString rb(std::string_view(bytes.data(), size));
        EXPECT_THROW(histogram.deserialize(rb, StatisticsFileVersion::V4), Exception) << size;
    }
}

TEST(Statistics, HistogramMCVSkewedAccuracy)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    const auto values = generateZipf(200000, 20000, 1.1, 42);
    const BruteForce truth(values);

    auto build = [&](UInt64 mcv_values)
    {
        auto stats = createHistogramStats(data_type, 64, mcv_values, 3);
        /// Several parts merged like `MergeTask` does.
        auto merged = createHistogramStats(data_type, 64, mcv_values, 4);
        for (size_t part = 0; part < 4; ++part)
        {
            auto part_stats = createHistogramStats(data_type, 64, mcv_values, 10 + part);
            part_stats->build(makeFloat64Column(values, part * 50000, (part + 1) * 50000));
            merged->merge(roundTrip(part_stats, data_type));
        }
        return merged;
    };

    auto large = build(256);
    auto tiny = build(1);
    checkHistogram(large, values);
    checkHistogram(tiny, values);

    /// The most frequent values are tracked, with bounded counts.
    std::vector<std::pair<UInt64, Float64>> by_frequency;
    for (const auto & [value, count] : truth.finite)
        by_frequency.emplace_back(count, value);
    std::sort(by_frequency.rbegin(), by_frequency.rend());
    const auto & histogram = getHistogram(large);
    const UInt64 delta = histogram.getMaximumError();
    EXPECT_LE(static_cast<Float64>(delta), 3.5 / 512 * static_cast<Float64>(values.size()));

    Float64 large_equal_error = 0;
    Float64 tiny_equal_error = 0;
    for (size_t i = 0; i < 50; ++i)
    {
        const auto [count, value] = by_frequency[i];
        const auto large_estimate = large->estimateEqual(Field(value));
        ASSERT_TRUE(large_estimate.has_value());
        EXPECT_LE(std::abs(*large_estimate - static_cast<Float64>(count)), static_cast<Float64>(delta));
        large_equal_error += std::abs(*large_estimate - static_cast<Float64>(count));
        const auto tiny_estimate = tiny->estimateEqual(Field(value));
        tiny_equal_error += tiny_estimate ? std::abs(*tiny_estimate - static_cast<Float64>(count)) : static_cast<Float64>(count);
    }
    /// Each error is bounded by delta, and most of the evicted share is recovered from the residual sketch.
    EXPECT_LT(large_equal_error, 0.1 * tiny_equal_error);
    EXPECT_LT(large_equal_error, 0.02 * static_cast<Float64>(values.size()));

    Float64 large_range_error = 0;
    Float64 tiny_range_error = 0;
    for (UInt64 step = 0; step <= 100; ++step)
    {
        const Float64 probe = static_cast<Float64>(step * 997);
        const Float64 expected = static_cast<Float64>(truth.less(probe, true));
        large_range_error += std::abs(*large->estimateLessOrEqual(Field(probe)) - expected);
        tiny_range_error += std::abs(*tiny->estimateLessOrEqual(Field(probe)) - expected);
    }
    EXPECT_LT(large_range_error, tiny_range_error);
}

TEST(Statistics, HistogramMCVConcurrentFirstAccess)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    const auto values = generateZipf(50000, 5000, 1.2, 8);
    auto stats = createHistogramStats(data_type, 64, 128, 1);
    stats->build(makeFloat64Column(values));
    stats = roundTrip(stats, data_type);

    constexpr size_t thread_count = 16;
    std::barrier<> start(static_cast<std::ptrdiff_t>(thread_count));
    std::vector<std::thread> threads;
    std::vector<std::vector<std::optional<Float64>>> results(thread_count);
    std::vector<std::exception_ptr> errors(thread_count);
    for (size_t thread = 0; thread < thread_count; ++thread)
    {
        threads.emplace_back(
            [&, thread]
            {
                start.arrive_and_wait();
                try
                {
                    for (size_t iteration = 0; iteration < 20; ++iteration)
                    {
                        results[thread].clear();
                        for (Float64 probe : {0.0, 7919.0, 15838.0, 12345.0, 50000.0})
                        {
                            results[thread].push_back(stats->estimateEqual(Field(probe)));
                            results[thread].push_back(stats->estimateLess(Field(probe)));
                            results[thread].push_back(stats->estimateGreaterOrEqual(Field(probe)));
                        }
                        if (getHistogram(stats).getBucketBounds().empty() || getHistogram(stats).getFrequentValues().empty())
                            throw std::logic_error("histogram is empty");
                        const auto summary = getHistogram(stats).getNonFrequentSummary();
                        if (summary.frequent_values == 0)
                            throw std::logic_error("no certainly frequent values");
                    }
                }
                catch (...)
                {
                    errors[thread] = std::current_exception();
                }
            });
    }
    for (auto & thread : threads)
        thread.join();

    for (size_t thread = 0; thread < thread_count; ++thread)
    {
        EXPECT_FALSE(errors[thread]);
        EXPECT_EQ(results[thread], results[0]);
    }
}

TEST(Statistics, HistogramMCVEqualityFallback)
{
    tryRegisterAggregateFunctions();
    const auto data_type = std::make_shared<DataTypeFloat64>();

    /// Half of the rows are one heavy hitter, the rest are distinct.
    std::vector<Float64> values;
    for (size_t i = 0; i < 100000; ++i)
        values.push_back(i % 2 ? 0.0 : static_cast<Float64>(i));

    auto make_stats = [&](std::vector<StatisticsType> other_types)
    {
        ColumnStatisticsDescription desc;
        desc.data_type = data_type;
        desc.types_to_desc.emplace(StatisticsType::Histogram, makeHistogramDescription(32, 16));
        for (auto type : other_types)
            desc.types_to_desc.emplace(type, SingleStatisticsDescription(type, nullptr, false));
        auto stats = MergeTreeStatisticsFactory::instance().get(desc);
        stats->build(makeFloat64Column(values));
        return stats;
    };

    /// An untracked value inside the bounds whose residual mass is below the KLL rank error.
    const Field untracked(Float64(31338));
    auto histogram_only = make_stats({});
    const auto & histogram = getHistogram(histogram_only);
    ASSERT_FALSE(histogram.estimateEqual(untracked).has_value());
    EXPECT_FALSE(histogram_only->estimateEqual(untracked).has_value());
    const auto summary = histogram.getNonFrequentSummary();
    EXPECT_GT(summary.maximum_error, 0u);
    EXPECT_EQ(summary.frequent_values, 1u);
    EXPECT_NEAR(summary.frequent_rows, 50000.0, static_cast<Float64>(summary.maximum_error));

    /// The heavy hitter itself is answered by the histogram.
    const auto heavy = histogram_only->estimateEqual(Field(0.0));
    ASSERT_TRUE(heavy.has_value());
    EXPECT_NEAR(*heavy, 50000.0, static_cast<Float64>(summary.maximum_error));

    /// With uniq, the non-MCV formula spreads the remaining rows over the remaining distinct values.
    for (auto uniq_type : {StatisticsType::Uniq, StatisticsType::UniqV2})
    {
        auto with_uniq = make_stats({uniq_type});
        const auto estimate = with_uniq->estimateEqual(untracked);
        ASSERT_TRUE(estimate.has_value());
        EXPECT_NEAR(*estimate, 1.0, 0.1);
        EXPECT_LE(*estimate, static_cast<Float64>(getHistogram(with_uniq).getMaximumError()));
    }

    /// Without uniq, the existing chain is used, capped at the maximum error.
    auto with_count_min = make_stats({StatisticsType::CountMinSketch});
    const auto count_min_estimate = with_count_min->estimateEqual(untracked);
    ASSERT_TRUE(count_min_estimate.has_value());
    EXPECT_LE(*count_min_estimate, static_cast<Float64>(getHistogram(with_count_min).getMaximumError()));
}

#endif

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
