#include <gtest/gtest.h>

#include <barrier>
#include <thread>

#include <config.h>

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

namespace
{

ASTPtr makeHistogramAST(UInt64 buckets)
{
    return makeASTFunction("histogram", make_intrusive<ASTLiteral>(buckets));
}

ColumnStatisticsPtr createHistogramStats(const DataTypePtr & data_type, UInt64 buckets, UInt64 random_seed = 0)
{
    ColumnStatisticsDescription desc;
    desc.data_type = data_type;
    desc.types_to_desc.emplace(
        StatisticsType::Histogram, SingleStatisticsDescription(StatisticsType::Histogram, makeHistogramAST(buckets), false));
    return MergeTreeStatisticsFactory::instance().get(desc, random_seed);
}

String serializeStatistics(const ColumnStatisticsPtr & stats)
{
    WriteBufferFromOwnString buf;
    stats->serialize(buf);
    return buf.str();
}

const StatisticsHistogram & getHistogram(const ColumnStatisticsPtr & stats)
{
    return assert_cast<const StatisticsHistogram &>(*stats->getStats().at(StatisticsType::Histogram));
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

    EXPECT_NO_THROW(factory.validate(make_description(makeHistogramAST(2), int_type), int_type));
    EXPECT_NO_THROW(factory.validate(make_description(makeHistogramAST(1024), int_type), int_type));

    EXPECT_THROW(factory.validate(make_description(makeASTFunction("histogram"), int_type), int_type), Exception);
    EXPECT_THROW(factory.validate(make_description(makeHistogramAST(1), int_type), int_type), Exception);
    EXPECT_THROW(factory.validate(make_description(makeHistogramAST(1025), int_type), int_type), Exception);
    EXPECT_THROW(
        factory.validate(make_description(makeASTFunction("histogram", make_intrusive<ASTLiteral>(Float64(8.0))), int_type), int_type),
        Exception);
    EXPECT_THROW(
        factory.validate(
            make_description(
                makeASTFunction("histogram", make_intrusive<ASTLiteral>(UInt64(8)), make_intrusive<ASTLiteral>(UInt64(16))), int_type),
            int_type),
        Exception);

    const auto string_type = std::make_shared<DataTypeString>();
    EXPECT_THROW(factory.validate(make_description(makeHistogramAST(8), string_type), string_type), Exception);

    SingleStatisticsDescription eight(StatisticsType::Histogram, makeHistogramAST(8), false);
    SingleStatisticsDescription another_eight(StatisticsType::Histogram, makeHistogramAST(8), false);
    SingleStatisticsDescription sixteen(StatisticsType::Histogram, makeHistogramAST(16), false);
    EXPECT_EQ(eight, another_eight);
    EXPECT_FALSE(eight == sixteen);
    EXPECT_EQ(eight.getTypeName(), "histogram(8)");

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
}

TEST(Statistics, HistogramRejectsCorruptPayload)
{
    const auto data_type = std::make_shared<DataTypeInt32>();
    StatisticsHistogram histogram(SingleStatisticsDescription(StatisticsType::Histogram, makeHistogramAST(8), false), data_type);

    String payload;
    {
        WriteBufferFromString buf(payload);
        writeBinary(UInt8(1), buf); /// payload version
        writeVarUInt(UInt64(8), buf); /// buckets
        writeVarUInt(UInt64(1), buf); /// non-null rows
        writeVarUInt(UInt64(0), buf); /// NaN rows
        writeVarUInt(UInt64(0), buf); /// -Inf rows
        writeVarUInt(UInt64(0), buf); /// +Inf rows
        writeBinary(UInt8(1), buf); /// has finite bounds
        writeBinary(Float64(0), buf); /// finite min
        writeBinary(Float64(0), buf); /// finite max
        writeVarUInt(UInt64(1024), buf); /// impossible k for histogram(8)
        writeVarUInt(UInt64(0), buf); /// retained items
        buf.finalize();
    }

    ReadBufferFromString rb(payload);
    EXPECT_THROW(histogram.deserialize(rb, StatisticsFileVersion::V4), Exception);
}

TEST(Statistics, HistogramReadsLegacyPayloadDeterministically)
{
    const auto data_type = std::make_shared<DataTypeInt32>();
    const auto description = SingleStatisticsDescription(StatisticsType::Histogram, makeHistogramAST(8), false);

    String payload;
    {
        WriteBufferFromString buf(payload);
        writeBinary(UInt8(1), buf); /// payload version
        writeVarUInt(UInt64(8), buf); /// buckets
        writeVarUInt(UInt64(3), buf); /// non-null rows
        writeVarUInt(UInt64(0), buf); /// NaN rows
        writeVarUInt(UInt64(0), buf); /// -Inf rows
        writeVarUInt(UInt64(0), buf); /// +Inf rows
        writeBinary(UInt8(1), buf); /// has finite bounds
        writeBinary(Float64(1), buf); /// finite min
        writeBinary(Float64(3), buf); /// finite max
        writeVarUInt(UInt64(200), buf); /// KLL k
        writeVarUInt(UInt64(3), buf); /// retained items
        for (UInt64 value = 1; value <= 3; ++value)
        {
            writeBinary(static_cast<Float64>(value), buf);
            writeVarUInt(UInt64(1), buf);
        }
        buf.finalize();
    }

    StatisticsHistogram first(description, data_type, 123);
    StatisticsHistogram second(description, data_type, 123);
    ReadBufferFromString first_input(payload);
    ReadBufferFromString second_input(payload);
    first.deserialize(first_input, StatisticsFileVersion::V4);
    second.deserialize(second_input, StatisticsFileVersion::V4);
    EXPECT_DOUBLE_EQ(*first.estimateLess(Field(Int64(3))), 2.0);

    WriteBufferFromOwnString first_output;
    WriteBufferFromOwnString second_output;
    first.serialize(first_output);
    second.serialize(second_output);
    EXPECT_EQ(first_output.str(), second_output.str());
    EXPECT_EQ(static_cast<UInt8>(first_output.str().front()), UInt8(2));
}

TEST(Statistics, HistogramRejectsOversizedLegacyRowCount)
{
    const auto data_type = std::make_shared<DataTypeInt32>();
    StatisticsHistogram histogram(SingleStatisticsDescription(StatisticsType::Histogram, makeHistogramAST(8), false), data_type);

    String payload;
    {
        WriteBufferFromString buf(payload);
        writeBinary(UInt8(1), buf); /// payload version
        writeVarUInt(std::numeric_limits<UInt64>::max(), buf); /// buckets: rejected before KLL sizing
        buf.finalize();
    }

    ReadBufferFromString invalid_buckets(payload);
    EXPECT_THROW(histogram.deserialize(invalid_buckets, StatisticsFileVersion::V4), Exception);

    payload.clear();
    {
        WriteBufferFromString buf(payload);
        writeBinary(UInt8(1), buf); /// payload version
        writeVarUInt(UInt64(8), buf); /// buckets
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

TEST(Statistics, HistogramDeterministicRandomness)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    auto build_range = [&](UInt64 seed, UInt64 begin, UInt64 end)
    {
        auto stats = createHistogramStats(data_type, 128, seed);
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
    auto first_merge = createHistogramStats(data_type, 128, 31);
    auto second_merge = createHistogramStats(data_type, 128, 31);
    first_merge->merge(left);
    first_merge->merge(right);
    second_merge->merge(left);
    second_merge->merge(right);
    EXPECT_EQ(serializeStatistics(first_merge), serializeStatistics(second_merge));
}

TEST(Statistics, HistogramConcurrentFirstAccess)
{
    const auto data_type = std::make_shared<DataTypeFloat64>();
    auto stats = createHistogramStats(data_type, 128, 42);
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

    auto stats = createHistogramStats(data_type, 4);
    stats->build(std::move(column));

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

    auto stats = createHistogramStats(data_type, 2);
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
    auto left = createHistogramStats(data_type, 512);
    auto right = createHistogramStats(data_type, 256);

    MutableColumnPtr left_column = data_type->createColumn();
    MutableColumnPtr right_column = data_type->createColumn();
    for (Int64 i = 0; i < 5000; ++i)
        left_column->insert(Field(i));
    for (Int64 i = 5000; i < 10000; ++i)
        right_column->insert(Field(i));
    left->build(std::move(left_column));
    right->build(std::move(right_column));

    EXPECT_TRUE(left->structureEquals(*right));
    left->merge(right);
    EXPECT_EQ(getHistogram(left).getBucketCount(), 256u);
    auto midpoint = left->estimateLess(Field(Int64(5000)));
    ASSERT_TRUE(midpoint.has_value());
    EXPECT_NEAR(*midpoint, 5000.0, 500.0);

    WriteBufferFromOwnString wb;
    left->serialize(wb);
    ReadBufferFromString rb(wb.str());
    auto restored = ColumnStatistics::deserialize(rb, data_type);
    ASSERT_TRUE(restored != nullptr);
    EXPECT_EQ(restored->getNumRows(), 10000u);
    EXPECT_EQ(getHistogram(restored).getBucketCount(), 256u);
    const auto & restored_bounds = getHistogram(restored).getBucketBounds();
    ASSERT_FALSE(restored_bounds.empty());
    EXPECT_DOUBLE_EQ(restored_bounds.front(), 0.0);
    EXPECT_DOUBLE_EQ(restored_bounds.back(), 9999.0);

    auto restored_midpoint = restored->estimateLess(Field(Int64(5000)));
    ASSERT_TRUE(restored_midpoint.has_value());
    EXPECT_NEAR(*restored_midpoint, *midpoint, 200.0);

    /// V2 stores native KLL state and the random-generator state, so querying and
    /// repeated load/store cycles must preserve the payload byte-for-byte.
    String previous_serialized = serializeStatistics(restored);
    for (size_t round = 0; round < 3; ++round)
    {
        ReadBufferFromString repeated_rb(previous_serialized);
        restored = ColumnStatistics::deserialize(repeated_rb, data_type);
        ASSERT_TRUE(restored != nullptr);
        const auto & repeated_bounds = getHistogram(restored).getBucketBounds();
        ASSERT_FALSE(repeated_bounds.empty());
        EXPECT_DOUBLE_EQ(repeated_bounds.front(), 0.0);
        EXPECT_DOUBLE_EQ(repeated_bounds.back(), 9999.0);
        auto repeated_midpoint = restored->estimateLess(Field(Int64(5000)));
        ASSERT_TRUE(repeated_midpoint.has_value());
        EXPECT_DOUBLE_EQ(*repeated_midpoint, *restored_midpoint);
        EXPECT_EQ(serializeStatistics(restored), previous_serialized);
    }

    auto empty_clone = restored->cloneEmpty();
    EXPECT_EQ(getHistogram(empty_clone).getBucketCount(), 256u);
    EXPECT_EQ(empty_clone->getNumRows(), 0u);
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
    EXPECT_NEAR(*estimate, 50.0, 10.0);
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
