#include <gtest/gtest.h>
#include <fmt/format.h>

#include <limits>
#include <memory>

#include <Poco/Util/MapConfiguration.h>
#include <stdexcept>
#include <utility>
#include <vector>

#include <Common/FieldAccurateComparison.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>

namespace DB
{

struct MergeTreeIndexConditionMinMaxTestAccess
{
    static BoolMask scalar(const MergeTreeIndexConditionMinMax & condition, const Range & range)
    {
        return condition.condition.checkInHyperrectangle({range}, condition.index_data_types);
    }

    static std::vector<BoolMask> bulk(
        const MergeTreeIndexConditionMinMax & condition,
        const MergeTreeIndexBulkGranulesMinMaxColumnar & granules)
    {
        Block block = condition.executeBulkActions(granules);
        const auto & can_be_true = block.getByName(condition.OUTPUT_CAN_BE_TRUE).column;
        const auto & can_be_false = block.getByName(condition.OUTPUT_CAN_BE_FALSE).column;
        auto value_at = [](const ColumnPtr & column, size_t row)
        {
            if (const auto * constant = typeid_cast<const ColumnConst *>(column.get()))
                return constant->getUInt(0) != 0;
            return assert_cast<const ColumnUInt8 &>(*column).getData()[row] != 0;
        };

        std::vector<BoolMask> result;
        result.reserve(granules.size);
        for (size_t i = 0; i < granules.size; ++i)
            result.emplace_back(value_at(can_be_true, i), value_at(can_be_false, i));
        return result;
    }
};

namespace
{

struct TypeCase
{
    String name;
    DataTypePtr index_type;
    DataTypePtr bound_type;
    std::vector<std::pair<Field, Field>> intervals;
    std::vector<Field> bounds;
    bool expect_bulk_fast_path = true;
};

ContextMutablePtr getRegisteredContext()
{
    tryRegisterFunctions();
    auto context = Context::createCopy(getContext().context);
    context->setConfig(new Poco::Util::MapConfiguration);
    context->setSetting("use_minmax_index_bulk_filtering", Field(true));
    context->setSetting("compile_expressions", Field(false));
    return context;
}

IndexDescription makeIndex(const DataTypePtr & type, const ContextPtr & context)
{
    ColumnsDescription columns;
    columns.add(ColumnDescription("x", type));
    auto indices = IndicesDescription::parse("idx_x x TYPE minmax GRANULARITY 1", columns, false, context);
    EXPECT_EQ(indices.size(), 1);
    return indices.front();
}

struct ConditionShape
{
    ActionsDAG dag;
    const ActionsDAG::Node * predicate = nullptr;
};

ConditionShape makeComparison(
    const ContextPtr & context,
    const DataTypePtr & key_type,
    const DataTypePtr & bound_type,
    const Field & bound,
    const String & operation)
{
    ConditionShape shape;
    const auto & key = shape.dag.addInput("x", key_type);
    const auto & literal = shape.dag.addColumn(bound_type->createColumnConst(1, bound), bound_type, "bound");
    const auto & predicate = shape.dag.addFunction(
        FunctionFactory::instance().get(operation, context), {&key, &literal}, "predicate");
    shape.predicate = &predicate;
    return shape;
}

std::unique_ptr<MergeTreeIndexBulkGranulesMinMaxColumnar> makeBulkGranules(
    const IndexDescription & index,
    const std::vector<std::pair<Field, Field>> & intervals)
{
    auto bulk = std::make_unique<MergeTreeIndexBulkGranulesMinMaxColumnar>(index.sample_block, intervals.size());
    for (const auto & [min, max] : intervals)
    {
        bulk->cols[0].min_col->insert(min);
        bulk->cols[0].max_col->insert(max);
        ++bulk->size;
    }
    return bulk;
}

bool evaluateTypedComparison(
    const ContextPtr & context,
    const String & operation,
    const DataTypePtr & left_type,
    const Field & left,
    const DataTypePtr & right_type,
    const Field & right)
{
    auto left_column = left_type->createColumn();
    left_column->insert(left);
    auto right_column = right_type->createColumnConst(1, right);
    ColumnsWithTypeAndName arguments{
        {std::move(left_column), left_type, "left"},
        {std::move(right_column), right_type, "right"},
    };
    auto function = FunctionFactory::instance().get(operation, context)->build(arguments);
    auto result = function->execute(arguments, function->getResultType(), 1, false);
    return result->getUInt(0) != 0;
}

std::vector<TypeCase> makeTypeCases()
{
    return {
        {
            "Int32_vs_Int64",
            std::make_shared<DataTypeInt32>(),
            std::make_shared<DataTypeInt64>(),
            {{Int64(-10), Int64(-5)}, {Int64(-1), Int64(1)}, {Int64(5), Int64(10)}},
            {Int64(-2147483649LL), Int64(-5), Int64(0), Int64(5), Int64(2147483648LL)},
        },
        {
            "UInt8_vs_UInt64",
            std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt64>(),
            {{UInt64(0), UInt64(4)}, {UInt64(5), UInt64(5)}, {UInt64(6), UInt64(255)}},
            {UInt64(0), UInt64(5), UInt64(255), UInt64(256), std::numeric_limits<UInt64>::max()},
        },
        {
            "Float32_vs_Float64",
            std::make_shared<DataTypeFloat32>(),
            std::make_shared<DataTypeFloat64>(),
            {{Float64(-2.5), Float64(-1.0)}, {Float64(-0.0), Float64(0.0)}, {Float64(1.0), Float64(2.5)}},
            {Float64(-1.5), Float64(0.0), Float64(1.5)},
            false,
        },
        {
            "Decimal64_same_scale",
            std::make_shared<DataTypeDecimal<Decimal64>>(18, 1),
            std::make_shared<DataTypeDecimal<Decimal64>>(18, 1),
            {
                {DecimalField<Decimal64>(Decimal64(330), 1), DecimalField<Decimal64>(Decimal64(333), 1)},
                {DecimalField<Decimal64>(Decimal64(334), 1), DecimalField<Decimal64>(Decimal64(340), 1)},
            },
            {
                DecimalField<Decimal64>(Decimal64(329), 1),
                DecimalField<Decimal64>(Decimal64(333), 1),
                DecimalField<Decimal64>(Decimal64(341), 1),
            },
        },
        {
            "Decimal64_cross_scale",
            std::make_shared<DataTypeDecimal<Decimal64>>(18, 1),
            std::make_shared<DataTypeDecimal<Decimal64>>(18, 2),
            {
                {DecimalField<Decimal64>(Decimal64(330), 1), DecimalField<Decimal64>(Decimal64(333), 1)},
                {DecimalField<Decimal64>(Decimal64(334), 1), DecimalField<Decimal64>(Decimal64(340), 1)},
            },
            {
                DecimalField<Decimal64>(Decimal64(3299), 2),
                DecimalField<Decimal64>(Decimal64(3333), 2),
                DecimalField<Decimal64>(Decimal64(3401), 2),
            },
            false,
        },
        {
            "DateTime64_cross_scale",
            std::make_shared<DataTypeDateTime64>(3, "UTC"),
            std::make_shared<DataTypeDateTime64>(4, "UTC"),
            {
                {DecimalField<DateTime64>(DateTime64(1230), 3), DecimalField<DateTime64>(DateTime64(1230), 3)},
                {DecimalField<DateTime64>(DateTime64(1231), 3), DecimalField<DateTime64>(DateTime64(1240), 3)},
            },
            {
                DecimalField<DateTime64>(DateTime64(12299), 4),
                DecimalField<DateTime64>(DateTime64(12305), 4),
                DecimalField<DateTime64>(DateTime64(12401), 4),
            },
        },
    };
}

BoolMask expectedMaskForOperation(const String & operation, const Range & condition, const Range & granule)
{
    BoolMask mask(condition.intersectsRange(granule), !condition.containsRange(granule));
    if (operation == "notEquals")
        mask = !mask;
    return mask;
}

Range conditionRange(const String & operation, const Field & bound)
{
    if (operation == "equals" || operation == "notEquals")
        return Range(bound);
    if (operation == "less")
        return Range::createRightBounded(bound, false);
    if (operation == "lessOrEquals")
        return Range::createRightBounded(bound, true);
    if (operation == "greater")
        return Range::createLeftBounded(bound, false);
    if (operation == "greaterOrEquals")
        return Range::createLeftBounded(bound, true);
    throw std::logic_error("unknown comparison operation");
}

}

TEST(MergeTreeIndexConditionMinMaxDifferential, TypedBoundsMatchScalarRangeEvaluation)
{
    auto context = getRegisteredContext();
    const std::vector<String> operations{
        "equals", "notEquals", "less", "lessOrEquals", "greater", "greaterOrEquals"};

    for (const auto & type_case : makeTypeCases())
    {
        auto index = makeIndex(type_case.index_type, context);
        auto bulk = makeBulkGranules(index, type_case.intervals);

        for (const auto & operation : operations)
        {
            for (const auto & bound : type_case.bounds)
            {
                SCOPED_TRACE(fmt::format("type={} operation={} bound={}", type_case.name, operation, bound));
                auto shape = makeComparison(context, type_case.index_type, type_case.bound_type, bound, operation);
                ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context, true);
                MergeTreeIndexConditionMinMax condition(index, filter_dag, context);
                if (!type_case.expect_bulk_fast_path)
                {
                    EXPECT_FALSE(condition.hasBulkFastPath());
                    continue;
                }
                ASSERT_TRUE(condition.hasBulkFastPath());

                const auto actual = MergeTreeIndexConditionMinMaxTestAccess::bulk(condition, *bulk);
                ASSERT_EQ(actual.size(), type_case.intervals.size());

                const auto predicate_range = conditionRange(operation, bound);
                for (size_t i = 0; i < type_case.intervals.size(); ++i)
                {
                    const auto & [min, max] = type_case.intervals[i];
                    const Range granule(min, true, max, true);
                    const auto expected = MergeTreeIndexConditionMinMaxTestAccess::scalar(condition, granule);
                    EXPECT_EQ(actual[i], expected) << "granule=" << i;
                    EXPECT_EQ(expected, expectedMaskForOperation(operation, predicate_range, granule)) << "granule=" << i;
                }
            }
        }
    }
}

TEST(MergeTreeIndexConditionMinMaxDifferential, FieldAndTypedComparisonAgreeForFiniteValues)
{
    auto context = getRegisteredContext();
    const std::vector<String> operations{"equals", "less", "lessOrEquals"};

    for (const auto & type_case : makeTypeCases())
    {
        for (const auto & [min, max] : type_case.intervals)
        {
            for (const auto & left : {min, max})
            {
                for (const auto & right : type_case.bounds)
                {
                    for (const auto & operation : operations)
                    {
                        SCOPED_TRACE(fmt::format(
                            "type={} operation={} left={} right={}", type_case.name, operation, left, right));
                        bool expected = false;
                        if (operation == "equals")
                            expected = accurateEquals(left, right);
                        else if (operation == "less")
                            expected = accurateLess(left, right);
                        else
                            expected = accurateLessOrEqual(left, right);

                        EXPECT_EQ(
                            evaluateTypedComparison(
                                context, operation, type_case.index_type, left, type_case.bound_type, right),
                            expected);
                    }
                }
            }
        }
    }
}

TEST(MergeTreeIndexConditionMinMaxDifferential, NaNOrderingDifferenceIsExplicit)
{
    auto context = getRegisteredContext();
    auto float64 = std::make_shared<DataTypeFloat64>();
    const Field finite = Float64(1.0);
    const Field nan = std::numeric_limits<Float64>::quiet_NaN();

    /// Range ordering puts NaN after every finite value, while SQL comparisons with NaN are
    /// unordered. The minmax DAG's dedicated NaN handling bridges this intentional difference.
    EXPECT_TRUE(accurateLess(finite, nan));
    EXPECT_FALSE(evaluateTypedComparison(context, "less", float64, finite, float64, nan));
    EXPECT_FALSE(accurateEquals(nan, nan));
    EXPECT_FALSE(evaluateTypedComparison(context, "equals", float64, nan, float64, nan));
}

TEST(MergeTreeIndexConditionMinMaxDifferential, AllNaNGranuleMatchesScalar)
{
    auto context = getRegisteredContext();
    auto float64 = std::make_shared<DataTypeFloat64>();
    auto index = makeIndex(float64, context);
    const Field nan = std::numeric_limits<Float64>::quiet_NaN();
    auto bulk = makeBulkGranules(index, {{nan, nan}});
    auto shape = makeComparison(context, float64, float64, Float64(0), "greater");
    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context, true);
    MergeTreeIndexConditionMinMax condition(index, filter_dag, context);
    ASSERT_TRUE(condition.hasBulkFastPath());

    const auto actual = MergeTreeIndexConditionMinMaxTestAccess::bulk(condition, *bulk);
    ASSERT_EQ(actual.size(), 1);
    EXPECT_EQ(actual[0], MergeTreeIndexConditionMinMaxTestAccess::scalar(condition, Range(nan, true, nan, true)));
    EXPECT_FALSE(actual[0].can_be_true);
}

TEST(MergeTreeIndexConditionMinMaxDifferential, MixedFiniteAndNaNGranuleMatchesScalar)
{
    auto context = getRegisteredContext();
    auto float64 = std::make_shared<DataTypeFloat64>();
    auto index = makeIndex(float64, context);
    const Field min = Float64(1);
    const Field max = std::numeric_limits<Float64>::quiet_NaN();
    auto bulk = makeBulkGranules(index, {{min, max}});
    auto shape = makeComparison(context, float64, float64, Float64(0), "greater");
    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context, true);
    MergeTreeIndexConditionMinMax condition(index, filter_dag, context);
    ASSERT_TRUE(condition.hasBulkFastPath());

    const auto actual = MergeTreeIndexConditionMinMaxTestAccess::bulk(condition, *bulk);
    ASSERT_EQ(actual.size(), 1);
    EXPECT_EQ(actual[0], MergeTreeIndexConditionMinMaxTestAccess::scalar(condition, Range(min, true, max, true)));
    EXPECT_TRUE(actual[0].can_be_true);
    EXPECT_TRUE(actual[0].can_be_false);
}

}
