#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSumPromQL.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cfenv>
#include <cmath>
#include <limits>
#include <utility>
#include <vector>


using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int TOO_MANY_ROWS;
}

namespace
{

constexpr Float64 half_ulp = 0x1p-53;
constexpr Float64 rounded_sum = 1. + 0x1p-52;
constexpr size_t exact_limb_count = 34;
constexpr size_t exact_state_size = 282;
constexpr UInt64 canonical_nan_bits = 0x7ff8000000000000;
using ExactLimbs = std::array<UInt64, exact_limb_count>;

AggregateFunctionPtr makeFunction(const String & name = "sumPromQL", DataTypePtr argument = std::make_shared<DataTypeFloat64>())
{
    tryRegisterAggregateFunctions();
    AggregateFunctionProperties properties;
    return AggregateFunctionFactory::instance().get(name, NullsAction::EMPTY, DataTypes{argument}, {}, properties);
}

DataTypePtr nullableGridType()
{
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()));
}

class AggregateState
{
public:
    explicit AggregateState(AggregateFunctionPtr function_)
        : function(std::move(function_)), column(function->getStateType()->createColumn())
    {
        states().insertDefault();
    }

    ColumnAggregateFunction & states() { return assert_cast<ColumnAggregateFunction &>(*column); }
    AggregateDataPtr place() { return states().getData().front(); }
    ConstAggregateDataPtr place() const { return assert_cast<const ColumnAggregateFunction &>(*column).getData().front(); }
    Arena & arena() { return states().createOrGetArena(); }

    void add(const IColumn & input)
    {
        const IColumn * input_ptr = &input;
        for (size_t row = 0; row < input.size(); ++row)
            function->add(place(), &input_ptr, row, &arena());
    }

    void merge(const AggregateState & rhs)
    {
        function->merge(place(), rhs.place(), &arena());
    }

    MutableColumnPtr finalize()
    {
        auto result = function->getResultType()->createColumn();
        function->insertResultInto(place(), *result, &arena());
        return result;
    }

    String serialize() const
    {
        WriteBufferFromOwnString buffer;
        function->serialize(place(), buffer, std::nullopt);
        return buffer.str();
    }

    void deserialize(const String & serialized)
    {
        ReadBufferFromString buffer(serialized);
        function->deserialize(place(), buffer, std::nullopt, &arena());
    }

private:
    AggregateFunctionPtr function;
    MutableColumnPtr column;
};

void addFloats(AggregateState & state, const std::vector<Float64> & values)
{
    auto input = ColumnFloat64::create();
    for (Float64 value : values)
        input->getData().push_back(value);
    state.add(*input);
}

void addGrids(AggregateState & state, const std::vector<Array> & grids)
{
    auto input = nullableGridType()->createColumn();
    for (const auto & grid : grids)
        input->insert(Field{grid});
    state.add(*input);
}

Float64 scalarResult(AggregateState & state)
{
    auto result = state.finalize();
    return (*result)[0].safeGet<Float64>();
}

void expectBits(Float64 actual, Float64 expected)
{
    EXPECT_EQ(std::bit_cast<UInt64>(actual), std::bit_cast<UInt64>(expected));
}

String serializeData(const AggregateFunctionSumPromQLData & data)
{
    WriteBufferFromOwnString buffer;
    data.serialize(buffer);
    return buffer.str();
}

void deserializeData(AggregateFunctionSumPromQLData & data, const String & serialized)
{
    ReadBufferFromString buffer(serialized);
    data.deserialize(buffer);
}

void setLittleEndianUInt64(String & bytes, size_t offset, UInt64 value)
{
    for (size_t byte = 0; byte < sizeof(value); ++byte)
        bytes[offset + byte] = static_cast<char>(value >> (8 * byte));
}

String exactWireState(UInt64 count, UInt8 flags, const ExactLimbs & limbs = {})
{
    String bytes(exact_state_size, '\0');
    bytes[0] = char{2};
    setLittleEndianUInt64(bytes, 1, count);
    bytes[9] = static_cast<char>(flags);
    for (size_t limb = 0; limb < limbs.size(); ++limb)
        setLittleEndianUInt64(bytes, 10 + sizeof(UInt64) * limb, limbs[limb]);
    return bytes;
}

ExactLimbs maximumFiniteLimbs()
{
    ExactLimbs limbs{};
    /// `max_finite / 2^-1074 = (2^53 - 1) * 2^2045`.
    const UInt64 significand = (UInt64{1} << 53) - 1;
    limbs[31] = significand << 61;
    limbs[32] = significand >> 3;
    return limbs;
}

ExactLimbs negateLimbs(ExactLimbs limbs)
{
    UInt64 carry = 1;
    for (UInt64 & limb : limbs)
    {
        limb = ~limb + carry;
        carry = carry != 0 && limb == 0;
    }
    return limbs;
}

void expectDataRejectedWithoutMutation(AggregateFunctionSumPromQLData & data, const String & invalid, int expected_code = 0)
{
    const String before = serializeData(data);
    try
    {
        deserializeData(data, invalid);
        ADD_FAILURE() << "Invalid scalar state was accepted";
    }
    catch (const Exception & exception)
    {
        if (expected_code != 0)
            EXPECT_EQ(exception.code(), expected_code);
    }
    EXPECT_EQ(serializeData(data), before);
}

void expectAggregateRejectedWithoutMutation(AggregateState & state, const String & invalid, int expected_code = 0)
{
    const String before = state.serialize();
    try
    {
        state.deserialize(invalid);
        ADD_FAILURE() << "Invalid scalar aggregate state was accepted";
    }
    catch (const Exception & exception)
    {
        if (expected_code != 0)
            EXPECT_EQ(exception.code(), expected_code);
    }
    EXPECT_EQ(state.serialize(), before);
}

template <typename Operation>
void expectCountOverflow(const Operation & operation)
{
    try
    {
        operation();
        ADD_FAILURE() << "Overflowing input count was accepted";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::TOO_MANY_ROWS);
    }
}

std::vector<String> orderedMergeTrees(
    const AggregateFunctionPtr & function, const std::vector<String> & leaves, size_t begin, size_t end)
{
    if (end - begin == 1)
        return {leaves[begin]};

    std::vector<String> results;
    for (size_t split = begin + 1; split < end; ++split)
    {
        const auto left_trees = orderedMergeTrees(function, leaves, begin, split);
        const auto right_trees = orderedMergeTrees(function, leaves, split, end);
        for (const auto & left_bytes : left_trees)
        {
            for (const auto & right_bytes : right_trees)
            {
                AggregateState left(function);
                AggregateState right(function);
                left.deserialize(left_bytes);
                right.deserialize(right_bytes);
                left.merge(right);
                results.push_back(left.serialize());
            }
        }
    }
    return results;
}

class FloatingEnvironmentGuard
{
public:
    FloatingEnvironmentGuard() : valid(std::fegetenv(&environment) == 0) {}

    ~FloatingEnvironmentGuard()
    {
        if (valid)
            std::fesetenv(&environment);
    }

    bool isValid() const { return valid; }

private:
    std::fenv_t environment{};
    bool valid;
};

}

TEST(SumPromQL, Float64OnlyAndNoParameters)
{
    auto function = makeFunction();
    EXPECT_EQ(function->getResultType()->getTypeId(), TypeIndex::Float64);
    EXPECT_THROW(makeFunction("sumPromQL", std::make_shared<DataTypeFloat32>()), Exception);
    EXPECT_THROW(makeFunction("sumPromQL", std::make_shared<DataTypeUInt64>()), Exception);
    EXPECT_THROW(
        makeFunction("sumPromQLForEach", std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>())), Exception);

    AggregateFunctionProperties properties;
    EXPECT_THROW(
        AggregateFunctionFactory::instance().get("sumPromQL", NullsAction::EMPTY, {}, {}, properties), Exception);
    EXPECT_THROW(
        AggregateFunctionFactory::instance().get(
            "sumPromQL", NullsAction::EMPTY, DataTypes{std::make_shared<DataTypeFloat64>()}, Array{Field{UInt64{1}}}, properties),
        Exception);
}

TEST(SumPromQL, AllTwentyFourPermutationsOfRoundingFixture)
{
    auto function = makeFunction();
    const std::array<Float64, 4> values{1., 0., half_ulp, half_ulp};
    std::array<size_t, 4> indices{0, 1, 2, 3};
    size_t permutations = 0;
    do
    {
        AggregateState state(function);
        addFloats(state, {values[indices[0]], values[indices[1]], values[indices[2]], values[indices[3]]});
        const Float64 result = scalarResult(state);
        expectBits(result, rounded_sum);
        EXPECT_EQ(std::ceil(result), 2.);
        ++permutations;
    }
    while (std::next_permutation(indices.begin(), indices.end()));
    EXPECT_EQ(permutations, 24u);
}

TEST(SumPromQL, CancellationAndCompleteStateMerge)
{
    auto function = makeFunction();
    AggregateState direct(function);
    addFloats(direct, {1e16, 1., -1e16});
    expectBits(scalarResult(direct), 1.);

    AggregateState left(function);
    AggregateState right(function);
    addFloats(left, {1e16, 1.});
    addFloats(right, {-1e16});
    right.merge(left);
    expectBits(scalarResult(right), 1.);

    AggregateState ordinary(makeFunction("sum"));
    addFloats(ordinary, {1e16, 1., -1e16});
    expectBits(scalarResult(ordinary), 0.);
}

TEST(SumPromQL, CorrectionMergeAvoidsDoubleRounding)
{
    auto function = makeFunction();
    for (bool reverse : {false, true})
    {
        SCOPED_TRACE(reverse ? "right merges left" : "left merges right");
        AggregateState left(function);
        AggregateState right(function);
        addFloats(left, {0x1p53, 1., -0x1p53});
        addFloats(right, {1., half_ulp, -1., half_ulp});
        if (reverse)
            right.merge(left);
        else
            left.merge(right);

        const Float64 result = scalarResult(reverse ? right : left);
        expectBits(result, rounded_sum);
        EXPECT_EQ(std::ceil(result), 2.);
    }
}

TEST(SumPromQL, RetainsResidualsBelowTheFirstRoundingBoundary)
{
    auto function = makeFunction();
    AggregateState state(function);
    addFloats(state, {1., half_ulp, 0x1p-106, 0x1p-106});
    const String before = state.serialize();
    expectBits(scalarResult(state), rounded_sum);
    EXPECT_EQ(state.serialize(), before);

    AggregateState restored(function);
    restored.deserialize(before);
    addFloats(restored, {-1., -half_ulp});
    expectBits(scalarResult(restored), 0x1p-105);
}

TEST(SumPromQL, PermutationsPartitionsAndOrderedMergeTreesHaveIdenticalExactState)
{
    auto function = makeFunction();
    struct Fixture
    {
        std::array<Float64, 4> values;
        Float64 expected;
    };
    const std::array<Fixture, 3> fixtures{{
        {{1., half_ulp, 0x1p-106, 0x1p-106}, rounded_sum},
        {{0x1p53, 1., -0x1p53, -0.}, 1.},
        {{std::numeric_limits<Float64>::max(), std::numeric_limits<Float64>::max(),
          -std::numeric_limits<Float64>::max(), std::numeric_limits<Float64>::denorm_min()},
         std::numeric_limits<Float64>::max()}}};

    for (const auto & fixture : fixtures)
    {
        AggregateState canonical(function);
        addFloats(canonical, std::vector<Float64>(fixture.values.begin(), fixture.values.end()));
        const String expected_state = canonical.serialize();
        std::array<size_t, 4> indices{0, 1, 2, 3};
        do
        {
            for (unsigned int cuts = 0; cuts < 8; ++cuts)
            {
                SCOPED_TRACE(::testing::Message() << "partition mask " << cuts << ", order " << ::testing::PrintToString(indices));
                std::vector<String> leaves;
                std::vector<Float64> block;
                for (size_t i = 0; i < indices.size(); ++i)
                {
                    block.push_back(fixture.values[indices[i]]);
                    if (i + 1 == indices.size() || (cuts & (1u << i)) != 0)
                    {
                        AggregateState leaf(function);
                        addFloats(leaf, block);
                        leaves.push_back(leaf.serialize());
                        block.clear();
                    }
                }
                const auto trees = orderedMergeTrees(function, leaves, 0, leaves.size());
                ASSERT_FALSE(trees.empty());
                for (const String & tree : trees)
                {
                    EXPECT_EQ(tree, expected_state);
                    AggregateState result(function);
                    result.deserialize(tree);
                    expectBits(scalarResult(result), fixture.expected);
                    EXPECT_EQ(result.serialize(), tree);
                }
            }
        }
        while (std::next_permutation(indices.begin(), indices.end()));
    }
}

TEST(SumPromQL, NonFiniteAddAndMerge)
{
    auto function = makeFunction();
    const Float64 inf = std::numeric_limits<Float64>::infinity();
    const Float64 nan = std::bit_cast<Float64>(UInt64{0xfff8000000001234});
    const Float64 max = std::numeric_limits<Float64>::max();
    const std::vector<std::vector<Float64>> cases{
        {inf, 1.}, {1., inf}, {-inf, 1.}, {1., -inf}, {inf, inf},
        {inf, -inf}, {-inf, inf}, {nan, 1.}, {1., nan}, {max, max}};
    for (const auto & values : cases)
    {
        Float64 expected = 0;
        for (Float64 value : values)
            expected += value;

        AggregateState direct(function);
        addFloats(direct, values);
        AggregateState left(function);
        AggregateState right(function);
        addFloats(left, {values.front()});
        addFloats(right, std::vector<Float64>(values.begin() + 1, values.end()));
        left.merge(right);
        for (Float64 result : {scalarResult(direct), scalarResult(left)})
        {
            if (std::isnan(expected))
                EXPECT_EQ(std::bit_cast<UInt64>(result), canonical_nan_bits);
            else
            {
                EXPECT_TRUE(std::isinf(result));
                EXPECT_EQ(std::signbit(result), std::signbit(expected));
            }
        }
    }

    /// An intermediate rounded overflow does not discard the exact finite state.
    AggregateState overflow(function);
    addFloats(overflow, {max, half_ulp, max, -max});
    expectBits(scalarResult(overflow), max);
}

TEST(SumPromQL, AllNonFiniteFlagsAreInvariantAcrossPermutationsAndMergeTrees)
{
    auto function = makeFunction();
    const Float64 inf = std::numeric_limits<Float64>::infinity();
    const std::array<Float64, 3> values{std::bit_cast<Float64>(UInt64{0xfff8000000001234}), inf, -inf};
    const String expected_state = exactWireState(3, 7);
    std::array<size_t, 3> indices{0, 1, 2};
    do
    {
        SCOPED_TRACE(::testing::Message() << "order " << ::testing::PrintToString(indices));
        AggregateState direct(function);
        addFloats(direct, {values[indices[0]], values[indices[1]], values[indices[2]]});
        EXPECT_EQ(direct.serialize(), expected_state);
        EXPECT_EQ(std::bit_cast<UInt64>(scalarResult(direct)), canonical_nan_bits);

        for (unsigned int cuts = 0; cuts < 4; ++cuts)
        {
            SCOPED_TRACE(::testing::Message() << "partition mask " << cuts);
            std::vector<String> leaves;
            std::vector<Float64> block;
            for (size_t i = 0; i < indices.size(); ++i)
            {
                block.push_back(values[indices[i]]);
                if (i + 1 == indices.size() || (cuts & (1u << i)) != 0)
                {
                    AggregateState leaf(function);
                    addFloats(leaf, block);
                    leaves.push_back(leaf.serialize());
                    block.clear();
                }
            }
            const auto trees = orderedMergeTrees(function, leaves, 0, leaves.size());
            ASSERT_FALSE(trees.empty());
            for (const String & tree : trees)
            {
                EXPECT_EQ(tree, expected_state);
                AggregateState result(function);
                result.deserialize(tree);
                EXPECT_EQ(std::bit_cast<UInt64>(scalarResult(result)), canonical_nan_bits);
                EXPECT_EQ(result.serialize(), expected_state);
            }
        }
    }
    while (std::next_permutation(indices.begin(), indices.end()));
}

TEST(SumPromQL, FiniteOverflowCanRecoverAfterFinalizationAndMerge)
{
    auto function = makeFunction();
    const Float64 max = std::numeric_limits<Float64>::max();
    const Float64 inf = std::numeric_limits<Float64>::infinity();
    for (Float64 sign : {1., -1.})
    {
        AggregateState direct(function);
        addFloats(direct, {sign * max, sign * max});
        const String before_get = direct.serialize();
        expectBits(scalarResult(direct), sign * inf);
        EXPECT_EQ(direct.serialize(), before_get);
        EXPECT_EQ(static_cast<UInt8>(before_get[9]), 0);
        addFloats(direct, {-sign * max});
        expectBits(scalarResult(direct), sign * max);

        AggregateState restored(function);
        restored.deserialize(before_get);
        AggregateState cancelling(function);
        addFloats(cancelling, {-sign * max});
        restored.merge(cancelling);
        expectBits(scalarResult(restored), sign * max);
        EXPECT_EQ(restored.serialize(), direct.serialize());
    }
}

TEST(SumPromQL, OverflowTieAndOneSubnormalBelowIt)
{
    auto function = makeFunction();
    const Float64 max = std::numeric_limits<Float64>::max();
    const Float64 inf = std::numeric_limits<Float64>::infinity();
    const Float64 tiny = std::numeric_limits<Float64>::denorm_min();
    for (Float64 sign : {1., -1.})
    {
        AggregateState state(function);
        addFloats(state, {sign * max, sign * 0x1p970});
        expectBits(scalarResult(state), sign * inf);
        addFloats(state, {-sign * tiny});
        expectBits(scalarResult(state), sign * max);

        AggregateState left(function);
        AggregateState right(function);
        addFloats(left, {sign * max});
        addFloats(right, {sign * 0x1p970, -sign * tiny});
        left.merge(right);
        expectBits(scalarResult(left), sign * max);
        EXPECT_EQ(left.serialize(), state.serialize());
    }
}

TEST(SumPromQL, RoundedFiniteOverflowIsNotAnInfinityInput)
{
    auto function = makeFunction();
    const Float64 max = std::numeric_limits<Float64>::max();
    const Float64 inf = std::numeric_limits<Float64>::infinity();
    AggregateState finite_overflow(function);
    addFloats(finite_overflow, {max, max});
    expectBits(scalarResult(finite_overflow), inf);
    AggregateState negative_infinity(function);
    addFloats(negative_infinity, {-inf});
    finite_overflow.merge(negative_infinity);
    expectBits(scalarResult(finite_overflow), -inf);

    AggregateState opposing_infinities(function);
    addFloats(opposing_infinities, {inf});
    opposing_infinities.merge(negative_infinity);
    EXPECT_EQ(std::bit_cast<UInt64>(scalarResult(opposing_infinities)), canonical_nan_bits);

    AggregateState signalling_nan(function);
    addFloats(signalling_nan, {std::bit_cast<Float64>(UInt64{0x7ff0000000000001})});
    EXPECT_EQ(std::bit_cast<UInt64>(scalarResult(signalling_nan)), canonical_nan_bits);
}

TEST(SumPromQL, SubnormalAndTiesToEvenBoundaries)
{
    auto function = makeFunction();
    const Float64 tiny = std::numeric_limits<Float64>::denorm_min();
    const Float64 largest_subnormal = std::bit_cast<Float64>(UInt64{0x000fffffffffffff});
    const Float64 minimum_normal = std::numeric_limits<Float64>::min();
    const Float64 second_successor = std::bit_cast<Float64>(UInt64{0x3ff0000000000002});
    const std::vector<std::pair<std::vector<Float64>, Float64>> cases{
        {{tiny, tiny, -tiny}, tiny},
        {{minimum_normal, -tiny}, largest_subnormal},
        {{-minimum_normal, tiny}, -largest_subnormal},
        {{largest_subnormal, tiny}, minimum_normal},
        {{1., half_ulp}, 1.},
        {{rounded_sum, half_ulp}, second_successor},
        {{-1., -half_ulp}, -1.},
        {{-rounded_sum, -half_ulp}, -second_successor},
        {{minimum_normal, -minimum_normal, -0.}, 0.}};
    for (const auto & [values, expected] : cases)
    {
        AggregateState state(function);
        addFloats(state, values);
        expectBits(scalarResult(state), expected);
    }
}

TEST(SumPromQL, FinalizationIsIndependentOfFloatingPointRoundingMode)
{
    FloatingEnvironmentGuard restore_environment;
    ASSERT_TRUE(restore_environment.isValid());
    auto function = makeFunction();
    for (int mode : {FE_TONEAREST, FE_DOWNWARD, FE_UPWARD, FE_TOWARDZERO})
    {
        SCOPED_TRACE(mode);
        ASSERT_EQ(std::fesetround(mode), 0);
        AggregateState even(function);
        addFloats(even, {1., half_ulp});
        expectBits(scalarResult(even), 1.);

        AggregateState above_tie(function);
        addFloats(above_tie, {1., half_ulp, 0x1p-106, 0x1p-106});
        expectBits(scalarResult(above_tie), rounded_sum);

        AggregateState negative(function);
        addFloats(negative, {-1., -half_ulp, -0x1p-106, -0x1p-106});
        expectBits(scalarResult(negative), -rounded_sum);
        EXPECT_EQ(std::fegetround(), mode);
    }
}

TEST(SumPromQL, EmptyAndSignedZero)
{
    auto function = makeFunction();
    AggregateState empty(function);
    expectBits(scalarResult(empty), 0.);
    for (const auto & values : std::vector<std::vector<Float64>>{{-0.}, {0., -0.}, {-0., 0.}})
    {
        AggregateState state(function);
        addFloats(state, values);
        expectBits(scalarResult(state), 0.);
    }
}

TEST(SumPromQL, NullablePresenceAndUnequalForEachLengths)
{
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());
    auto nullable_function = makeFunction("sumPromQL", nullable_type);
    AggregateState nulls(nullable_function);
    EXPECT_TRUE(nulls.finalize()->isNullAt(0));
    auto input = nullable_type->createColumn();
    input->insertDefault();
    input->insertDefault();
    nulls.add(*input);
    EXPECT_TRUE(nulls.finalize()->isNullAt(0));

    AggregateState present(nullable_function);
    auto present_input = nullable_type->createColumn();
    present_input->insert(Field{0.});
    present.add(*present_input);
    present.merge(nulls);
    expectBits(scalarResult(present), 0.);

    auto function = makeFunction("sumPromQLForEach", nullableGridType());
    AggregateState empty(function);
    addGrids(empty, {Array{}});
    Field empty_result = (*empty.finalize())[0];
    EXPECT_TRUE(empty_result.safeGet<Array>().empty());

    AggregateState left(function);
    AggregateState right(function);
    addGrids(left, {Array{Field{}, Field{1.}}});
    addGrids(right, {Array{Field{2.}}, Array{Field{}, Field{3.}, Field{}}});
    left.merge(right);
    Field result = (*left.finalize())[0];
    const auto & values = result.safeGet<Array>();
    ASSERT_EQ(values.size(), 3u);
    expectBits(values[0].safeGet<Float64>(), 2.);
    expectBits(values[1].safeGet<Float64>(), 4.);
    EXPECT_TRUE(values[2].isNull());

    AggregateState grows(function);
    addGrids(grows, {Array{Field{1.}}, Array{Field{half_ulp}, Field{}}, Array{Field{half_ulp}}});
    Field grows_result = (*grows.finalize())[0];
    ASSERT_EQ(grows_result.safeGet<Array>().size(), 2u);
    expectBits(grows_result.safeGet<Array>()[0].safeGet<Float64>(), rounded_sum);
    EXPECT_TRUE(grows_result.safeGet<Array>()[1].isNull());
}

TEST(SumPromQL, OwnedForEachStatesSurviveSourceDestruction)
{
    auto function = makeFunction("sumPromQLForEach", nullableGridType());
    auto owned_column = function->getStateType()->createColumn();
    auto & owned = assert_cast<ColumnAggregateFunction &>(*owned_column);
    Array finalized_left;
    Array finalized_right;
    {
        AggregateState left(function);
        AggregateState right(function);
        addGrids(left, {Array{Field{1.}}, Array{Field{half_ulp}}});
        addGrids(right, {Array{Field{0.}}, Array{Field{half_ulp}}});
        Field left_result = (*left.finalize())[0];
        Field right_result = (*right.finalize())[0];
        finalized_left = left_result.safeGet<Array>();
        finalized_right = right_result.safeGet<Array>();
        expectBits(finalized_left.front().safeGet<Float64>(), 1.);
        expectBits(finalized_right.front().safeGet<Float64>(), half_ulp);
        owned.insertFrom(left.place());
        owned.insertFrom(right.place());
        EXPECT_NE(owned.getData()[0], left.place());
        EXPECT_NE(owned.getData()[1], right.place());
    }

    function->merge(owned.getData()[0], owned.getData()[1], &owned.createOrGetArena());
    auto output = function->getResultType()->createColumn();
    function->insertResultInto(owned.getData()[0], *output, &owned.createOrGetArena());
    Field result = (*output)[0];
    const Float64 value = result.safeGet<Array>().front().safeGet<Float64>();
    expectBits(value, rounded_sum);
    EXPECT_EQ(std::ceil(value), 2.);

    /// Finalizing the source grids first irreversibly loses the left lane's half-ULP correction.
    AggregateState finalized_control(function);
    addGrids(finalized_control, {finalized_left, finalized_right});
    Field control = (*finalized_control.finalize())[0];
    const Float64 control_value = control.safeGet<Array>().front().safeGet<Float64>();
    expectBits(control_value, 1.);
    EXPECT_EQ(std::ceil(control_value), 1.);
}

TEST(SumPromQL, SerializationPreservesExactResidualAndRejectsOldLayout)
{
    auto function = makeFunction();
    AggregateState source(function);
    addFloats(source, {1., half_ulp});
    const String serialized = source.serialize();
    ExactLimbs limbs{};
    /// `1 / 2^-1074 = 2^1074` and `2^-53 / 2^-1074 = 2^1021`.
    limbs[16] = UInt64{1} << 50;
    limbs[15] = UInt64{1} << 61;
    const String expected = exactWireState(2, 0, limbs);
    ASSERT_EQ(serialized.size(), exact_state_size);
    EXPECT_EQ(serialized, expected);

    AggregateFunctionSumPromQLData data;
    data.add(1.);
    data.add(half_ulp);
    EXPECT_EQ(serializeData(data), expected);

    AggregateState restored(function);
    restored.deserialize(serialized);
    addFloats(restored, {half_ulp});
    expectBits(scalarResult(restored), rounded_sum);

    for (UInt8 version : {UInt8{0}, UInt8{1}, UInt8{3}, UInt8{255}})
    {
        String invalid = serialized;
        invalid[0] = static_cast<char>(version);
        expectAggregateRejectedWithoutMutation(restored, invalid, ErrorCodes::INCORRECT_DATA);
        expectDataRejectedWithoutMutation(data, invalid, ErrorCodes::INCORRECT_DATA);
    }
    String legacy_state(17, '\0');
    legacy_state[0] = char{1};
    setLittleEndianUInt64(legacy_state, 1, std::bit_cast<UInt64>(Float64{1.}));
    setLittleEndianUInt64(legacy_state, 9, std::bit_cast<UInt64>(half_ulp));
    expectAggregateRejectedWithoutMutation(restored, legacy_state, ErrorCodes::INCORRECT_DATA);
    expectDataRejectedWithoutMutation(data, legacy_state, ErrorCodes::INCORRECT_DATA);
    expectBits(scalarResult(restored), rounded_sum);

    auto grid_function = makeFunction("sumPromQLForEach", nullableGridType());
    AggregateState grid(grid_function);
    addGrids(grid, {Array{Field{1.}, Field{}}, Array{Field{half_ulp}}});
    AggregateState restored_grid(grid_function);
    restored_grid.deserialize(grid.serialize());
    addGrids(restored_grid, {Array{Field{half_ulp}}});
    Field result = (*restored_grid.finalize())[0];
    ASSERT_EQ(result.safeGet<Array>().size(), 2u);
    expectBits(result.safeGet<Array>()[0].safeGet<Float64>(), rounded_sum);
    EXPECT_TRUE(result.safeGet<Array>()[1].isNull());
}

TEST(SumPromQL, ConsecutiveSerializedRecordsConsumeExactlyOneStateEach)
{
    auto function = makeFunction();
    AggregateState first_source(function);
    AggregateState second_source(function);
    addFloats(first_source, {1., half_ulp, 0x1p-106, 0x1p-106});
    addFloats(second_source, {-std::numeric_limits<Float64>::max()});
    const String first_record = first_source.serialize();
    const String second_record = second_source.serialize();
    ASSERT_EQ(first_record.size(), exact_state_size);
    ASSERT_EQ(second_record.size(), exact_state_size);
    const String records = first_record + second_record;

    ReadBufferFromString data_buffer(records);
    AggregateFunctionSumPromQLData first_data;
    AggregateFunctionSumPromQLData second_data;
    first_data.deserialize(data_buffer);
    EXPECT_EQ(data_buffer.count(), exact_state_size);
    EXPECT_EQ(serializeData(first_data), first_record);
    expectBits(first_data.get(), rounded_sum);
    second_data.deserialize(data_buffer);
    EXPECT_EQ(data_buffer.count(), 2 * exact_state_size);
    EXPECT_TRUE(data_buffer.eof());
    EXPECT_EQ(serializeData(second_data), second_record);
    expectBits(second_data.get(), -std::numeric_limits<Float64>::max());

    ReadBufferFromString aggregate_buffer(records);
    AggregateState first_restored(function);
    AggregateState second_restored(function);
    function->deserialize(first_restored.place(), aggregate_buffer, std::nullopt, &first_restored.arena());
    EXPECT_EQ(aggregate_buffer.count(), exact_state_size);
    EXPECT_EQ(first_restored.serialize(), first_record);
    expectBits(scalarResult(first_restored), rounded_sum);
    function->deserialize(second_restored.place(), aggregate_buffer, std::nullopt, &second_restored.arena());
    EXPECT_EQ(aggregate_buffer.count(), 2 * exact_state_size);
    EXPECT_TRUE(aggregate_buffer.eof());
    EXPECT_EQ(second_restored.serialize(), second_record);
    expectBits(scalarResult(second_restored), -std::numeric_limits<Float64>::max());
}

TEST(SumPromQL, EveryTruncatedScalarPrefixPreservesThePreviousState)
{
    auto function = makeFunction();
    AggregateState source(function);
    addFloats(source, {-std::numeric_limits<Float64>::max(), half_ulp, 0x1p-106});
    const String serialized = source.serialize();
    ASSERT_EQ(serialized.size(), exact_state_size);

    AggregateState restored(function);
    addFloats(restored, {1., half_ulp, 0x1p-106, 0x1p-106});
    AggregateFunctionSumPromQLData data;
    for (Float64 value : {1., half_ulp, 0x1p-106, 0x1p-106})
        data.add(value);
    for (size_t length = 0; length < serialized.size(); ++length)
    {
        SCOPED_TRACE(length);
        const String truncated = serialized.substr(0, length);
        expectAggregateRejectedWithoutMutation(restored, truncated);
        expectDataRejectedWithoutMutation(data, truncated);
    }
    expectBits(scalarResult(restored), rounded_sum);
    expectBits(data.get(), rounded_sum);
}

TEST(SumPromQL, MalformedScalarFlagsCountAndMagnitudePreserveThePreviousState)
{
    ExactLimbs one_unit{};
    one_unit[0] = 1;
    ExactLimbs above_maximum = maximumFiniteLimbs();
    above_maximum[0] = 1;
    ExactLimbs lowest_twos_complement{};
    lowest_twos_complement.back() = UInt64{1} << 63;
    const std::vector<String> invalid_states{
        exactWireState(1, 8),
        exactWireState(1, 255),
        exactWireState(0, 1),
        exactWireState(1, 3),
        exactWireState(2, 7),
        exactWireState(0, 0, one_unit),
        exactWireState(1, 1, one_unit),
        exactWireState(1, 0, above_maximum),
        exactWireState(1, 0, negateLimbs(above_maximum)),
        exactWireState(std::numeric_limits<UInt64>::max(), 0, lowest_twos_complement)};

    auto function = makeFunction();
    AggregateState restored(function);
    addFloats(restored, {1., half_ulp, 0x1p-106, 0x1p-106});
    AggregateFunctionSumPromQLData data;
    for (Float64 value : {1., half_ulp, 0x1p-106, 0x1p-106})
        data.add(value);
    for (size_t i = 0; i < invalid_states.size(); ++i)
    {
        SCOPED_TRACE(i);
        expectAggregateRejectedWithoutMutation(restored, invalid_states[i], ErrorCodes::INCORRECT_DATA);
        expectDataRejectedWithoutMutation(data, invalid_states[i], ErrorCodes::INCORRECT_DATA);
    }

    for (Float64 sign : {1., -1.})
    {
        const ExactLimbs limbs = sign > 0 ? maximumFiniteLimbs() : negateLimbs(maximumFiniteLimbs());
        const String boundary = exactWireState(1, 0, limbs);
        restored.deserialize(boundary);
        deserializeData(data, boundary);
        EXPECT_EQ(restored.serialize(), boundary);
        EXPECT_EQ(serializeData(data), boundary);
        expectBits(scalarResult(restored), sign * std::numeric_limits<Float64>::max());
        expectBits(data.get(), sign * std::numeric_limits<Float64>::max());
    }

    const String all_nonfinite_kinds = exactWireState(3, 7);
    restored.deserialize(all_nonfinite_kinds);
    deserializeData(data, all_nonfinite_kinds);
    EXPECT_EQ(std::bit_cast<UInt64>(scalarResult(restored)), canonical_nan_bits);
    EXPECT_EQ(std::bit_cast<UInt64>(data.get()), canonical_nan_bits);
}

TEST(SumPromQL, MaximumCountMagnitudeBoundaryIsCheckedExactly)
{
    /// Independently expand `(2^64 - 1) * (2^53 - 1) * 2^2045` into base `2^64` limbs.
    /// The product is `(2^117 - 2^64 - 2^53 + 1) * 2^2045`, not a production validation result.
    ExactLimbs maximum_bound{};
    maximum_bound[31] = UInt64{0x2000000000000000};
    maximum_bound[32] = UInt64{0xdffc000000000000};
    maximum_bound[33] = UInt64{0x0003ffffffffffff};
    ExactLimbs above_bound = maximum_bound;
    above_bound[0] = 1;

    auto function = makeFunction();
    const UInt64 maximum_count = std::numeric_limits<UInt64>::max();
    const Float64 inf = std::numeric_limits<Float64>::infinity();
    for (bool negative : {false, true})
    {
        SCOPED_TRACE(negative);
        const String boundary = exactWireState(maximum_count, 0, negative ? negateLimbs(maximum_bound) : maximum_bound);
        AggregateFunctionSumPromQLData data;
        AggregateState state(function);
        deserializeData(data, boundary);
        state.deserialize(boundary);
        EXPECT_EQ(serializeData(data), boundary);
        EXPECT_EQ(state.serialize(), boundary);
        expectBits(data.get(), negative ? -inf : inf);
        expectBits(scalarResult(state), negative ? -inf : inf);

        const String invalid = exactWireState(maximum_count, 0, negative ? negateLimbs(above_bound) : above_bound);
        expectDataRejectedWithoutMutation(data, invalid, ErrorCodes::INCORRECT_DATA);
        expectAggregateRejectedWithoutMutation(state, invalid, ErrorCodes::INCORRECT_DATA);
    }
}

TEST(SumPromQL, TotalInputCountOverflowDoesNotMutateScalarState)
{
    const UInt64 maximum_count = std::numeric_limits<UInt64>::max();
    const String full = exactWireState(maximum_count, 0);
    AggregateFunctionSumPromQLData data;
    deserializeData(data, full);
    for (Float64 value : {0., std::numeric_limits<Float64>::denorm_min(),
                          std::numeric_limits<Float64>::infinity(), std::bit_cast<Float64>(canonical_nan_bits)})
    {
        expectCountOverflow([&] { data.add(value); });
        EXPECT_EQ(serializeData(data), full);
    }

    AggregateFunctionSumPromQLData one;
    one.add(-std::numeric_limits<Float64>::infinity());
    expectCountOverflow([&] { data.merge(one); });
    EXPECT_EQ(serializeData(data), full);

    auto function = makeFunction();
    AggregateState state(function);
    state.deserialize(full);
    expectCountOverflow([&] { addFloats(state, {1.}); });
    EXPECT_EQ(state.serialize(), full);
    AggregateState right(function);
    addFloats(right, {-1.});
    expectCountOverflow([&] { state.merge(right); });
    EXPECT_EQ(state.serialize(), full);

    const String almost_full = exactWireState(maximum_count - 1, 0);
    deserializeData(data, almost_full);
    AggregateFunctionSumPromQLData one_zero;
    one_zero.add(0.);
    data.merge(one_zero);
    EXPECT_EQ(serializeData(data), full);
    AggregateFunctionSumPromQLData empty;
    data.merge(empty);
    EXPECT_EQ(serializeData(data), full);
    expectCountOverflow([&] { data.add(0.); });
    EXPECT_EQ(serializeData(data), full);
}

TEST(SumPromQL, InternalDataSelfMergeRetainsResidualAndChecksCountOverflow)
{
    /// Only the internal value type supports aliasing; aggregate merges use distinct owned states.
    AggregateFunctionSumPromQLData data;
    data.add(1.);
    data.add(half_ulp);
    data.merge(data);
    expectBits(data.get(), 2.);
    data.add(0x1p-52);
    expectBits(data.get(), 2. + 0x1p-51);

    const UInt64 maximum_count = std::numeric_limits<UInt64>::max();
    deserializeData(data, exactWireState(maximum_count / 2, 0));
    data.merge(data);
    const String before = exactWireState(maximum_count - 1, 0);
    EXPECT_EQ(serializeData(data), before);
    expectCountOverflow([&] { data.merge(data); });
    EXPECT_EQ(serializeData(data), before);
}

TEST(SumPromQL, OwnedNullableStatesSurviveSourceDestruction)
{
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());
    auto function = makeFunction("sumPromQL", nullable_type);
    auto owned_column = function->getStateType()->createColumn();
    auto & owned = assert_cast<ColumnAggregateFunction &>(*owned_column);
    {
        AggregateState left(function);
        AggregateState right(function);
        AggregateState nulls(function);
        auto left_input = nullable_type->createColumn();
        left_input->insert(Field{1.});
        left_input->insert(Field{half_ulp});
        left.add(*left_input);
        auto right_input = nullable_type->createColumn();
        right_input->insert(Field{0.});
        right_input->insert(Field{half_ulp});
        right.add(*right_input);
        owned.insertFrom(left.place());
        owned.insertFrom(right.place());
        owned.insertFrom(nulls.place());
        EXPECT_NE(owned.getData()[0], left.place());
        EXPECT_NE(owned.getData()[1], right.place());
        EXPECT_NE(owned.getData()[2], nulls.place());
    }

    function->merge(owned.getData()[0], owned.getData()[1], &owned.createOrGetArena());
    auto output = function->getResultType()->createColumn();
    function->insertResultInto(owned.getData()[0], *output, &owned.createOrGetArena());
    function->insertResultInto(owned.getData()[2], *output, &owned.createOrGetArena());
    expectBits((*output)[0].safeGet<Float64>(), rounded_sum);
    EXPECT_TRUE(output->isNullAt(1));
}

TEST(SumPromQL, CorruptNullableChildIsSafeToDestroy)
{
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());
    auto function = makeFunction("sumPromQL", nullable_type);
    AggregateState source(function);
    auto input = nullable_type->createColumn();
    input->insert(Field{1.});
    source.add(*input);
    const String serialized = source.serialize();
    ASSERT_EQ(serialized.size(), 1 + exact_state_size);
    ASSERT_EQ(serialized.front(), char{1});
    for (bool truncate : {false, true})
    {
        String invalid = serialized;
        if (truncate)
            invalid.pop_back();
        else
            invalid[1 + 9] = char{8};
        AggregateState victim(function);
        EXPECT_THROW(victim.deserialize(invalid), Exception);
        /// The outer nullable presence flag need not roll back, but its state must remain destructible.
    }
}

TEST(SumPromQL, CorruptLaterForEachCellIsSafeToDestroyAfterGrowth)
{
    auto function = makeFunction("sumPromQLForEach", nullableGridType());
    AggregateState source(function);
    addGrids(source, {Array{Field{1.}, Field{2.}, Field{3.}}});
    const String serialized = source.serialize();
    const size_t foreach_prefix_size = sizeof(size_t);
    ASSERT_EQ(serialized.size(), foreach_prefix_size + 3 * (1 + exact_state_size));
    ASSERT_EQ(serialized.front(), char{3});
    const size_t second_child_offset = foreach_prefix_size + (1 + exact_state_size) + 1;
    ASSERT_EQ(serialized[second_child_offset], char{2});
    for (bool truncate : {false, true})
    {
        String invalid = serialized;
        if (truncate)
            invalid.resize(second_child_offset + exact_state_size - 1);
        else
            invalid[second_child_offset + 9] = char{8};
        AggregateState victim(function);
        addGrids(victim, {Array{Field{0.}}});
        EXPECT_THROW(victim.deserialize(invalid), Exception);
        /// Earlier cells may already change; only ownership and destruction safety are required here.
    }
}

TEST(SumPromQL, LaterForEachCellCountOverflowDuringMergeIsSafeToDestroy)
{
    auto function = makeFunction("sumPromQLForEach", nullableGridType());
    AggregateState source(function);
    addGrids(source, {Array{Field{1.}, Field{2.}}});
    String full_later_cell = source.serialize();
    const size_t foreach_prefix_size = sizeof(size_t);
    ASSERT_EQ(full_later_cell.size(), foreach_prefix_size + 2 * (1 + exact_state_size));
    ASSERT_EQ(full_later_cell.front(), char{2});
    const size_t second_child_offset = foreach_prefix_size + (1 + exact_state_size) + 1;
    ASSERT_EQ(full_later_cell[second_child_offset], char{2});
    setLittleEndianUInt64(full_later_cell, second_child_offset + 1, std::numeric_limits<UInt64>::max());

    {
        AggregateState destination(function);
        AggregateState right(function);
        destination.deserialize(full_later_cell);
        EXPECT_EQ(destination.serialize(), full_later_cell);
        addGrids(right, {Array{Field{3.}, Field{4.}}});
        const String right_before = right.serialize();
        expectCountOverflow([&] { destination.merge(right); });
        EXPECT_EQ(right.serialize(), right_before);
        /// The first cell may already be merged; discard the partially changed destination without finalizing it.
    }
}
