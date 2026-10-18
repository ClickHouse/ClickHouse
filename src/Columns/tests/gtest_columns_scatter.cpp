#include <Columns/ColumnsScatter.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>

#include <gtest/gtest.h>
#include <pcg_random.hpp>

using namespace DB;

namespace
{

/// Deterministic per-test rng (seed logged so failures are reproducible).
pcg64 & rng()
{
    static pcg64 generator = []
    {
        UInt64 seed = randomSeed();
        std::cerr << "gtest_columns_scatter seed: " << seed << '\n';
        return pcg64(seed);
    }();
    return generator;
}

std::vector<UInt16> makePids(size_t n, size_t num_shards)
{
    std::vector<UInt16> pids(n);
    for (auto & pid : pids)
        pid = static_cast<UInt16>(rng()() % num_shards);
    return pids;
}

/// Fill a freshly created fixed-width column with `n` rows of random bytes. Valid content for
/// ColumnVector / ColumnDecimal / ColumnFixedString: the scatter contract is byte-preservation.
MutableColumnPtr fillFixedRandom(MutableColumnPtr column, size_t n)
{
    auto raw = column->insertRawUninitialized(n);
    for (auto & byte : raw)
        byte = static_cast<char>(rng()());
    return column;
}

/// Oracle input as the scatter contract requires: wrappers stripped at every nesting level, top-level
/// LowCardinality preserved. A wrapped composite is not a usable oracle: `IColumn::scatter` on a
/// composite holding a `ColumnSparse` element duplicated one source value across shards.
ColumnPtr materializeForOracle(const IColumn & source)
{
    if (source.getDataType() == TypeIndex::LowCardinality)
        return source.convertToFullColumnIfConst();
    return recursiveRemoveLowCardinality(source.convertToFullIfWrapped());
}

/// Oracle: `IColumn::scatter` per (materialized) source plus `insertRangeFrom` concatenation. Same
/// algorithm as the module's fallback, so Fallback tests compare values with it and rely on their
/// kernel, type, and ownership assertions for the rest.
MutableColumns referenceScatter(std::span<const IColumn * const> sources, const std::vector<std::vector<UInt16>> & pids, size_t num_shards)
{
    MutableColumns result(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        result[s] = materializeForOracle(*sources[0])->cloneEmpty();
    for (size_t b = 0; b < sources.size(); ++b)
    {
        IColumn::Selector selector(pids[b].size());
        for (size_t j = 0; j < pids[b].size(); ++j)
            selector[j] = pids[b][j];
        auto full = materializeForOracle(*sources[b]);
        auto parts = full->scatter(num_shards, selector);
        for (size_t s = 0; s < num_shards; ++s)
            if (parts[s]->size())
                result[s]->insertRangeFrom(*parts[s], 0, parts[s]->size());
    }
    return result;
}

void expectColumnsBitIdentical(const IColumn & expected, const IColumn & actual, const std::string & context)
{
    ASSERT_EQ(expected.size(), actual.size()) << context;
    ASSERT_EQ(expected.getDataType(), actual.getDataType()) << context;
    if (expected.size() == 0)
        return;
    if (expected.isFixedAndContiguous())
    {
        const auto expected_raw = expected.getRawData();
        const auto actual_raw = actual.getRawData();
        ASSERT_EQ(expected_raw.size(), actual_raw.size()) << context;
        ASSERT_EQ(0, memcmp(expected_raw.data(), actual_raw.data(), expected_raw.size())) << context;
        return;
    }
    /// Variable-length / composite: per-row byte-exact comparison of the serialized form.
    Arena expected_arena;
    Arena actual_arena;
    for (size_t i = 0; i < expected.size(); ++i)
    {
        const char * expected_begin = nullptr;
        const char * actual_begin = nullptr;
        const auto expected_value = expected.serializeValueIntoArena(i, expected_arena, expected_begin, nullptr);
        const auto actual_value = actual.serializeValueIntoArena(i, actual_arena, actual_begin, nullptr);
        ASSERT_EQ(expected_value, actual_value) << context << " row " << i << " (Field-level: expected=" << expected[i].dump()
                                                << " actual=" << actual[i].dump() << ")";
    }
}

/// Run the module scatter and compare bit-exactly with the oracle.
void checkEquivalence(
    std::span<const IColumn * const> sources,
    const std::vector<std::vector<UInt16>> & pids,
    size_t num_shards,
    bool with_precounted = false)
{
    std::vector<std::span<const UInt16>> pid_spans;
    for (const auto & p : pids)
        pid_spans.emplace_back(p);

    std::vector<UInt32> counts(num_shards, 0);
    ColumnsScatter::countRowsPerShard(pid_spans, counts);
    std::span<const UInt32> counts_arg;
    if (with_precounted)
        counts_arg = counts;

    auto result = ColumnsScatter::scatter(sources, pid_spans, num_shards, counts_arg);
    auto expected = referenceScatter(sources, pids, num_shards);

    ASSERT_EQ(num_shards, result.size());
    for (size_t s = 0; s < num_shards; ++s)
    {
        ASSERT_EQ(counts[s], result[s]->size()) << "shard " << s;
        expectColumnsBitIdentical(*expected[s], *result[s], "shard " + std::to_string(s) + " of " + std::to_string(num_shards));
    }
}

/// Equivalence, counts, and the named-kernel trace assertion.
void checkTypedKernel(
    std::span<const IColumn * const> sources,
    const std::vector<std::vector<UInt16>> & pids,
    size_t num_shards,
    ColumnsScatter::ScatterKernelId expected_kernel)
{
    ColumnsScatter::DispatchTrace trace;
    auto * previous = ColumnsScatter::exchangeDispatchTrace(&trace);
    checkEquivalence(sources, pids, num_shards);
    ColumnsScatter::exchangeDispatchTrace(previous);
    ASSERT_EQ(1u, trace.entries.size());
    ASSERT_EQ(expected_kernel, trace.entries[0].kernel);
}

/// {a, b} and {b, a}: a wrapper in either position must be normalized before the typed kernel runs.
void checkBothOrders(const IColumn & a, const IColumn & b, size_t num_shards, ColumnsScatter::ScatterKernelId expected)
{
    for (bool a_first : {true, false})
    {
        SCOPED_TRACE(a_first ? "a first" : "b first");
        std::vector<const IColumn *> sources{a_first ? &a : &b, a_first ? &b : &a};
        std::vector<std::vector<UInt16>> pids{makePids(sources[0]->size(), num_shards), makePids(sources[1]->size(), num_shards)};
        checkTypedKernel(sources, pids, num_shards, expected);
    }
}

void checkFixedTypeEquivalence(const IColumn & prototype)
{
    SCOPED_TRACE(prototype.getName());
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::FixedWidth, ColumnsScatter::plannedKernel(prototype));

    struct Case
    {
        size_t num_sources;
        size_t rows_per_source;
        size_t num_shards;
    };
    /// One shard, direct (8), write-combining (256), and a second write-combining fanout (512).
    for (const auto & test_case : std::initializer_list<Case>{{1, 1000, 1}, {1, 1000, 8}, {3, 700, 8}, {2, 5000, 256}, {2, 3000, 512}})
    {
        std::vector<MutableColumnPtr> owned;
        std::vector<const IColumn *> sources;
        std::vector<std::vector<UInt16>> pids;
        for (size_t b = 0; b < test_case.num_sources; ++b)
        {
            owned.push_back(fillFixedRandom(prototype.cloneEmpty(), test_case.rows_per_source));
            sources.push_back(owned.back().get());
            pids.push_back(makePids(test_case.rows_per_source, test_case.num_shards));
        }
        checkTypedKernel(sources, pids, test_case.num_shards, ColumnsScatter::ScatterKernelId::FixedWidth);
    }
}

MutableColumnPtr makeStrings(size_t n, size_t max_length)
{
    auto column = ColumnString::create();
    for (size_t i = 0; i < n; ++i)
    {
        std::string value(rng()() % (max_length + 1), static_cast<char>('a' + (i % 26)));
        column->insertData(value.data(), value.size());
    }
    return column;
}

MutableColumnPtr makeLowCardinalityStrings(size_t n, size_t dict_size)
{
    const auto type = DataTypeLowCardinality(std::make_shared<DataTypeString>());
    auto column = type.createColumn();
    for (size_t i = 0; i < n; ++i)
    {
        std::string value = "value_" + std::to_string(rng()() % dict_size);
        column->insertData(value.data(), value.size());
    }
    return column;
}

}

TEST(ColumnsScatter, FixedWidthVectorTypes)
{
    checkFixedTypeEquivalence(*ColumnUInt8::create());
    checkFixedTypeEquivalence(*ColumnUInt16::create());
    checkFixedTypeEquivalence(*ColumnUInt32::create());
    checkFixedTypeEquivalence(*ColumnUInt64::create());
    checkFixedTypeEquivalence(*ColumnUInt128::create());
    checkFixedTypeEquivalence(*ColumnUInt256::create());
    checkFixedTypeEquivalence(*ColumnInt8::create());
    checkFixedTypeEquivalence(*ColumnInt16::create());
    checkFixedTypeEquivalence(*ColumnInt32::create());
    checkFixedTypeEquivalence(*ColumnInt64::create());
    checkFixedTypeEquivalence(*ColumnInt128::create());
    checkFixedTypeEquivalence(*ColumnInt256::create());
    checkFixedTypeEquivalence(*ColumnBFloat16::create());
    checkFixedTypeEquivalence(*ColumnFloat32::create());
    checkFixedTypeEquivalence(*ColumnFloat64::create());
    checkFixedTypeEquivalence(*ColumnUUID::create());
    checkFixedTypeEquivalence(*ColumnIPv4::create());
    checkFixedTypeEquivalence(*ColumnIPv6::create());
}

TEST(ColumnsScatter, FixedWidthDecimalTypes)
{
    checkFixedTypeEquivalence(*ColumnDecimal<Decimal32>::create(0, 2));
    checkFixedTypeEquivalence(*ColumnDecimal<Decimal64>::create(0, 4));
    checkFixedTypeEquivalence(*ColumnDecimal<Decimal128>::create(0, 10));
    checkFixedTypeEquivalence(*ColumnDecimal<Decimal256>::create(0, 20));
    checkFixedTypeEquivalence(*ColumnDecimal<DateTime64>::create(0, 3));
    checkFixedTypeEquivalence(*ColumnDecimal<Time64>::create(0, 6));
}

TEST(ColumnsScatter, FixedString)
{
    checkFixedTypeEquivalence(*ColumnFixedString::create(1));
    checkFixedTypeEquivalence(*ColumnFixedString::create(3));  /// generic-width kernel
    checkFixedTypeEquivalence(*ColumnFixedString::create(16)); /// SWWC-capable width
    checkFixedTypeEquivalence(*ColumnFixedString::create(32)); /// generic-width kernel
}

TEST(ColumnsScatter, ZeroRowSourceAmongNonEmpty)
{
    auto a = fillFixedRandom(ColumnUInt64::create(), 100);
    auto b = ColumnUInt64::create(); /// empty
    auto c = fillFixedRandom(ColumnUInt64::create(), 50);
    std::vector<const IColumn *> sources{a.get(), b.get(), c.get()};
    std::vector<std::vector<UInt16>> pids{makePids(100, 4), {}, makePids(50, 4)};
    checkEquivalence(sources, pids, 4);
}

TEST(ColumnsScatter, AllRowsToOneShard)
{
    auto column = fillFixedRandom(ColumnUInt64::create(), 500);
    std::vector<const IColumn *> sources{column.get()};
    std::vector<std::vector<UInt16>> pids{std::vector<UInt16>(500, 0)};
    checkEquivalence(sources, pids, 8);
}

TEST(ColumnsScatter, PrecountedRowsPerShardMatchesInternalCounting)
{
    auto column = fillFixedRandom(ColumnUInt64::create(), 2000);
    std::vector<const IColumn *> sources{column.get()};
    std::vector<std::vector<UInt16>> pids{makePids(2000, 16)};
    checkEquivalence(sources, pids, 16, /*with_precounted=*/true);
    checkEquivalence(sources, pids, 16, /*with_precounted=*/false);
}

/// Enough rows per shard that full 64-byte lines stream through the non-temporal stores.
TEST(ColumnsScatter, SwwcManyLinesPerShard)
{
    auto a = fillFixedRandom(ColumnUInt64::create(), 64 << 10);
    auto b = fillFixedRandom(ColumnUInt64::create(), 64 << 10);
    std::vector<const IColumn *> sources{a.get(), b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(64 << 10, 256), makePids(64 << 10, 256)};
    checkEquivalence(sources, pids, 256);
}

TEST(ColumnsScatter, ConstMixedWithFull)
{
    auto full = fillFixedRandom(ColumnUInt64::create(), 300);
    auto const_column = ColumnConst::create(fillFixedRandom(ColumnUInt64::create(), 1), 200);
    checkBothOrders(*const_column, *full, 8, ColumnsScatter::ScatterKernelId::FixedWidth);
}

TEST(ColumnsScatter, TwoConstsDifferentValuesMaterialize)
{
    auto value_a = ColumnUInt64::create();
    value_a->insert(42u);
    auto value_b = ColumnUInt64::create();
    value_b->insert(43u);
    auto const_a = ColumnConst::create(std::move(value_a), 100);
    auto const_b = ColumnConst::create(std::move(value_b), 150);
    std::vector<const IColumn *> sources{const_a.get(), const_b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(100, 4), makePids(150, 4)};
    checkEquivalence(sources, pids, 4);
}

TEST(ColumnsScatter, AllConstEqualValuesStayCompact)
{
    auto make_const = [](size_t rows)
    {
        auto value = ColumnUInt64::create();
        value->insert(7u);
        return ColumnConst::create(std::move(value), rows);
    };
    auto const_a = make_const(100);
    auto const_b = make_const(60);
    std::vector<const IColumn *> sources{const_a.get(), const_b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(100, 4), makePids(60, 4)};
    std::vector<std::span<const UInt16>> pid_spans{pids[0], pids[1]};

    ColumnsScatter::DispatchTrace trace;
    auto * previous = ColumnsScatter::exchangeDispatchTrace(&trace);
    auto result = ColumnsScatter::scatter(sources, pid_spans, 4);
    ColumnsScatter::exchangeDispatchTrace(previous);

    ASSERT_EQ(1u, trace.entries.size());
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::ConstCompact, trace.entries[0].kernel);

    size_t total = 0;
    for (const auto & shard : result)
    {
        ASSERT_TRUE(shard->isConst() || shard->empty());
        total += shard->size();
        if (shard->size())
            ASSERT_EQ(7u, (*shard)[0].safeGet<UInt64>());
    }
    ASSERT_EQ(160u, total);
}

/// -0.0 and +0.0 compare equal by value but differ in bytes. The compact path must not collapse them into one const.
TEST(ColumnsScatter, ConstBitExactNotOrderingEqual)
{
    auto value_pos = ColumnFloat64::create();
    value_pos->insert(0.0);
    auto value_neg = ColumnFloat64::create();
    double negative_zero = -0.0;
    value_neg->insertData(reinterpret_cast<const char *>(&negative_zero), sizeof(negative_zero));
    auto const_pos = ColumnConst::create(std::move(value_pos), 40);
    auto const_neg = ColumnConst::create(std::move(value_neg), 40);
    std::vector<const IColumn *> sources{const_pos.get(), const_neg.get()};
    std::vector<std::vector<UInt16>> pids{makePids(40, 2), makePids(40, 2)};
    std::vector<std::span<const UInt16>> pid_spans{pids[0], pids[1]};

    auto result = ColumnsScatter::scatter(sources, pid_spans, 2);

    /// The -0.0 bit patterns across shards must add up to the second source's rows.
    size_t negative_bits = 0;
    for (const auto & shard : result)
    {
        const auto & data = assert_cast<const ColumnFloat64 &>(*shard).getData();
        for (Float64 value : data)
        {
            UInt64 bits = 0;
            memcpy(&bits, &value, sizeof(bits));
            negative_bits += (bits == 0x8000000000000000ULL);
        }
    }
    ASSERT_EQ(40u, negative_bits);
}

TEST(ColumnsScatter, SparseNormalizedBeforeDispatch)
{
    /// Sparse UInt64: values column row 0 is the shared default, offsets list the non-default rows.
    auto values = ColumnUInt64::create();
    values->insert(0u); /// default
    values->insert(11u);
    values->insert(22u);
    auto offsets = ColumnUInt64::create();
    offsets->insert(3u);
    offsets->insert(7u);
    auto sparse = ColumnSparse::create(std::move(values), std::move(offsets), 20);

    auto full = fillFixedRandom(ColumnUInt64::create(), 30);
    checkBothOrders(*sparse, *full, 4, ColumnsScatter::ScatterKernelId::FixedWidth);
}

TEST(ColumnsScatter, StringEquivalence)
{
    auto a = makeStrings(2000, 20);
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::String, ColumnsScatter::plannedKernel(*a));
    auto b = makeStrings(1000, 20);

    std::vector<const IColumn *> sources{a.get(), b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(2000, 8), makePids(1000, 8)};
    checkTypedKernel(sources, pids, 8, ColumnsScatter::ScatterKernelId::String);

    /// Empty strings among rows past 64 bytes, at one shard and at two write-combining shard counts.
    auto mixed = ColumnString::create();
    for (size_t i = 0; i < 600; ++i)
    {
        std::string value(i % 3 == 0 ? 0 : (i % 5 == 0 ? 100 + rng()() % 50 : rng()() % 10), static_cast<char>('a' + (i % 26)));
        mixed->insertData(value.data(), value.size());
    }
    for (size_t num_shards : {1uz, 256uz, 512uz})
    {
        std::vector<const IColumn *> single{mixed.get()};
        std::vector<std::vector<UInt16>> single_pids{makePids(600, num_shards)};
        checkTypedKernel(single, single_pids, num_shards, ColumnsScatter::ScatterKernelId::String);
    }

    auto all_empty = ColumnString::create();
    for (size_t i = 0; i < 100; ++i)
        all_empty->insertDefault();
    auto empty_column = ColumnString::create();
    std::vector<const IColumn *> with_empty{a.get(), empty_column.get(), all_empty.get()};
    std::vector<std::vector<UInt16>> with_empty_pids{makePids(2000, 4), {}, makePids(100, 4)};
    checkTypedKernel(with_empty, with_empty_pids, 4, ColumnsScatter::ScatterKernelId::String);
}

TEST(ColumnsScatter, NullableEquivalence)
{
    auto make_nullable_fixed = [](size_t n)
    {
        auto nested = fillFixedRandom(ColumnUInt64::create(), n);
        auto null_map = ColumnUInt8::create();
        for (size_t i = 0; i < n; ++i)
            null_map->insert(static_cast<UInt8>(rng()() % 4 == 0));
        return ColumnNullable::create(std::move(nested), std::move(null_map));
    };
    auto a = make_nullable_fixed(800);
    auto b = make_nullable_fixed(400);
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::Nullable, ColumnsScatter::plannedKernel(*a));
    std::vector<const IColumn *> sources{a.get(), b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(800, 8), makePids(400, 8)};
    checkTypedKernel(sources, pids, 8, ColumnsScatter::ScatterKernelId::Nullable);

    /// Recursion from the null map into the String kernel.
    auto make_nullable_string = [](size_t n)
    {
        auto nested = makeStrings(n, 15);
        auto null_map = ColumnUInt8::create();
        for (size_t i = 0; i < n; ++i)
            null_map->insert(static_cast<UInt8>(rng()() % 3 == 0));
        return ColumnNullable::create(std::move(nested), std::move(null_map));
    };
    auto c = make_nullable_string(500);
    auto d = make_nullable_string(300);
    std::vector<const IColumn *> string_sources{c.get(), d.get()};
    std::vector<std::vector<UInt16>> string_pids{makePids(500, 4), makePids(300, 4)};
    checkTypedKernel(string_sources, string_pids, 4, ColumnsScatter::ScatterKernelId::Nullable);
}

TEST(ColumnsScatter, TupleEquivalence)
{
    auto make_tuple = [](size_t n)
    {
        MutableColumns elements;
        elements.push_back(fillFixedRandom(ColumnUInt32::create(), n));
        elements.push_back(makeStrings(n, 12));
        return ColumnTuple::create(std::move(elements));
    };
    auto a = make_tuple(600);
    auto b = make_tuple(250);
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::Tuple, ColumnsScatter::plannedKernel(*a));
    std::vector<const IColumn *> sources{a.get(), b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(600, 8), makePids(250, 8)};
    checkTypedKernel(sources, pids, 8, ColumnsScatter::ScatterKernelId::Tuple);
}

/// A sparse element hidden inside a Tuple in one chunk, in both chunk orders: recursive
/// normalization has to strip it before the typed kernels run.
TEST(ColumnsScatter, TupleWithSparseElement)
{
    auto make_dense_tuple = [](size_t n)
    {
        MutableColumns elements;
        elements.push_back(fillFixedRandom(ColumnUInt64::create(), n));
        return ColumnTuple::create(std::move(elements));
    };
    auto make_sparse_tuple = [](size_t n)
    {
        auto values = ColumnUInt64::create();
        values->insert(0u);
        values->insert(77u);
        auto offsets = ColumnUInt64::create();
        offsets->insert(2u);
        MutableColumns elements;
        elements.push_back(ColumnSparse::create(std::move(values), std::move(offsets), n));
        return ColumnTuple::create(std::move(elements));
    };
    auto dense = make_dense_tuple(50);
    auto sparse = make_sparse_tuple(30);
    checkBothOrders(*sparse, *dense, 4, ColumnsScatter::ScatterKernelId::Tuple);
}

TEST(ColumnsScatter, EmptyTupleRowCountOnly)
{
    auto a = ColumnTuple::create(120);
    auto b = ColumnTuple::create(80);
    std::vector<const IColumn *> sources{a.get(), b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(120, 4), makePids(80, 4)};
    checkTypedKernel(sources, pids, 4, ColumnsScatter::ScatterKernelId::Tuple);
}

TEST(ColumnsScatter, ArrayEquivalence)
{
    auto make_array = [](size_t n, auto make_nested)
    {
        auto offsets = ColumnArray::ColumnOffsets::create();
        size_t total = 0;
        for (size_t i = 0; i < n; ++i)
        {
            total += rng()() % 5;
            offsets->insert(total);
        }
        return ColumnArray::create(make_nested(total), std::move(offsets));
    };
    auto fixed_nested = [](size_t total) { return fillFixedRandom(ColumnUInt64::create(), total); };
    auto string_nested = [](size_t total) { return makeStrings(total, 10); };

    auto a = make_array(500, fixed_nested);
    auto b = make_array(200, fixed_nested);
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::Array, ColumnsScatter::plannedKernel(*a));
    std::vector<const IColumn *> sources{a.get(), b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(500, 8), makePids(200, 8)};
    checkTypedKernel(sources, pids, 8, ColumnsScatter::ScatterKernelId::Array);

    auto c = make_array(300, string_nested);
    std::vector<const IColumn *> string_sources{c.get()};
    std::vector<std::vector<UInt16>> string_pids{makePids(300, 4)};
    checkTypedKernel(string_sources, string_pids, 4, ColumnsScatter::ScatterKernelId::Array);
}

TEST(ColumnsScatter, MapStatisticsPropagated)
{
    auto keys = fillFixedRandom(ColumnUInt64::create(), 60);
    auto values = fillFixedRandom(ColumnUInt64::create(), 60);
    auto offsets = ColumnArray::ColumnOffsets::create();
    for (size_t i = 0; i < 20; ++i)
        offsets->insert(3 * (i + 1));
    auto statistics = std::make_shared<ColumnMap::Statistics>(3.0, 20);
    auto map_base = ColumnMap::create(std::move(keys), std::move(values), std::move(offsets));
    auto map = ColumnMap::create(map_base->getNestedColumnPtr(), statistics);

    std::vector<const IColumn *> sources{map.get()};
    auto pids = makePids(20, 4);
    std::vector<std::span<const UInt16>> pid_spans{pids};
    auto result = ColumnsScatter::scatter(sources, pid_spans, 4);
    for (const auto & shard : result)
        ASSERT_EQ(statistics.get(), assert_cast<const ColumnMap &>(*shard).getStatistics().get());
}

TEST(ColumnsScatter, MapEquivalence)
{
    auto make_map = [](size_t n)
    {
        auto keys = fillFixedRandom(ColumnUInt64::create(), 3 * n);
        auto values = fillFixedRandom(ColumnUInt64::create(), 3 * n);
        auto offsets = ColumnArray::ColumnOffsets::create();
        for (size_t i = 0; i < n; ++i)
            offsets->insert(3 * (i + 1));
        return ColumnMap::create(std::move(keys), std::move(values), std::move(offsets));
    };
    auto a = make_map(300);
    auto b = make_map(150);
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::Map, ColumnsScatter::plannedKernel(*a));
    std::vector<const IColumn *> sources{a.get(), b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(300, 4), makePids(150, 4)};
    checkTypedKernel(sources, pids, 4, ColumnsScatter::ScatterKernelId::Map);
}

TEST(ColumnsScatter, LowCardinalityPreservesTypeAndSharesDictionary)
{
    auto column = makeLowCardinalityStrings(1000, 16);
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::LowCardinality, ColumnsScatter::plannedKernel(*column));

    std::vector<const IColumn *> sources{column.get()};
    std::vector<std::vector<UInt16>> pids{makePids(1000, 8)};
    std::vector<std::span<const UInt16>> pid_spans{pids[0]};

    ColumnsScatter::DispatchTrace trace;
    auto * previous = ColumnsScatter::exchangeDispatchTrace(&trace);
    auto result = ColumnsScatter::scatter(sources, pid_spans, 8);
    ColumnsScatter::exchangeDispatchTrace(previous);
    ASSERT_EQ(1u, trace.entries.size());
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::LowCardinality, trace.entries[0].kernel);

    /// Every shard must stay LowCardinality and share one dictionary object, as
    /// `ColumnLowCardinality::scatter` does - neither of which the value oracle can see.
    const IColumn * shared_dictionary = nullptr;
    for (const auto & shard : result)
    {
        ASSERT_EQ(TypeIndex::LowCardinality, shard->getDataType());
        const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(*shard);
        ASSERT_TRUE(low_cardinality.isSharedDictionary());
        if (!shared_dictionary)
            shared_dictionary = low_cardinality.getDictionaryPtr().get();
        else
            ASSERT_EQ(shared_dictionary, low_cardinality.getDictionaryPtr().get());
    }

    auto expected = referenceScatter(sources, pids, 8);
    for (size_t s = 0; s < 8; ++s)
        expectColumnsBitIdentical(*expected[s], *result[s], "LC shard " + std::to_string(s));
}

TEST(ColumnsScatter, LowCardinalityMultiSourceAndConstMixed)
{
    /// Per-source dictionaries force the kernel through `IColumn::scatter` per source.
    auto a = makeLowCardinalityStrings(400, 8);
    auto b = makeLowCardinalityStrings(200, 24);
    std::vector<const IColumn *> sources{a.get(), b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(400, 4), makePids(200, 4)};
    checkTypedKernel(sources, pids, 4, ColumnsScatter::ScatterKernelId::LowCardinality);

    /// Normalization must strip the Const and preserve the LowCardinality, in both orders.
    auto lc_full = makeLowCardinalityStrings(300, 8);
    auto lc_value = makeLowCardinalityStrings(1, 1);
    auto lc_const = ColumnConst::create(std::move(lc_value), 100);
    checkBothOrders(*lc_const, *lc_full, 4, ColumnsScatter::ScatterKernelId::LowCardinality);
}

TEST(ColumnsScatter, ConstStringMixedWithFull)
{
    auto value = ColumnString::create();
    value->insertData("const_payload", 13);
    auto const_column = ColumnConst::create(std::move(value), 150);
    auto full = makeStrings(250, 18);
    checkBothOrders(*const_column, *full, 4, ColumnsScatter::ScatterKernelId::String);
}

/// Misuse must fail loudly. A debug or sanitizer build aborts on a thrown `LOGICAL_ERROR` by design.
/// That abort is not catchable, so the throw itself is only asserted in release.
TEST(ColumnsScatter, NegativeMisuseThrows)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "LOGICAL_ERROR aborts under debug/sanitizer builds; the throwing contract is asserted in release builds";
#else
    auto column = fillFixedRandom(ColumnUInt64::create(), 10);
    std::vector<const IColumn *> sources{column.get()};
    auto pids = makePids(10, 4);
    std::vector<std::span<const UInt16>> pid_spans{pids};
    std::vector<std::span<const UInt16>> empty_spans;
    std::vector<UInt32> bad_counts(3, 0);

    EXPECT_THROW((void)ColumnsScatter::scatter(std::span<const IColumn * const>{}, empty_spans, 4), Exception);
    EXPECT_THROW((void)ColumnsScatter::scatter(sources, empty_spans, 4), Exception);
    EXPECT_THROW((void)ColumnsScatter::scatter(sources, pid_spans, 0), Exception);
    EXPECT_THROW((void)ColumnsScatter::scatter(sources, pid_spans, 4, bad_counts), Exception);
    auto short_pids = makePids(5, 4);
    std::vector<std::span<const UInt16>> short_spans{short_pids};
    EXPECT_THROW((void)ColumnsScatter::scatter(sources, short_spans, 4), Exception);
    /// Same TypeIndex, different value widths: silent corruption in the raw-byte kernel if this goes unchecked.
    auto fixed_4 = fillFixedRandom(ColumnFixedString::create(4), 10);
    auto fixed_8 = fillFixedRandom(ColumnFixedString::create(8), 10);
    std::vector<const IColumn *> mixed_sources{fixed_4.get(), fixed_8.get()};
    auto pids_a = makePids(10, 4);
    auto pids_b = makePids(10, 4);
    std::vector<std::span<const UInt16>> mixed_spans{pids_a, pids_b};
    EXPECT_THROW((void)ColumnsScatter::scatter(mixed_sources, mixed_spans, 4), Exception);
    /// Same TypeIndex again, and out-of-bounds element indexing if this goes unchecked.
    MutableColumns one_element;
    one_element.push_back(fillFixedRandom(ColumnUInt64::create(), 10));
    auto tuple_1 = ColumnTuple::create(std::move(one_element));
    MutableColumns two_elements;
    two_elements.push_back(fillFixedRandom(ColumnUInt64::create(), 10));
    two_elements.push_back(fillFixedRandom(ColumnUInt64::create(), 10));
    auto tuple_2 = ColumnTuple::create(std::move(two_elements));
    std::vector<const IColumn *> mixed_tuples{tuple_1.get(), tuple_2.get()};
    EXPECT_THROW((void)ColumnsScatter::scatter(mixed_tuples, mixed_spans, 4), Exception);
    auto ints = fillFixedRandom(ColumnUInt32::create(), 10);
    std::vector<const IColumn *> mixed_types{column.get(), ints.get()};
    EXPECT_THROW((void)ColumnsScatter::scatter(mixed_types, mixed_spans, 4), Exception);
#endif
}

TEST(ColumnsScatter, ReplicatedNormalizedBeforeDispatch)
{
    auto nested = ColumnUInt64::create();
    for (UInt64 value : {100u, 200u, 300u, 400u, 500u})
        nested->insert(value);
    auto indexes = ColumnUInt64::create();
    for (size_t i = 0; i < 30; ++i)
        indexes->insert(rng()() % 5);
    const ColumnPtr nested_ptr = std::move(nested);
    const ColumnPtr indexes_ptr = std::move(indexes);
    auto replicated = ColumnReplicated::create(nested_ptr, indexes_ptr);

    auto full = fillFixedRandom(ColumnUInt64::create(), 40);
    checkBothOrders(*replicated, *full, 4, ColumnsScatter::ScatterKernelId::FixedWidth);
}

/// The staging invariant from `ScatterScratch`: a cursor seeded mid-line (as the join's workers do,
/// at prefix-sum offsets) must fill the partial head line with regular stores, then stream aligned
/// lines, then drain the residual. Scattering every shard into one shared buffer at prefix-sum
/// offsets is what leaves most seeds unaligned.
TEST(ColumnsScatter, MisalignedCursorSeedingSwwc)
{
    const size_t n = 64 << 10;
    const size_t fanout = 256; /// SWWC regime
    const size_t width = 8;

    auto payload = fillFixedRandom(ColumnUInt64::create(), n);
    const char * data = payload->getRawData().data();
    auto pids = makePids(n, fanout);

    std::vector<size_t> counts(fanout, 0);
    for (UInt16 pid : pids)
        ++counts[pid];
    std::vector<size_t> prefix(fanout, 0);
    for (size_t p = 1; p < fanout; ++p)
        prefix[p] = prefix[p - 1] + counts[p - 1];

    PaddedPODArray<char> destination;
    destination.resize(n * width);
    size_t misaligned_seeds = 0;
    ColumnsScatter::ScatterScratch scratch;
    scratch.init(fanout, /*use_swwc=*/true);
    for (size_t p = 0; p < fanout; ++p)
    {
        char * cursor = destination.data() + prefix[p] * width;
        misaligned_seeds += (reinterpret_cast<uintptr_t>(cursor) & 63) != 0;
        scratch.seed(p, cursor);
    }
    /// Assert the coverage is real: with random counts most prefix offsets land mid-line.
    ASSERT_GT(misaligned_seeds, fanout / 2);

    ColumnsScatter::scatterPidChunk(width, pids.data(), data, n, /*use_swwc=*/true, scratch);
    scratch.drain();

    std::vector<size_t> cursor_rows(fanout, 0);
    for (size_t i = 0; i < n; ++i)
    {
        const size_t p = pids[i];
        UInt64 expected = 0;
        memcpy(&expected, data + i * width, width);
        UInt64 actual = 0;
        memcpy(&actual, destination.data() + (prefix[p] + cursor_rows[p]) * width, width);
        ASSERT_EQ(expected, actual) << "row " << i;
        ++cursor_rows[p];
    }
    for (size_t p = 0; p < fanout; ++p)
        ASSERT_EQ(destination.data() + (prefix[p] + counts[p]) * width, scratch.cursors[p]) << "shard " << p;
}


/// The whole dispatch table: every supported family reaches its named kernel; every type without
/// a dedicated kernel takes the fallback.
TEST(ColumnsScatter, DispatchTableComplete)
{
    tryRegisterAggregateFunctions();
    using ColumnsScatter::ScatterKernelId;
    ASSERT_EQ(ScatterKernelId::FixedWidth, ColumnsScatter::plannedKernel(*ColumnUInt64::create()));
    ASSERT_EQ(ScatterKernelId::FixedWidth, ColumnsScatter::plannedKernel(*ColumnFixedString::create(3)));
    ASSERT_EQ(ScatterKernelId::FixedWidth, ColumnsScatter::plannedKernel(*ColumnDecimal<Decimal64>::create(0, 3)));
    ASSERT_EQ(ScatterKernelId::String, ColumnsScatter::plannedKernel(*ColumnString::create()));
    ASSERT_EQ(
        ScatterKernelId::Nullable,
        ColumnsScatter::plannedKernel(*ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create())));
    ASSERT_EQ(ScatterKernelId::Tuple, ColumnsScatter::plannedKernel(*ColumnTuple::create(1)));
    ASSERT_EQ(
        ScatterKernelId::Array,
        ColumnsScatter::plannedKernel(*ColumnArray::create(ColumnUInt64::create(), ColumnArray::ColumnOffsets::create())));
    {
        auto map_column = DataTypeFactory::instance().get("Map(UInt64, UInt64)")->createColumn();
        ASSERT_EQ(ScatterKernelId::Map, ColumnsScatter::plannedKernel(*map_column));
    }
    ASSERT_EQ(ScatterKernelId::LowCardinality, ColumnsScatter::plannedKernel(*makeLowCardinalityStrings(1, 1)));
    for (const char * type_name : {"Variant(UInt64, String)", "Dynamic", "AggregateFunction(count)", "JSON"})
    {
        auto column = DataTypeFactory::instance().get(type_name)->createColumn();
        ASSERT_EQ(ScatterKernelId::Fallback, ColumnsScatter::plannedKernel(*column)) << type_name;
    }
}

/// A Variant whose nested representations differ across chunks (full against sparse), so
/// normalization has to strip the sparse alternative before the cross-chunk append.
TEST(ColumnsScatter, VariantMixedNestedRepresentation)
{
    const size_t n = 64;
    auto make_discriminators = [](size_t rows)
    {
        auto discriminators = ColumnVariant::ColumnDiscriminators::create();
        for (size_t i = 0; i < rows; ++i)
            discriminators->insertValue(0);
        return discriminators;
    };
    auto make_offsets = [](size_t rows)
    {
        auto offsets = ColumnVariant::ColumnOffsets::create();
        for (size_t i = 0; i < rows; ++i)
            offsets->insertValue(i);
        return offsets;
    };
    auto make_full = [&](size_t rows) -> ColumnPtr
    {
        auto nested = ColumnUInt64::create();
        for (size_t i = 0; i < rows; ++i)
            nested->insertValue(i);
        Columns variants;
        variants.push_back(std::move(nested));
        return ColumnVariant::create(make_discriminators(rows), make_offsets(rows), variants);
    };
    auto make_sparse = [&](size_t rows) -> ColumnPtr
    {
        auto values = ColumnUInt64::create();
        values->insertValue(0);
        auto sparse_offsets = ColumnUInt64::create();
        for (size_t i = 0; i < rows; ++i)
        {
            values->insertValue(i);
            sparse_offsets->insertValue(i);
        }
        auto sparse_nested = ColumnSparse::create(std::move(values), std::move(sparse_offsets), rows);
        Columns variants;
        variants.push_back(std::move(sparse_nested));
        return ColumnVariant::create(make_discriminators(rows), make_offsets(rows), variants);
    };

    auto full_column = make_full(n);
    auto sparse_column = make_sparse(n);
    std::vector<const IColumn *> sources{full_column.get(), sparse_column.get()};
    std::vector<std::vector<UInt16>> pids{makePids(n, 3), makePids(n, 3)};
    std::vector<std::span<const UInt16>> pid_spans{pids[0], pids[1]};

    ColumnsScatter::DispatchTrace trace;
    auto * previous = ColumnsScatter::exchangeDispatchTrace(&trace);
    auto result = ColumnsScatter::scatter(sources, pid_spans, 3);
    ColumnsScatter::exchangeDispatchTrace(previous);
    ASSERT_EQ(1u, trace.entries.size());
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::Fallback, trace.entries[0].kernel);

    auto reference_a = make_full(n);
    auto reference_b = make_full(n);
    std::vector<const IColumn *> reference_sources{reference_a.get(), reference_b.get()};
    auto expected = referenceScatter(reference_sources, pids, 3);
    for (size_t s = 0; s < 3; ++s)
        expectColumnsBitIdentical(*expected[s], *result[s], "variant shard " + std::to_string(s));
}

/// A QBit with one sparse `FixedString` bit-group element: normalization recurses through
/// `forEachSubcolumn`, so the sparse element is stripped even inside a composite that no type switch names.
TEST(ColumnsScatter, QBitMixedNestedRepresentation)
{
    constexpr size_t dimension = 8;
    constexpr size_t bytes = 1;
    constexpr size_t bit_groups = 16;
    auto build_qbit = [&](size_t rows, bool sparse_one_element) -> ColumnPtr
    {
        MutableColumns elements;
        for (size_t g = 0; g < bit_groups; ++g)
        {
            if (sparse_one_element && g == 3)
            {
                auto values = ColumnFixedString::create(bytes);
                values->insertDefault();
                auto offsets = ColumnUInt64::create();
                for (size_t i = 0; i < rows; ++i)
                    if (i % 2 == 0)
                    {
                        const char value = static_cast<char>('A' + (i % 20));
                        values->insertData(&value, 1);
                        offsets->insertValue(i);
                    }
                MutableColumnPtr values_base = std::move(values);
                MutableColumnPtr offsets_base = std::move(offsets);
                elements.push_back(ColumnSparse::create(std::move(values_base), std::move(offsets_base), rows));
            }
            else
            {
                auto group = ColumnFixedString::create(bytes);
                for (size_t i = 0; i < rows; ++i)
                {
                    const char value = static_cast<char>('a' + ((i + g) % 20));
                    group->insertData(&value, 1);
                }
                elements.push_back(std::move(group));
            }
        }
        MutableColumnPtr tuple = ColumnTuple::create(std::move(elements));
        return ColumnQBit::create(std::move(tuple), dimension, /*stride=*/dimension);
    };

    auto full_column = build_qbit(120, false);
    auto sparse_column = build_qbit(90, true);
    std::vector<const IColumn *> sources{full_column.get(), sparse_column.get()};
    std::vector<std::vector<UInt16>> pids{makePids(120, 3), makePids(90, 3)};
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::Fallback, ColumnsScatter::plannedKernel(*full_column));
    checkEquivalence(sources, pids, 3);
}

/// A type without a dedicated kernel: values preserved through the fallback, and the result stays `Dynamic`.
TEST(ColumnsScatter, DynamicFallbackEquivalence)
{
    auto type = DataTypeFactory::instance().get("Dynamic");
    auto column_a = type->createColumn();
    auto column_b = type->createColumn();
    for (size_t i = 0; i < 200; ++i)
    {
        column_a->insert(i % 3 == 0 ? Field("text_" + std::to_string(i)) : Field(static_cast<UInt64>(i)));
        if (i < 120)
            column_b->insert(i % 2 == 0 ? Field(-static_cast<Int64>(i)) : Field("b_" + std::to_string(i)));
    }
    std::vector<const IColumn *> sources{column_a.get(), column_b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(200, 4), makePids(120, 4)};
    std::vector<std::span<const UInt16>> pid_spans{pids[0], pids[1]};

    ColumnsScatter::DispatchTrace trace;
    auto * previous = ColumnsScatter::exchangeDispatchTrace(&trace);
    auto result = ColumnsScatter::scatter(sources, pid_spans, 4);
    ColumnsScatter::exchangeDispatchTrace(previous);
    ASSERT_EQ(1u, trace.entries.size());
    ASSERT_EQ(ColumnsScatter::ScatterKernelId::Fallback, trace.entries[0].kernel);

    auto expected = referenceScatter(sources, pids, 4);
    for (size_t s = 0; s < 4; ++s)
    {
        ASSERT_EQ(TypeIndex::Dynamic, result[s]->getDataType()) << "shard " << s;
        ASSERT_EQ(expected[s]->size(), result[s]->size()) << "shard " << s;
        for (size_t i = 0; i < expected[s]->size(); ++i)
            ASSERT_EQ((*expected[s])[i], (*result[s])[i]) << "shard " << s << " row " << i;
    }
}

/// LowCardinality nested inside a composite must stay LowCardinality per shard. The
/// serialize-based value oracle cannot see that.
TEST(ColumnsScatter, LowCardinalityInsideCompositesPreserved)
{
    auto make_tuple = [](size_t rows)
    {
        MutableColumns elements;
        elements.push_back(makeLowCardinalityStrings(rows, 6));
        elements.push_back(fillFixedRandom(ColumnUInt64::create(), rows));
        return ColumnTuple::create(std::move(elements));
    };
    for (size_t num_sources : {1uz, 2uz})
    {
        std::vector<MutableColumnPtr> owned;
        std::vector<const IColumn *> sources;
        std::vector<std::vector<UInt16>> pids;
        std::vector<std::span<const UInt16>> pid_spans;
        for (size_t b = 0; b < num_sources; ++b)
        {
            owned.push_back(make_tuple(150));
            sources.push_back(owned.back().get());
            pids.push_back(makePids(150, 4));
        }
        for (const auto & p : pids)
            pid_spans.emplace_back(p);
        auto result = ColumnsScatter::scatter(sources, pid_spans, 4);
        const IColumn * shared_nested_dictionary = nullptr;
        for (const auto & shard : result)
        {
            const auto & tuple = assert_cast<const ColumnTuple &>(*shard);
            ASSERT_EQ(TypeIndex::LowCardinality, tuple.getColumn(0).getDataType());
            /// One dictionary shared across shards, nested or not.
            if (num_sources == 1)
            {
                const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(tuple.getColumn(0));
                if (!shared_nested_dictionary)
                    shared_nested_dictionary = low_cardinality.getDictionaryPtr().get();
                else
                    ASSERT_EQ(shared_nested_dictionary, low_cardinality.getDictionaryPtr().get());
            }
        }
        auto expected = referenceScatter(sources, pids, 4);
        for (size_t s = 0; s < 4; ++s)
            expectColumnsBitIdentical(*expected[s], *result[s], "tuple-lc shard " + std::to_string(s));
    }

    auto make_array = [](size_t rows)
    {
        auto offsets = ColumnArray::ColumnOffsets::create();
        size_t total = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            total += 1 + (rng()() % 3);
            offsets->insert(total);
        }
        return ColumnArray::create(makeLowCardinalityStrings(total, 5), std::move(offsets));
    };
    auto array_a = make_array(120);
    auto array_b = make_array(80);
    std::vector<const IColumn *> sources{array_a.get(), array_b.get()};
    std::vector<std::vector<UInt16>> pids{makePids(120, 4), makePids(80, 4)};
    std::vector<std::span<const UInt16>> pid_spans{pids[0], pids[1]};
    auto result = ColumnsScatter::scatter(sources, pid_spans, 4);
    for (const auto & shard : result)
    {
        const auto & array = assert_cast<const ColumnArray &>(*shard);
        ASSERT_EQ(TypeIndex::LowCardinality, array.getData().getDataType());
    }
    auto expected = referenceScatter(sources, pids, 4);
    for (size_t s = 0; s < 4; ++s)
        expectColumnsBitIdentical(*expected[s], *result[s], "array-lc shard " + std::to_string(s));
}


/// `AggregateFunction` states through the fallback. Outputs view the source arena. A cross-source
/// append deep-copies through `ensureOwnership`. The states are arena-allocating `groupArray` ones with
/// distinct per-row payloads, so a misroute or permutation changes the serialized value. Every non-result
/// owner is destroyed before the results are read. Under ASan that catches a break in the result-to-arena
/// ownership chain.
TEST(ColumnsScatter, AggregateFunctionFallbackEquivalence)
{
    tryRegisterAggregateFunctions();
    auto type = DataTypeFactory::instance().get("AggregateFunction(groupArray, UInt64)");
    auto function = typeid_cast<const DataTypeAggregateFunction &>(*type).getFunction();
    auto make_states = [&](size_t rows, UInt64 salt)
    {
        auto values = ColumnUInt64::create();
        for (size_t i = 0; i < rows; ++i)
            values->insertValue(salt * 1000003 + i);
        const IColumn * arguments[1] = {values.get()};
        auto column = type->createColumn();
        auto & aggregate_column = typeid_cast<ColumnAggregateFunction &>(*column);
        Arena & arena = aggregate_column.createOrGetArena();
        for (size_t i = 0; i < rows; ++i)
        {
            column->insertDefault();
            function->add(aggregate_column.getData()[i], arguments, i, &arena);
        }
        return column;
    };
    for (size_t num_sources : {1uz, 2uz})
    {
        std::vector<MutableColumnPtr> owned;
        std::vector<const IColumn *> sources;
        std::vector<std::vector<UInt16>> pids;
        std::vector<std::span<const UInt16>> pid_spans;
        for (size_t b = 0; b < num_sources; ++b)
        {
            owned.push_back(make_states(100, b + 1));
            sources.push_back(owned.back().get());
            pids.push_back(makePids(100, 4));
        }
        for (const auto & p : pids)
            pid_spans.emplace_back(p);

        ColumnsScatter::DispatchTrace trace;
        auto * previous = ColumnsScatter::exchangeDispatchTrace(&trace);
        auto result = ColumnsScatter::scatter(sources, pid_spans, 4);
        ColumnsScatter::exchangeDispatchTrace(previous);
        ASSERT_EQ(1u, trace.entries.size());
        ASSERT_EQ(ColumnsScatter::ScatterKernelId::Fallback, trace.entries[0].kernel);

        /// Materialize the expectations into plain Fields first, then drop every non-result owner:
        /// reading the shards afterwards must hold up on the results' own ownership chain alone.
        auto expected = referenceScatter(sources, pids, 4);
        std::vector<std::vector<Field>> expected_fields(4);
        for (size_t s = 0; s < 4; ++s)
            for (size_t i = 0; i < expected[s]->size(); ++i)
                expected_fields[s].push_back((*expected[s])[i]);
        expected.clear();
        sources.clear();
        owned.clear();

        for (size_t s = 0; s < 4; ++s)
        {
            ASSERT_EQ(TypeIndex::AggregateFunction, result[s]->getDataType()) << "shard " << s;
            ASSERT_EQ(expected_fields[s].size(), result[s]->size()) << "shard " << s;
            for (size_t i = 0; i < expected_fields[s].size(); ++i)
                ASSERT_EQ(expected_fields[s][i], (*result[s])[i]) << "shard " << s << " row " << i;
        }
    }
}

/// Chunk kernels in the composition the join runs: histogram accumulated chunk by chunk through the
/// interleaved lanes and reduced once, one exact uninitialized allocation per shard, cursors seeded
/// once per column, then every chunk of every column scattered from the same pids.
TEST(ColumnsScatter, ChunkKernelsComposeLikeTheJoin)
{
    const size_t n = 10000;
    const size_t fanout = 64;
    /// Two uneven chunks, so the cursors and the lanes must carry state across calls.
    const std::array<std::pair<size_t, size_t>, 2> chunks{{{0, 3000}, {3000, n - 3000}}};

    auto keys = fillFixedRandom(ColumnUInt64::create(), n);
    auto payload = fillFixedRandom(ColumnUInt32::create(), n);
    const char * keys_raw = keys->getRawData().data();
    const char * payload_raw = payload->getRawData().data();
    const auto pids = makePids(n, fanout);

    std::vector<UInt64> expected_hist(fanout, 0);
    for (UInt16 pid : pids)
        ++expected_hist[pid];

    std::vector<UInt64> hist(fanout, 0);
    std::vector<UInt64> lanes(4 * fanout, 0);
    for (const auto & [offset, rows] : chunks)
        ColumnsScatter::histogramPidChunk(pids.data() + offset, rows, hist.data(), lanes.data(), fanout);
    ColumnsScatter::reduceHistogramLanes(hist.data(), lanes.data(), fanout);
    ASSERT_EQ(expected_hist, hist);

    const bool use_swwc = fanout >= ColumnsScatter::SWWC_MIN_FANOUT; /// false at 64: direct regime
    ColumnsScatter::ScatterScratch scratch;
    scratch.init(fanout, use_swwc);

    auto scatter_column = [&](const IColumn & sample, const char * raw_data, size_t width)
    {
        MutableColumns shards(fanout);
        for (size_t p = 0; p < fanout; ++p)
        {
            auto [column, raw] = ColumnsScatter::allocateUninitializedFixed(sample, hist[p]);
            shards[p] = std::move(column);
            scratch.seed(p, raw.data());
        }
        for (const auto & [offset, rows] : chunks)
            ColumnsScatter::scatterPidChunk(width, pids.data() + offset, raw_data + offset * width, rows, use_swwc, scratch);
        scratch.drain();
        return shards;
    };
    MutableColumns key_shards = scatter_column(*keys, keys_raw, 8);
    MutableColumns payload_shards = scatter_column(*payload, payload_raw, 4);

    std::vector<size_t> cursor(fanout, 0);
    for (size_t i = 0; i < n; ++i)
    {
        const size_t p = pids[i];
        UInt64 expected_key = 0;
        memcpy(&expected_key, keys_raw + i * 8, 8);
        UInt64 actual_key = 0;
        memcpy(&actual_key, key_shards[p]->getRawData().data() + cursor[p] * 8, 8);
        ASSERT_EQ(expected_key, actual_key) << "row " << i;
        UInt32 expected_payload = 0;
        memcpy(&expected_payload, payload_raw + i * 4, 4);
        UInt32 actual_payload = 0;
        memcpy(&actual_payload, payload_shards[p]->getRawData().data() + cursor[p] * 4, 4);
        ASSERT_EQ(expected_payload, actual_payload) << "row " << i;
        ++cursor[p];
    }
    for (size_t p = 0; p < fanout; ++p)
    {
        ASSERT_EQ(hist[p], key_shards[p]->size()) << "shard " << p;
        ASSERT_EQ(hist[p], payload_shards[p]->size()) << "shard " << p;
    }
}
