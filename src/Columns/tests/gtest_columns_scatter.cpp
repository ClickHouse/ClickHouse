#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsScatter.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Common/randomSeed.h>
#include <Common/thread_local_rng.h>

#include <cmath>
#include <cstring>
#include <string>
#include <vector>

using namespace DB;

namespace
{

pcg64 rng(randomSeed()); // NOLINT(cert-err58-cpp,bugprone-throwing-static-initialization)

// ── Test helpers ──────────────────────────────────────────────────────────────

/// Build pids of length n with values in [0, num_shards).
std::vector<UInt32> randomPids(size_t n, size_t num_shards)
{
    std::vector<UInt32> pids(n);
    for (auto & p : pids)
        p = static_cast<UInt32>(rng() % num_shards);
    return pids;
}

/// Reference: per-source-column legacy IColumn::scatter + insertRangeFrom.
MutableColumns referenceScatter(
    const std::vector<const IColumn *> & source_columns, const std::vector<std::vector<UInt32>> & pids_per_source, size_t num_shards)
{
    MutableColumns dst(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        dst[s] = source_columns[0]->cloneEmpty();

    for (size_t b = 0; b < source_columns.size(); ++b)
    {
        const auto & pids = pids_per_source[b];
        IColumn::Selector sel(pids.size());
        for (size_t j = 0; j < pids.size(); ++j)
            sel[j] = pids[j];
        auto parts = source_columns[b]->scatter(num_shards, sel);
        for (size_t s = 0; s < num_shards; ++s)
            dst[s]->insertRangeFrom(*parts[s], 0, parts[s]->size());
    }
    return dst;
}

void assertColumnsEqual(const IColumn & a, const IColumn & b)
{
    ASSERT_EQ(a.size(), b.size()) << a.getName();
    for (size_t i = 0; i < a.size(); ++i)
        ASSERT_EQ(a[i], b[i]) << a.getName() << " row " << i;
}

// ── Column factories ──────────────────────────────────────────────────────────

ColumnPtr makeUInt64Col(size_t num_rows, UInt64 start = 0)
{
    auto col = ColumnUInt64::create();
    col->reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        col->insertValue(start + i);
    return col;
}

ColumnPtr makeUInt32Col(size_t num_rows)
{
    auto col = ColumnUInt32::create();
    for (size_t i = 0; i < num_rows; ++i)
        col->insertValue(static_cast<UInt32>(i));
    return col;
}

ColumnPtr makeDecimal64Col(size_t num_rows)
{
    auto col = ColumnDecimal<Decimal64>::create(0, 2);
    for (size_t i = 0; i < num_rows; ++i)
        col->insertValue(Decimal64{static_cast<Int64>(i * 100)});
    return col;
}

ColumnPtr makeTime64Col(size_t num_rows)
{
    auto col = ColumnDecimal<Time64>::create(0, 3);
    for (size_t i = 0; i < num_rows; ++i)
        col->insertValue(Time64{static_cast<Int64>(i * 1000)});
    return col;
}

ColumnPtr makeArrayCol(size_t num_rows)
{
    auto data = ColumnUInt32::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    UInt64 off = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        const size_t len = i % 4;
        for (size_t k = 0; k < len; ++k)
            data->insertValue(static_cast<UInt32>(i * 10 + k));
        off += len;
        offsets->insertValue(off);
    }
    return ColumnArray::create(std::move(data), std::move(offsets));
}

ColumnPtr makeFixedStringCol(size_t num_rows, size_t fs_len)
{
    auto col = ColumnFixedString::create(fs_len);
    std::string s(fs_len, '\0');
    for (size_t i = 0; i < num_rows; ++i)
    {
        std::memcpy(s.data(), &i, std::min(sizeof(i), fs_len));
        col->insertData(s.data(), fs_len);
    }
    return col;
}

ColumnPtr makeStringCol(size_t num_rows)
{
    auto col = ColumnString::create();
    for (size_t i = 0; i < num_rows; ++i)
    {
        const std::string s = "str_" + std::to_string(i);
        col->insertData(s.data(), s.size());
    }
    return col;
}

ColumnPtr makeLowCardinalityStringCol(size_t num_rows, size_t num_distinct = 8)
{
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto col = type->createColumn();
    for (size_t i = 0; i < num_rows; ++i)
    {
        const std::string s = "lc_" + std::to_string(i % num_distinct);
        col->insertData(s.data(), s.size());
    }
    return col;
}

ColumnPtr makeConstLowCardinalityString(const std::string & value, size_t num_rows)
{
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto one = type->createColumn();
    one->insertData(value.data(), value.size());
    return ColumnConst::create(std::move(one), num_rows);
}

ColumnPtr makeNullableStringCol(size_t num_rows)
{
    auto nested = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (i % 3 == 0)
        {
            nested->insertDefault();
            null_map->insertValue(1);
        }
        else
        {
            const std::string s = "ns_" + std::to_string(i);
            nested->insertData(s.data(), s.size());
            null_map->insertValue(0);
        }
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

ColumnPtr makeTupleCol(size_t num_rows)
{
    MutableColumns elems;
    auto u32 = ColumnUInt32::create();
    auto str = ColumnString::create();
    for (size_t i = 0; i < num_rows; ++i)
    {
        u32->insertValue(static_cast<UInt32>(i));
        const std::string s = "t_" + std::to_string(i);
        str->insertData(s.data(), s.size());
    }
    elems.push_back(std::move(u32));
    elems.push_back(std::move(str));
    return ColumnTuple::create(std::move(elems));
}

// ── Core equivalence helper ───────────────────────────────────────────────────

void checkEquivalence(const std::vector<ColumnPtr> & cols, const std::vector<std::vector<UInt32>> & pids, size_t num_shards)
{
    ASSERT_EQ(cols.size(), pids.size());

    std::vector<const IColumn *> col_ptrs(cols.size());
    std::vector<std::span<const UInt32>> pid_spans(cols.size());
    for (size_t b = 0; b < cols.size(); ++b)
    {
        col_ptrs[b] = cols[b].get();
        pid_spans[b] = pids[b];
    }

    auto got = ColumnsScatter::scatter(col_ptrs, pid_spans, num_shards);
    auto ref = referenceScatter(col_ptrs, pids, num_shards);

    ASSERT_EQ(got.size(), num_shards);
    ASSERT_EQ(ref.size(), num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        assertColumnsEqual(*got[s], *ref[s]);
}

} // anonymous namespace

// ── Tests ─────────────────────────────────────────────────────────────────────

TEST(ColumnsScatter, UInt64SingleSource)
{
    constexpr size_t num_rows = 1000;
    constexpr size_t num_shards = 8;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeUInt64Col(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, UInt64BatchedSources)
{
    constexpr size_t num_rows = 500;
    constexpr size_t num_shards = 16;
    constexpr size_t num_batches = 4;
    std::vector<ColumnPtr> cols;
    std::vector<std::vector<UInt32>> pids;
    for (size_t b = 0; b < num_batches; ++b)
    {
        cols.push_back(makeUInt64Col(num_rows, b * num_rows));
        pids.push_back(randomPids(num_rows, num_shards));
    }
    checkEquivalence(cols, pids, num_shards);
}

TEST(ColumnsScatter, UInt32)
{
    constexpr size_t num_rows = 300;
    constexpr size_t num_shards = 4;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeUInt32Col(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, Decimal64)
{
    constexpr size_t num_rows = 400;
    constexpr size_t num_shards = 5;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeDecimal64Col(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, FixedString)
{
    constexpr size_t num_rows = 200;
    constexpr size_t num_shards = 6;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeFixedStringCol(num_rows, 16)}, {pids}, num_shards);
}

TEST(ColumnsScatter, String)
{
    constexpr size_t num_rows = 300;
    constexpr size_t num_shards = 7;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeStringCol(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, StringBatched)
{
    constexpr size_t num_rows = 200;
    constexpr size_t num_shards = 8;
    constexpr size_t num_batches = 3;
    std::vector<ColumnPtr> cols;
    std::vector<std::vector<UInt32>> pids;
    for (size_t b = 0; b < num_batches; ++b)
    {
        cols.push_back(makeStringCol(num_rows));
        pids.push_back(randomPids(num_rows, num_shards));
    }
    checkEquivalence(cols, pids, num_shards);
}

TEST(ColumnsScatter, NullableString)
{
    constexpr size_t num_rows = 300;
    constexpr size_t num_shards = 4;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeNullableStringCol(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, NullableStringBatched)
{
    constexpr size_t num_rows = 200;
    constexpr size_t num_shards = 5;
    constexpr size_t num_batches = 4;
    std::vector<ColumnPtr> cols;
    std::vector<std::vector<UInt32>> pids;
    for (size_t b = 0; b < num_batches; ++b)
    {
        cols.push_back(makeNullableStringCol(num_rows));
        pids.push_back(randomPids(num_rows, num_shards));
    }
    checkEquivalence(cols, pids, num_shards);
}

TEST(ColumnsScatter, Tuple)
{
    constexpr size_t num_rows = 250;
    constexpr size_t num_shards = 4;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeTupleCol(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, TupleBatched)
{
    constexpr size_t num_rows = 200;
    constexpr size_t num_shards = 5;
    constexpr size_t num_batches = 3;
    std::vector<ColumnPtr> cols;
    std::vector<std::vector<UInt32>> pids;
    for (size_t b = 0; b < num_batches; ++b)
    {
        cols.push_back(makeTupleCol(num_rows));
        pids.push_back(randomPids(num_rows, num_shards));
    }
    checkEquivalence(cols, pids, num_shards);
}

TEST(ColumnsScatter, Time64)
{
    constexpr size_t num_rows = 400;
    constexpr size_t num_shards = 5;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeTime64Col(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, Time64Batched)
{
    constexpr size_t num_rows = 300;
    constexpr size_t num_shards = 6;
    constexpr size_t num_batches = 3;
    std::vector<ColumnPtr> cols;
    std::vector<std::vector<UInt32>> pids;
    for (size_t b = 0; b < num_batches; ++b)
    {
        cols.push_back(makeTime64Col(num_rows));
        pids.push_back(randomPids(num_rows, num_shards));
    }
    checkEquivalence(cols, pids, num_shards);
}

TEST(ColumnsScatter, FallbackArray)
{
    // ColumnArray has no fast path: getDataType() == TypeIndex::Array lands on
    // the table's default slot (scatterFallback).
    constexpr size_t num_rows = 300;
    constexpr size_t num_shards = 4;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeArrayCol(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, FallbackConst)
{
    // ColumnConst reports its nested type's index from getDataType(); the
    // isConst() guard must route it to the fallback instead of a fast-path
    // kernel that would assert_cast onto the wrong concrete column.
    constexpr size_t num_rows = 300;
    constexpr size_t num_shards = 4;
    auto pids = randomPids(num_rows, num_shards);
    ColumnPtr const_col = ColumnConst::create(makeUInt64Col(1), num_rows);
    checkEquivalence({const_col}, {pids}, num_shards);
}

TEST(ColumnsScatter, AllRowsToOneShard)
{
    constexpr size_t num_rows = 500;
    constexpr size_t num_shards = 8;
    std::vector<UInt32> pids(num_rows, 0u);
    checkEquivalence({makeUInt64Col(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, ZeroRowSource)
{
    // One empty source column interleaved with a non-empty one.
    constexpr size_t num_shards = 4;
    auto col_empty = makeUInt64Col(0);
    auto col_nonempty = makeUInt64Col(100);
    std::vector<UInt32> pids_empty;
    auto pids_nonempty = randomPids(100, num_shards);
    checkEquivalence({col_empty, col_nonempty}, {pids_empty, pids_nonempty}, num_shards);
}

TEST(ColumnsScatter, SingleShard)
{
    constexpr size_t num_rows = 300;
    std::vector<UInt32> pids(num_rows, 0u);
    checkEquivalence({makeUInt64Col(num_rows)}, {pids}, 1);
}

TEST(ColumnsScatter, HighShardCount)
{
    // num_shards = 512 > SCATTER_INLINE_SHARDS (256) — exercises the heap-backed
    // InlinedVector path.
    constexpr size_t num_rows = 500;
    constexpr size_t num_shards = 512;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeUInt64Col(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, HighShardCountString)
{
    // num_shards = 512 > SCATTER_INLINE_SHARDS (256) — exercises the heap-backed
    // path of the String kernel (three InlinedVector scratch arrays).
    constexpr size_t num_rows = 500;
    constexpr size_t num_shards = 512;
    auto pids = randomPids(num_rows, num_shards);
    checkEquivalence({makeStringCol(num_rows)}, {pids}, num_shards);
}

TEST(ColumnsScatter, LargeDecimalBatched)
{
    // Decimal256: 32-byte NativeType, tests large-element reinterpret path.
    constexpr size_t num_rows = 200;
    constexpr size_t num_shards = 4;
    auto make = [&]() -> ColumnPtr
    {
        auto col = ColumnDecimal<Decimal256>::create(0, 10);
        for (size_t i = 0; i < num_rows; ++i)
            col->insertValue(Decimal256{static_cast<Int256>(i)});
        return col;
    };
    std::vector<ColumnPtr> cols = {make(), make()};
    auto p0 = randomPids(num_rows, num_shards);
    auto p1 = randomPids(num_rows, num_shards);
    checkEquivalence(cols, {p0, p1}, num_shards);
}

namespace
{
/// Independent oracle for batches whose chunks mix concrete representations.
/// `referenceScatter` is only correct on homogeneous full inputs, so materialize
/// every source first and scatter the full versions. `ColumnsScatter::scatter` must
/// reach the same result while normalizing representations internally.
///
/// NOTE: `convertToFullIfNeeded` strips `LowCardinality`, so this oracle only checks
/// per-row *values*, not the physical representation. It must not be used to assert that
/// `LowCardinality` is preserved — see `LowCardinalityPreservesType`, which checks the
/// representation contract explicitly.
void checkScatterAgainstMaterialized(
    const std::vector<ColumnPtr> & cols, const std::vector<std::vector<UInt32>> & pids, size_t num_shards)
{
    std::vector<const IColumn *> col_ptrs(cols.size());
    std::vector<std::span<const UInt32>> pid_spans(cols.size());
    for (size_t b = 0; b < cols.size(); ++b)
    {
        col_ptrs[b] = cols[b].get();
        pid_spans[b] = pids[b];
    }
    auto got = ColumnsScatter::scatter(col_ptrs, pid_spans, num_shards);

    std::vector<ColumnPtr> full(cols.size());
    std::vector<const IColumn *> full_ptrs(cols.size());
    for (size_t b = 0; b < cols.size(); ++b)
    {
        full[b] = cols[b]->convertToFullIfNeeded();
        full_ptrs[b] = full[b].get();
    }
    auto ref = referenceScatter(full_ptrs, pids, num_shards);

    ASSERT_EQ(got.size(), num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        assertColumnsEqual(*got[s], *ref[s]);
}

ColumnPtr makeConstUInt64(UInt64 value, size_t num_rows)
{
    auto one = ColumnUInt64::create();
    one->insertValue(value);
    return ColumnConst::create(std::move(one), num_rows);
}

ColumnPtr makeConstString(const std::string & value, size_t num_rows)
{
    auto one = ColumnString::create();
    one->insertData(value.data(), value.size());
    return ColumnConst::create(std::move(one), num_rows);
}
}

TEST(ColumnsScatter, MixedConstFirstUInt64)
{
    // Const chunk first: the fallback must not clone a ColumnConst destination and
    // then insert materialized values into it.
    constexpr size_t num_shards = 5;
    const size_t n0 = 200;
    const size_t n1 = 300;
    const size_t n2 = 250;
    std::vector<ColumnPtr> cols = {makeConstUInt64(777, n0), makeUInt64Col(n1, 10), makeConstUInt64(888, n2)};
    std::vector<std::vector<UInt32>> pids = {randomPids(n0, num_shards), randomPids(n1, num_shards), randomPids(n2, num_shards)};
    checkScatterAgainstMaterialized(cols, pids, num_shards);
}

TEST(ColumnsScatter, MixedMaterializedFirstUInt64)
{
    // Materialized chunk first: the fast typed kernel must not assert_cast the later
    // ColumnConst onto ColumnVector.
    constexpr size_t num_shards = 4;
    const size_t n0 = 300;
    const size_t n1 = 200;
    std::vector<ColumnPtr> cols = {makeUInt64Col(n0, 0), makeConstUInt64(42, n1)};
    std::vector<std::vector<UInt32>> pids = {randomPids(n0, num_shards), randomPids(n1, num_shards)};
    checkScatterAgainstMaterialized(cols, pids, num_shards);
}

TEST(ColumnsScatter, TwoConstsDifferentValues)
{
    // Homogeneous ColumnConst but with different constant values across chunks: a
    // single ColumnConst destination cannot hold both, so it must be materialized.
    constexpr size_t num_shards = 4;
    const size_t n0 = 250;
    const size_t n1 = 250;
    std::vector<ColumnPtr> cols = {makeConstUInt64(5, n0), makeConstUInt64(9, n1)};
    std::vector<std::vector<UInt32>> pids = {randomPids(n0, num_shards), randomPids(n1, num_shards)};
    checkScatterAgainstMaterialized(cols, pids, num_shards);
}

TEST(ColumnsScatter, MixedConstMaterializedString)
{
    // Variable-width fallback path with a mixed ColumnConst(String) / ColumnString batch.
    constexpr size_t num_shards = 5;
    const size_t n0 = 200;
    const size_t n1 = 150;
    const size_t n2 = 220;
    std::vector<ColumnPtr> cols = {makeStringCol(n0), makeConstString("konst", n1), makeStringCol(n2)};
    std::vector<std::vector<UInt32>> pids = {randomPids(n0, num_shards), randomPids(n1, num_shards), randomPids(n2, num_shards)};
    checkScatterAgainstMaterialized(cols, pids, num_shards);
}

TEST(ColumnsScatter, TupleWithSparseElement)
{
    // A Tuple element can have a different concrete representation across chunks (full in
    // one, ColumnSparse in another). The recursive tuple scatter must normalize element
    // columns before dispatch, otherwise the typed kernel assert_casts the sparse element.
    constexpr size_t num_shards = 4;
    const size_t n0 = 300;
    const size_t n1 = 250;

    auto build_full_tuple = [](size_t rows) -> ColumnPtr
    {
        auto u = ColumnUInt64::create();
        for (size_t i = 0; i < rows; ++i)
            u->insertValue(i * 3 + 1);
        MutableColumns elems;
        elems.push_back(std::move(u));
        return ColumnTuple::create(std::move(elems));
    };

    auto build_sparse_tuple = [](size_t rows) -> ColumnPtr
    {
        auto vals = ColumnUInt64::create();
        vals->insertValue(0); // index 0 is the sparse default
        vals->insertValue(7);
        vals->insertValue(9);
        auto offs = ColumnUInt64::create();
        offs->insertValue(5);
        offs->insertValue(100);
        MutableColumns elems;
        elems.push_back(ColumnSparse::create(std::move(vals), std::move(offs), rows));
        return ColumnTuple::create(std::move(elems));
    };

    std::vector<ColumnPtr> cols = {build_full_tuple(n0), build_sparse_tuple(n1)};
    std::vector<std::vector<UInt32>> pids = {randomPids(n0, num_shards), randomPids(n1, num_shards)};
    checkScatterAgainstMaterialized(cols, pids, num_shards);
}

TEST(ColumnsScatter, LowCardinalityPreservesType)
{
    // A ColumnConst(ColumnLowCardinality(String)) under a LowCardinality(String) header must be
    // scattered WITHOUT stripping LowCardinality: the output chunk's physical column type has to
    // stay LowCardinality to match the transform's port/header contract. scatter() materializes
    // only Const/Sparse/Replicated wrappers (leaving LowCardinality intact) and routes the full
    // LowCardinality column to the fallback, which clones an empty LowCardinality and preserves
    // the type. A regression that called convertToFullIfNeeded would materialize the source to a
    // plain ColumnString and silently break the contract.
    constexpr size_t num_shards = 5;
    const size_t n0 = 200;
    const size_t n1 = 300;
    const size_t n2 = 150;

    std::vector<ColumnPtr> cols
        = {makeLowCardinalityStringCol(n0), makeConstLowCardinalityString("konst_lc", n1), makeLowCardinalityStringCol(n2)};
    std::vector<std::vector<UInt32>> pids = {randomPids(n0, num_shards), randomPids(n1, num_shards), randomPids(n2, num_shards)};

    std::vector<const IColumn *> col_ptrs(cols.size());
    std::vector<std::span<const UInt32>> pid_spans(cols.size());
    for (size_t b = 0; b < cols.size(); ++b)
    {
        col_ptrs[b] = cols[b].get();
        pid_spans[b] = pids[b];
    }
    auto got = ColumnsScatter::scatter(col_ptrs, pid_spans, num_shards);

    // Representation contract: every scattered shard must remain LowCardinality.
    ASSERT_EQ(got.size(), num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        ASSERT_EQ(got[s]->getDataType(), TypeIndex::LowCardinality) << "shard " << s << " lost LowCardinality";

    // Value contract: compare against a full-column reference. Materialize both sides only for
    // the value comparison here — never for the scatter under test above.
    std::vector<ColumnPtr> full(cols.size());
    std::vector<const IColumn *> full_ptrs(cols.size());
    for (size_t b = 0; b < cols.size(); ++b)
    {
        full[b] = cols[b]->convertToFullIfNeeded();
        full_ptrs[b] = full[b].get();
    }
    auto ref = referenceScatter(full_ptrs, pids, num_shards);
    for (size_t s = 0; s < num_shards; ++s)
    {
        auto got_full = got[s]->convertToFullIfNeeded();
        assertColumnsEqual(*got_full, *ref[s]);
    }
}

TEST(ColumnsScatter, FallbackArrayMixedNestedRepresentation)
{
    // Array(UInt64) goes through scatterFallback because it has no fast-path dispatch
    // entry. When one chunk has a full nested ColumnUInt64 and another has a
    // ColumnSparse(UInt64) nested column with the same logical values, the cross-chunk
    // insertRangeFrom in scatterFallback must not fail due to representation mismatch.
    // Before the fix, ColumnUInt64::insertRangeFrom asserts-cast on the sparse source
    // and causes a failure in sanitized builds.
    constexpr size_t NUM_SHARDS = 3;
    const size_t N = 8;

    // Chunk 0: full nested data. Each row is a single-element array [[1],[2],...,[N]].
    auto make_full_array = [](size_t n) -> ColumnPtr
    {
        auto data = ColumnUInt64::create();
        auto offs = ColumnArray::ColumnOffsets::create();
        for (size_t i = 0; i < n; ++i)
        {
            data->insertValue(static_cast<UInt64>(i + 1));
            offs->insertValue(static_cast<UInt64>(i + 1));
        }
        return ColumnArray::create(std::move(data), std::move(offs));
    };

    // Chunk 1: sparse nested data with identical logical values [[1],[2],...,[N]].
    // ColumnSparse: values[0] = default (0), values[k] = k for k = 1..N.
    // All positions 0..N-1 are non-default (listed in offsets).
    auto make_sparse_array = [](size_t n) -> ColumnPtr
    {
        auto vals = ColumnUInt64::create();
        vals->insertValue(0); // default value at index 0 of vals
        auto sparse_offs = ColumnUInt64::create();
        for (size_t i = 0; i < n; ++i)
        {
            vals->insertValue(static_cast<UInt64>(i + 1)); // non-default value
            sparse_offs->insertValue(static_cast<UInt64>(i)); // position i is non-default
        }
        auto sparse_nested = ColumnSparse::create(std::move(vals), std::move(sparse_offs), n);
        auto offs = ColumnArray::ColumnOffsets::create();
        for (size_t i = 0; i < n; ++i)
            offs->insertValue(static_cast<UInt64>(i + 1));
        return ColumnArray::create(std::move(sparse_nested), std::move(offs));
    };

    auto full_col = make_full_array(N);
    auto sparse_col = make_sparse_array(N);

    std::vector<std::vector<UInt32>> pids = {randomPids(N, NUM_SHARDS), randomPids(N, NUM_SHARDS)};

    std::vector<const IColumn *> col_ptrs = {full_col.get(), sparse_col.get()};
    std::vector<std::span<const UInt32>> pid_spans = {pids[0], pids[1]};
    // Must not throw or produce garbage despite mismatched nested representations.
    auto got = ColumnsScatter::scatter(col_ptrs, pid_spans, NUM_SHARDS);

    // Oracle: scatter two full-nested arrays with the same values and same pids.
    auto ref_col0 = make_full_array(N);
    auto ref_col1 = make_full_array(N);
    std::vector<const IColumn *> ref_ptrs = {ref_col0.get(), ref_col1.get()};
    auto ref = referenceScatter(ref_ptrs, pids, NUM_SHARDS);

    ASSERT_EQ(got.size(), NUM_SHARDS);
    for (size_t s = 0; s < NUM_SHARDS; ++s)
        assertColumnsEqual(*got[s], *ref[s]);
}

TEST(ColumnsScatter, FallbackMapMixedNestedRepresentation)
{
    // Map(UInt64, UInt64) goes through scatterFallback (no fast-path entry). ColumnMap wraps a
    // ColumnArray(Tuple(key, value)); ColumnMap::scatter preserves that nested representation.
    // When one chunk's value element is a full ColumnUInt64 and another's is a ColumnSparse(UInt64)
    // with the same logical values, the cross-chunk insertRangeFrom in scatterFallback must not
    // fail. Without the Map branch in deepNormalizeForFallback, ColumnUInt64::insertRangeFrom
    // assert-casts on the sparse source and fails in sanitized builds.
    constexpr size_t NUM_SHARDS = 3;
    const size_t N = 8;

    auto make_offsets = [](size_t n) -> MutableColumnPtr
    {
        auto offs = ColumnArray::ColumnOffsets::create();
        for (size_t i = 0; i < n; ++i)
            offs->insertValue(static_cast<UInt64>(i + 1));
        return offs;
    };

    auto make_full_keys = [](size_t n) -> MutableColumnPtr
    {
        auto keys = ColumnUInt64::create();
        for (size_t i = 0; i < n; ++i)
            keys->insertValue(static_cast<UInt64>(i + 1));
        return keys;
    };

    // Chunk 0: full nested value column. Each row is a single-entry map {i+1: i+1}.
    auto make_full_map = [&](size_t n) -> ColumnPtr
    {
        auto values = ColumnUInt64::create();
        for (size_t i = 0; i < n; ++i)
            values->insertValue(static_cast<UInt64>(i + 1));
        auto tuple = ColumnTuple::create(Columns{make_full_keys(n), std::move(values)});
        auto nested = ColumnArray::create(std::move(tuple), make_offsets(n));
        return ColumnMap::create(nested);
    };

    // Chunk 1: sparse nested value column with identical logical values.
    // ColumnSparse: values[0] = default (0), values[k] = k for k = 1..N; all positions non-default.
    auto make_sparse_map = [&](size_t n) -> ColumnPtr
    {
        auto vals = ColumnUInt64::create();
        vals->insertValue(0); // default value at index 0
        auto sparse_offs = ColumnUInt64::create();
        for (size_t i = 0; i < n; ++i)
        {
            vals->insertValue(static_cast<UInt64>(i + 1));
            sparse_offs->insertValue(static_cast<UInt64>(i));
        }
        auto sparse_values = ColumnSparse::create(std::move(vals), std::move(sparse_offs), n);
        auto tuple = ColumnTuple::create(Columns{make_full_keys(n), std::move(sparse_values)});
        auto nested = ColumnArray::create(std::move(tuple), make_offsets(n));
        return ColumnMap::create(nested);
    };

    auto full_col = make_full_map(N);
    auto sparse_col = make_sparse_map(N);

    std::vector<std::vector<UInt32>> pids = {randomPids(N, NUM_SHARDS), randomPids(N, NUM_SHARDS)};

    std::vector<const IColumn *> col_ptrs = {full_col.get(), sparse_col.get()};
    std::vector<std::span<const UInt32>> pid_spans = {pids[0], pids[1]};
    // Must not throw or produce garbage despite mismatched nested representations.
    auto got = ColumnsScatter::scatter(col_ptrs, pid_spans, NUM_SHARDS);

    // Oracle: scatter two full-nested maps with the same values and same pids.
    auto ref_col0 = make_full_map(N);
    auto ref_col1 = make_full_map(N);
    std::vector<const IColumn *> ref_ptrs = {ref_col0.get(), ref_col1.get()};
    auto ref = referenceScatter(ref_ptrs, pids, NUM_SHARDS);

    ASSERT_EQ(got.size(), NUM_SHARDS);
    for (size_t s = 0; s < NUM_SHARDS; ++s)
        assertColumnsEqual(*got[s], *ref[s]);
}

TEST(ColumnsScatter, FallbackVariantMixedNestedRepresentation)
{
    // Variant goes through scatterFallback (no fast-path entry). ColumnVariant::scatter preserves
    // each alternative's physical representation, and ColumnVariant::insertRangeFrom appends via the
    // nested alternative's insertRangeFrom. A batch where one chunk's UInt64 alternative is a full
    // ColumnUInt64 and another's is a ColumnSparse(UInt64) must not fail: the generic
    // deepNormalizeForFallback recursion normalizes each alternative before the cross-source append.
    constexpr size_t NUM_SHARDS = 3;
    const size_t N = 8;

    // Single-alternative Variant(UInt64), all rows non-null (discriminator 0).
    auto make_discriminators = [](size_t n) -> MutableColumnPtr
    {
        auto discr = ColumnVariant::ColumnDiscriminators::create();
        for (size_t i = 0; i < n; ++i)
            discr->insertValue(0);
        return discr;
    };
    auto make_offsets = [](size_t n) -> MutableColumnPtr
    {
        auto offs = ColumnVariant::ColumnOffsets::create();
        for (size_t i = 0; i < n; ++i)
            offs->insertValue(i);
        return offs;
    };

    // Chunk 0: full nested alternative [0, 1, ..., N-1].
    auto make_full_variant = [&](size_t n) -> ColumnPtr
    {
        auto nested = ColumnUInt64::create();
        for (size_t i = 0; i < n; ++i)
            nested->insertValue(static_cast<UInt64>(i));
        Columns variants;
        variants.push_back(std::move(nested));
        return ColumnVariant::create(make_discriminators(n), make_offsets(n), variants);
    };

    // Chunk 1: sparse nested alternative with identical logical values.
    auto make_sparse_variant = [&](size_t n) -> ColumnPtr
    {
        auto vals = ColumnUInt64::create();
        vals->insertValue(0); // default value at index 0
        auto sparse_offs = ColumnUInt64::create();
        for (size_t i = 0; i < n; ++i)
        {
            vals->insertValue(static_cast<UInt64>(i));
            sparse_offs->insertValue(static_cast<UInt64>(i));
        }
        auto sparse_nested = ColumnSparse::create(std::move(vals), std::move(sparse_offs), n);
        Columns variants;
        variants.push_back(std::move(sparse_nested));
        return ColumnVariant::create(make_discriminators(n), make_offsets(n), variants);
    };

    auto full_col = make_full_variant(N);
    auto sparse_col = make_sparse_variant(N);

    std::vector<std::vector<UInt32>> pids = {randomPids(N, NUM_SHARDS), randomPids(N, NUM_SHARDS)};

    std::vector<const IColumn *> col_ptrs = {full_col.get(), sparse_col.get()};
    std::vector<std::span<const UInt32>> pid_spans = {pids[0], pids[1]};
    // Must not throw or produce garbage despite mismatched nested representations.
    auto got = ColumnsScatter::scatter(col_ptrs, pid_spans, NUM_SHARDS);

    // Oracle: scatter two full-nested variants with the same values and same pids.
    auto ref_col0 = make_full_variant(N);
    auto ref_col1 = make_full_variant(N);
    std::vector<const IColumn *> ref_ptrs = {ref_col0.get(), ref_col1.get()};
    auto ref = referenceScatter(ref_ptrs, pids, NUM_SHARDS);

    ASSERT_EQ(got.size(), NUM_SHARDS);
    for (size_t s = 0; s < NUM_SHARDS; ++s)
        assertColumnsEqual(*got[s], *ref[s]);
}

TEST(ColumnsScatter, ConstBitExactNotOrderingEqual)
{
    // The all-const compact path must use byte-exact equality, not compareAt (ordering equality).
    // compareAt treats +0.0 and -0.0 as equal, but scatter physically splits the column and must
    // preserve exact bytes. Two const chunks carrying +0.0 and -0.0 must not collapse to one
    // payload: rows from the -0.0 chunk must keep their sign bit.
    constexpr size_t NUM_SHARDS = 3;
    const size_t N0 = 10;
    const size_t N1 = 7;

    auto make_const = [](double value, size_t n) -> ColumnPtr
    {
        auto one = ColumnFloat64::create();
        one->insertValue(value);
        return ColumnConst::create(std::move(one), n);
    };

    auto pos_zero = make_const(0.0, N0);
    auto neg_zero = make_const(-0.0, N1);

    std::vector<std::vector<UInt32>> pids = {randomPids(N0, NUM_SHARDS), randomPids(N1, NUM_SHARDS)};
    std::vector<const IColumn *> col_ptrs = {pos_zero.get(), neg_zero.get()};
    std::vector<std::span<const UInt32>> pid_spans = {pids[0], pids[1]};

    auto got = ColumnsScatter::scatter(col_ptrs, pid_spans, NUM_SHARDS);

    ASSERT_EQ(got.size(), NUM_SHARDS);
    size_t neg_zeros = 0;
    size_t total = 0;
    for (size_t s = 0; s < NUM_SHARDS; ++s)
    {
        auto full = got[s]->convertToFullColumnIfConst();
        const auto & data = assert_cast<const ColumnFloat64 &>(*full).getData();
        for (const double v : data)
            if (std::signbit(v))
                ++neg_zeros;
        total += data.size();
    }
    EXPECT_EQ(total, N0 + N1);
    // Every row from the -0.0 chunk must keep its sign bit; with the buggy compareAt shortcut they
    // would all be rewritten to +0.0 and neg_zeros would be 0.
    EXPECT_EQ(neg_zeros, N1) << "negative-zero rows were rewritten to +0.0 (ordering-equality shortcut)";
}

TEST(ColumnsScatter, ConstColumnStaysCompact)
{
    // A homogeneous batch of equal-valued ColumnConst sources must not be
    // materialized into full columns: scatter() must return ColumnConst per shard,
    // preserving O(1)-memory behavior for constant-key workloads like GROUP BY
    // toUInt64(1). Before the fix, the batch was expanded via materializeTransparentWrappers
    // before the all-const check existed.
    constexpr size_t NUM_SHARDS = 4;
    constexpr UInt64 kValue = 99;

    auto make_const = [](size_t n) -> ColumnPtr
    {
        auto one = ColumnUInt64::create();
        one->insertValue(kValue);
        return ColumnConst::create(std::move(one), n);
    };

    std::vector<ColumnPtr> cols = {make_const(100), make_const(200), make_const(150)};
    std::vector<std::vector<UInt32>> pids = {
        randomPids(100, NUM_SHARDS),
        randomPids(200, NUM_SHARDS),
        randomPids(150, NUM_SHARDS),
    };

    std::vector<const IColumn *> col_ptrs(cols.size());
    std::vector<std::span<const UInt32>> pid_spans(cols.size());
    for (size_t b = 0; b < cols.size(); ++b)
    {
        col_ptrs[b] = cols[b].get();
        pid_spans[b] = pids[b];
    }
    auto got = ColumnsScatter::scatter(col_ptrs, pid_spans, NUM_SHARDS);

    ASSERT_EQ(got.size(), NUM_SHARDS);
    size_t total_rows = 0;
    for (size_t s = 0; s < NUM_SHARDS; ++s)
    {
        ASSERT_TRUE(got[s]->isConst()) << "shard " << s << " must remain ColumnConst (no materialization)";
        for (size_t i = 0; i < got[s]->size(); ++i)
            ASSERT_EQ((*got[s])[i], Field(UInt64(kValue)));
        total_rows += got[s]->size();
    }
    ASSERT_EQ(total_rows, 100u + 200u + 150u);
}
