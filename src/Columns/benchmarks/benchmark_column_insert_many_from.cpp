#include <cstddef>
#include <cstdint>
#include <random>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>
#include <benchmark/benchmark.h>

using namespace DB;

static constexpr size_t ROWS = 65536;

static ColumnPtr mockColumn(const DataTypePtr & type, size_t rows)
{
    const auto * type_array = typeid_cast<const DataTypeArray *>(type.get());
    if (type_array)
    {
        auto data_col = mockColumn(type_array->getNestedType(), rows);
        auto offset_col = ColumnArray::ColumnOffsets::create(rows);
        auto & offsets = offset_col->getData();
        std::mt19937_64 random_engine(std::random_device{}());
        std::uniform_int_distribution<size_t> random_length(0, 9);
        size_t offset = 0;
        for (auto & current_offset : offsets)
        {
            offset += random_length(random_engine);
            current_offset = offset;
        }
        auto new_data_col = data_col->replicate(offsets);

        return ColumnArray::create(new_data_col, std::move(offset_col));
    }

    auto type_not_nullable = removeNullable(type);
    auto column = type->createColumn();
    for (size_t i = 0; i < rows; ++i)
    {
        if (i % 100)
            column->insertDefault();
        else if (isInt(type_not_nullable))
            column->insert(i);
        else if (isFloat(type_not_nullable))
        {
            double d = static_cast<double>(i);
            column->insert(d);
        }
        else if (isString(type_not_nullable))
        {
            String s = "helloworld";
            column->insert(s);
        }
        else
            column->insertDefault();
    }
    return std::move(column);
}

static ColumnPtr mockNonEmptyStringColumn(size_t rows)
{
    auto column = DataTypeFactory::instance().get("String")->createColumn();
    const String value = "helloworld123456";

    for (size_t i = 0; i < rows; ++i)
        column->insert(value);

    return column;
}

static ColumnPtr mockRepeatedArrayColumn(const DataTypePtr & type, size_t array_size)
{
    const auto * type_array = typeid_cast<const DataTypeArray *>(type.get());
    chassert(type_array);

    const auto & nested_type = type_array->getNestedType();
    auto data_col = nested_type->createColumn();
    for (size_t i = 0; i < array_size; ++i)
    {
        if (isString(removeLowCardinality(removeNullable(nested_type))))
            data_col->insert(String("repeated"));
        else
            data_col->insertDefault();
    }

    auto offsets_col = ColumnArray::ColumnOffsets::create(1);
    offsets_col->getData()[0] = array_size;
    return ColumnArray::create(std::move(data_col), std::move(offsets_col));
}


static NO_INLINE void insertManyFrom(IColumn & dst, const IColumn & src)
{
    size_t size = src.size();
    dst.insertManyFrom(src, size / 2, size);
}

static NO_INLINE void insertFromRepeatedly(IColumn & dst, const IColumn & src, size_t length)
{
    for (size_t i = 0; i < length; ++i)
        dst.insertFrom(src, 0);
}

static NO_INLINE void insertManyFromRepeatedly(IColumn & dst, const IColumn & src, size_t length)
{
    dst.insertManyFrom(src, 0, length);
}

static void reserveRepeatedArray(IColumn & dst, const IColumn & src, size_t length)
{
    auto & dst_array = assert_cast<ColumnArray &>(dst);
    const auto & src_array = assert_cast<const ColumnArray &>(src);
    dst_array.getOffsets().reserve_exact(length);
    dst_array.getData().reserve(length * src_array.getData().size());

    if (auto * dst_string = typeid_cast<ColumnString *>(&dst_array.getData()))
    {
        const auto & src_string = assert_cast<const ColumnString &>(src_array.getData());
        dst_string->getChars().reserve_exact(length * src_string.getChars().size());
    }
}

static NO_INLINE void insertManyFromNotNullable(ColumnNullable & dst, const IColumn & src, size_t position, size_t length)
{
    dst.insertManyFromNotNullable(src, position, length);
}

static NO_INLINE void insertManyFromNotNullableScalar(ColumnNullable & dst, const IColumn & src, size_t position, size_t length)
{
    for (size_t i = 0; i < length; ++i)
        dst.insertFromNotNullable(src, position);
}

static NO_INLINE void insertManyFromNullableSource(ColumnNullable & dst, const IColumn & src, size_t position, size_t length)
{
    dst.insertManyFrom(src, position, length);
}

static NO_INLINE void insertManyFromNullableSourceScalar(ColumnNullable & dst, const IColumn & src, size_t position, size_t length)
{
    for (size_t i = 0; i < length; ++i)
        dst.insertFrom(src, position);
}


template <const std::string & str_type>
static void BM_insertManyFrom(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    auto src = mockColumn(type, ROWS);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto dst = type->createColumn();
        dst->reserve(ROWS);
        state.ResumeTiming();

        insertManyFrom(*dst, *src);
        benchmark::DoNotOptimize(dst);
    }
}

template <const std::string & str_type>
static void BM_insertFromRepeatedlyArray(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    const size_t array_size = static_cast<size_t>(state.range(0));
    const size_t length = static_cast<size_t>(state.range(1));
    auto src = mockRepeatedArrayColumn(type, array_size);
    MutableColumnPtr dst;

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        dst = type->createColumn();
        reserveRepeatedArray(*dst, *src, length);
        state.ResumeTiming();

        insertFromRepeatedly(*dst, *src, length);
        benchmark::DoNotOptimize(dst);
    }
}

template <const std::string & str_type>
static void BM_insertManyFromRepeatedlyArray(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    const size_t array_size = static_cast<size_t>(state.range(0));
    const size_t length = static_cast<size_t>(state.range(1));
    auto src = mockRepeatedArrayColumn(type, array_size);
    MutableColumnPtr dst;

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        dst = type->createColumn();
        reserveRepeatedArray(*dst, *src, length);
        state.ResumeTiming();

        insertManyFromRepeatedly(*dst, *src, length);
        benchmark::DoNotOptimize(dst);
    }
}

template <const std::string & str_type, bool use_bulk_insert, bool nullable_source>
static void BM_insertRepeatedNullableImpl(benchmark::State & state)
{
    auto nullable_type = DataTypeFactory::instance().get(str_type);
    auto type_not_nullable = removeNullable(nullable_type);
    auto src_not_nullable = isString(type_not_nullable)
        ? mockNonEmptyStringColumn(ROWS)
        : mockColumn(type_not_nullable, ROWS);
    auto src = nullable_source ? makeNullable(src_not_nullable) : src_not_nullable;
    const size_t length = static_cast<size_t>(state.range(0));
    /// Unlike ROWS / 2, this position contains a non-default Int64 value.
    constexpr size_t position = 100;

    if (length == 0 || ROWS % length != 0)
    {
        state.SkipWithError("The repetition count must be a nonzero divisor of ROWS");
        return;
    }

    auto dst = nullable_type->createColumn();
    auto & dst_nullable = assert_cast<ColumnNullable &>(*dst);
    /// Pre-fill a whole output block so both offsets and string bytes have capacity.
    /// reserve(length) alone need not reserve the string payload. Keep destination
    /// construction, buffer growth, reset, and destruction outside the timed work.
    insertManyFromNotNullable(dst_nullable, *src_not_nullable, position, ROWS);

    for ([[maybe_unused]] auto _ : state)
    {
        state.PauseTiming();
        dst_nullable.popBack(dst_nullable.size());
        state.ResumeTiming();

        /// Amortize timing-control overhead over a block, including for tiny fanouts.
        for (size_t offset = 0; offset < ROWS; offset += length)
        {
            if constexpr (nullable_source)
            {
                if constexpr (use_bulk_insert)
                    insertManyFromNullableSource(dst_nullable, *src, position, length);
                else
                    insertManyFromNullableSourceScalar(dst_nullable, *src, position, length);
            }
            else
            {
                if constexpr (use_bulk_insert)
                    insertManyFromNotNullable(dst_nullable, *src, position, length);
                else
                    insertManyFromNotNullableScalar(dst_nullable, *src, position, length);
            }
        }
        benchmark::DoNotOptimize(dst);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(ROWS));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(dst->byteSize()));
}

template <const std::string & str_type>
static void BM_insertManyFromNotNullable(benchmark::State & state)
{
    BM_insertRepeatedNullableImpl<str_type, true, false>(state);
}

template <const std::string & str_type>
static void BM_insertManyFromNotNullableScalar(benchmark::State & state)
{
    BM_insertRepeatedNullableImpl<str_type, false, false>(state);
}

/// Stream joins with join_use_nulls can make the right source Nullable before
/// MergeJoin sees it. Measure that separate, already-bulk path explicitly.
template <const std::string & str_type>
static void BM_insertManyFromNullableSource(benchmark::State & state)
{
    BM_insertRepeatedNullableImpl<str_type, true, true>(state);
}

template <const std::string & str_type>
static void BM_insertManyFromNullableSourceScalar(benchmark::State & state)
{
    BM_insertRepeatedNullableImpl<str_type, false, true>(state);
}

static const String type_int64 = "Int64";
static const String type_nullable_int64 = "Nullable(Int64)";
static const String type_string = "String";
static const String type_nullable_string = "Nullable(String)";
static const String type_decimal = "Decimal128(3)";
static const String type_nullable_decimal = "Nullable(Decimal128(3))";

static const String type_array_int64 = "Array(Int64)";
static const String type_array_nullable_int64 = "Array(Nullable(Int64))";
static const String type_array_string = "Array(String)";
static const String type_array_nullable_string = "Array(Nullable(String))";
static const String type_array_low_cardinality_string = "Array(LowCardinality(String))";

BENCHMARK_TEMPLATE(BM_insertManyFrom, type_int64);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_nullable_int64);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_string);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_nullable_string);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_decimal);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_nullable_decimal);

BENCHMARK_TEMPLATE(BM_insertManyFrom, type_array_int64);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_array_nullable_int64);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_array_string);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_array_nullable_string);

#define REGISTER_ARRAY_REPEATED_BENCHMARKS(type) \
    BENCHMARK_TEMPLATE(BM_insertFromRepeatedlyArray, type) \
        ->Args({0, 2})->Args({1, 2})->Args({2, 2})->Args({16, 2}) \
        ->Args({2, 4})->Args({2, 16})->Args({2, 256}) \
        ->Args({0, ROWS})->Args({1, ROWS})->Args({2, ROWS})->Args({16, ROWS}); \
    BENCHMARK_TEMPLATE(BM_insertManyFromRepeatedlyArray, type) \
        ->Args({0, 2})->Args({1, 2})->Args({2, 2})->Args({16, 2}) \
        ->Args({2, 4})->Args({2, 16})->Args({2, 256}) \
        ->Args({0, ROWS})->Args({1, ROWS})->Args({2, ROWS})->Args({16, ROWS})

REGISTER_ARRAY_REPEATED_BENCHMARKS(type_array_int64);
REGISTER_ARRAY_REPEATED_BENCHMARKS(type_array_string);
REGISTER_ARRAY_REPEATED_BENCHMARKS(type_array_low_cardinality_string);

#undef REGISTER_ARRAY_REPEATED_BENCHMARKS

BENCHMARK_TEMPLATE(BM_insertManyFromNotNullable, type_nullable_int64)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromNotNullable, type_nullable_string)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromNotNullable, type_nullable_decimal)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);

BENCHMARK_TEMPLATE(BM_insertManyFromNotNullableScalar, type_nullable_int64)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromNotNullableScalar, type_nullable_string)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromNotNullableScalar, type_nullable_decimal)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);

BENCHMARK_TEMPLATE(BM_insertManyFromNullableSource, type_nullable_int64)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromNullableSource, type_nullable_string)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromNullableSource, type_nullable_decimal)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);

BENCHMARK_TEMPLATE(BM_insertManyFromNullableSourceScalar, type_nullable_int64)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromNullableSourceScalar, type_nullable_string)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromNullableSourceScalar, type_nullable_decimal)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
