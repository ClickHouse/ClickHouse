#include <cstddef>
#include <Columns/ColumnLowCardinality.h>
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
        for (size_t i = 0; i < data_col->size(); ++i)
            offsets[i] = offsets[i - 1] + (rand() % 10);
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


static NO_INLINE void insertManyFrom(IColumn & dst, const IColumn & src)
{
    size_t size = src.size();
    dst.insertManyFrom(src, size / 2, size);
}

static ColumnPtr mockLowCardinalityColumn(const DataTypePtr & type, size_t rows)
{
    auto column = type->createColumn();
    const auto nested_type = removeLowCardinality(type);

    if (isString(nested_type))
    {
        const String value = "helloworld123456";
        for (size_t i = 0; i < rows; ++i)
            column->insert(value);
    }
    else
    {
        for (size_t i = 0; i < rows; ++i)
            column->insert(static_cast<UInt64>(i % 256 + 1));
    }

    return std::move(column);
}

template <const std::string & str_type>
static void BM_insertManyFromLowCardinality(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    auto src = mockLowCardinalityColumn(type, ROWS);
    const size_t position = src->size() / 2;
    const size_t length = state.range(0);

    for (auto _ : state)
    {
        state.PauseTiming();
        auto dst = type->createColumn();
        dst->reserve(length);
        state.ResumeTiming();

        dst->insertManyFrom(*src, position, length);
        benchmark::DoNotOptimize(dst);
    }
}

template <const std::string & str_type>
static void BM_insertManyFromLowCardinalityShared(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    auto src = mockLowCardinalityColumn(type, ROWS);
    const auto & src_low_cardinality = assert_cast<const ColumnLowCardinality &>(*src);
    const size_t position = src->size() / 2;
    const size_t length = state.range(0);

    for (auto _ : state)
    {
        state.PauseTiming();
        auto dst = type->createColumn();
        auto & dst_low_cardinality = assert_cast<ColumnLowCardinality &>(*dst);
        dst_low_cardinality.setSharedDictionary(src_low_cardinality.getDictionaryPtr());
        dst->reserve(length);
        state.ResumeTiming();

        dst->insertManyFrom(*src, position, length);
        benchmark::DoNotOptimize(dst);
    }
}


template <const std::string & str_type>
static void BM_insertManyFrom(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    auto src = mockColumn(type, ROWS);

    for (auto _ : state)
    {
        state.PauseTiming();
        auto dst = type->createColumn();
        dst->reserve(ROWS);
        state.ResumeTiming();

        insertManyFrom(*dst, *src);
        benchmark::DoNotOptimize(dst);
    }
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
static const String type_low_cardinality_string = "LowCardinality(String)";
static const String type_low_cardinality_uint64 = "LowCardinality(UInt64)";

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

BENCHMARK_TEMPLATE(BM_insertManyFromLowCardinality, type_low_cardinality_string)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromLowCardinality, type_low_cardinality_uint64)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromLowCardinalityShared, type_low_cardinality_string)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
BENCHMARK_TEMPLATE(BM_insertManyFromLowCardinalityShared, type_low_cardinality_uint64)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(ROWS);
