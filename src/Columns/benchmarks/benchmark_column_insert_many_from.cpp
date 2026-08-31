#include <cstddef>
#include <random>
#include <Columns/IColumn.h>
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
    auto src = mockRepeatedArrayColumn(type, static_cast<size_t>(state.range(0)));

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto dst = type->createColumn();
        dst->reserve(ROWS);
        state.ResumeTiming();

        insertFromRepeatedly(*dst, *src, ROWS);
        benchmark::DoNotOptimize(dst);
    }
}

template <const std::string & str_type>
static void BM_insertManyFromRepeatedlyArray(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    auto src = mockRepeatedArrayColumn(type, static_cast<size_t>(state.range(0)));

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto dst = type->createColumn();
        dst->reserve(ROWS);
        state.ResumeTiming();

        insertManyFromRepeatedly(*dst, *src, ROWS);
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

BENCHMARK_TEMPLATE(BM_insertFromRepeatedlyArray, type_array_int64)->Arg(0)->Arg(1)->Arg(2)->Arg(16);
BENCHMARK_TEMPLATE(BM_insertFromRepeatedlyArray, type_array_string)->Arg(0)->Arg(1)->Arg(2)->Arg(16);
BENCHMARK_TEMPLATE(BM_insertFromRepeatedlyArray, type_array_low_cardinality_string)->Arg(0)->Arg(1)->Arg(2)->Arg(16);

BENCHMARK_TEMPLATE(BM_insertManyFromRepeatedlyArray, type_array_int64)->Arg(0)->Arg(1)->Arg(2)->Arg(16);
BENCHMARK_TEMPLATE(BM_insertManyFromRepeatedlyArray, type_array_string)->Arg(0)->Arg(1)->Arg(2)->Arg(16);
BENCHMARK_TEMPLATE(BM_insertManyFromRepeatedlyArray, type_array_low_cardinality_string)->Arg(0)->Arg(1)->Arg(2)->Arg(16);
