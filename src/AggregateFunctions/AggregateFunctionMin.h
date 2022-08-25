#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <base/StringRef.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Common/config.h>
#include <Common/TargetSpecific.h>

#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <limits>
#include <type_traits>

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

template <typename T>
struct AggregateFunctionStandaloneMinData
{
    bool has_value = false;
    T min{ std::numeric_limits<T>::max() };


    void ALWAYS_INLINE inline add(T value)
    {
        has_value = true;
        min = std::min(min, value);
    }

    MULTITARGET_FUNCTION_AVX2_SSE42(
        MULTITARGET_FUNCTION_HEADER(void),
        addManyImpl,
        MULTITARGET_FUNCTION_BODY((const T * __restrict ptr, size_t row_begin, size_t row_end)
        {
          const auto * last_element = ptr + row_end;
          ptr += row_begin;

          if constexpr (std::is_floating_point_v<T>)
          {
              constexpr size_t unroll_block = 64 / sizeof(T);
              std::vector<T> partial_min(unroll_block, std::numeric_limits<T>::max());

              const auto * unrolled_end = ptr + ((row_end - row_begin) / unroll_block * unroll_block);
              while (ptr < unrolled_end)
              {
                  for (size_t i = 0; i < unroll_block; i++)
                  {
                      partial_min[i] = std::min(partial_min[i], ptr[i]);
                  }
                  ptr += unroll_block;
              }

              min = std::min(min, *std::min_element(partial_min.begin(), partial_min.end()));
          }
          while (ptr < last_element)
          {
              min = std::min(min, *ptr);
              ptr++;
          }
        })
    )

    /// Vectorized version
    template <typename Value>
    void NO_INLINE addMany(const Value * __restrict ptr, size_t start, size_t end)
    {
        has_value = true;
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX2))
        {
            addManyImplAVX2(ptr, start, end);
            return;
        }
        else if (isArchSupported(TargetArch::SSE42))
        {
            addManyImplSSE42(ptr, start, end);
            return;
        }
#endif
        addManyImpl(ptr, start, end);
    }

    void merge(const AggregateFunctionStandaloneMinData & rhs)
    {
        if (rhs.has_value)
            add(rhs.min);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(has_value, buf);
        if (has_value)
            writeBinary(min, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(has_value, buf);
        if (has_value)
            readBinary(min, buf);
    }

    T get() const
    {
        return has_value ? min : T{};
    }
};



template <typename T>
class AggregateFunctionMin final : public IAggregateFunctionDataHelper<AggregateFunctionStandaloneMinData<T>, AggregateFunctionMin<T>>
{
    using ColVecType = ColumnVectorOrDecimal<T>;
public:
    explicit AggregateFunctionMin(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<AggregateFunctionStandaloneMinData<T>, AggregateFunctionMin<T>>({type}, {})
    {
        if (!type->isComparable())
            throw Exception("Illegal type " + type->getName() + " of argument of aggregate function " + getName()
                + " because the values of that data type are not comparable", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override { return "min"; }

    DataTypePtr getReturnType() const override
    {
        auto result_type = this->argument_types.at(0);
//        if constexpr (T::is_nullable)
//            return makeNullable(result_type);
        return result_type;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        this->data(place).add(assert_cast<const T &>(column.getData()[row_num]));
    }

//    void addManyDefaults(
//        AggregateDataPtr __restrict place,
//        const IColumn ** columns,
//        size_t length,
//        Arena * arena) const override
//    {
//        this->data(place).addManyDefaults(*columns[0], length, arena);
//    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i])
                {
                    this->data(place).add(assert_cast<const T &>(column.getData()[i]));
                }
            }
        }
        else
        {
            this->data(place).addMany(column.getData().data(), row_begin, row_end);
        }
    }

//    void addBatchSinglePlaceNotNull( /// NOLINT
//        size_t row_begin,
//        size_t row_end,
//        AggregateDataPtr place,
//        const IColumn ** columns,
//        const UInt8 * null_map,
//        Arena * arena,
//        ssize_t if_argument_pos = -1) const override
//    {
//        if constexpr (is_any)
//            if (this->data(place).has())
//                return;
//
//        if (if_argument_pos >= 0)
//        {
//            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
//            for (size_t i = row_begin; i < row_end; ++i)
//            {
//                if (!null_map[i] && flags[i])
//                {
//                    this->data(place).changeIfBetter(*columns[0], i, arena);
//                    if constexpr (is_any)
//                        break;
//                }
//            }
//        }
//        else
//        {
//            for (size_t i = row_begin; i < row_end; ++i)
//            {
//                if (!null_map[i])
//                {
//                    this->data(place).changeIfBetter(*columns[0], i, arena);
//                    if constexpr (is_any)
//                        break;
//                }
//            }
//        }
//    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    bool allocatesMemoryInArena() const override
    {
        return false;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColVecType &>(to).getData().push_back(this->data(place).get());
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        return false;
    }
#endif
};

}
