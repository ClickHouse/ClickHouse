#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

#include <base/StringRef.h>
#include <base/defines.h>
#include <Common/TargetSpecific.h>
#include <Common/assert_cast.h>
#include <Common/config.h>

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
}

template <typename T>
struct AggregateFunctionStandaloneMinData
{
    /// We use the max value as the initial value allows us to avoid checking if any value has been set before (has_value).
    /// This is safe with integers, but not with floats (NaN is a problem) or Decimal (not a straightforward max), etc.
    static constexpr bool can_use_numeric_max = is_integer<T> && !is_big_int_v<T>;

    bool has_value = false;

    T min{ std::numeric_limits<T>::max() };

    void ALWAYS_INLINE inline add(T value)
    {
        if constexpr (!can_use_numeric_max)
        {
            if (!has_value)
            {
                has_value = true;
                min = value;
                return;
            }
        }
        has_value = true;
        min = std::min(min, value);
    }

    void ALWAYS_INLINE inline addManyDefaults(T value, size_t /*length*/) { add(value); }

    MULTITARGET_FUNCTION_AVX2_SSE42(
        MULTITARGET_FUNCTION_HEADER(
        template <bool add_all_elements, bool add_if_cond_zero>
        void NO_INLINE
        ), addManyImpl, MULTITARGET_FUNCTION_BODY(
        (const T * __restrict ptr, const UInt8 * __restrict condition_map [[maybe_unused]], size_t row_begin, size_t row_end)
        {
              size_t count = row_end - row_begin;
              ptr += row_begin;
              if constexpr (!add_all_elements)
                  condition_map += row_begin;

              size_t i = 0;
              if constexpr (!can_use_numeric_max)
              {
                  /// If we are not using integers we need to ensure min and has_value are initialized to any valid value from the input
                  if constexpr (add_all_elements)
                  {
                      chassert(row_end - row_begin);
                      if (!has_value)
                      {
                          has_value = true;
                          min = *ptr;
                      }
                  }
                  else
                  {
                      for (; i < count; i++)
                      {
                          if (!condition_map[i] == add_if_cond_zero)
                          {
                              has_value = true;
                              min = ptr[i];
                              break;
                          }
                      }
                      if (i == count)
                          return;
                  }
              }

              T aux{min};  /// Need an auxiliary variable for "compiler reasons", otherwise it won't use SIMD
              if constexpr (std::is_floating_point<T>::value)
              {
                  constexpr size_t unroll_block = 256 / sizeof(T);
                  size_t unrolled_end = i + (((count - i) / unroll_block) * unroll_block);

                  if (i < unrolled_end)
                  {
                      std::vector<T> partial_min(unroll_block, aux);

                      while (i < unrolled_end)
                      {
                          for (size_t j = 0; i < unroll_block; i++)
                          {
                              if constexpr (add_all_elements)
                              {
                                  partial_min[j] = std::min(partial_min[j], ptr[i+j]);
                              }
                              else
                              {
                                  if (!condition_map[i+j] == add_if_cond_zero)
                                      partial_min[j] = std::min(partial_min[j], ptr[i+j]);
                              }
                          }
                          i += unroll_block;
                      }
                      for (size_t j = 0; i < unroll_block; i++)
                          aux = std::min(aux, partial_min[j]);
                  }
              }

              for (; i < count; i++)
              {
                  if constexpr (add_all_elements)
                  {
                      aux = std::min(aux, ptr[i]);
                  }
                  else
                  {
                      if (!condition_map[i] == add_if_cond_zero)
                          aux = std::min(aux, ptr[i]);
                  }
              }

              min = aux;
              has_value = true;
        })
    )

    /// Vectorized version
    template <typename Value>
    void addMany(const Value * __restrict ptr, size_t start, size_t end)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX2))
        {
            addManyImplAVX2<true, false>(ptr, nullptr, start, end);
            return;
        }
        else if (isArchSupported(TargetArch::SSE42))
        {
            addManyImplSSE42<true, false>(ptr, nullptr, start, end);
            return;
        }
#endif
        addManyImpl<true, false>(ptr, nullptr, start, end);
    }

    template <typename Value>
    void addManyNotNull(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t start, size_t end)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX2))
        {
            addManyImplAVX2<false, true>(ptr, null_map, start, end);
            return;
        }
        else if (isArchSupported(TargetArch::SSE42))
        {
            addManyImplSSE42<false, true>(ptr, null_map, start, end);
            return;
        }
#endif
        addManyImpl<false, true>(ptr, null_map, start, end);
    }

    template <typename Value>
    void addManyConditional(const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX2))
        {
            addManyImplAVX2<false, false>(ptr, condition_map, start, end);
            return;
        }
        else if (isArchSupported(TargetArch::SSE42))
        {
            addManyImplSSE42<false, false>(ptr, condition_map, start, end);
            return;
        }
#endif
        addManyImpl<false, false>(ptr, condition_map, start, end);
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

    DataTypePtr getReturnType() const override { return this->argument_types.at(0); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        this->data(place).add(assert_cast<const T &>(column.getData()[row_num]));
    }

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
            this->data(place).addManyConditional(column.getData().data(), flags.data(), row_begin, row_end);
        }
        else
        {
            this->data(place).addMany(column.getData().data(), row_begin, row_end);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos = -1) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);

        if (if_argument_pos >= 0)
        {
            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            auto final_null_flags = std::make_unique<UInt8[]>(row_end);
            for (size_t i = row_begin; i < row_end; ++i)
                final_null_flags[i] = null_map[i] & !if_flags[i];

            this->data(place).addManyNotNull(column.getData().data(), final_null_flags.get(), row_begin, row_end);
        }
        else
        {
            this->data(place).addManyNotNull(column.getData().data(), null_map, row_begin, row_end);
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        this->data(place).addManyDefaults(assert_cast<const T &>(column.getData()[0]), length);
    }

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
