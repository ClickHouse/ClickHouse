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
    extern const int NOT_IMPLEMENTED;
}

template <typename T>
struct AggregateFunctionMinDataNumeric
{
    using ColVecType = ColumnVectorOrDecimal<T>;
    static constexpr bool is_native_integer = std::is_integral_v<T>;
    bool has_value = false;
    T min{};

    void ALWAYS_INLINE inline add(T value)
    {
        if (!has_value)
            min = value;
        else
            min = std::min(min, value);
        has_value = true;
    }

    void ALWAYS_INLINE inline add(const IColumn & column, size_t row_num, Arena *)
    {
        const auto & col = assert_cast<const ColVecType &>(column);
        add(col.getData()[row_num]);
    }

    void ALWAYS_INLINE inline addManyDefaults(const IColumn & column, size_t /*length*/, Arena * arena) { add(column, 0, arena); }

    void insertResultInto(IColumn & to) const
    {
        if (has_value)
            assert_cast<ColVecType &>(to).getData().push_back(min);
        else
            assert_cast<ColVecType &>(to).insertDefault();
    }

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
                  if (!has_value)
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
              if constexpr (std::is_floating_point_v<T>)
              {
                  constexpr size_t unroll_block = 256 / sizeof(T);
                  size_t unrolled_end = i + (((count - i) / unroll_block) * unroll_block);

                  if (i < unrolled_end)
                  {
                      T partial_min[unroll_block];
                      for (size_t j = 0; i < unroll_block; i++)
                          partial_min[j] = aux;

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
    void addMany(const IColumn & column, size_t start, size_t end, Arena *)
    {
        const auto & col = assert_cast<const ColVecType &>(column);
        auto * ptr = col.getData().data();
#if USE_MULTITARGET_CODE
        if constexpr (is_native_integer)
        {
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
        }
#endif
        addManyImpl<true, false>(ptr, nullptr, start, end);
    }

    void addManyNotNull(const IColumn & column, const UInt8 * __restrict null_map, size_t start, size_t end, Arena *)
    {
        const auto & col = assert_cast<const ColVecType &>(column);
        auto * ptr = col.getData().data();
#if USE_MULTITARGET_CODE
        if constexpr (is_native_integer)
        {
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
        }
#endif
        addManyImpl<false, true>(ptr, null_map, start, end);
    }

    void addManyConditional(const IColumn & column, const UInt8 * __restrict condition_map, size_t start, size_t end, Arena *)
    {
        const auto & col = assert_cast<const ColVecType &>(column);
        auto * ptr = col.getData().data();
#if USE_MULTITARGET_CODE
        if constexpr (is_native_integer)
        {
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
        }
#endif
        addManyImpl<false, false>(ptr, condition_map, start, end);
    }

    void merge(const AggregateFunctionMinDataNumeric & rhs, Arena *)
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

    void read(ReadBuffer & buf, Arena *)
    {
        readBinary(has_value, buf);
        if (has_value)
            readBinary(min, buf);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool is_compilable = canBeNativeType<T>();

    static llvm::Value * getValuePtrFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        static constexpr size_t value_offset_from_structure = offsetof(AggregateFunctionMinDataNumeric<T>, min);

        auto * type = toNativeType<T>(builder);
        auto * value_ptr_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_ptr, value_offset_from_structure);
        auto * value_ptr = b.CreatePointerCast(value_ptr_with_offset, type->getPointerTo());

        return value_ptr;
    }

    static llvm::Value * getValueFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * type = toNativeType<T>(builder);
        auto * value_ptr = getValuePtrFromAggregateDataPtr(builder, aggregate_data_ptr);

        return b.CreateLoad(type, value_ptr);
    }

    static void compileChange(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_ptr = b.CreatePointerCast(aggregate_data_ptr, b.getInt1Ty()->getPointerTo());
        b.CreateStore(b.getInt1(true), has_value_ptr);

        auto * value_ptr = getValuePtrFromAggregateDataPtr(b, aggregate_data_ptr);
        b.CreateStore(value_to_check, value_ptr);
    }

    static void compileAdd(
        llvm::IRBuilderBase & builder,
        llvm::Value * aggregate_data_ptr,
        const DataTypes &,
        const std::vector<llvm::Value *> & argument_values)
    {
        if constexpr (!is_compilable)
            return;

        llvm::Value * value_to_check = argument_values[0];

        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_ptr = b.CreatePointerCast(aggregate_data_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_value = b.CreateLoad(b.getInt1Ty(), has_value_ptr);

        auto * value = getValueFromAggregateDataPtr(b, aggregate_data_ptr);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());
        auto * if_should_not_change = llvm::BasicBlock::Create(head->getContext(), "if_should_not_change", head->getParent());

        llvm::Value * should_change_after_comparison = nullptr;
        if constexpr (std::is_floating_point_v<T>)
            should_change_after_comparison = b.CreateFCmpOLT(value_to_check, value);
        else if constexpr (std::numeric_limits<T>::is_signed)
            should_change_after_comparison = b.CreateICmpSLT(value_to_check, value);
        else
            should_change_after_comparison = b.CreateICmpULT(value_to_check, value);

        b.CreateCondBr(b.CreateOr(b.CreateNot(has_value_value), should_change_after_comparison), if_should_change, if_should_not_change);

        b.SetInsertPoint(if_should_change);
        compileChange(builder, aggregate_data_ptr, value_to_check);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_should_not_change);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    static void
    compileChangeMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        auto * value_src = getValueFromAggregateDataPtr(builder, aggregate_data_src_ptr);
        compileChange(builder, aggregate_data_dst_ptr, value_src);
    }

    static void compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        if constexpr (!is_compilable)
            return;

        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_dst = b.CreateLoad(b.getInt1Ty(), has_value_dst_ptr);

        auto * value_dst = getValueFromAggregateDataPtr(b, aggregate_data_dst_ptr);

        auto * has_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_src = b.CreateLoad(b.getInt1Ty(), has_value_src_ptr);

        auto * value_src = getValueFromAggregateDataPtr(b, aggregate_data_src_ptr);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());
        auto * if_should_not_change = llvm::BasicBlock::Create(head->getContext(), "if_should_not_change", head->getParent());


        llvm::Value * should_change_after_comparison = nullptr;
        if constexpr (std::is_floating_point_v<T>)
            should_change_after_comparison = b.CreateFCmpOLT(value_src, value_dst);
        else if constexpr (std::numeric_limits<T>::is_signed)
            should_change_after_comparison = b.CreateICmpSLT(value_src, value_dst);
        else
            should_change_after_comparison = b.CreateICmpULT(value_src, value_dst);

        b.CreateCondBr(
            b.CreateAnd(has_value_src, b.CreateOr(b.CreateNot(has_value_dst), should_change_after_comparison)),
            if_should_change,
            if_should_not_change);

        b.SetInsertPoint(if_should_change);
        compileChangeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_should_not_change);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    static llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
    {
        if constexpr (!is_compilable)
            return nullptr;
        return getValueFromAggregateDataPtr(builder, aggregate_data_ptr);
    }
#endif
};

template <typename ColumnType>
struct AggregateFunctionMinDataString
{
    Int32 size = -1; /// -1 indicates that there is no value.
    Int32 capacity = 0; /// power of two or zero
    char * large_data;

    static constexpr Int32 AUTOMATIC_STORAGE_SIZE = 64;
    static constexpr Int32 MAX_SMALL_STRING_SIZE = AUTOMATIC_STORAGE_SIZE - sizeof(size) - sizeof(capacity) - sizeof(large_data);
    char small_data[MAX_SMALL_STRING_SIZE]; /// Including the terminating zero.

    bool ALWAYS_INLINE has() const { return size >= 0; }

    const char * ALWAYS_INLINE getData() const { return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data; }

    StringRef ALWAYS_INLINE getStringRef() const { return StringRef(getData(), size); }

    void setImpl(StringRef value, Arena * arena)
    {
        Int32 value_size = value.size;
        if (value_size <= MAX_SMALL_STRING_SIZE)
        {
            /// Don't free large_data here.
            size = value_size;

            if (size > 0)
                memcpy(small_data, value.data, size);
        }
        else
        {
            if (capacity < value_size)
            {
                /// Don't free large_data here.
                capacity = roundUpToPowerOfTwoOrZero(value_size);
                large_data = arena->alloc(capacity);
            }

            size = value_size;
            memcpy(large_data, value.data, size);
        }
    }

    void set(const IColumn & column, size_t row_num, Arena * arena)
    {
        setImpl(assert_cast<const ColumnType &>(column).getDataAtWithTerminatingZero(row_num), arena);
    }

    void ALWAYS_INLINE inline add(const StringRef & s, Arena * arena)
    {
        if (!has() || s < getStringRef())
            setImpl(s, arena);
    }

    void ALWAYS_INLINE inline add(const IColumn & column, size_t row_num, Arena * arena)
    {
        StringRef s = assert_cast<const ColumnType &>(column).getDataAtWithTerminatingZero(row_num);
        add(s, arena);
    }

    void ALWAYS_INLINE inline addManyDefaults(const IColumn & column, size_t /*length*/, Arena * arena) { add(column, 0, arena); }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            assert_cast<ColumnType &>(to).insertDataWithTerminatingZero(getData(), size);
        else
            assert_cast<ColumnType &>(to).insertDefault();
    }

    template <bool add_all_elements, bool add_if_cond_zero>
    void NO_INLINE
    addManyImpl(const IColumn & column, const UInt8 * __restrict condition_map [[maybe_unused]], size_t start, size_t end, Arena * arena)
    {
        const auto & col = assert_cast<const ColumnType &>(column);
        if constexpr (!add_all_elements)
        {
            while (!condition_map[start] != add_if_cond_zero && start < end)
                start++;
            if (start >= end)
                return;
        }

        StringRef lowest = col.getDataAtWithTerminatingZero(start);
        for (size_t i = start + 1; i < end; i++)
        {
            if (!add_all_elements && !condition_map[i] != add_if_cond_zero)
                continue;
            StringRef idx_ref = col.getDataAtWithTerminatingZero(i);
            if (idx_ref < lowest)
                lowest = idx_ref;
        }

        add(lowest, arena);
    }

    void addMany(const IColumn & column, size_t start, size_t end, Arena * arena)
    {
        addManyImpl<true, false>(column, nullptr, start, end, arena);
    }

    void addManyNotNull(const IColumn & column, const UInt8 * __restrict null_map, size_t start, size_t end, Arena * arena)
    {
        addManyImpl<false, true>(column, null_map, start, end, arena);
    }

    void addManyConditional(const IColumn & column, const UInt8 * __restrict condition_map, size_t start, size_t end, Arena * arena)
    {
        addManyImpl<false, false>(column, condition_map, start, end, arena);
    }

    void merge(const AggregateFunctionMinDataString & rhs, Arena * arena)
    {
        if (rhs.has())
            add(rhs.getStringRef(), arena);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(size, buf);
        if (has())
            buf.write(getData(), size);
    }

    void read(ReadBuffer & buf, Arena * arena)
    {
        Int32 rhs_size;
        readBinary(rhs_size, buf);

        if (rhs_size >= 0)
        {
            if (rhs_size <= MAX_SMALL_STRING_SIZE)
            {
                /// Don't free large_data here.

                size = rhs_size;

                if (size > 0)
                    buf.read(small_data, size);
            }
            else
            {
                if (capacity < rhs_size)
                {
                    capacity = static_cast<UInt32>(roundUpToPowerOfTwoOrZero(rhs_size));
                    /// Don't free large_data here.
                    large_data = arena->alloc(capacity);
                }

                size = rhs_size;
                buf.read(large_data, size);
            }
        }
        else
        {
            /// Don't free large_data here.
            size = rhs_size;
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool is_compilable = false;
#endif
};

static_assert(
    sizeof(AggregateFunctionMinDataString<ColumnString>) == AggregateFunctionMinDataString<ColumnString>::AUTOMATIC_STORAGE_SIZE,
    "Incorrect size of SingleValueDataString struct");

template <typename T, typename Data>
class AggregateFunctionMin final : public IAggregateFunctionDataHelper<Data, AggregateFunctionMin<T, Data>>
{
public:
    explicit AggregateFunctionMin(const DataTypePtr & type) : IAggregateFunctionDataHelper<Data, AggregateFunctionMin<T, Data>>({type}, {})
    {
        if (!type->isComparable())
            throw Exception("Illegal type " + type->getName() + " of argument of aggregate function " + getName()
                + " because the values of that data type are not comparable", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override { return "min"; }

    DataTypePtr getReturnType() const override { return this->argument_types.at(0); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).add(*columns[0], row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos)
        const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            this->data(place).addManyConditional(*columns[0], flags.data(), row_begin, row_end, arena);
        }
        else
        {
            this->data(place).addMany(*columns[0], row_begin, row_end, arena);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            auto final_null_flags = std::make_unique<UInt8[]>(row_end);
            for (size_t i = row_begin; i < row_end; ++i)
                final_null_flags[i] = null_map[i] & !if_flags[i];

            this->data(place).addManyNotNull(*columns[0], final_null_flags.get(), row_begin, row_end, arena);
        }
        else
        {
            this->data(place).addManyNotNull(*columns[0], null_map, row_begin, row_end, arena);
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena * arena) const override
    {
        this->data(place).addManyDefaults(*columns[0], length, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return false;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        if constexpr (!Data::is_compilable)
            return false;

        return canBeNativeType(*this->argument_types[0]);
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (!Data::is_compilable)
            throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
        else
        {
            llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
            b.CreateMemSet(
                aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), this->sizeOfData(), llvm::assumeAligned(this->alignOfData()));
        }
    }

    void compileAdd(
        llvm::IRBuilderBase & builder,
        llvm::Value * aggregate_data_ptr,
        const DataTypes & datatypes,
        const std::vector<llvm::Value *> & argument_values) const override
    {
        if constexpr (!Data::is_compilable)
            throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
        else
            Data::compileAdd(builder, aggregate_data_ptr, datatypes, argument_values);
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        if constexpr (!Data::is_compilable)
            throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
        else
            Data::compileMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (!Data::is_compilable)
            throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
        else
            return Data::compileGetResult(builder, aggregate_data_ptr);
    }
#endif
};
}
