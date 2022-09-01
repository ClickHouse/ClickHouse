#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <DataTypes/IDataType.h>

#include <base/StringRef.h>
#include <base/defines.h>
#include <Common/TargetSpecific.h>
#include <Common/assert_cast.h>
#include <Common/config.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <vector>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
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
struct ComparatorMin
{
    using Type = T;
    static inline const String name{"min"};

    static ALWAYS_INLINE inline bool cmp(const T & a, const T & b) { return a < b; }
    static ALWAYS_INLINE inline const T & cmp_get(const T & a, const T & b) { return std::min(a, b); }
    static ALWAYS_INLINE inline bool column_compareAt(const IColumn & column, size_t n, size_t m)
    {
        return column.compareAt(n, m, column, /* nan/null direction hint = */ 1) < 0;
    }
#if USE_EMBEDDED_COMPILER
    static ALWAYS_INLINE inline llvm::Value * llvm_cmp(llvm::IRBuilder<> & b, llvm::Value * value_to_check, llvm::Value * value)
    {
        if constexpr (!canBeNativeType<T>())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported type");
        if constexpr (std::is_floating_point_v<Type>)
            return b.CreateFCmpOLT(value_to_check, value);
        if constexpr (std::numeric_limits<Type>::is_signed)
            return b.CreateICmpSLT(value_to_check, value);
        else
            return b.CreateICmpULT(value_to_check, value);
    }
#endif
};

template <typename T>
struct ComparatorMax
{
    using Type = T;
    static inline const String name{"max"};

    static ALWAYS_INLINE inline bool cmp(const T & a, const T & b) { return a > b; }
    static ALWAYS_INLINE inline const T & cmp_get(const T & a, const T & b) { return std::max(a, b); }
    static ALWAYS_INLINE inline bool column_compareAt(const IColumn & column, size_t n, size_t m)
    {
        return column.compareAt(n, m, column, /* nan/null direction hint = */ 1) > 0;
    }
#if USE_EMBEDDED_COMPILER
    static ALWAYS_INLINE inline llvm::Value * llvm_cmp(llvm::IRBuilder<> & b, llvm::Value * value_to_check, llvm::Value * value)
    {
        if constexpr (!canBeNativeType<T>())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported type");
        if constexpr (std::is_floating_point_v<Type>)
            return b.CreateFCmpOGT(value_to_check, value);
        if constexpr (std::numeric_limits<Type>::is_signed)
            return b.CreateICmpSGT(value_to_check, value);
        else
            return b.CreateICmpUGT(value_to_check, value);
    }
#endif
};

template <typename Comparator>
struct AggregateFunctionsMinMaxDataNumeric
{
    using ComparatorType = Comparator;
    using T = typename Comparator::Type;
    using ColVecType = ColumnVectorOrDecimal<T>;
    static constexpr bool supported_multitarget_type = std::is_integral_v<T> || std::is_floating_point_v<T>;

    bool has_value = false;
    T value{};

    void ALWAYS_INLINE inline add(T new_value)
    {
        if (!has_value)
            value = new_value;
        else
            value = Comparator::cmp_get(value, new_value);
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
            assert_cast<ColVecType &>(to).getData().push_back(value);
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
                      value = *ptr;
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
                              value = ptr[i];
                              break;
                          }
                      }
                      if (i == count)
                          return;
                  }
              }

              T aux{value}; /// Need an auxiliary variable for "compiler reasons", otherwise it won't use SIMD
              if constexpr (std::is_floating_point_v<T>)
              {
                  constexpr size_t unroll_block = 512 / sizeof(T);
                  size_t unrolled_end = i + (((count - i) / unroll_block) * unroll_block);

                  if (i < unrolled_end)
                  {
                      T partial_min[unroll_block];
                      for (size_t unroll_it = 0; unroll_it < unroll_block; unroll_it++)
                          partial_min[unroll_it] = aux;

                      while (i < unrolled_end)
                      {
                          for (size_t unroll_it = 0; unroll_it < unroll_block; unroll_it++)
                          {
                              if constexpr (add_all_elements)
                              {
                                  partial_min[unroll_it] = Comparator::cmp_get(partial_min[unroll_it], ptr[i + unroll_it]);
                              }
                              else
                              {
                                  if (!condition_map[i + unroll_it] == add_if_cond_zero)
                                      partial_min[unroll_it] = Comparator::cmp_get(partial_min[unroll_it], ptr[i + unroll_it]);
                              }
                          }
                          i += unroll_block;
                      }
                      for (size_t unroll_it = 0; unroll_it < unroll_block; unroll_it++)
                          aux = Comparator::cmp_get(aux, partial_min[unroll_it]);
                  }
              }

              for (; i < count; i++)
              {
                  if constexpr (add_all_elements)
                  {
                      aux = Comparator::cmp_get(aux, ptr[i]);
                  }
                  else
                  {
                      if (!condition_map[i] == add_if_cond_zero)
                          aux = Comparator::cmp_get(aux, ptr[i]);
                  }
              }

              value = aux;
              has_value = true;
        })
    )

    /// Vectorized version
    void addMany(const IColumn & column, size_t start, size_t end, Arena *)
    {
        const auto & col = assert_cast<const ColVecType &>(column);
        auto * ptr = col.getData().data();
#if USE_MULTITARGET_CODE
        if constexpr (supported_multitarget_type)
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
        if constexpr (supported_multitarget_type)
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
        if constexpr (supported_multitarget_type)
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

    void merge(const AggregateFunctionsMinMaxDataNumeric & rhs, Arena *)
    {
        if (rhs.has_value)
            add(rhs.value);
    }

    void write(WriteBuffer & buf, const ISerialization &) const
    {
        writeBinary(has_value, buf);
        if (has_value)
            writeBinary(value, buf);
    }

    void read(ReadBuffer & buf, const ISerialization &, Arena *)
    {
        readBinary(has_value, buf);
        if (has_value)
            readBinary(value, buf);
    }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = canBeNativeType<T>();

    static llvm::Value * getValuePtrFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        static constexpr size_t value_offset_from_structure = offsetof(AggregateFunctionsMinMaxDataNumeric<Comparator>, value);

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

        llvm::Value * should_change_after_comparison = Comparator::llvm_cmp(b, value_to_check, value);
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

        llvm::Value * should_change_after_comparison = Comparator::llvm_cmp(b, value_src, value_dst);
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

template <typename Comparator>
struct AggregateFunctionsMinMaxDataString
{
    using ComparatorType = Comparator;
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
        setImpl(assert_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num), arena);
    }

    void ALWAYS_INLINE inline add(const StringRef & s, Arena * arena)
    {
        if (!has() || Comparator::cmp(s, getStringRef()))
            setImpl(s, arena);
    }

    void ALWAYS_INLINE inline add(const IColumn & column, size_t row_num, Arena * arena)
    {
        StringRef s = assert_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num);
        add(s, arena);
    }

    void ALWAYS_INLINE inline addManyDefaults(const IColumn & column, size_t /*length*/, Arena * arena) { add(column, 0, arena); }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            assert_cast<ColumnString &>(to).insertDataWithTerminatingZero(getData(), size);
        else
            assert_cast<ColumnString &>(to).insertDefault();
    }

    template <bool add_all_elements, bool add_if_cond_zero>
    void NO_INLINE
    addManyImpl(const IColumn & column, const UInt8 * __restrict condition_map [[maybe_unused]], size_t start, size_t end, Arena * arena)
    {
        const auto & col = assert_cast<const ColumnString &>(column);
        if constexpr (!add_all_elements)
        {
            while (!condition_map[start] != add_if_cond_zero && start < end)
                start++;
            if (start >= end)
                return;
        }

        StringRef idx = col.getDataAtWithTerminatingZero(start);
        for (size_t i = start + 1; i < end; i++)
        {
            if (!add_all_elements && !condition_map[i] != add_if_cond_zero)
                continue;
            StringRef iref = col.getDataAtWithTerminatingZero(i);
            if (Comparator::cmp(iref, idx))
                idx = iref;
        }

        add(idx, arena);
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

    void merge(const AggregateFunctionsMinMaxDataString & rhs, Arena * arena)
    {
        if (rhs.has())
            add(rhs.getStringRef(), arena);
    }

    void write(WriteBuffer & buf, const ISerialization &) const
    {
        writeBinary(size, buf);
        if (has())
            buf.write(getData(), size);
    }

    void read(ReadBuffer & buf, const ISerialization &, Arena * arena)
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
    sizeof(AggregateFunctionsMinMaxDataString<ComparatorMin<StringRef>>)
        == AggregateFunctionsMinMaxDataString<ComparatorMin<StringRef>>::AUTOMATIC_STORAGE_SIZE,
    "Incorrect size of SingleValueDataString struct");


template <typename Comparator>
struct AggregateFunctionsMinMaxDataGeneric
{
    using ComparatorType = Comparator;

private:
    Field value;

    bool has() const { return !value.isNull(); }

    void ALWAYS_INLINE inline add(const Field & new_value)
    {
        if (!has() || Comparator::cmp(new_value, value))
            value = new_value;
    }

public:
    void ALWAYS_INLINE inline add(const IColumn & column, size_t row_num, Arena *)
    {
        if (!has())
            column.get(row_num, value);
        else
        {
            Field new_value;
            column.get(row_num, new_value);
            add(new_value);
        }
    }

    void ALWAYS_INLINE inline addManyDefaults(const IColumn & column, size_t /*length*/, Arena * arena) { add(column, 0, arena); }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            to.insert(value);
        else
            to.insertDefault();
    }

    template <bool add_all_elements, bool add_if_cond_zero>
    void NO_INLINE
    addManyImpl(const IColumn & column, const UInt8 * __restrict condition_map [[maybe_unused]], size_t start, size_t end, Arena * arena)
    {
        if constexpr (!add_all_elements)
        {
            while (!condition_map[start] != add_if_cond_zero && start < end)
                start++;
            if (start >= end)
                return;
        }

        size_t idx = start;
        for (size_t i = start + 1; i < end; i++)
        {
            if (!add_all_elements && !condition_map[i] != add_if_cond_zero)
                continue;
            if (Comparator::column_compareAt(column, i, idx))
                idx = i;
        }

        add(column, idx, arena);
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

    void merge(const AggregateFunctionsMinMaxDataGeneric & rhs, Arena *)
    {
        if (rhs.has())
            add(rhs.value);
    }

    void write(WriteBuffer & buf, const ISerialization & serialization) const
    {
        if (has())
        {
            writeBinary(true, buf);
            serialization.serializeBinary(value, buf);
        }
        else
            writeBinary(false, buf);
    }

    void read(ReadBuffer & buf, const ISerialization & serialization, Arena *)
    {
        bool is_not_null;
        readBinary(is_not_null, buf);

        if (is_not_null)
            serialization.deserializeBinary(value, buf);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool is_compilable = false;
#endif
};

template <typename Data>
class AggregateFunctionsMinMax final : public IAggregateFunctionDataHelper<Data, AggregateFunctionsMinMax<Data>>
{
    SerializationPtr serialization;

public:
    explicit AggregateFunctionsMinMax(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionsMinMax<Data>>({type}, {}), serialization(type->getDefaultSerialization())
    {
        if (!type->isComparable())
            throw Exception(
                "Illegal type " + type->getName() + " of argument of aggregate function " + getName()
                    + " because the values of that data type are not comparable",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override { return Data::ComparatorType::name; }

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
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, arena);
    }

    bool allocatesMemoryInArena() const override { return false; }

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
