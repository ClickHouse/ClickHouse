#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <base/StringRef.h>
#include <Common/assert_cast.h>


#include <Common/config.h>

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

/** Aggregate functions that store one of passed values.
  * For example: any, anyLast.
  */


/// For numeric values.
template <typename T>
struct SingleValueDataFixed
{
private:
    using Self = SingleValueDataFixed;
    using ColVecType = ColumnVectorOrDecimal<T>;

    bool has_value = false; /// We need to remember if at least one value has been passed. This is necessary for AggregateFunctionIf.
    T value;

public:
    static constexpr bool is_nullable = false;
    static constexpr bool is_any = false;

    bool has() const { return has_value; }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            assert_cast<ColVecType &>(to).getData().push_back(value);
        else
            assert_cast<ColVecType &>(to).insertDefault();
    }

    void write(WriteBuffer & buf, const ISerialization & /*serialization*/) const
    {
        writeBinary(has(), buf);
        if (has())
            writeBinary(value, buf);
    }

    void read(ReadBuffer & buf, const ISerialization & /*serialization*/, Arena *)
    {
        readBinary(has_value, buf);
        if (has())
            readBinary(value, buf);
    }


    void change(const IColumn & column, size_t row_num, Arena *)
    {
        has_value = true;
        value = assert_cast<const ColVecType &>(column).getData()[row_num];
    }

    /// Assuming to.has()
    void change(const Self & to, Arena *)
    {
        has_value = true;
        value = to.value;
    }

    bool changeFirstTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeFirstTime(const Self & to, Arena * arena)
    {
        if (!has() && to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeEveryTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self & to, Arena * arena)
    {
        if (to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has() || assert_cast<const ColVecType &>(column).getData()[row_num] < value)
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || to.value < value))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has() || assert_cast<const ColVecType &>(column).getData()[row_num] > value)
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || to.value > value))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool isEqualTo(const Self & to) const { return has() && to.value == value; }

    bool isEqualTo(const IColumn & column, size_t row_num) const
    {
        return has() && assert_cast<const ColVecType &>(column).getData()[row_num] == value;
    }

    static bool allocatesMemoryInArena() { return false; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = true;

    static llvm::Value * getValuePtrFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        static constexpr size_t value_offset_from_structure = offsetof(SingleValueDataFixed<T>, value);

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

    static void
    compileChangeMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        auto * value_src = getValueFromAggregateDataPtr(builder, aggregate_data_src_ptr);

        compileChange(builder, aggregate_data_dst_ptr, value_src);
    }

    static void compileChangeFirstTime(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_ptr = b.CreatePointerCast(aggregate_data_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_value = b.CreateLoad(b.getInt1Ty(), has_value_ptr);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());
        auto * if_should_not_change = llvm::BasicBlock::Create(head->getContext(), "if_should_not_change", head->getParent());

        b.CreateCondBr(has_value_value, if_should_not_change, if_should_change);

        b.SetInsertPoint(if_should_not_change);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_should_change);
        compileChange(builder, aggregate_data_ptr, value_to_check);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    static void
    compileChangeFirstTimeMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_dst = b.CreateLoad(b.getInt1Ty(), has_value_dst_ptr);

        auto * has_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_src = b.CreateLoad(b.getInt1Ty(), has_value_src_ptr);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());
        auto * if_should_not_change = llvm::BasicBlock::Create(head->getContext(), "if_should_not_change", head->getParent());

        b.CreateCondBr(b.CreateAnd(b.CreateNot(has_value_dst), has_value_src), if_should_change, if_should_not_change);

        b.SetInsertPoint(if_should_change);
        compileChangeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_should_not_change);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    static void compileChangeEveryTime(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        compileChange(builder, aggregate_data_ptr, value_to_check);
    }

    static void
    compileChangeEveryTimeMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_src = b.CreateLoad(b.getInt1Ty(), has_value_src_ptr);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());
        auto * if_should_not_change = llvm::BasicBlock::Create(head->getContext(), "if_should_not_change", head->getParent());

        b.CreateCondBr(has_value_src, if_should_change, if_should_not_change);

        b.SetInsertPoint(if_should_change);
        compileChangeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_should_not_change);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    static llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
    {
        return getValueFromAggregateDataPtr(builder, aggregate_data_ptr);
    }

#endif
};


/** For strings. Short strings are stored in the object itself, and long strings are allocated separately.
  * NOTE It could also be suitable for arrays of numbers.
  */
struct SingleValueDataString //-V730
{
private:
    using Self = SingleValueDataString;

    Int32 size = -1; /// -1 indicates that there is no value.
    Int32 capacity = 0; /// power of two or zero
    char * large_data;

public:
    static constexpr Int32 AUTOMATIC_STORAGE_SIZE = 64;
    static constexpr Int32 MAX_SMALL_STRING_SIZE = AUTOMATIC_STORAGE_SIZE - sizeof(size) - sizeof(capacity) - sizeof(large_data);

private:
    char small_data[MAX_SMALL_STRING_SIZE]; /// Including the terminating zero.

public:
    static constexpr bool is_nullable = false;
    static constexpr bool is_any = false;

    bool has() const { return size >= 0; }

    const char * getData() const { return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data; }

    StringRef getStringRef() const { return StringRef(getData(), size); }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            assert_cast<ColumnString &>(to).insertDataWithTerminatingZero(getData(), size);
        else
            assert_cast<ColumnString &>(to).insertDefault();
    }

    void write(WriteBuffer & buf, const ISerialization & /*serialization*/) const
    {
        writeBinary(size, buf);
        if (has())
            buf.write(getData(), size);
    }

    void read(ReadBuffer & buf, const ISerialization & /*serialization*/, Arena * arena)
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

    /// Assuming to.has()
    void changeImpl(StringRef value, Arena * arena)
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

    void change(const IColumn & column, size_t row_num, Arena * arena)
    {
        changeImpl(assert_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num), arena);
    }

    void change(const Self & to, Arena * arena) { changeImpl(to.getStringRef(), arena); }

    bool changeFirstTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeFirstTime(const Self & to, Arena * arena)
    {
        if (!has() && to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeEveryTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self & to, Arena * arena)
    {
        if (to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool isEqualTo(const Self & to) const { return has() && to.getStringRef() == getStringRef(); }

    bool isEqualTo(const IColumn & column, size_t row_num) const
    {
        return has() && assert_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num) == getStringRef();
    }

    static bool allocatesMemoryInArena() { return true; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = false;

#endif
};

static_assert(
    sizeof(SingleValueDataString) == SingleValueDataString::AUTOMATIC_STORAGE_SIZE, "Incorrect size of SingleValueDataString struct");


/// For any other value types.
struct SingleValueDataGeneric
{
private:
    using Self = SingleValueDataGeneric;

    Field value;

public:
    static constexpr bool is_nullable = false;
    static constexpr bool is_any = false;

    bool has() const { return !value.isNull(); }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            to.insert(value);
        else
            to.insertDefault();
    }

    void write(WriteBuffer & buf, const ISerialization & serialization) const
    {
        if (!value.isNull())
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

    void change(const IColumn & column, size_t row_num, Arena *) { column.get(row_num, value); }

    void change(const Self & to, Arena *) { value = to.value; }

    bool changeFirstTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeFirstTime(const Self & to, Arena * arena)
    {
        if (!has() && to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeEveryTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self & to, Arena * arena)
    {
        if (to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool isEqualTo(const IColumn & column, size_t row_num) const { return has() && value == column[row_num]; }

    bool isEqualTo(const Self & to) const { return has() && to.value == value; }

    static bool allocatesMemoryInArena() { return false; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = false;

#endif
};


/** What is the difference between the aggregate functions any, anyLast
  *  (the condition that the stored value is replaced by a new one,
  *   as well as, of course, the name).
  */

template <typename Data>
struct AggregateFunctionAnyData : Data
{
    using Self = AggregateFunctionAnyData;
    static constexpr bool is_any = true;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeFirstTime(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeFirstTime(to, arena); }
    void addManyDefaults(const IColumn & column, size_t /*length*/, Arena * arena) { this->changeFirstTime(column, 0, arena); }

    static const char * name() { return "any"; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = Data::is_compilable;

    static void compileChangeIfBetter(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        Data::compileChangeFirstTime(builder, aggregate_data_ptr, value_to_check);
    }

    static void
    compileChangeIfBetterMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        Data::compileChangeFirstTimeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

#endif
};

template <typename Data>
struct AggregateFunctionAnyLastData : Data
{
    using Self = AggregateFunctionAnyLastData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeEveryTime(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeEveryTime(to, arena); }
    void addManyDefaults(const IColumn & column, size_t /*length*/, Arena * arena) { this->changeEveryTime(column, 0, arena); }

    static const char * name() { return "anyLast"; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = Data::is_compilable;

    static void compileChangeIfBetter(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        Data::compileChangeEveryTime(builder, aggregate_data_ptr, value_to_check);
    }

    static void
    compileChangeIfBetterMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        Data::compileChangeEveryTimeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

#endif
};

template <typename Data>
struct AggregateFunctionSingleValueOrNullData : Data
{
    static constexpr bool is_nullable = true;

    using Self = AggregateFunctionSingleValueOrNullData;

    bool first_value = true;
    bool is_null = false;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (first_value)
        {
            first_value = false;
            this->change(column, row_num, arena);
            return true;
        }
        else if (!this->isEqualTo(column, row_num))
        {
            is_null = true;
        }
        return false;
    }

    bool changeIfBetter(const Self & to, Arena * arena)
    {
        if (first_value)
        {
            first_value = false;
            this->change(to, arena);
            return true;
        }
        else if (!this->isEqualTo(to))
        {
            is_null = true;
        }
        return false;
    }

    void addManyDefaults(const IColumn & column, size_t /*length*/, Arena * arena) { this->changeIfBetter(column, 0, arena); }

    void insertResultInto(IColumn & to) const
    {
        if (is_null || first_value)
        {
            to.insertDefault();
        }
        else
        {
            ColumnNullable & col = typeid_cast<ColumnNullable &>(to);
            col.getNullMapColumn().insertDefault();
            this->Data::insertResultInto(col.getNestedColumn());
        }
    }

    static const char * name() { return "singleValueOrNull"; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = false;

#endif
};

/** Implement 'heavy hitters' algorithm.
  * Selects most frequent value if its frequency is more than 50% in each thread of execution.
  * Otherwise, selects some arbitrary value.
  * http://www.cs.umd.edu/~samir/498/karp.pdf
  */
template <typename Data>
struct AggregateFunctionAnyHeavyData : Data
{
    UInt64 counter = 0;

    using Self = AggregateFunctionAnyHeavyData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (this->isEqualTo(column, row_num))
        {
            ++counter;
        }
        else
        {
            if (counter == 0)
            {
                this->change(column, row_num, arena);
                ++counter;
                return true;
            }
            else
                --counter;
        }
        return false;
    }

    bool changeIfBetter(const Self & to, Arena * arena)
    {
        if (this->isEqualTo(to))
        {
            counter += to.counter;
        }
        else
        {
            if ((!this->has() && to.has()) || counter < to.counter)
            {
                this->change(to, arena);
                return true;
            }
            else
                counter -= to.counter;
        }
        return false;
    }

    void addManyDefaults(const IColumn & column, size_t length, Arena * arena)
    {
        for (size_t i = 0; i < length; ++i)
            changeIfBetter(column, 0, arena);
    }

    void write(WriteBuffer & buf, const ISerialization & serialization) const
    {
        Data::write(buf, serialization);
        writeBinary(counter, buf);
    }

    void read(ReadBuffer & buf, const ISerialization & serialization, Arena * arena)
    {
        Data::read(buf, serialization, arena);
        readBinary(counter, buf);
    }

    static const char * name() { return "anyHeavy"; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = false;

#endif
};


template <typename Data>
class AggregateFunctionsSingleValue final : public IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>>
{
    static constexpr bool is_any = Data::is_any;

private:
    SerializationPtr serialization;

public:
    explicit AggregateFunctionsSingleValue(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>>({type}, {})
        , serialization(type->getDefaultSerialization())
    {
    }

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override
    {
        auto result_type = this->argument_types.at(0);
        if constexpr (Data::is_nullable)
            return makeNullable(result_type);
        return result_type;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).changeIfBetter(*columns[0], row_num, arena);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena * arena) const override
    {
        this->data(place).addManyDefaults(*columns[0], length, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos)
        const override
    {
        if constexpr (is_any)
            if (this->data(place).has())
                return;
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i])
                {
                    this->data(place).changeIfBetter(*columns[0], i, arena);
                    if constexpr (is_any)
                        break;
                }
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; ++i)
            {
                this->data(place).changeIfBetter(*columns[0], i, arena);
                if constexpr (is_any)
                    break;
            }
        }
    }

    void addBatchSinglePlaceNotNull( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if constexpr (is_any)
            if (this->data(place).has())
                return;

        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (!null_map[i] && flags[i])
                {
                    this->data(place).changeIfBetter(*columns[0], i, arena);
                    if constexpr (is_any)
                        break;
                }
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (!null_map[i])
                {
                    this->data(place).changeIfBetter(*columns[0], i, arena);
                    if constexpr (is_any)
                        break;
                }
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).changeIfBetter(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::allocatesMemoryInArena(); }

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
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        b.CreateMemSet(
            aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), this->sizeOfData(), llvm::assumeAligned(this->alignOfData()));
    }

    void compileAdd(
        llvm::IRBuilderBase & builder,
        llvm::Value * aggregate_data_ptr,
        const DataTypes &,
        const std::vector<llvm::Value *> & argument_values) const override
    {
        if constexpr (Data::is_compilable)
        {
            Data::compileChangeIfBetter(builder, aggregate_data_ptr, argument_values[0]);
        }
        else
        {
            throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        if constexpr (Data::is_compilable)
        {
            Data::compileChangeIfBetterMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        }
        else
        {
            throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (Data::is_compilable)
        {
            return Data::compileGetResult(builder, aggregate_data_ptr);
        }
        else
        {
            throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

#endif
};

/// any, anyLast, anyHeavy, etc...
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data>
static IAggregateFunction *
createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<TYPE>>>(argument_type); /// NOLINT
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDate::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDateTime::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::DateTime64)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DateTime64>>>(argument_type);
    if (which.idx == TypeIndex::Decimal32)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Decimal32>>>(argument_type);
    if (which.idx == TypeIndex::Decimal64)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Decimal64>>>(argument_type);
    if (which.idx == TypeIndex::Decimal128)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Decimal128>>>(argument_type);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<Data<SingleValueDataString>>(argument_type);

    return new AggregateFunctionTemplate<Data<SingleValueDataGeneric>>(argument_type);
}

}
