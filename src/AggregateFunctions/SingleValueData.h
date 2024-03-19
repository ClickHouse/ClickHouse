#pragma once

#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <base/StringRef.h>

namespace DB
{
class Arena;
class ReadBuffer;
struct Settings;
class WriteBuffer;

/// Base class for Aggregation data that stores one of passed values: min, any, argMax...
/// It's setup as a virtual class so we can avoid templates when we need to extend them (argMax, SingleValueOrNull)
struct SingleValueDataBase
{
    /// Any subclass (numeric, string, generic) must be smaller than MAX_STORAGE_SIZE
    /// We use this knowledge to create composite data classes that use them directly by reserving a 'memory_block'
    /// For example argMin holds 1 of these (for the result), while keeping a template for the value
    static constexpr UInt32 MAX_STORAGE_SIZE = 64;

    virtual ~SingleValueDataBase() = default;
    virtual bool has() const = 0;
    virtual void insertResultInto(IColumn &) const = 0;
    virtual void write(WriteBuffer &, const ISerialization &) const = 0;
    virtual void read(ReadBuffer &, const ISerialization &, Arena *) = 0;

    virtual bool isEqualTo(const IColumn & column, size_t row_num) const = 0;
    virtual bool isEqualTo(const SingleValueDataBase &) const = 0;
    virtual void set(const IColumn &, size_t row_num, Arena *) = 0;
    virtual void set(const SingleValueDataBase &, Arena *) = 0;
    virtual bool setIfSmaller(const IColumn &, size_t row_num, Arena *) = 0;
    virtual bool setIfSmaller(const SingleValueDataBase &, Arena *) = 0;
    virtual bool setIfGreater(const IColumn &, size_t row_num, Arena *) = 0;
    virtual bool setIfGreater(const SingleValueDataBase &, Arena *) = 0;

    /// Given a column, sets the internal value to the smallest or greatest value from the column
    /// Used to implement batch min/max
    virtual void setSmallest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena);
    virtual void setGreatest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena);
    virtual void setSmallestNotNullIf(const IColumn &, const UInt8 * __restrict, const UInt8 * __restrict, size_t, size_t, Arena *);
    virtual void setGreatestNotNullIf(const IColumn &, const UInt8 * __restrict, const UInt8 * __restrict, size_t, size_t, Arena *);

    /// Given a column returns the index of the smallest or greatest value in it
    /// Doesn't return anything if the column is empty
    /// There are used to implement argMin / argMax
    virtual std::optional<size_t> getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end) const;
    virtual std::optional<size_t> getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end) const;
    virtual std::optional<size_t> getSmallestIndexNotNullIf(
        const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end) const;
    virtual std::optional<size_t> getGreatestIndexNotNullIf(
        const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end) const;
};


#define FOR_SINGLE_VALUE_NUMERIC_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(UInt128) \
    M(UInt256) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Int128) \
    M(Int256) \
    M(Float32) \
    M(Float64) \
    M(Decimal32) \
    M(Decimal64) \
    M(Decimal128) \
    M(Decimal256) \
    M(DateTime64)

/// For numeric values (without inheritance, for performance sensitive functions and JIT)
template <typename T>
struct SingleValueDataFixed
{
    static constexpr bool is_compilable = true;
    using Self = SingleValueDataFixed;
    using ColVecType = ColumnVectorOrDecimal<T>;

    T value = T{};
    /// We need to remember if at least one value has been passed.
    /// This is necessary for AggregateFunctionIf, merging states, JIT (where simple add is used), etc
    bool has_value = false;

    bool has() const { return has_value; }
    void insertResultInto(IColumn & to) const;
    void write(WriteBuffer & buf, const ISerialization &) const;
    void read(ReadBuffer & buf, const ISerialization &, Arena *);
    bool isEqualTo(const IColumn & column, size_t index) const;
    bool isEqualTo(const Self & to) const;

    void set(const IColumn & column, size_t row_num, Arena *);
    void set(const Self & to, Arena *);

    bool setIfSmaller(const T & to);
    bool setIfGreater(const T & to);

    bool setIfSmaller(const Self & to, Arena * arena);
    bool setIfGreater(const Self & to, Arena * arena);
    bool setIfSmaller(const IColumn & column, size_t row_num, Arena * arena);
    bool setIfGreater(const IColumn & column, size_t row_num, Arena * arena);
    void setSmallest(const IColumn & column, size_t row_begin, size_t row_end, Arena *);
    void setGreatest(const IColumn & column, size_t row_begin, size_t row_end, Arena *);
    void setSmallestNotNullIf(
        const IColumn & column,
        const UInt8 * __restrict null_map,
        const UInt8 * __restrict if_map,
        size_t row_begin,
        size_t row_end,
        Arena *);
    void setGreatestNotNullIf(
        const IColumn & column,
        const UInt8 * __restrict null_map,
        const UInt8 * __restrict if_map,
        size_t row_begin,
        size_t row_end,
        Arena *);

    std::optional<size_t> getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end) const;
    std::optional<size_t> getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end) const;
    std::optional<size_t> getSmallestIndexNotNullIf(
        const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end) const;
    std::optional<size_t> getGreatestIndexNotNullIf(
        const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end) const;

    static bool allocatesMemoryInArena() { return false; }

#if USE_EMBEDDED_COMPILER
    static constexpr size_t has_value_offset = offsetof(Self, has_value);
    static constexpr size_t value_offset = offsetof(Self, value);

    static bool isCompilable(const IDataType & type);
    static llvm::Value * getValuePtrFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr);
    static llvm::Value * getValueFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr);
    static llvm::Value * getHasValuePtrFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr);
    static llvm::Value * getHasValueFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr);

    static void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr);
    static llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr);

    static void compileSetValueFromNumber(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check);
    static void
    compileSetValueFromAggregation(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * aggregate_data_src_ptr);

    static void compileAny(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check);
    static void compileAnyMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr);

    static void compileAnyLast(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check);
    static void
    compileAnyLastMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr);

    template <bool isMin>
    static void compileMinMax(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check);
    template <bool isMin>
    static void
    compileMinMaxMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr);
    static void compileMin(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check);
    static void compileMinMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr);
    static void compileMax(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check);
    static void compileMaxMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr);
#endif
};

#define DISPATCH(TYPE) \
    extern template struct SingleValueDataFixed<TYPE>; \
    static_assert( \
        sizeof(SingleValueDataFixed<TYPE>) <= SingleValueDataBase::MAX_STORAGE_SIZE, "Incorrect size of SingleValueDataFixed struct");

FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

/// For numeric values inheriting from SingleValueDataBase
template <typename T>
struct SingleValueDataNumeric final : public SingleValueDataBase
{
    using Self = SingleValueDataNumeric<T>;
    using Base = SingleValueDataFixed<T>;

private:
    /// 32 bytes for types of 256 bits, + 8 bytes for the virtual table pointer.
    static constexpr size_t base_memory_reserved_size = 40;
    struct alignas(alignof(Base)) PrivateMemory
    {
        char memory[base_memory_reserved_size];
        Base & get() { return *reinterpret_cast<Base *>(memory); }
        const Base & get() const { return *reinterpret_cast<const Base *>(memory); }
    };
    static_assert(sizeof(Base) <= base_memory_reserved_size);

    PrivateMemory memory;

public:
    static constexpr bool is_compilable = false;

    SingleValueDataNumeric();
    ~SingleValueDataNumeric() override;

    bool has() const override;
    void insertResultInto(IColumn & to) const override;
    void write(WriteBuffer & buf, const ISerialization & serialization) const override;
    void read(ReadBuffer & buf, const ISerialization & serialization, Arena * arena) override;
    bool isEqualTo(const IColumn & column, size_t index) const override;
    bool isEqualTo(const SingleValueDataBase & to) const override;

    void set(const IColumn & column, size_t row_num, Arena * arena) override;
    void set(const SingleValueDataBase & to, Arena * arena) override;

    bool setIfSmaller(const SingleValueDataBase & to, Arena * arena) override;
    bool setIfGreater(const SingleValueDataBase & to, Arena * arena) override;
    bool setIfSmaller(const IColumn & column, size_t row_num, Arena * arena) override;
    bool setIfGreater(const IColumn & column, size_t row_num, Arena * arena) override;
    void setSmallest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena) override;
    void setGreatest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena) override;
    void setSmallestNotNullIf(
        const IColumn & column,
        const UInt8 * __restrict null_map,
        const UInt8 * __restrict if_map,
        size_t row_begin,
        size_t row_end,
        Arena * arena) override;
    void setGreatestNotNullIf(
        const IColumn & column,
        const UInt8 * __restrict null_map,
        const UInt8 * __restrict if_map,
        size_t row_begin,
        size_t row_end,
        Arena * arena) override;

    std::optional<size_t> getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end) const override;
    std::optional<size_t> getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end) const override;
    std::optional<size_t> getSmallestIndexNotNullIf(
        const IColumn & column,
        const UInt8 * __restrict null_map,
        const UInt8 * __restrict if_map,
        size_t row_begin,
        size_t row_end) const override;
    std::optional<size_t> getGreatestIndexNotNullIf(
        const IColumn & column,
        const UInt8 * __restrict null_map,
        const UInt8 * __restrict if_map,
        size_t row_begin,
        size_t row_end) const override;

    static bool allocatesMemoryInArena() { return false; }
};

#define DISPATCH(TYPE) \
    extern template struct SingleValueDataNumeric<TYPE>; \
    static_assert( \
        sizeof(SingleValueDataNumeric<TYPE>) <= SingleValueDataBase::MAX_STORAGE_SIZE, "Incorrect size of SingleValueDataNumeric struct");

FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH


/** For strings. Short strings are stored in the object itself, and long strings are allocated separately.
  * NOTE It could also be suitable for arrays of numbers.
//  */
struct SingleValueDataString final : public SingleValueDataBase
{
    static constexpr bool is_compilable = false;
    using Self = SingleValueDataString;

    /// 0 size indicates that there is no value. Empty string must have terminating '\0' and, therefore, size of empty string is 1
    UInt32 size = 0;
    UInt32 capacity = 0; /// power of two or zero
    char * large_data; /// Always allocated in an arena

    //// TODO: Maybe instead of a virtual class we need to go with a std::variant of the 3 to avoid reserving space for the vtable
    static constexpr UInt32 MAX_SMALL_STRING_SIZE
        = SingleValueDataBase::MAX_STORAGE_SIZE - sizeof(size) - sizeof(capacity) - sizeof(large_data) - sizeof(SingleValueDataBase);
    static constexpr UInt32 MAX_STRING_SIZE = std::numeric_limits<Int32>::max();

private:
    char small_data[MAX_SMALL_STRING_SIZE]; /// Including the terminating zero.

    char * getDataMutable();
    const char * getData() const;
    StringRef getStringRef() const;
    void allocateLargeDataIfNeeded(UInt32 size_to_reserve, Arena * arena);
    void changeImpl(StringRef value, Arena * arena);

public:
    bool has() const override { return size != 0; }
    void insertResultInto(IColumn & to) const override;
    void write(WriteBuffer & buf, const ISerialization & /*serialization*/) const override;
    void read(ReadBuffer & buf, const ISerialization & /*serialization*/, Arena * arena) override;

    bool isEqualTo(const IColumn & column, size_t row_num) const override;
    bool isEqualTo(const SingleValueDataBase &) const override;
    void set(const IColumn & column, size_t row_num, Arena * arena) override;
    void set(const SingleValueDataBase &, Arena * arena) override;

    bool setIfSmaller(const IColumn & column, size_t row_num, Arena * arena) override;
    bool setIfSmaller(const SingleValueDataBase &, Arena * arena) override;

    bool setIfGreater(const IColumn & column, size_t row_num, Arena * arena) override;
    bool setIfGreater(const SingleValueDataBase &, Arena * arena) override;

    static bool allocatesMemoryInArena() { return true; }
};

static_assert(sizeof(SingleValueDataString) == SingleValueDataBase::MAX_STORAGE_SIZE, "Incorrect size of SingleValueDataString struct");


/// For any other value types.
struct SingleValueDataGeneric final : public SingleValueDataBase
{
    static constexpr bool is_compilable = false;

private:
    using Self = SingleValueDataGeneric;
    Field value;

public:
    bool has() const override { return !value.isNull(); }
    void insertResultInto(IColumn & to) const override;
    void write(WriteBuffer & buf, const ISerialization & serialization) const override;
    void read(ReadBuffer & buf, const ISerialization & serialization, Arena *) override;

    bool isEqualTo(const IColumn & column, size_t row_num) const override;
    bool isEqualTo(const SingleValueDataBase & other) const override;
    void set(const IColumn & column, size_t row_num, Arena *) override;
    void set(const SingleValueDataBase & other, Arena *) override;

    bool setIfSmaller(const IColumn & column, size_t row_num, Arena * arena) override;
    bool setIfSmaller(const SingleValueDataBase & other, Arena *) override;
    bool setIfGreater(const IColumn & column, size_t row_num, Arena * arena) override;
    bool setIfGreater(const SingleValueDataBase & other, Arena *) override;

    static bool allocatesMemoryInArena() { return false; }
};

static_assert(sizeof(SingleValueDataGeneric) <= SingleValueDataBase::MAX_STORAGE_SIZE, "Incorrect size of SingleValueDataGeneric struct");

/// min, max, any, anyLast, anyHeavy, etc...
template <template <typename, bool...> class AggregateFunctionTemplate, bool unary, bool... isMin>
static IAggregateFunction *
createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    if constexpr (unary)
        assertUnary(name, argument_types);
    else
        assertBinary(name, argument_types);

    const DataTypePtr & value_type = unary ? argument_types[0] : argument_types[1];
    WhichDataType which(value_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<SingleValueDataFixed<TYPE>, isMin...>(argument_types); /// NOLINT
    FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<SingleValueDataFixed<DataTypeDate::FieldType>, isMin...>(argument_types);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<SingleValueDataFixed<DataTypeDateTime::FieldType>, isMin...>(argument_types);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<SingleValueDataString, isMin...>(argument_types);

    return new AggregateFunctionTemplate<SingleValueDataGeneric, isMin...>(argument_types);
}

/// Helper to allocate enough memory to store any derived class
struct SingleValueDataBaseMemoryBlock
{
    std::aligned_union_t<
        SingleValueDataBase::MAX_STORAGE_SIZE,
        SingleValueDataNumeric<Decimal256>, /// We check all types in generateSingleValueFromTypeIndex
        SingleValueDataString,
        SingleValueDataGeneric>
        memory;
    SingleValueDataBase & get() { return *reinterpret_cast<SingleValueDataBase *>(&memory); }
    const SingleValueDataBase & get() const { return *reinterpret_cast<const SingleValueDataBase *>(&memory); }
};

static_assert(alignof(SingleValueDataBaseMemoryBlock) == 8);

/// For Data classes that want to compose on top of SingleValueDataBase values, like argMax or singleValueOrNull
/// It will build the object based on the type idx on the memory block provided
void generateSingleValueFromTypeIndex(TypeIndex idx, SingleValueDataBaseMemoryBlock & data);

bool singleValueTypeAllocatesMemoryInArena(TypeIndex idx);
}
