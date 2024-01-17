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
    static constexpr int nan_direction_hint = 1;
    /// Any subclass (numeric, string, generic) must be smaller than MAX_STORAGE_SIZE
    /// We use this knowledge to create composite data classes that use them directly by reserving a 'memory_block'
    /// For example argMin holds 2 of these objects
    static constexpr UInt32 MAX_STORAGE_SIZE = 64;

    /// Helper to allocate enough memory to store any derived class and subclasses will be misaligned
    /// alignas is necessary as otherwise alignof(memory_block) == 1
    struct alignas(MAX_STORAGE_SIZE) memory_block
    {
        char memory[SingleValueDataBase::MAX_STORAGE_SIZE];
        SingleValueDataBase & get() { return *reinterpret_cast<SingleValueDataBase *>(memory); }
        const SingleValueDataBase & get() const { return *reinterpret_cast<const SingleValueDataBase *>(memory); }
    };
    static_assert(alignof(memory_block) == SingleValueDataBase::MAX_STORAGE_SIZE);

    virtual ~SingleValueDataBase() { }
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
    /// Doesn't return anything if the column is empty. In some cases (SingleValueDataFixed<T>) it will also return
    /// empty if the stored value is already the smallest/greatest
    /// There are used to implement argMin / argMax
    virtual std::optional<size_t> getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end);
    virtual std::optional<size_t> getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end);
    static std::optional<size_t> getSmallestIndexNotNullIf(
        const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end);
    static std::optional<size_t> getGreatestIndexNotNullIf(
        const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end);
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

/// For numeric values
template <typename T>
struct SingleValueDataFixed final : public SingleValueDataBase
{
    static constexpr bool is_compilable = true;
    using Self = SingleValueDataFixed;
    using ColVecType = ColumnVectorOrDecimal<T>;

    T value = T{};
    /// We need to remember if at least one value has been passed.
    /// This is necessary for AggregateFunctionIf, merging states, JIT (where simple add is used), etc
    bool has_value = false;

    ~SingleValueDataFixed() override { }

    bool has() const override { return has_value; }
    void insertResultInto(IColumn & to) const override;
    void write(WriteBuffer & buf, const ISerialization &) const override;
    void read(ReadBuffer & buf, const ISerialization &, Arena *) override;
    bool isEqualTo(const IColumn & column, size_t index) const override;
    bool isEqualTo(const SingleValueDataBase & to) const override;

    void set(const IColumn & column, size_t row_num, Arena *) override;
    void set(const SingleValueDataBase & to, Arena *) override;

    bool setIfSmaller(const T & to);
    bool setIfGreater(const T & to);

    bool setIfSmaller(const SingleValueDataBase & to, Arena * arena) override;
    bool setIfGreater(const SingleValueDataBase & to, Arena * arena) override;
    bool setIfSmaller(const IColumn & column, size_t row_num, Arena * arena) override;
    bool setIfGreater(const IColumn & column, size_t row_num, Arena * arena) override;
    void setSmallest(const IColumn & column, size_t row_begin, size_t row_end, Arena *) override;
    void setGreatest(const IColumn & column, size_t row_begin, size_t row_end, Arena *) override;
    void setSmallestNotNullIf(
        const IColumn & column,
        const UInt8 * __restrict null_map,
        const UInt8 * __restrict if_map,
        size_t row_begin,
        size_t row_end,
        Arena *) override;
    void setGreatestNotNullIf(
        const IColumn & column,
        const UInt8 * __restrict null_map,
        const UInt8 * __restrict if_map,
        size_t row_begin,
        size_t row_end,
        Arena *) override;

    std::optional<size_t> getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end) override;
    std::optional<size_t> getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end) override;

    static bool allocatesMemoryInArena() { return false; }

#if USE_EMBEDDED_COMPILER
    static constexpr size_t has_value_offset = offsetof(SingleValueDataFixed<T>, has_value);
    static constexpr size_t value_offset = offsetof(SingleValueDataFixed<T>, value);

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

/** For strings. Short strings are stored in the object itself, and long strings are allocated separately.
  * NOTE It could also be suitable for arrays of numbers.
//  */
struct SingleValueDataString final : public SingleValueDataBase
{
    static constexpr bool is_compilable = false;
    using Self = SingleValueDataString;

    /// 0 size indicates that there is no value. Empty string must has terminating '\0' and, therefore, size of empty string is 1
    UInt32 size = 0;
    UInt32 capacity = 0; /// power of two or zero
    char * large_data;

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
template <template <typename, bool...> class AggregateFunctionTemplate, bool... isMin>
static IAggregateFunction *
createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<SingleValueDataFixed<TYPE>, isMin...>(argument_type); /// NOLINT
    FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<SingleValueDataFixed<DataTypeDate::FieldType>, isMin...>(argument_type);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<SingleValueDataFixed<DataTypeDateTime::FieldType>, isMin...>(argument_type);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<SingleValueDataString, isMin...>(argument_type);

    return new AggregateFunctionTemplate<SingleValueDataGeneric, isMin...>(argument_type);
}

/// For Data classes that want to compose on top of SingleValueDataBase values, like argMax or singleValueOrNull
/// It will build the object based on the type idx on the memory block provided
void generateSingleValueFromTypeIndex(TypeIndex idx, SingleValueDataBase::memory_block & data);

bool singleValueTypeAllocatesMemoryInArena(TypeIndex idx);
}
