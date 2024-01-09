#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <base/StringRef.h>
#include <Common/Arena.h>
#include <Common/assert_cast.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NOT_IMPLEMENTED;
extern const int TOO_LARGE_STRING_SIZE;
extern const int LOGICAL_ERROR;
}

/** Aggregate functions that store one of passed values.
  * For example: min, max, any, anyLast.
  */
struct SingleValueDataBase
{
    static constexpr int nan_direction_hint = 1;

    virtual ~SingleValueDataBase() { }
    virtual bool has() const = 0;
    virtual void insertResultInto(IColumn &) const = 0;
    virtual void write(WriteBuffer &, const ISerialization &) const = 0;
    virtual void read(ReadBuffer &, const ISerialization &, Arena *) = 0;
    virtual bool isEqualTo(const IColumn & column, size_t row_num) const = 0;

    virtual void set(const IColumn &, size_t row_num, Arena *) = 0;
    virtual void set(const SingleValueDataBase &, Arena *) = 0;
    virtual bool setIfSmaller(const IColumn &, size_t row_num, Arena *) = 0;
    virtual bool setIfSmaller(const SingleValueDataBase &, Arena *) = 0;
    virtual bool setIfGreater(const IColumn &, size_t row_num, Arena *) = 0;
    virtual bool setIfGreater(const SingleValueDataBase &, Arena *) = 0;

    static std::optional<size_t> getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end);
    static std::optional<size_t> getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end);
    static std::optional<size_t> getSmallestIndexNotNullIf(
        const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end);
    static std::optional<size_t> getGreatestIndexNotNullIf(
        const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end);

    virtual void setSmallest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena);
    virtual void setGreatest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena);
    virtual void setSmallestNotNullIf(const IColumn &, const UInt8 * __restrict, const UInt8 * __restrict, size_t, size_t, Arena *);
    virtual void setGreatestNotNullIf(const IColumn &, const UInt8 * __restrict, const UInt8 * __restrict, size_t, size_t, Arena *);
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

/// For numeric values.
template <typename T>
struct SingleValueDataFixed final : public SingleValueDataBase
{
    using Self = SingleValueDataFixed;
    using ColVecType = ColumnVectorOrDecimal<T>;

    T value = T{};
    bool has_value = false; /// We need to remember if at least one value has been passed. This is necessary for AggregateFunctionIf.

    ~SingleValueDataFixed() override { }

    bool has() const override { return has_value; }

    void insertResultInto(IColumn & to) const override
    {
        if (has())
            assert_cast<ColVecType &>(to).getData().push_back(value);
        else
            assert_cast<ColVecType &>(to).insertDefault();
    }

    void write(WriteBuffer & buf, const ISerialization & /*serialization*/) const override
    {
        writeBinary(has(), buf);
        if (has())
            writeBinaryLittleEndian(value, buf);
    }

    void read(ReadBuffer & buf, const ISerialization & /*serialization*/, Arena *) override
    {
        readBinary(has_value, buf);
        if (has())
            readBinaryLittleEndian(value, buf);
    }

    bool isEqualTo(const IColumn & column, size_t index) const override
    {
        return has() && assert_cast<const ColVecType &>(column).getData()[index] == value;
    }

    bool isEqualTo(const Self & to) const { return has() && to.value == value; }

    void set(const IColumn & column, size_t row_num, Arena *) override
    {
        has_value = true;
        value = assert_cast<const ColVecType &>(column).getData()[row_num];
    }

    void set(const SingleValueDataBase & to, Arena *) override
    {
        has_value = true;
        value = assert_cast<const Self &>(to).value;
    }

    bool setIfSmaller(const T & to)
    {
        if (!has_value || to < value)
        {
            has_value = true;
            value = to;
            return true;
        }
        return false;
    }

    bool setIfGreater(const T & to)
    {
        if (!has_value || to > value)
        {
            has_value = true;
            value = to;
            return true;
        }
        return false;
    }

    bool setIfSmaller(const SingleValueDataBase & to, Arena * arena) override
    {
        auto const & other = assert_cast<const Self &>(to);
        if (other.has() && (!has() || other.value < value))
        {
            set(other, arena);
            return true;
        }
        else
            return false;
    }

    bool setIfGreater(const SingleValueDataBase & to, Arena * arena) override
    {
        auto const & other = assert_cast<const Self &>(to);
        if (other.has() && (!has() || other.value > value))
        {
            set(to, arena);
            return true;
        }
        else
            return false;
    }

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

    //    static std::optional<size_t> getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end) override;
    //    static std::optional<size_t> getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end);
    //    static std::optional<size_t> getSmallestIndexNotNullIf(
    //            const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end);
    //    static std::optional<size_t> getGreatestIndexNotNullIf(
    //            const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end);

    static bool allocatesMemoryInArena() { return false; }
};

#define DISPATCH(TYPE) extern template struct SingleValueDataFixed<TYPE>;

FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH


/** For strings. Short strings are stored in the object itself, and long strings are allocated separately.
  * NOTE It could also be suitable for arrays of numbers.
//  */
struct SingleValueDataString final : public SingleValueDataBase
{
    using Self = SingleValueDataString;

    /// 0 size indicates that there is no value. Empty string must has terminating '\0' and, therefore, size of empty string is 1
    UInt32 size = 0;
    UInt32 capacity = 0; /// power of two or zero
    char * large_data;

    static constexpr UInt32 AUTOMATIC_STORAGE_SIZE = 64;
    //// TODO: Maybe instead of a virtual class we need to go with a std::variant of the 3 to avoid reserving space for the vtable
    static constexpr UInt32 MAX_SMALL_STRING_SIZE
        = AUTOMATIC_STORAGE_SIZE - sizeof(size) - sizeof(capacity) - sizeof(large_data) - sizeof(SingleValueDataBase);
    static constexpr UInt32 MAX_STRING_SIZE = std::numeric_limits<Int32>::max();

private:
    char small_data[MAX_SMALL_STRING_SIZE]; /// Including the terminating zero.

    char * getDataMutable() { return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data; }

    const char * getData() const
    {
        const char * data_ptr = size <= MAX_SMALL_STRING_SIZE ? small_data : large_data;
        /// It must always be terminated with null-character
        chassert(0 < size);
        chassert(data_ptr[size - 1] == '\0');
        return data_ptr;
    }

    StringRef getStringRef() const { return StringRef(getData(), size); }

    void allocateLargeDataIfNeeded(UInt32 size_to_reserve, Arena * arena)
    {
        if (capacity < size_to_reserve)
        {
            if (unlikely(MAX_STRING_SIZE < size_to_reserve))
                throw Exception(
                    ErrorCodes::TOO_LARGE_STRING_SIZE, "String size is too big ({}), maximum: {}", size_to_reserve, MAX_STRING_SIZE);

            size_t rounded_capacity = roundUpToPowerOfTwoOrZero(size_to_reserve);
            chassert(rounded_capacity <= MAX_STRING_SIZE + 1); /// rounded_capacity <= 2^31
            capacity = static_cast<UInt32>(rounded_capacity);

            /// Don't free large_data here.
            large_data = arena->alloc(capacity);
        }
    }

    void changeImpl(StringRef value, Arena * arena)
    {
        if (unlikely(MAX_STRING_SIZE < value.size))
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "String size is too big ({}), maximum: {}", value.size, MAX_STRING_SIZE);

        UInt32 value_size = static_cast<UInt32>(value.size);

        if (value_size <= MAX_SMALL_STRING_SIZE)
        {
            /// Don't free large_data here.
            size = value_size;

            if (size > 0)
                memcpy(small_data, value.data, size);
        }
        else
        {
            allocateLargeDataIfNeeded(value_size, arena);
            size = value_size;
            memcpy(large_data, value.data, size);
        }
    }

public:
    bool has() const override { return size; }

    void insertResultInto(IColumn & to) const override;
    void write(WriteBuffer & buf, const ISerialization & /*serialization*/) const override;
    void read(ReadBuffer & buf, const ISerialization & /*serialization*/, Arena * arena) override;
    bool isEqualTo(const IColumn & column, size_t row_num) const override;
    bool isEqualTo(const Self & to) const;
    void set(const IColumn & column, size_t row_num, Arena * arena) override;
    void set(const SingleValueDataBase & to, Arena * arena) override;

    bool setIfSmaller(const IColumn & column, size_t row_num, Arena * arena) override;
    bool setIfSmaller(const SingleValueDataBase & to, Arena * arena) override;

    bool setIfGreater(const IColumn & column, size_t row_num, Arena * arena) override;
    bool setIfGreater(const SingleValueDataBase & to, Arena * arena) override;

    static bool allocatesMemoryInArena() { return true; }
};

static_assert(
    sizeof(SingleValueDataString) == SingleValueDataString::AUTOMATIC_STORAGE_SIZE, "Incorrect size of SingleValueDataString struct");


/// For any other value types.
struct SingleValueDataGeneric final : public SingleValueDataBase
{
private:
    using Self = SingleValueDataGeneric;
    Field value;

public:
    bool has() const override { return !value.isNull(); }

    void insertResultInto(IColumn & to) const override
    {
        if (has())
            to.insert(value);
        else
            to.insertDefault();
    }

    void write(WriteBuffer & buf, const ISerialization & serialization) const override
    {
        if (!value.isNull())
        {
            writeBinary(true, buf);
            serialization.serializeBinary(value, buf, {});
        }
        else
            writeBinary(false, buf);
    }

    void read(ReadBuffer & buf, const ISerialization & serialization, Arena *) override
    {
        bool is_not_null;
        readBinary(is_not_null, buf);

        if (is_not_null)
            serialization.deserializeBinary(value, buf, {});
    }

    bool isEqualTo(const IColumn & column, size_t row_num) const override { return has() && value == column[row_num]; }

    bool isEqualTo(const Self & to) const { return has() && to.value == value; }

    void set(const IColumn & column, size_t row_num, Arena *) override { column.get(row_num, value); }

    void set(const SingleValueDataBase & other, Arena *) override
    {
        auto const & to = assert_cast<const Self &>(other);
        value = to.value;
    }

    bool setIfSmaller(const IColumn & column, size_t row_num, Arena * arena) override
    {
        if (!has())
        {
            set(column, row_num, arena);
            return true;
        }
        else
        {
            Field new_value;
            column.get(row_num, new_value);
            if (new_value < value)
            {
                value = new_value;
                return true;
            }
            else
                return false;
        }
    }

    bool setIfSmaller(const SingleValueDataBase & other, Arena *) override
    {
        auto const & to = assert_cast<const Self &>(other);
        if (!to.has())
            return false;
        if (!has() || to.value < value)
        {
            value = to.value;
            return true;
        }
        else
            return false;
    }

    bool setIfGreater(const IColumn & column, size_t row_num, Arena * arena) override
    {
        if (!has())
        {
            set(column, row_num, arena);
            return true;
        }
        else
        {
            Field new_value;
            column.get(row_num, new_value);
            if (new_value > value)
            {
                value = new_value;
                return true;
            }
            else
                return false;
        }
    }

    bool setIfGreater(const SingleValueDataBase & other, Arena *) override
    {
        auto const & to = assert_cast<const Self &>(other);
        if (!to.has())
            return false;
        if (!has() || to.value > value)
        {
            value = to.value;
            return true;
        }
        else
            return false;
    }

    static bool allocatesMemoryInArena() { return false; }
};
}
