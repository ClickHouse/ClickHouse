#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <DataTypes/IDataType.h>
#include <common/StringRef.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/** Aggregate functions that store one of passed values.
  * For example: min, max, any, anyLast.
  */


/// For numeric values.
template <typename T>
struct SingleValueDataFixed
{
private:
    using Self = SingleValueDataFixed;

    bool has_value = false; /// We need to remember if at least one value has been passed. This is necessary for AggregateFunctionIf.
    T value;

public:
    bool has() const
    {
        return has_value;
    }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            static_cast<ColumnVector<T> &>(to).getData().push_back(value);
        else
            static_cast<ColumnVector<T> &>(to).insertDefault();
    }

    void write(WriteBuffer & buf, const IDataType & /*data_type*/) const
    {
        writeBinary(has(), buf);
        if (has())
            writeBinary(value, buf);
    }

    void read(ReadBuffer & buf, const IDataType & /*data_type*/, Arena *)
    {
        readBinary(has_value, buf);
        if (has())
            readBinary(value, buf);
    }


    void change(const IColumn & column, size_t row_num, Arena *)
    {
        has_value = true;
        value = static_cast<const ColumnVector<T> &>(column).getData()[row_num];
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
        if (!has() || static_cast<const ColumnVector<T> &>(column).getData()[row_num] < value)
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
        if (!has() || static_cast<const ColumnVector<T> &>(column).getData()[row_num] > value)
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

    bool isEqualTo(const Self & to) const
    {
        return has() && to.value == value;
    }

    bool isEqualTo(const IColumn & column, size_t row_num) const
    {
        return has() && static_cast<const ColumnVector<T> &>(column).getData()[row_num] == value;
    }
};


/** For strings. Short strings are stored in the object itself, and long strings are allocated separately.
  * NOTE It could also be suitable for arrays of numbers.
  */
struct SingleValueDataString
{
private:
    using Self = SingleValueDataString;

    Int32 size = -1;    /// -1 indicates that there is no value.
    Int32 capacity = 0;    /// power of two or zero
    char * large_data;

public:
    static constexpr Int32 AUTOMATIC_STORAGE_SIZE = 64;
    static constexpr Int32 MAX_SMALL_STRING_SIZE = AUTOMATIC_STORAGE_SIZE - sizeof(size) - sizeof(capacity) - sizeof(large_data);

private:
    char small_data[MAX_SMALL_STRING_SIZE]; /// Including the terminating zero.

public:
    bool has() const
    {
        return size >= 0;
    }

    const char * getData() const
    {
        return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data;
    }

    StringRef getStringRef() const
    {
        return StringRef(getData(), size);
    }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            static_cast<ColumnString &>(to).insertDataWithTerminatingZero(getData(), size);
        else
            static_cast<ColumnString &>(to).insertDefault();
    }

    void write(WriteBuffer & buf, const IDataType & /*data_type*/) const
    {
        writeBinary(size, buf);
        if (has())
            buf.write(getData(), size);
    }

    void read(ReadBuffer & buf, const IDataType & /*data_type*/, Arena * arena)
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
        changeImpl(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num), arena);
    }

    void change(const Self & to, Arena * arena)
    {
        changeImpl(to.getStringRef(), arena);
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
        if (!has() || static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num) < getStringRef())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || to.getStringRef() < getStringRef()))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has() || static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num) > getStringRef())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || to.getStringRef() > getStringRef()))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool isEqualTo(const Self & to) const
    {
        return has() && to.getStringRef() == getStringRef();
    }

    bool isEqualTo(const IColumn & column, size_t row_num) const
    {
        return has() && static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num) == getStringRef();
    }
};

static_assert(
    sizeof(SingleValueDataString) == SingleValueDataString::AUTOMATIC_STORAGE_SIZE,
    "Incorrect size of SingleValueDataString struct");


/// For any other value types.
struct SingleValueDataGeneric
{
private:
    using Self = SingleValueDataGeneric;

    Field value;

public:
    bool has() const
    {
        return !value.isNull();
    }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            to.insert(value);
        else
            to.insertDefault();
    }

    void write(WriteBuffer & buf, const IDataType & data_type) const
    {
        if (!value.isNull())
        {
            writeBinary(true, buf);
            data_type.serializeBinary(value, buf);
        }
        else
            writeBinary(false, buf);
    }

    void read(ReadBuffer & buf, const IDataType & data_type, Arena *)
    {
        bool is_not_null;
        readBinary(is_not_null, buf);

        if (is_not_null)
            data_type.deserializeBinary(value, buf);
    }

    void change(const IColumn & column, size_t row_num, Arena *)
    {
        column.get(row_num, value);
    }

    void change(const Self & to, Arena *)
    {
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
        if (!has())
        {
            change(column, row_num, arena);
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
        if (!has())
        {
            change(column, row_num, arena);
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

    bool isEqualTo(const IColumn & column, size_t row_num) const
    {
        return has() && value == column[row_num];
    }

    bool isEqualTo(const Self & to) const
    {
        return has() && to.value == value;
    }
};


/** What is the difference between the aggregate functions min, max, any, anyLast
  *  (the condition that the stored value is replaced by a new one,
  *   as well as, of course, the name).
  */

template <typename Data>
struct AggregateFunctionMinData : Data
{
    using Self = AggregateFunctionMinData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeIfLess(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena)                        { return this->changeIfLess(to, arena); }

    static const char * name() { return "min"; }
};

template <typename Data>
struct AggregateFunctionMaxData : Data
{
    using Self = AggregateFunctionMaxData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeIfGreater(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena)                        { return this->changeIfGreater(to, arena); }

    static const char * name() { return "max"; }
};

template <typename Data>
struct AggregateFunctionAnyData : Data
{
    using Self = AggregateFunctionAnyData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeFirstTime(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena)                        { return this->changeFirstTime(to, arena); }

    static const char * name() { return "any"; }
};

template <typename Data>
struct AggregateFunctionAnyLastData : Data
{
    using Self = AggregateFunctionAnyLastData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeEveryTime(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena)                        { return this->changeEveryTime(to, arena); }

    static const char * name() { return "anyLast"; }
};


/** Implement 'heavy hitters' algorithm.
  * Selects most frequent value if its frequency is more than 50% in each thread of execution.
  * Otherwise, selects some arbitary value.
  * http://www.cs.umd.edu/~samir/498/karp.pdf
  */
template <typename Data>
struct AggregateFunctionAnyHeavyData : Data
{
    size_t counter = 0;

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

    void write(WriteBuffer & buf, const IDataType & data_type) const
    {
        Data::write(buf, data_type);
        writeBinary(counter, buf);
    }

    void read(ReadBuffer & buf, const IDataType & data_type, Arena * arena)
    {
        Data::read(buf, data_type, arena);
        readBinary(counter, buf);
    }

    static const char * name() { return "anyHeavy"; }
};


template <typename Data>
class AggregateFunctionsSingleValue final : public IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>>
{
private:
    DataTypePtr type;

public:
    AggregateFunctionsSingleValue(const DataTypePtr & type) : type(type)
    {
        if (StringRef(Data::name()) == StringRef("min")
            || StringRef(Data::name()) == StringRef("max"))
        {
            if (!type->isComparable())
                throw Exception("Illegal type " + type->getName() + " of argument of aggregate function " + getName()
                    + " because the values of that data type are not comparable", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override
    {
        return type;
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).changeIfBetter(*columns[0], row_num, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).changeIfBetter(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf, *type.get());
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).read(buf, *type.get(), arena);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        this->data(place).insertResultInto(to);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
