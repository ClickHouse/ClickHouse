#pragma once

#include <Core/FieldVisitors.h>
#include <AggregateFunctions/IUnaryAggregateFunction.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <Common/typeid_cast.h>

namespace DB
{


/** Counts the number of unique values up to no more than specified in the parameter.
  *
  * Example: uniqUpTo(3)(UserID)
  * - will count the number of unique visitors, return 1, 2, 3 or 4 if visitors > = 4.
  *
  * For strings, a non-cryptographic hash function is used, due to which the calculation may be a bit inaccurate.
  */

template <typename T>
struct __attribute__((__packed__)) AggregateFunctionUniqUpToData
{
/** If count == threshold + 1 - this means that it is "overflowed" (values greater than threshold).
  * In this case (for example, after calling the merge function), the `data` array does not necessarily contain the initialized values
  * - example: combine a state in which there are few values, with another state that has overflowed;
  *   then set count to `threshold + 1`, and values from another state are not copied.
  */
    UInt8 count = 0;

    T data[0];


    size_t size() const
    {
        return count;
    }

    /// threshold - for how many elements there is room in a `data`.
    void insert(T x, UInt8 threshold)
    {
        /// The state is already full - nothing needs to be done.
        if (count > threshold)
            return;

        /// Linear search for the matching element.
        for (size_t i = 0; i < count; ++i)
            if (data[i] == x)
                return;

        /// Did not find the matching element. If there is room for one more element, insert it.
        if (count < threshold)
            data[count] = x;

        /// After increasing count, the state may be overflowed.
        ++count;
    }

    void merge(const AggregateFunctionUniqUpToData<T> & rhs, UInt8 threshold)
    {
        if (count > threshold)
            return;

        if (rhs.count > threshold)
        {
        /// If `rhs` is overflowed, then set `count` too also overflowed for the current state.
            count = rhs.count;
            return;
        }

        for (size_t i = 0; i < rhs.count; ++i)
            insert(rhs.data[i], threshold);
    }

    void write(WriteBuffer & wb, UInt8 threshold) const
    {
        writeBinary(count, wb);

        /// Write values only if the state is not overflowed. Otherwise, they are not needed, and only the fact that the state is overflowed is important.
        if (count <= threshold)
            wb.write(reinterpret_cast<const char *>(&data[0]), count * sizeof(data[0]));
    }

    void read(ReadBuffer & rb, UInt8 threshold)
    {
        readBinary(count, rb);

        if (count <= threshold)
            rb.read(reinterpret_cast<char *>(&data[0]), count * sizeof(data[0]));
    }


    void addImpl(const IColumn & column, size_t row_num, UInt8 threshold)
    {
        insert(static_cast<const ColumnVector<T> &>(column).getData()[row_num], threshold);
    }
};


/// For strings, their hashes are remembered.
template <>
struct AggregateFunctionUniqUpToData<String> : AggregateFunctionUniqUpToData<UInt64>
{
    void addImpl(const IColumn & column, size_t row_num, UInt8 threshold)
    {
        /// Keep in mind that calculations are approximate.
        StringRef value = column.getDataAt(row_num);
        insert(CityHash_v1_0_2::CityHash64(value.data, value.size), threshold);
    }
};


constexpr UInt8 uniq_upto_max_threshold = 100;

template <typename T>
class AggregateFunctionUniqUpTo final : public IUnaryAggregateFunction<AggregateFunctionUniqUpToData<T>, AggregateFunctionUniqUpTo<T>>
{
private:
    UInt8 threshold = 5;    /// Default value if the parameter is not specified.

public:
    size_t sizeOfData() const override
    {
        return sizeof(AggregateFunctionUniqUpToData<T>) + sizeof(T) * threshold;
    }

    String getName() const override { return "uniqUpTo"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void setArgument(const DataTypePtr & argument)
    {
    }

    void setParameters(const Array & params) override
    {
        if (params.size() != 1)
            throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 threshold_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (threshold_param > uniq_upto_max_threshold)
            throw Exception("Too large parameter for aggregate function " + getName() + ". Maximum: " + toString(uniq_upto_max_threshold),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = threshold_param;
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        this->data(place).addImpl(column, row_num, threshold);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), threshold);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf, threshold);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf, threshold);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).size());
    }
};


/** For multiple arguments. To compute, hashes them.
  * You can pass multiple arguments as is; You can also pass one argument - a tuple.
  * But (for the possibility of effective implementation), you can not pass several arguments, among which there are tuples.
  */
template <bool argument_is_tuple>
class AggregateFunctionUniqUpToVariadic final : public IAggregateFunctionHelper<AggregateFunctionUniqUpToData<UInt64>>
{
private:
    size_t num_args = 0;
    UInt8 threshold = 5;    /// Default value if the parameter is not specified.

public:
    size_t sizeOfData() const override
    {
        return sizeof(AggregateFunctionUniqUpToData<UInt64>) + sizeof(UInt64) * threshold;
    }

    String getName() const override { return "uniqUpTo"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void setArguments(const DataTypes & arguments) override
    {
        if (argument_is_tuple)
            num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
        else
            num_args = arguments.size();
    }

    void setParameters(const Array & params) override
    {
        if (params.size() != 1)
            throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 threshold_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (threshold_param > uniq_upto_max_threshold)
            throw Exception("Too large parameter for aggregate function " + getName() + ". Maximum: " + toString(uniq_upto_max_threshold),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = threshold_param;
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).insert(UniqVariadicHash<false, argument_is_tuple>::apply(num_args, columns, row_num), threshold);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), threshold);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf, threshold);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf, threshold);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).size());
    }

    static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *arena)
    {
        static_cast<const AggregateFunctionUniqUpToVariadic &>(*that).add(place, columns, row_num, arena);
    }

    IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};


}
