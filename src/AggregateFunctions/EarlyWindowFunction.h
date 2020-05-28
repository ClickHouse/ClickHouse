#pragma once

#include <Columns/ColumnString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/AggregateFunctionNull.h>

namespace DB
{

template <typename T, typename D>
struct EarlyWindowFunctionLastGeneralData
{
    T time = 0;
    D value;

    void add(const T timestamp, const IColumn *column, const size_t row_num)
    {
        if (time <= timestamp)
        {
            time = timestamp;
            value = assert_cast<const ColumnVector<D> *>(column)->getData()[row_num];
        }
    }

    void merge(const EarlyWindowFunctionLastGeneralData & other)
    {
        if (time <= other.time)
        {
            time = other.time;
            value = other.value;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(time, buf);
        writeBinary(value, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(time, buf);
        readBinary(value, buf);
    }

    static String getName()
    {
        return "windowLast";
    }
};

template <typename T>
struct EarlyWindowFunctionLastStringData
{
    T time = 0;
    String value;

    void add(const T timestamp, const IColumn *column, const size_t row_num)
    {
        if (time <= timestamp)
        {
            time = timestamp;
            value = assert_cast<const ColumnString *>(column)->getDataAt(row_num).data;
        }
    }

    void merge(const EarlyWindowFunctionLastStringData & other)
    {
        if (time <= other.time)
        {
            time = other.time;
            value = other.value;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(time, buf);
        writeBinary(value, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(time, buf);
        readBinary(value, buf);
    }

    static String getName()
    {
        return "windowLast";
    }
};

template <typename T, typename D>
struct EarlyWindowFunctionFirstGeneralData
{
    T time = 0;
    D value;
    bool init = false;

    void add(const T timestamp, const IColumn *column, const size_t row_num)
    {
        if (!init || time > timestamp)
        {
            time = timestamp;
            value = assert_cast<const ColumnVector<D> *>(column)->getData()[row_num];
            init = true;
        }
    }

    void merge(const EarlyWindowFunctionFirstGeneralData & other)
    {
        if (time <= other.time)
        {
            time = other.time;
            value = other.value;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(time, buf);
        writeBinary(value, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(time, buf);
        readBinary(value, buf);
    }

    static String getName()
    {
        return "windowFirst";
    }
};

template <typename T>
struct EarlyWindowFunctionFirstStringData
{
    T time = 0;
    String value;
    bool init = false;

    void add(const T timestamp, const IColumn *column, const size_t row_num)
    {
        if (!init || time > timestamp)
        {
            time = timestamp;
            value = assert_cast<const ColumnString *>(column)->getDataAt(row_num).data;
            init = true;
        }
    }

    void merge(const EarlyWindowFunctionFirstStringData & other)
    {
        if (time <= other.time)
        {
            time = other.time;
            value = other.value;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(time, buf);
        writeBinary(value, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(time, buf);
        readBinary(value, buf);
    }

    static String getName()
    {
        return "windowFirst";
    }
};

template <typename T, typename Data>
class EarlyWindowFunction final
    : public IAggregateFunctionDataHelper<Data, EarlyWindowFunction<T, Data>>
{
private:
    DataTypePtr & data_type;

public:
    String getName() const override
    {
        return Data::getName();
    }

    EarlyWindowFunction(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, EarlyWindowFunction<T, Data>>(arguments, params)
        , data_type(this->argument_types[1])
    {
    }

    DataTypePtr getReturnType() const override
    {
        return data_type;
    }

    AggregateFunctionPtr getOwnNullAdapter(const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array & params) const override
    {
        return std::make_shared<AggregateFunctionNullVariadic<false, false>>(nested_function, arguments, params);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        const auto timestamp = assert_cast<const ColumnVector<T> *>(columns[0])->getData()[row_num];

        this->data(place).add(timestamp, columns[1], row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        to.insert(this->data(place).value);
    }
};

}
