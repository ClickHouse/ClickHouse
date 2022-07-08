#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <base/range.h>


namespace DB
{
struct Settings;

template <typename T = UInt64>
class AggregateFunctionCategoricalIV final : public IAggregateFunctionHelper<AggregateFunctionCategoricalIV<T>>
{
private:
    size_t category_count;

public:
    AggregateFunctionCategoricalIV(const DataTypes & arguments_, const Array & params_) :
        IAggregateFunctionHelper<AggregateFunctionCategoricalIV<T>> {arguments_, params_},
        category_count {arguments_.size() - 1}
    {
        // notice: argument types has been checked before
    }

    String getName() const override
    {
        return "categoricalInformationValue";
    }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override
    {
        memset(place, 0, sizeOfData());
    }

    void destroy(AggregateDataPtr __restrict) const noexcept override
    {
        // nothing
    }

    bool hasTrivialDestructor() const override
    {
        return true;
    }

    size_t sizeOfData() const override
    {
        return sizeof(T) * (category_count + 1) * 2;
    }

    size_t alignOfData() const override
    {
        return alignof(T);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto * y_col = static_cast<const ColumnUInt8 *>(columns[category_count]);
        bool y = y_col->getData()[row_num];

        for (size_t i : collections::range(0, category_count))
        {
            const auto * x_col = static_cast<const ColumnUInt8 *>(columns[i]);
            bool x = x_col->getData()[row_num];

            if (x)
                reinterpret_cast<T *>(place)[i * 2 + size_t(y)] += 1;
        }

        reinterpret_cast<T *>(place)[category_count * 2 + size_t(y)] += 1;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        for (size_t i : collections::range(0, category_count + 1))
        {
            reinterpret_cast<T *>(place)[i * 2] += reinterpret_cast<const T *>(rhs)[i * 2];
            reinterpret_cast<T *>(place)[i * 2 + 1] += reinterpret_cast<const T *>(rhs)[i * 2 + 1];
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        buf.write(place, sizeOfData());
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        buf.read(place, sizeOfData());
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeNumber<Float64>>()
        );
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override /// NOLINT
    {
        auto & col = static_cast<ColumnArray &>(to);
        auto & data_col = static_cast<ColumnFloat64 &>(col.getData());
        auto & offset_col = static_cast<ColumnArray::ColumnOffsets &>(
            col.getOffsetsColumn()
        );

        data_col.reserve(data_col.size() + category_count);

        T sum_no = reinterpret_cast<const T *>(place)[category_count * 2];
        T sum_yes = reinterpret_cast<const T *>(place)[category_count * 2 + 1];

        Float64 rev_no = 1. / sum_no;
        Float64 rev_yes = 1. / sum_yes;

        for (size_t i : collections::range(0, category_count))
        {
            T no = reinterpret_cast<const T *>(place)[i * 2];
            T yes = reinterpret_cast<const T *>(place)[i * 2 + 1];

            data_col.insertValue((no * rev_no - yes * rev_yes) * (log(no * rev_no) - log(yes * rev_yes)));
        }

        offset_col.insertValue(data_col.size());
    }
};

}
