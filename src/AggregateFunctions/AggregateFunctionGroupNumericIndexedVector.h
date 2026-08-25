#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/assert_cast.h>

/// Include this last — see the reason inside
#include <AggregateFunctions/AggregateFunctionGroupNumericIndexedVectorData.h>

namespace DB
{

template <typename VectorImpl, typename... TArgs>
class AggregateFunctionNumericIndexedVector final : public IAggregateFunctionDataHelper<
                                                        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl>,
                                                        AggregateFunctionNumericIndexedVector<VectorImpl, TArgs...>>
{
protected:
    std::tuple<TArgs...> init_args_tuple;

public:
    using IndexType = typename VectorImpl::IndexType;
    using ValueType = typename VectorImpl::ValueType;

    template <typename... IArgs>
    explicit AggregateFunctionNumericIndexedVector(const DataTypes & types, const Array & params, IArgs &&... args)
        : IAggregateFunctionDataHelper<
              AggregateFunctionGroupNumericIndexedVectorData<VectorImpl>,
              AggregateFunctionNumericIndexedVector<VectorImpl, TArgs...>>({types}, {params}, createResultType())
        , init_args_tuple(std::tuple<TArgs...>(std::forward<IArgs>(args)...))
    {
    }

    String getName() const override { return AggregateFunctionGroupNumericIndexedVectorData<VectorImpl>::name(); }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeNumber<Float64>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & data_lhs = this->data(place);
        if (!data_lhs.init)
        {
            data_lhs.init = true;
            std::apply(
                [&data_lhs](auto &&... unpacked_args)
                { data_lhs.vector.initialize(std::forward<decltype(unpacked_args)>(unpacked_args)...); },
                init_args_tuple);
        }

        IndexType index = assert_cast<const ColumnVector<IndexType> &>(*columns[0]).getData()[row_num];
        ValueType value = assert_cast<const ColumnVector<ValueType> &>(*columns[1]).getData()[row_num];

        data_lhs.vector.addValue(index, value);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & data_lhs = this->data(place);
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & data_rhs = this->data(rhs);

        if (!data_rhs.init)
            return;

        if (!data_lhs.init)
        {
            data_lhs.init = true;
            data_lhs.vector.deepCopyFrom(data_rhs.vector);
        }
        else
        {
            data_lhs.vector.merge(data_rhs.vector);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).vector.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).init = true;
        this->data(place).vector.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<Float64> &>(to).getData().push_back(this->data(place).vector.getAllValueSum());
    }
};

}
