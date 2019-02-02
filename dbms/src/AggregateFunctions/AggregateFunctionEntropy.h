#pragma once

#include <AggregateFunctions/FactoryHelpers.h>

/// These must be exposed in header for the purpose of dynamic compilation.
#include <AggregateFunctions/QuantileReservoirSampler.h>
#include <AggregateFunctions/QuantileReservoirSamplerDeterministic.h>
#include <AggregateFunctions/QuantileExact.h>
#include <AggregateFunctions/QuantileExactWeighted.h>
#include <AggregateFunctions/QuantileTiming.h>
#include <AggregateFunctions/QuantileTDigest.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/QuantilesCommon.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <type_traits>

#include <cmath>
#include <AggregateFunctions/UniqVariadicHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** Calculates Shannon Entropy, using HashMap and computing empirical distribution function
  */
template <typename Value>
struct EntropyData
{
    using Weight = UInt64;
    using Map = HashMap <
    Value, Weight,
    HashCRC32<Value>,
    HashTableGrower<4>,
    HashTableAllocatorWithStackMemory<sizeof(std::pair<Value, Weight>) * (1 << 3)>
    >;

    Map map;

    void add(const Value &x)
    {
        if (!isNaN(x))
            ++map[x];
    }

    void add(const Value &x, const Weight &weight)
    {
        if (!isNaN(x))
            map[x] += weight;
    }

    void merge(const EntropyData &rhs)
    {
        for (const auto &pair : rhs.map)
            map[pair.first] += pair.second;
    }

    void serialize(WriteBuffer &buf) const
    {
        map.write(buf);
    }

    void deserialize(ReadBuffer &buf)
    {
        typename Map::Reader reader(buf);
        while (reader.next())
        {
            const auto &pair = reader.get();
            map[pair.first] = pair.second;
        }
    }

    Float64 get() const
    {
        Float64 ShannonEntropy = 0;
        UInt64 TotalValue = 0;
        for (const auto & pair : map)
        {
            TotalValue += pair.second;
        }
        Float64 cur_proba;
        Float64 log2e = 1 / std::log(2);
        for (const auto & pair : map)
        {
            cur_proba = Float64(pair.second) / TotalValue;
            ShannonEntropy -= cur_proba * std::log(cur_proba) * log2e;
        }

        return ShannonEntropy;
    }
};

template <typename Value, typename UserValue>
class AggregateFunctionEntropy final : public IAggregateFunctionDataHelper<EntropyData<Value>,
        AggregateFunctionEntropy<Value, UserValue>>
{
public:
    AggregateFunctionEntropy()
    {}

    String getName() const override { return "entropy"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (!std::is_same_v<UInt128, Value>)
        {
            const auto &column = static_cast<const ColumnVector <UserValue> &>(*columns[0]);
            this->data(place).add(column.getData()[row_num]);
        }
        else
        {
            this->data(place).add(UniqVariadicHash<true, false>::apply(1, columns, row_num));

        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto &column = dynamic_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(this->data(place).get());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

};

}
