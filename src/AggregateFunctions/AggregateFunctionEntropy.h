#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/NaNUtils.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>

#include <cmath>


namespace DB
{

/** Calculates Shannon Entropy, using HashMap and computing empirical distribution function.
  * Entropy is measured in bits (base-2 logarithm is used).
  */
template <typename Value>
struct EntropyData
{
    using Weight = UInt64;

    using HashingMap = HASH_TABLE_WITH_STACK_MEMORY(HashMap, Value, Weight,
        HashCRC32<Value>, HashTableGrower<4>);

    /// For the case of pre-hashed values.
    using TrivialMap = HASH_TABLE_WITH_STACK_MEMORY(HashMap, Value, Weight,
        UInt128TrivialHash, HashTableGrower<4>);

    using Map = std::conditional_t<std::is_same_v<UInt128, Value>, TrivialMap, HashingMap>;

    Map map;

    void add(const Value & x)
    {
        if (!isNaN(x))
            ++map[x];
    }

    void add(const Value & x, const Weight & weight)
    {
        if (!isNaN(x))
            map[x] += weight;
    }

    void merge(const EntropyData & rhs)
    {
        for (const auto & pair : rhs.map)
            map[pair.getKey()] += pair.getMapped();
    }

    void serialize(WriteBuffer & buf) const
    {
        map.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        typename Map::Reader reader(buf);
        while (reader.next())
        {
            const auto & pair = reader.get();
            map[pair.first] = pair.second;
        }
    }

    Float64 get() const
    {
        UInt64 total_value = 0;
        for (const auto & pair : map)
            total_value += pair.getMapped();

        Float64 shannon_entropy = 0;
        for (const auto & pair : map)
        {
            Float64 frequency = Float64(pair.getMapped()) / total_value;
            shannon_entropy -= frequency * log2(frequency);
        }

        return shannon_entropy;
    }
};


template <typename Value>
class AggregateFunctionEntropy final : public IAggregateFunctionDataHelper<EntropyData<Value>, AggregateFunctionEntropy<Value>>
{
private:
    size_t num_args;

public:
    AggregateFunctionEntropy(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<EntropyData<Value>, AggregateFunctionEntropy<Value>>(argument_types_, {})
        , num_args(argument_types_.size())
    {
    }

    String getName() const override { return "entropy"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (!std::is_same_v<UInt128, Value>)
        {
            /// Here we manage only with numerical types
            const auto & column = assert_cast<const ColumnVector <Value> &>(*columns[0]);
            this->data(place).add(column.getData()[row_num]);
        }
        else
        {
            this->data(place).add(UniqVariadicHash<true, false>::apply(num_args, columns, row_num));
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

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        auto & column = assert_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(this->data(place).get());
    }
};

}
