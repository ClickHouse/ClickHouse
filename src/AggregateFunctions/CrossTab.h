#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/HashTable/HashMap.h>
#include <AggregateFunctions/UniqVariadicHash.h>


/** Aggregate function that calculates statistics on top of cross-tab:
  * - histogram of every argument and every pair of elements.
  * These statistics include:
  * - Cramer's V;
  * - Theil's U;
  * - contingency coefficient;
  * It can be interpreted as interdependency coefficient between arguments;
  * or non-parametric correlation coefficient.
  */
namespace DB
{

struct CrossTabData
{
    /// Total count.
    UInt64 count = 0;

    /// Count of every value of the first and second argument (values are pre-hashed).
    /// Note: non-cryptographic 64bit hash is used, it means that the calculation is approximate.
    HashMapWithStackMemory<UInt64, UInt64, TrivialHash, 4> count_a;
    HashMapWithStackMemory<UInt64, UInt64, TrivialHash, 4> count_b;

    /// Count of every pair of values. We pack two hashes into UInt128.
    HashMapWithStackMemory<UInt128, UInt64, UInt128Hash, 4> count_ab;


    void add(UInt64 hash1, UInt64 hash2)
    {
        ++count;
        ++count_a[hash1];
        ++count_b[hash2];

        UInt128 hash_pair{hash1, hash2};
        ++count_ab[hash_pair];
    }

    void merge(const CrossTabData & other)
    {
        count += other.count;
        for (const auto & [key, value] : other.count_a)
            count_a[key] += value;
        for (const auto & [key, value] : other.count_b)
            count_b[key] += value;
        for (const auto & [key, value] : other.count_ab)
            count_ab[key] += value;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(count, buf);
        count_a.write(buf);
        count_b.write(buf);
        count_ab.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(count, buf);
        count_a.read(buf);
        count_b.read(buf);
        count_ab.read(buf);
    }
};


template <typename Data>
class AggregateFunctionCrossTab : public IAggregateFunctionDataHelper<Data, AggregateFunctionCrossTab<Data>>
{
public:
    AggregateFunctionCrossTab(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionCrossTab<Data>>({arguments}, {})
    {
    }

    String getName() const override
    {
        return Data::getName();
    }

    bool allocatesMemoryInArena() const override
    {
        return false;
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    void add(
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        size_t row_num,
        Arena *) const override
    {
        UInt64 hash1 = UniqVariadicHash<false, false>::apply(1, &columns[0], row_num);
        UInt64 hash2 = UniqVariadicHash<false, false>::apply(1, &columns[1], row_num);

        this->data(place).add(hash1, hash2);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        Float64 result = this->data(place).getResult();
        auto & column = static_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(result);
    }
};

}
