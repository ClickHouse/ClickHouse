#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/HashSet.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

/// State: a hash set of SipHash128 digests plus a "found duplicate" flag.
struct AggregateFunctionHasNoDuplicatesData
{
    using Set = HashSet<UInt128, UInt128TrivialHash>;
    Set set;
    bool has_duplicate = false;
};

/// Internal aggregate function `__hasNoDuplicates(*)`.
/// Maintains a hash set of row hashes.  On each `add`, it inserts the
/// SipHash128 of all columns; if the key already exists, it sets a flag
/// and signals early termination via the `IAggregateFunction` interface.
/// Returns 1 (UInt8) when all rows are distinct, 0 otherwise.
class AggregateFunctionHasNoDuplicates final
    : public IAggregateFunctionDataHelper<AggregateFunctionHasNoDuplicatesData, AggregateFunctionHasNoDuplicates>
{
public:
    explicit AggregateFunctionHasNoDuplicates(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionHasNoDuplicatesData, AggregateFunctionHasNoDuplicates>(
              argument_types_, {}, std::make_shared<DataTypeUInt8>())
    {
    }

    String getName() const override { return "__hasNoDuplicates"; }

    bool allocatesMemoryInArena() const override { return false; }

    bool isEarlyTerminable() const override { return true; }

    bool shouldTerminateEarly(ConstAggregateDataPtr __restrict place) const override
    {
        return data(place).has_duplicate;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = data(place);
        if (state.has_duplicate)
            return;

        SipHash hash;
        for (size_t i = 0; i < argument_types.size(); ++i)
            columns[i]->updateHashWithValue(row_num, hash);

        const auto key = hash.get128();
        bool inserted;
        state.set.emplace(key, inserted);
        if (!inserted)
            state.has_duplicate = true;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & dst = data(place);
        const auto & src = data(rhs);

        if (src.has_duplicate)
        {
            dst.has_duplicate = true;
            return;
        }

        for (const auto & elem : src.set)
        {
            if (dst.has_duplicate)
                return;

            bool inserted;
            dst.set.emplace(elem.getValue(), inserted);
            if (!inserted)
                dst.has_duplicate = true;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = data(place);
        writeBinaryLittleEndian(state.has_duplicate, buf);
        writeBinaryLittleEndian(state.set.size(), buf);
        for (const auto & elem : state.set)
            writeBinaryLittleEndian(elem.getValue(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & state = data(place);
        readBinaryLittleEndian(state.has_duplicate, buf);
        size_t size;
        readBinaryLittleEndian(size, buf);
        state.set.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            UInt128 key;
            readBinaryLittleEndian(key, buf);
            state.set.insert(key);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt8 &>(to).getData().push_back(data(place).has_duplicate ? 0 : 1);
    }
};

}
