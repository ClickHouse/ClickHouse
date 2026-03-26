#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/HashSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpersArena.h>


namespace DB
{

/// State: an exact set of serialized row keys plus a "found duplicate" flag.
struct AggregateFunctionHasNoDuplicatesData
{
    using Set = HashSetWithSavedHashWithStackMemory<std::string_view, StringViewHash, 4>;
    Set set;
    bool has_duplicate = false;
};

/// Aggregate function `allUnique(*)`.
/// Maintains a set of serialized row representations.  On each `add`, it
/// serializes all column values into the Arena and tries to insert into
/// the set; if the key already exists (exact match), it sets a flag and
/// signals early termination via the `IAggregateFunction` interface.
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

    String getName() const override { return "allUnique"; }

    bool allocatesMemoryInArena() const override { return true; }

    bool isEarlyTerminable() const override { return true; }

    bool shouldTerminateEarly(ConstAggregateDataPtr __restrict place) const override
    {
        return data(place).has_duplicate;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & state = data(place);
        if (state.has_duplicate)
            return;

        /// Per SQL standard, rows containing NULL in any column are never
        /// considered duplicates, so we skip them entirely.
        for (size_t i = 0; i < argument_types.size(); ++i)
            if (columns[i]->isNullAt(row_num))
                return;

        /// Serialize all column values into a contiguous Arena region
        /// to form an exact composite key.
        const char * begin = nullptr;
        std::string_view value;
        for (size_t i = 0; i < argument_types.size(); ++i)
        {
            auto settings = IColumn::SerializationSettings::createForAggregationState();
            auto cur_ref = columns[i]->serializeValueIntoArena(row_num, *arena, begin, &settings);
            value = std::string_view{cur_ref.data() - value.size(), value.size() + cur_ref.size()};
        }

        Set::LookupResult it;
        bool inserted;
        state.set.emplace(SerializedKeyHolder{value, *arena}, it, inserted);
        if (!inserted)
            state.has_duplicate = true;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
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

            Set::LookupResult it;
            bool inserted;
            dst.set.emplace(ArenaKeyHolder{elem.getValue(), *arena}, it, inserted);
            if (!inserted)
                dst.has_duplicate = true;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = data(place);
        writeBinaryLittleEndian(state.has_duplicate, buf);
        writeVarUInt(state.set.size(), buf);
        for (const auto & elem : state.set)
            writeStringBinary(elem.getValue(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & state = data(place);
        readBinaryLittleEndian(state.has_duplicate, buf);
        size_t size;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
            state.set.insert(readStringBinaryInto(*arena, buf));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt8 &>(to).getData().push_back(static_cast<UInt8>(data(place).has_duplicate ? 0 : 1));
    }

private:
    using Set = AggregateFunctionHasNoDuplicatesData::Set;
};

}
