#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/HashTable/HashMap.h>
#include "AggregateFunctions/FactoryHelpers.h"
#include "DataTypes/DataTypesNumber.h"
#include "base/types.h"
#include "AggregateFunctionGraphFactory.h"


#define AGGREGATE_FUNCTION_GRAPH_MAX_SIZE 0xFFFFFF

namespace DB
{

struct DirectionalGraphGenericData
{
    HashMap<StringRef, std::vector<StringRef>> graph{};
    size_t edges_count = 0;

    void merge(const DirectionalGraphGenericData & rhs);
    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf, Arena* arena);
    void add(const IColumn ** columns, size_t row_num, Arena * arena);
    bool isTree() const;
};

struct BidirectionalGraphGenericData : DirectionalGraphGenericData {
    void add(const IColumn ** columns, size_t row_num, Arena * arena);
    bool isTree() const;
    size_t componentsCount() const;
};

template<typename Data, typename UnderlyingT, size_t ExpectedParameters = 0>
class GraphOperationGeneral
    : public IAggregateFunctionDataHelper<Data, GraphOperationGeneral<Data, UnderlyingT, ExpectedParameters>>
{
public:
    static constexpr size_t kExpectedParameters = ExpectedParameters;

    GraphOperationGeneral(const DataTypePtr & data_type_, const Array & parameters_)
        : IAggregateFunctionDataHelper<Data, GraphOperationGeneral>(
            {data_type_}, parameters_) {
    }

    String getName() const final { return UnderlyingT::name; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const final
    {
        this->data(place).add(columns, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const final
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const final
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const final
    {
        this->data(place).deserialize(buf, arena);
    }

    StringRef serializeFieldToArena(const Field& field, Arena* arena) const {
        const char* begin = nullptr;
        return this->argument_types[0]->createColumnConst(1, field)->serializeValueIntoArena(0, *arena, begin);
    }

    decltype(auto) calculateOperation(ConstAggregateDataPtr __restrict place, Arena* arena) const {
      return static_cast<const UnderlyingT&>(*this).calculateOperation(place, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const final
    {
        auto result = calculateOperation(place, arena);
        assert_cast<ColumnVector<decltype(result)>&>(to).getData().push_back(std::move(result));
    }


    bool allocatesMemoryInArena() const final { return true; }
};

}
