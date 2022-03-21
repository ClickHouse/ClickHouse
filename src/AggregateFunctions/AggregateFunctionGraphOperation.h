#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>
#include "AggregateFunctions/FactoryHelpers.h"
#include "DataTypes/DataTypesNumber.h"
#include "base/types.h"
#include "AggregateFunctionGraphFactory.h"
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>


#define AGGREGATE_FUNCTION_GRAPH_MAX_SIZE 0xFFFFFF

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

struct DirectionalGraphGenericData
{
    HashMap<StringRef, std::vector<StringRef>> graph{};
    size_t edges_count = 0;

    void merge(const DirectionalGraphGenericData & rhs) {
      edges_count += rhs.edges_count;
      if (unlikely(edges_count > AGGREGATE_FUNCTION_GRAPH_MAX_SIZE)) {
          throw Exception("Too large graph size", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
      }
      for (const auto & elem : rhs.graph) {
          auto& children = graph[elem.getKey()];
          children.insert(children.end(), elem.getMapped().begin(), elem.getMapped().end());
      }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(graph.size(), buf);
        for (const auto & elem : graph) {
            writeStringBinary(elem.getKey(), buf);
            writeVarUInt(elem.getMapped().size(), buf);
            for (StringRef child : elem.getMapped()) {
              writeStringBinary(child, buf);
            }
        }
    }

    void deserialize(ReadBuffer & buf, Arena* arena)
    {
        graph = {};
        edges_count = 0;
        size_t size;
        readVarUInt(size, buf);
        
        graph.reserve(size);
        for (size_t i = 0; i < size; ++i) {
          StringRef key = readStringBinaryInto(*arena, buf);
          size_t children_count = 0;
          readVarUInt(children_count, buf);
          edges_count += children_count;
          if (unlikely(edges_count > AGGREGATE_FUNCTION_GRAPH_MAX_SIZE)) {
            throw Exception("Too large graph size to serialize", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
          }
          auto& children = graph[key];
          children.reserve(children_count);
          for (size_t child_idx = 0; child_idx < children_count; ++child_idx) {
            children.push_back(readStringBinaryInto(*arena, buf));
          }
        }
    }

    void add(const IColumn ** columns, size_t row_num, Arena * arena)
    {
        const char * begin = nullptr;
        StringRef key = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
        StringRef value = columns[1]->serializeValueIntoArena(row_num, *arena, begin);
        graph[key].push_back(value);
        ++edges_count;
    }
};

struct BidirectionalGraphGenericData : DirectionalGraphGenericData {
    void add(const IColumn ** columns, size_t row_num, Arena * arena) {
        const char * begin = nullptr;
        StringRef key = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
        StringRef value = columns[1]->serializeValueIntoArena(row_num, *arena, begin);
        graph[key].push_back(value);
        graph[value].push_back(key);
        edges_count += 2;
    }
};

template<typename Data, typename OpType>
class GraphOperationGeneral
    : public IAggregateFunctionDataHelper<Data, GraphOperationGeneral<Data, OpType>>
{
public:
    GraphOperationGeneral(const DataTypePtr & data_type_, const Array & parameters_)
        : IAggregateFunctionDataHelper<BidirectionalGraphGenericData, GraphOperationGeneral>(
            {data_type_}, parameters_) {
    }

    String getName() const final { return OpType::name; }
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

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

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const final
    {
        assert_cast<ColumnVector<UInt64>&>(to).getData().push_back(calculateOperation(place, arena));
    }

    decltype(auto) calculateOperation(ConstAggregateDataPtr __restrict place, Arena* arena) const {
      return static_cast<const OpType&>(*this).calculateOperation(std::move(place), arena);
    }

    bool allocatesMemoryInArena() const final { return true; }
};

}
