#pragma once

#include <optional>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <boost/preprocessor/cat.hpp>
#include "Common/HashTable/HashSet.h"
#include <Common/HashTable/HashMap.h>
#include "Columns/ColumnNullable.h"
#include "DataTypes/DataTypesNumber.h"
#include "FactoryHelpers.h"
#include "Helpers.h"
#include "IAggregateFunction.h"
#include "base/types.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

template <typename Data, typename UnderlyingT, size_t ExpectedParameters = 0>
class GraphOperation : public IAggregateFunctionDataHelper<Data, GraphOperation<Data, UnderlyingT, ExpectedParameters>>
{
public:
    using IAggregateFunctionDataHelper<Data, GraphOperation<Data, UnderlyingT, ExpectedParameters>>::data;
    using Vertex = typename Data::Vertex;
    using VertexSet = typename Data::VertexSet;
    using VertexMap = typename Data::VertexMap;
    using GraphType = typename Data::GraphType;
    static constexpr size_t kExpectedParameters = ExpectedParameters;

    GraphOperation(const DataTypePtr & data_type_, const Array & parameters_)
        : IAggregateFunctionDataHelper<Data, GraphOperation>({data_type_}, parameters_)
    {
        for (const auto & parameter : parameters_)
            if (parameter.isNull())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can't pass NULL to {} parameters", getName());
    }

    String getName() const final { return UnderlyingT::name; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const final
    {
        data(place).add(columns, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const final { data(place).merge(data(rhs)); }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const final
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const final
    {
        data(place).deserialize(buf, arena);
    }

    Vertex getVertexFromField(const Field & field, Arena * arena) const
    {
        if constexpr (std::is_same_v<Vertex, StringRef>)
        {
            const char * begin = nullptr;
            return this->argument_types[0]->createColumnConst(1, field)->serializeValueIntoArena(0, *arena, begin);
        }
        else
        {
            return field.get<Vertex>();
        }
    }

    decltype(auto) calculateOperation(ConstAggregateDataPtr __restrict place, Arena * arena) const
    {
        return static_cast<const UnderlyingT &>(*this).calculateOperation(place, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const final
    {
        auto result = calculateOperation(place, arena);
        if constexpr (requires { result == std::nullopt; })
        {
            using ResultT = typename decltype(result)::value_type;
            auto & column = assert_cast<ColumnNullable &>(to);
            assert_cast<ColumnVector<ResultT> &>(column.getNestedColumn()).getData().push_back(result.value_or(ResultT{}));
            column.getNullMapData().push_back(!result.has_value());
        }
        else
        {
            assert_cast<ColumnVector<decltype(result)> &>(to).getData().push_back(std::move(result));
        }
    }

    constexpr bool allocatesMemoryInArena() const final { return Data::allocatesMemoryInArena(); }
};

}

#define INHERIT_GRAPH_OPERATION_USINGS(...) \
    using __VA_ARGS__::GraphOperation; \
    using __VA_ARGS__::data; \
    using __VA_ARGS__::getName; \
    using __VA_ARGS__::getVertexFromField; \
    using __VA_ARGS__::parameters; \
    using Vertex = typename __VA_ARGS__::Vertex; \
    using VertexSet = typename __VA_ARGS__::VertexSet; \
    using VertexMap = typename __VA_ARGS__::VertexMap; \
    using GraphType = typename __VA_ARGS__::GraphType;

#define INSTANTIATE_GRAPH_OPERATION_FACTORY(operation) \
    AggregateFunctionPtr BOOST_PP_CAT(createGraphOperation, operation)( \
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *) \
    { \
        assertBinary(name, argument_types); \
        if (operation<StringRef>::kExpectedParameters != 0) \
        { \
            if (parameters.size() != operation<StringRef>::kExpectedParameters) \
                throw Exception( \
                    "Aggregate function " + name + " requires " + std::to_string(operation<StringRef>::kExpectedParameters) \
                        + " parameters", \
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH); \
        } \
        else \
            assertNoParameters(name, parameters); \
        if (!argument_types[0]->equals(*argument_types[1])) \
            throw Exception( \
                "Parameters for aggregate function " + name + " should be of equal types. Got " + argument_types[0]->getName() + " and " \
                    + argument_types[1]->getName(), \
                ErrorCodes::BAD_ARGUMENTS); \
        AggregateFunctionPtr result{createWithNumericType<operation>(*argument_types[0], argument_types[0], parameters)}; \
        if (!result) \
            result.reset(new operation<StringRef>(argument_types[0], parameters)); \
        return result; \
    }

#define INSTANTIATE_UNARY_GRAPH_OPERATION(data_type, operation) template class operation<data_type>;

#define INSTANTIATE_GRAPH_OPERATION(operation) \
    INSTANTIATE_UNARY_GRAPH_OPERATION(StringRef, operation) \
    FOR_NUMERIC_TYPES(INSTANTIATE_UNARY_GRAPH_OPERATION, operation) \
    INSTANTIATE_GRAPH_OPERATION_FACTORY(operation)
