#pragma once

#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/WriteHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class AggregateFunctionBy final : public IAggregateFunctionHelper<AggregateFunctionBy>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;

public:
    AggregateFunctionBy(AggregateFunctionPtr nested_, const DataTypes & arguments, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionBy>(arguments, params_)
        , nested_func(nested_), num_arguments(arguments.size())
    {
        assert(parameters == nested_func->getParameters());
//        for (const auto & type : arguments)
//            printf("%s\n", type.get()->getName().c_str());
//            if (!isArray(type))
//                throw Exception("All arguments for aggregate function " + getName() + " must be arrays", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override
    {
        return nested_func->getName() + "By";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_func->create(place);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_func->destroy(place);
    }

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        // todo implement
        const IColumn * ans[num_arguments - 1];
        for (size_t k = 0; k < num_arguments - 1; k++)
        {
            ans[k] = columns[k];
//            printf("%s\n", columns[k]->getName().c_str());
            for (size_t i = 0; i < columns[k]->size(); i++) {
                Field f;
                columns[k]->get(i, f);
                printf("%s, ", toString(f).c_str());
            }
//            printf("\n\n\n");

        }
        nested_func->add(place, ans, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        printf("we are in merge funk\n");
        nested_func->merge(place, rhs, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_func->serialize(place, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_func->deserialize(place, buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(place, to, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
