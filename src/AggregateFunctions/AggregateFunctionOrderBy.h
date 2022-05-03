#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/assert_cast.h>
#include <vector>
#include <boost/algorithm/string.hpp>

#include <cctype>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class AggregateFunctionOrderBy final : public IAggregateFunctionHelper<AggregateFunctionOrderBy>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;

    /*
     * Input: sort parameters. For example "ASC, DESC, ASC"
     * for first, second and third column of aggregate function input
     *
     * Output: vector of sort directions (true is upper, false is lower)
     */
    std::vector<int8_t> parseStringToVector(String & s) const {
        std::vector<int8_t> columnToDirection;
        String curDirection;
        for (auto c : s)
        {
            if (c == ',')
            {
                int8_t direction = parseColumnDirection(curDirection);
                columnToDirection.push_back(direction);
                curDirection = "";
                continue;
            }
            else
            {
                curDirection += c;
            }
        }
        int8_t direction = parseColumnDirection(curDirection);
        columnToDirection.push_back(direction);
        return columnToDirection;
    }

    int8_t parseColumnDirection(String & s) const {
        boost::trim(s);
        std::for_each(s.begin(), s.end(), [](char & c) {
            c = ::tolower(c);
        });
        if (s == "asc") {
            return 1;
        }
        else if (s == "desc")
        {
            return -1;
        }
        else if (s.empty())
        {
            return 0;
        }
        else
        {
            throw Exception("Cannot parse orderBy on \"" + s + "\"", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

public:
    AggregateFunctionOrderBy(AggregateFunctionPtr nested_, const DataTypes & arguments, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionOrderBy>(arguments, params_)
        , nested_func(nested_), num_arguments(arguments.size())
    {
        if (arguments.size() == 0)
            throw Exception(
                "Aggregate function " + getName() + " require at least one parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        if (arguments[num_arguments - 1]->getName() != "String")
            throw Exception(
                "Last parameter for aggregate function " + getName() + " must be String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override
    {
        return nested_func->getName() + "OrderBy";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    void create(AggregateDataPtr place) const override
    {
        nested_func->create(place);
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        nested_func->destroy(place);
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return nested_func->sizeOfData();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
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

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Field field;
        columns[num_arguments - 1]->get(0, field);

        auto & orderByQuery = field.get<std::string>();
        std::vector<int8_t> columnToDirection;

        if (orderByQuery.size() != 0)
            columnToDirection = parseStringToVector(orderByQuery);

        Columns newColumns;
        const IColumn * ans[num_arguments - 1];
        for (size_t k = 0; k < num_arguments - 1; k++)
        {
            if (columnToDirection[k] == 0) {
                ans[k] = columns[k];
                continue;
            }
            else
            {
                IColumn::PermutationSortDirection direction;
                if (columnToDirection[k] == 1)
                {
                    direction = IColumn::PermutationSortDirection::Ascending;
                }
                else
                {
                    direction = IColumn::PermutationSortDirection::Descending;
                }

                ColumnArray::Permutation permutation;
                columns[k]->getPermutation(direction, IColumn::PermutationSortStability::Stable, 0, 1, permutation);
                ColumnPtr sortedColumnPtr = columns[k]->permute(permutation, 0);
                newColumns.push_back(sortedColumnPtr);

                ans[k] = newColumns[k].get();
            }
        }
        nested_func->add(place, ans, row_num, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(place, to, arena);
    }
};
}
