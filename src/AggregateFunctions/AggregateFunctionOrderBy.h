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
#include <algorithm>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <cctype>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct OrderByData
{
    using Array = PODArray<PODArray<Field>>;

    Array columnsData{};
    std::string query = "";

    void initColumnData(size_t size)
    {
        columnsData.resize_fill(size);
    }

    void setQuery(std::string & str)
    {
        query = str;
    }

    void addValue(size_t i, Field & f)
    {
        columnsData[i].emplace_back(f);
    }

    void merge(const OrderByData & rhs, Arena *)
    {
        for (size_t i = 0; i < rhs.columnsData.size(); i++)
        {
            for (size_t j = 0; j < rhs.columnsData[i].size(); j++)
            {
                columnsData[i].emplace_back(rhs.columnsData[i][j]);
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(columnsData.size(), buf);
        for (size_t i = 0; i < columnsData.size(); i++)
        {
            writeVarUInt(columnsData[i].size(), buf);
            for (size_t j = 0; j < columnsData[i].size(); j++)
            {
                writeStringBinary(columnsData[i][j].dump(), buf);
            }
        }
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t arraySize, columnSize;
        readVarUInt(arraySize, buf);
        columnsData.resize_fill(arraySize);
        for (size_t i = 0; i < arraySize; i++)
        {
            readVarUInt(columnSize, buf);
            for (size_t j = 0; j < columnSize; j++)
            {
                auto s = readStringBinaryInto(*arena, buf);
                columnsData[i].emplace_back(Field(s.toView()));
            }
        }
    }

};

class AggregateFunctionOrderBy final : public IAggregateFunctionDataHelper<OrderByData, AggregateFunctionOrderBy>
{
private:
    static constexpr auto prefix_size = sizeof(OrderByData);
    AggregateFunctionPtr nested_func;
    size_t num_arguments;
    DataTypes arguments_;
    std::string query;

    /*
     * Input: sort parameters. For example "ASC, DESC, ASC"
     * for first, second and third column of aggregate function input
     *
     * Output: vector of sort directions (true is upper, false is lower)
     */
    std::vector<int8_t> parseStringToVector(const String & s) const
    {
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

    int8_t parseColumnDirection(String & s) const
    {
        boost::trim(s);
        std::for_each(s.begin(), s.end(), [](char & c) {
            c = ::tolower(c);
        });
        if (s == "asc")
        {
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

    IColumn::PermutationSortDirection getDirection(uint8_t directionVal) const
    {
        IColumn::PermutationSortDirection direction;
        if (directionVal == 1)
        {
            direction = IColumn::PermutationSortDirection::Ascending;
        }
        else
        {
            direction = IColumn::PermutationSortDirection::Descending;
        }
        return direction;
    }

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

public:
    explicit AggregateFunctionOrderBy(AggregateFunctionPtr nested_, const DataTypes & arguments, const Array & params_)
        : IAggregateFunctionDataHelper(arguments, params_)
        , nested_func(nested_), num_arguments(arguments.size()), arguments_(arguments)
    {
        printf("arguments: %zu, params: %zu\n", arguments.size(), params_.size());
        printf("param=%s\n", params_[0].get<String>().c_str());
        if (arguments.size() == 0)
            throw Exception(
                "Aggregate function " + getName() + " require at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        // if (arguments[num_arguments - 1]->getName() != "String")
            // throw Exception(
                // "Last parameter for aggregate function combinator " + getName() + " must be String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        query = params_[0].get<String>();
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
        new (place) OrderByData;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        this->data(place).~OrderByData();
        nested_func->destroy(getNestedPlace(place));
    }

    size_t alignOfData() const override
    {
        return prefix_size + nested_func->alignOfData();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).merge(data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /*version*/) const override
    {
        data(place).serialize(buf);

    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /*version*/, Arena * arena) const override
    {
        data(place).deserialize(buf, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * /*arena*/) const override
    {
        // init columns
        if (data(place).columnsData.empty())
        {
            data(place).initColumnData(num_arguments);
        }

        // init query
        // if (data(place).query.empty())
        // {
        //     Field field;
        //     columns[num_arguments - 1]->get(0, field);
        //     data(place).setQuery(field.get<std::string>());
        // }

        for (size_t k = 0; k < num_arguments; k++)
        {
            Field columnField;
            columns[k]->get(row_num, columnField);

            data(place).addValue(k, columnField);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        // auto columnToDirection = parseStringToVector(data(place).query);
        auto columnToDirection = parseStringToVector(query);
        IColumn * columns[num_arguments];

        for (size_t k = 0; k < num_arguments; k++)
        {
            auto newColumn = arguments_[k]->createColumn();
            for (size_t i = 0; i < data(place).columnsData[k].size(); i++)
            {
                auto a = data(place).columnsData[k][i];
                newColumn->insert(a);
            }
            columns[k] = newColumn.get();
        }

        const IColumn * ans[num_arguments];
        for (size_t k = 0; k < num_arguments; k++)
        {
            if (columnToDirection[k] == 0)
            {
                ans[k] = columns[k];
                continue;
            }
            else
            {
                IColumn::PermutationSortDirection direction = getDirection(columnToDirection[k]);

                // get sort permutation
                ColumnArray::Permutation permutation;
                columns[k]->getPermutation(direction, IColumn::PermutationSortStability::Stable, 0, 1, permutation);

                // init new column for inserting sorted data
                auto newColumn = arguments_[k]->createColumn();
                newColumn->reserve(data(place).columnsData[k].size());

                // sort based on permutation
                for (size_t i = 0; i < data(place).columnsData[k].size(); i++)
                {
                    auto field =  data(place).columnsData[k][permutation[i]];
                    newColumn->insert(field);
                }

                ans[k] = newColumn.get();
            }
        }
        for (size_t row_num = 0; row_num < data(place).columnsData[0].size(); row_num++)
            nested_func->add(getNestedPlace(place), ans, row_num, arena);

        nested_func->insertResultInto(getNestedPlace(place), to, arena);
    }

    AggregateFunctionPtr getNestedFunction() const override
    {
        return nested_func;
    }
};
}
