#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/AggregationCommon.h>

namespace DB
{

    namespace ErrorCodes
    {
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    }

struct AggregateFunctionOrderByData
{
    AggregateFunctionOrderByData() = default;
    explicit AggregateFunctionOrderByData(size_t num_arguments): value(num_arguments), arena(nullptr) {}

    using Array = std::vector<IColumn*>;

    Array value;
    std::once_flag initialized;

    Arena* arena;
};

/** Adaptor for aggregate functions.
  * Adding -OrderBy suffix to aggregate function
**/

class AggregateFunctionOrderBy final : public IAggregateFunctionDataHelper<AggregateFunctionOrderByData, AggregateFunctionOrderBy>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;
    size_t prefix_size;
    size_t num_sort_args;

    AggregateDataPtr getNestedPlace(AggregateDataPtr place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr place) const noexcept
    {
        return place + prefix_size;
    }


public:
    AggregateFunctionOrderBy(AggregateFunctionPtr nested, const DataTypes & arguments, const Array & params)
    : IAggregateFunctionDataHelper<AggregateFunctionOrderByData, AggregateFunctionOrderBy>(arguments, {params})
    , nested_func(nested), num_arguments(arguments.size()), num_sort_args(params.back().safeGet<UInt64>())
    {
        prefix_size = sizeof(AggregateFunctionOrderByData);

        if (arguments.empty())
            throw Exception("Aggregate function " + getName() + " require at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
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
        new (place) AggregateFunctionOrderByData(num_arguments);
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr place) const noexcept override {
        data(place).~AggregateFunctionOrderByData();
        nested_func->destroy(getNestedPlace(place));
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        std::call_once(data(place).initialized, [this, place, columns]() {
            for (size_t i = 0; i < num_arguments; ++i) {
                data(place).value[i] = columns[i]->cloneEmpty().detach();
            }
        });

        for (size_t i = 0; i < num_arguments; ++i) {
            data(place).value[i]->insertFrom(*columns[i], row_num);
        }
        data(place).arena = arena;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(getNestedPlace(place), rhs, arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        nested_func->serialize(getNestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        nested_func->deserialize(getNestedPlace(place), buf, arena);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        const IColumn* db_cols[num_arguments];
        for (size_t i = 0; i < num_arguments; ++i) {
            db_cols[i] = data(place).value[i];
        }
        IColumn::Permutation perm(db_cols[0]->size());
        for (size_t i = 0; i < perm.size(); ++i) {
            perm[i] = i;
        }

        std::sort(perm.begin(), perm.end(), [this, &db_cols](auto l, auto r) {
            for (size_t i = num_sort_args; i > 0; --i) {
                auto col = db_cols[num_arguments - i];
                int compare_res = col->compareAt(l, r, *col, -1);
                if (compare_res != 0)
                    return compare_res < 0;
            }
            return false;
        });
        for (size_t i = 0; i < data(place).value[0]->size(); ++i) {
            nested_func->add(getNestedPlace(place), db_cols, perm[i], data(place).arena);
        }
        nested_func->insertResultInto(getNestedPlace(place), to);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }
};

}
