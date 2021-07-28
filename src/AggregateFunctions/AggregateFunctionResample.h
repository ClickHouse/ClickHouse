#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/assert_cast.h>
#include <common/arithmeticOverflow.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

template <typename Key>
class AggregateFunctionResample final : public IAggregateFunctionHelper<AggregateFunctionResample<Key>>
{
private:
    /// Sanity threshold to avoid creation of too large arrays. The choice of this number is arbitrary.
    const size_t MAX_ELEMENTS = 1048576;

    AggregateFunctionPtr nested_function;

    size_t last_col;

    Key begin;
    Key end;
    size_t step;

    size_t total;
    size_t align_of_data;
    size_t size_of_data;

public:
    AggregateFunctionResample(
        AggregateFunctionPtr nested_function_,
        Key begin_,
        Key end_,
        size_t step_,
        const DataTypes & arguments,
        const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionResample<Key>>{arguments, params}
        , nested_function{nested_function_}
        , last_col{arguments.size() - 1}
        , begin{begin_}
        , end{end_}
        , step{step_}
        , total{0}
        , align_of_data{nested_function->alignOfData()}
        , size_of_data{(nested_function->sizeOfData() + align_of_data - 1) / align_of_data * align_of_data}
    {
        // notice: argument types has been checked before
        if (step == 0)
            throw Exception("The step given in function "
                    + getName() + " should not be zero",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (end < begin)
            total = 0;
        else
        {
            Key dif;
            size_t sum;
            if (common::subOverflow(end, begin, dif)
                || common::addOverflow(static_cast<size_t>(dif), step, sum))
            {
                throw Exception("Overflow in internal computations in function " + getName()
                    + ". Too large arguments", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
            }

            total = (sum - 1) / step; // total = (end - begin + step - 1) / step
        }

        if (total > MAX_ELEMENTS)
            throw Exception("The range given in function "
                    + getName() + " contains too many elements",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    String getName() const override
    {
        return nested_function->getName() + "Resample";
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return total * size_of_data;
    }

    size_t alignOfData() const override
    {
        return align_of_data;
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        for (size_t i = 0; i < total; ++i)
        {
            try
            {
                nested_function->create(place + i * size_of_data);
            }
            catch (...)
            {
                for (size_t j = 0; j < i; ++j)
                    nested_function->destroy(place + j * size_of_data);
                throw;
            }
        }
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->destroy(place + i * size_of_data);
    }

    void add(
        AggregateDataPtr place,
        const IColumn ** columns,
        size_t row_num,
        Arena * arena) const override
    {
        Key key;

        if constexpr (static_cast<Key>(-1) < 0)
            key = columns[last_col]->getInt(row_num);
        else
            key = columns[last_col]->getUInt(row_num);

        if (key < begin || key >= end)
            return;

        size_t pos = (key - begin) / step;

        nested_function->add(place + pos * size_of_data, columns, row_num, arena);
    }

    void merge(
        AggregateDataPtr place,
        ConstAggregateDataPtr rhs,
        Arena * arena) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->merge(place + i * size_of_data, rhs + i * size_of_data, arena);
    }

    void serialize(
        ConstAggregateDataPtr place,
        WriteBuffer & buf) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->serialize(place + i * size_of_data, buf);
    }

    void deserialize(
        AggregateDataPtr place,
        ReadBuffer & buf,
        Arena * arena) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->deserialize(place + i * size_of_data, buf, arena);
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(nested_function->getReturnType());
    }

    void insertResultInto(
        AggregateDataPtr place,
        IColumn & to,
        Arena * arena) const override
    {
        auto & col = assert_cast<ColumnArray &>(to);
        auto & col_offsets = assert_cast<ColumnArray::ColumnOffsets &>(col.getOffsetsColumn());

        for (size_t i = 0; i < total; ++i)
            nested_function->insertResultInto(place + i * size_of_data, col.getData(), arena);

        col_offsets.getData().push_back(col.getData().size());
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

namespace AgrResampleDocs
{
const char * doc = R"(
Lets you divide data into groups, and then separately aggregates the data in those groups. Groups are created by splitting the values from one column into intervals.

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**Arguments**

-   `start` — Starting value of the whole required interval for `resampling_key` values.
-   `stop` — Ending value of the whole required interval for `resampling_key` values. The whole interval does not include the `stop` value `[start, stop)`.
-   `step` — Step for separating the whole interval into subintervals. The `aggFunction` is executed over each of those subintervals independently.
-   `resampling_key` — Column whose values are used for separating data into intervals.
-   `aggFunction_params` — `aggFunction` parameters.

**Returned values**

-   Array of `aggFunction` results for each subinterval.

**Example**

Consider the `people` table with the following data:

``` text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

Let’s get the names of the people whose age lies in the intervals of `[30,60)` and `[60,75)`. Since we use integer representation for age, we get ages in the `[30, 59]` and `[60,74]` intervals.

To aggregate names in an array, we use the [groupArray](../../sql-reference/aggregate-functions/reference/grouparray.md#agg_function-grouparray) aggregate function. It takes one argument. In our case, it’s the `name` column. The `groupArrayResample` function should use the `age` column to aggregate names by age. To define the required intervals, we pass the `30, 75, 30` arguments into the `groupArrayResample` function.

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

Consider the results.

`Jonh` is out of the sample because he’s too young. Other people are distributed according to the specified age intervals.

Now let’s count the total number of people and their average wage in the specified age intervals.

``` sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

``` text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```
)";
}

}
