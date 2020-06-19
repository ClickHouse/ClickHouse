#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitors.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <limits>

#include <DataTypes/DataTypeArray.h>

#include <Common/ArenaAllocator.h>

#include <type_traits>

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB
{


Int8 CriticalValuesTableFirst[20][20] =
{
       {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
       {-1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2},
       {-1, -1, -1, -1, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8},
       {-1, -1, -1, 0, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10, 11, 11, 12, 13, 13},
       {-1, -1, 0, 1, 2, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 17, 18, 19, 20},
       {-1, -1, 1, 2, 3, 5, 6, 8, 10, 11, 13, 14, 16, 17, 19, 21, 22, 24, 25, 27},
       {-1, -1, 1, 3, 5, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34},
       {-1, 0, 2, 4, 6, 8, 10, 13, 15, 17, 19, 22, 24, 26, 29, 31, 34, 36, 38, 41},
       {-1, 0, 2, 4, 7, 10, 12, 15, 17, 21, 23, 26, 28, 31, 34, 37, 39, 42, 45, 48},
       {-1, 0, 3, 5, 8, 11, 14, 17, 20, 23, 26, 29, 33, 36, 39, 42, 45, 48, 52, 55},
       {-1, 0, 3, 6, 9, 13, 16, 19, 23, 26, 30, 33, 37, 40, 44, 47, 51, 55, 58, 62},
       {-1, 1, 4, 7, 11, 14, 18, 22, 26, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69},
       {-1, 1, 4, 8, 12, 16, 20, 24, 28, 33, 37, 41, 45, 50, 54, 59, 63, 67, 72, 76},
       {-1, 1, 5, 9, 13, 17, 22, 26, 31, 36, 40, 45, 50, 55, 59, 64, 67, 74, 78, 83},
       {-1, 1, 5, 10, 14, 19, 24, 29, 34, 39, 44, 49, 54, 59, 64, 70, 75, 80, 85, 90},
       {-1, 1, 6, 11, 15, 21, 26, 31, 37, 42, 47, 53, 59, 64, 70, 75, 81, 86, 92, 98},
       {-1, 2, 6, 11, 17, 22, 28, 34, 39, 45, 51, 57, 63, 67, 75, 81, 87, 93, 99, 105},
       {-1, 2, 7, 12, 18, 24, 30, 36, 42, 48, 55, 61, 67, 74, 80, 86, 93, 99, 106, 112},
       {-1, 2, 7, 13, 19, 25, 32, 38, 45, 52, 58, 65, 72, 78, 85, 92, 99, 106, 113, 119},
       {-1, 2, 8, 14, 20, 27, 34, 41, 48, 55, 62, 69, 76, 83, 90, 98, 105, 112, 119, 127}
};




//TO DO - change to different
Int8 CriticalValuesTableSecond[20][20] =
{
        {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
        {-1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2},
        {-1, -1, -1, -1, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8},
        {-1, -1, -1, 0, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10, 11, 11, 12, 13, 13},
        {-1, -1, 0, 1, 2, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 17, 18, 19, 20},
        {-1, -1, 1, 2, 3, 5, 6, 8, 10, 11, 13, 14, 16, 17, 19, 21, 22, 24, 25, 27},
        {-1, -1, 1, 3, 5, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34},
        {-1, 0, 2, 4, 6, 8, 10, 13, 15, 17, 19, 22, 24, 26, 29, 31, 34, 36, 38, 41},
        {-1, 0, 2, 4, 7, 10, 12, 15, 17, 21, 23, 26, 28, 31, 34, 37, 39, 42, 45, 48},
        {-1, 0, 3, 5, 8, 11, 14, 17, 20, 23, 26, 29, 33, 36, 39, 42, 45, 48, 52, 55},
        {-1, 0, 3, 6, 9, 13, 16, 19, 23, 26, 30, 33, 37, 40, 44, 47, 51, 55, 58, 62},
        {-1, 1, 4, 7, 11, 14, 18, 22, 26, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69},
        {-1, 1, 4, 8, 12, 16, 20, 24, 28, 33, 37, 41, 45, 50, 54, 59, 63, 67, 72, 76},
        {-1, 1, 5, 9, 13, 17, 22, 26, 31, 36, 40, 45, 50, 55, 59, 64, 67, 74, 78, 83},
        {-1, 1, 5, 10, 14, 19, 24, 29, 34, 39, 44, 49, 54, 59, 64, 70, 75, 80, 85, 90},
        {-1, 1, 6, 11, 15, 21, 26, 31, 37, 42, 47, 53, 59, 64, 70, 75, 81, 86, 92, 98},
        {-1, 2, 6, 11, 17, 22, 28, 34, 39, 45, 51, 57, 63, 67, 75, 81, 87, 93, 99, 105},
        {-1, 2, 7, 12, 18, 24, 30, 36, 42, 48, 55, 61, 67, 74, 80, 86, 93, 99, 106, 112},
        {-1, 2, 7, 13, 19, 25, 32, 38, 45, 52, 58, 65, 72, 78, 85, 92, 99, 106, 113, 119},
        {-1, 2, 8, 14, 20, 27, 34, 41, 48, 55, 62, 69, 76, 83, 90, 98, 105, 112, 119, 127}
};

template <template <typename> class Comparator>
struct ComparePairFirst final
{
    template <typename X>
    bool operator()(const std::pair<X, UInt8> & lhs, const std::pair<X, UInt8> & rhs) const
    {
        return Comparator<X>{}(lhs.first, rhs.first);
    }
};

template <typename X = Float64, typename Y = Float64>
struct AggregateFunctionMannWhitneyData final
{
    size_t size_x = 0;
    size_t size_y = 0;

    using Allocator = MixedAlignedArenaAllocator<alignof(std::pair<X, UInt8>), 4096>;
    using Array = PODArray<std::pair<X, UInt8>, 32, Allocator>;

    Array values;
};


template <typename X = Float64, typename Y = Float64>
class AggregateFunctionMannWhitney : public
                                  IAggregateFunctionDataHelper<
                                      AggregateFunctionMannWhitneyData<X, Y>,
                                      AggregateFunctionMannWhitney<X, Y>
                                  >
{
    using Data = AggregateFunctionMannWhitneyData<X, Y>;

private:
    Float64 significance_level;

public:
    AggregateFunctionMannWhitney(
        Float64 sglvl_,
        const DataTypes & arguments,
        const Array & params
    ):
        IAggregateFunctionDataHelper<
            AggregateFunctionMannWhitneyData<X, Y>,
            AggregateFunctionMannWhitney<X, Y>
        > ({arguments}, params), significance_level(sglvl_)
    {
        // notice: arguments has been in factory
    }

    String getName() const override
    {
        return "Mann-Whitney";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Int8>>();
    }

    void insert_x(Data & a, const std::pair<X, UInt8> & x, Arena * arena) const
    {
        a.size_x += 1;
        a.values.push_back(x, arena);
    }

    void insert_y(Data & a, const std::pair<X, UInt8> & y, Arena * arena) const
    {
        a.size_y += 1;
        a.values.push_back(y, arena);
    }

    void add(
        AggregateDataPtr place,
        const IColumn ** columns,
        size_t row_num,
        Arena * arena
    ) const override
    {
        auto & a = this->data(place);

        auto new_x = assert_cast<const ColumnVector<X> *>(columns[0])->getData()[row_num];
        auto new_y = assert_cast<const ColumnVector<X> *>(columns[1])->getData()[row_num];

        auto new_arg_x = std::make_pair(new_x, 1);
        auto new_arg_y = std::make_pair(new_y, 2);

        a.size_x += 1;
        a.size_y += 1;

        a.values.push_back(new_arg_x, arena);
        a.values.push_back(new_arg_y, arena);
    }

    void merge(
        AggregateDataPtr place,
        ConstAggregateDataPtr rhs,
        Arena * arena
    ) const override
    {
        auto & a = this->data(place);
        auto & b = this->data(rhs);

        if (b.size_x || b.size_y)
        {
            for (size_t i = 0; i < b.size_x + b.size_y; ++i)
            {
                if (b.values[i].second == 1)
                {
                    insert_x(a, b.values[i], arena);
                }
                else
                {
                    insert_y(a, b.values[i], arena);
                }
            }
        }
    }

    void serialize(
        ConstAggregateDataPtr place,
        WriteBuffer & buf
    ) const override
    {
        const auto & value = this->data(place).values;
        size_t size_x = this->data(place).size_x;
        writeVarUInt(size_x, buf);
        size_t size_y = this->data(place).size_y;
        writeVarUInt(size_y, buf);
        buf.write(reinterpret_cast<const char *>(value.data()), (size_x + size_y) * sizeof(value[0]));
    }

    void deserialize(
        AggregateDataPtr place,
        ReadBuffer & buf,
        Arena * arena
    ) const override
    {
        size_t size_x = 0;
        readVarUInt(size_x, buf);

        size_t size_y = 0;
        readVarUInt(size_y, buf);

        auto & value = this->data(place).values;

        value.resize(size_x + size_y, arena);
        buf.read(reinterpret_cast<char *>(value.data()), (size_x + size_y) * sizeof(value[0]));
    }

    void insertResultInto(
        AggregateDataPtr place,
        IColumn & to
    ) const override
    {
        const auto & value = this->data(place).values;
        size_t size_x = this->data(place).size_x;
        size_t size_y = this->data(place).size_y;

        if(size_x < 2)
        {
            throw Exception("Aggregate function " + getName() + " requires xxxx to be of size > 1", ErrorCodes::BAD_ARGUMENTS);
        }

        //minimal size
        if (size_x < 2 || size_y < 2)
        {
            throw Exception("Aggregate function " + getName() + " requires samples to be of size > 1", ErrorCodes::BAD_ARGUMENTS);
        }

        if (significance_level != 0.05 || significance_level != 0.01)
        {
            throw Exception("Aggregate function " + getName() + " requires parameter to bo 0.01 or 0.05", ErrorCodes::BAD_ARGUMENTS);
        }

        //create a copy of values not to format data
        PODArrayWithStackMemory<std::pair<Float64, UInt8>, 32> tmp_values;
        tmp_values.resize(size_x + size_y);
        for (size_t j = 0; j < size_x + size_y; ++ j)
        {
            tmp_values[j] = static_cast<std::pair<Float64, UInt8>>(value[j]);
        }

        //sort values
        std::sort(std::begin(tmp_values), std::end(tmp_values), ComparePairFirst<std::less>{});

        for (size_t j = 0; j < size_x + size_y;)
        {
            //replace x_values with their ranks
            size_t rank = j + 1;
            size_t same = 1;
            size_t cur_sum = rank;
            size_t cur_start = j;

            while (j < size_x + size_y - 1)
            {
                if (tmp_values[j].first == tmp_values[j + 1].first)
                {
                    // rank of (j + 1)th number
                    rank += 1;
                    same++;
                    cur_sum += rank;
                    j++;
                }
                else
                {
                    break;
                }
            }

            // insert rank is calculated as average of ranks of equal values
            Float64 insert_rank = static_cast<Float64>(cur_sum) / same;
            for (size_t i = cur_start; i <= j; ++i)
            {
                tmp_values[i].first = insert_rank;
            }
            j++;
        }

        //count sum of x and y ranks
        Float64 x_answ = static_cast<Float64>(0);
        Float64 y_answ = static_cast<Float64>(0);
        //TO DO - add it to the previous for()
        for (size_t j = 0; j < size_x + size_y; ++ j)
        {
            if (tmp_values[j].second == 1)
            {
                x_answ += tmp_values[j].first;
            }
            else
            {
                y_answ += tmp_values[j].first;
            }
        }

        x_answ = (static_cast<Float64>(size_x) * size_y) + static_cast<Float64>(size_x) * (size_x + 1) / 2 - x_answ;
        y_answ = (static_cast<Float64>(size_x) * size_y) + static_cast<Float64>(size_x) * (size_x + 1) / 2 - y_answ;


        if (x_answ < y_answ)
        {
            x_answ = y_answ;
        }

        auto & column = static_cast<ColumnVector<Int8> &>(to);
        if (significance_level == 0.01)
        {
            if (x_answ >= static_cast<Float64>(CriticalValuesTableFirst[size_x - 1][size_y - 1]))
            {
                column.getData().push_back(static_cast<Int8>(1));
            }
            else
            {
                column.getData().push_back(static_cast<Int8>(0));
            }
        }
        else
        {
            if (x_answ >= CriticalValuesTableSecond[size_x - 1][size_y - 1])
            {
                column.getData().push_back(static_cast<Int8>(1));
            }
            else
            {
                column.getData().push_back(static_cast<Int8>(0));
            }
        }
    }

};

};