#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <limits>

namespace DB
{

// our algorithm immplementation via vectors:
// https://gist.github.com/ltybc-coder/792748cfdb2f7cadef424ffb7b011c71
// col, col, bool
template <typename X, typename Y, typename Ret>
struct AggregateFunctionWelchTTestData final
{
    // hard-codded values - part of the algorithm
    double criticalValuesAt80[102] = { -1,
                                       1.376,1.061,0.978,0.941,0.92,0.906,0.896,0.889,0.883,0.879,
                                       0.876,0.873,0.87,0.868,0.866,0.865,0.863,0.862,0.861,0.86,
                                       0.859,0.858,0.858,0.857,0.856,0.856,0.855,0.855,0.854,0.854,
                                       0.853,0.853,0.853,0.852,0.852,0.852,0.851,0.851,0.851,0.851,
                                       0.85,0.85,0.85,0.85,0.85,0.85,0.849,0.849,0.849,0.849,
                                       0.849,0.849,0.849,0.849,0.849,0.848,0.848,0.848,0.848,0.848,
                                       0.848,0.848,0.848,0.847,0.847,0.847,0.847,0.847,0.847,0.847,
                                       0.847,0.847,0.847,0.846,0.846,0.846,0.846,0.846,0.846,0.846,
                                       0.846,0.846,0.846,0.846,0.846,0.846,0.846,0.846,0.846,0.846,
                                       0.846,0.846,0.845,0.845,0.845,0.845,0.845,0.845,0.845,0.845,0.842
    };
    double criticalValuesAt90[102] = { -1,
                                       3.078,1.886,1.638,1.533,1.476,1.44,1.415,1.397,1.383,1.372,
                                       1.363,1.356,1.35,1.345,1.341,1.337,1.333,1.33,1.328,1.325,
                                       1.323,1.321,1.319,1.318,1.316,1.315,1.314,1.313,1.311,1.31,
                                       1.309,1.309,1.308,1.307,1.306,1.306,1.305,1.304,1.304,1.303,
                                       1.303,1.302,1.302,1.301,1.301,1.3,1.3,1.299,1.299,1.299,
                                       1.298,1.298,1.298,1.297,1.297,1.297,1.297,1.296,1.296,1.296,
                                       1.296,1.295,1.295,1.295,1.295,1.295,1.294,1.294,1.294,1.294,
                                       1.294,1.293,1.293,1.293,1.293,1.293,1.293,1.292,1.292,1.292,
                                       1.292,1.292,1.292,1.292,1.292,1.291,1.291,1.291,1.291,1.291,
                                       1.291,1.291,1.291,1.291,1.291,1.29,1.29,1.29,1.29,1.29,1.282
    };

    double criticalValuesAt95[102] = { -1,
                                       6.314,2.92,2.353,2.132,2.015,1.943,1.895,1.86,1.833,1.812,
                                       1.796,1.782,1.771,1.761,1.753,1.746,1.74,1.734,1.729,1.725,
                                       1.721,1.717,1.714,1.711,1.708,1.706,1.703,1.701,1.699,1.697,
                                       1.696,1.694,1.692,1.691,1.69,1.688,1.687,1.686,1.685,1.684,
                                       1.683,1.682,1.681,1.68,1.679,1.679,1.678,1.677,1.677,1.676,
                                       1.675,1.675,1.674,1.674,1.673,1.673,1.672,1.672,1.671,1.671,
                                       1.67,1.67,1.669,1.669,1.669,1.668,1.668,1.668,1.667,1.667,
                                       1.667,1.666,1.666,1.666,1.665,1.665,1.665,1.665,1.664,1.664,
                                       1.664,1.664,1.663,1.663,1.663,1.663,1.663,1.662,1.662,1.662,
                                       1.662,1.662,1.661,1.661,1.661,1.661,1.661,1.661,1.66,1.66,1.645
    };

    double criticalValuesAt975[102] = { -1,
                                        12.706,4.303,3.182,2.776,2.571,2.447,2.365,2.306,2.262,2.228,
                                        2.201,2.179,2.16,2.145,2.131,2.12,2.11,2.101,2.093,2.086,
                                        2.08,2.074,2.069,2.064,2.06,2.056,2.052,2.048,2.045,2.042,
                                        2.04,2.037,2.035,2.032,2.03,2.028,2.026,2.024,2.023,2.021,
                                        2.02,2.018,2.017,2.015,2.014,2.013,2.012,2.011,2.01,2.009,
                                        2.008,2.007,2.006,2.005,2.004,2.003,2.002,2.002,2.001,2,
                                        2,1.999,1.998,1.998,1.997,1.997,1.996,1.995,1.995,1.994,
                                        1.994,1.993,1.993,1.993,1.992,1.992,1.991,1.991,1.99,1.99,
                                        1.99,1.989,1.989,1.989,1.988,1.988,1.988,1.987,1.987,1.987,
                                        1.986,1.986,1.986,1.986,1.985,1.985,1.985,1.984,1.984,1.984,1.96
    };

    double criticalValuesAt99[102] = { -1,
                                       31.821,6.965,4.541,3.747,3.365,3.143,2.998,2.896,2.821,2.764,
                                       2.718,2.681,2.65,2.624,2.602,2.583,2.567,2.552,2.539,2.528,
                                       2.518,2.508,2.5,2.492,2.485,2.479,2.473,2.467,2.462,2.457,
                                       2.453,2.449,2.445,2.441,2.438,2.434,2.431,2.429,2.426,2.423,
                                       2.421,2.418,2.416,2.414,2.412,2.41,2.408,2.407,2.405,2.403,
                                       2.402,2.4,2.399,2.397,2.396,2.395,2.394,2.392,2.391,2.39,
                                       2.389,2.388,2.387,2.386,2.385,2.384,2.383,2.382,2.382,2.381,
                                       2.38,2.379,2.379,2.378,2.377,2.376,2.376,2.375,2.374,2.374,
                                       2.373,2.373,2.372,2.372,2.371,2.37,2.37,2.369,2.369,2.368,
                                       2.368,2.368,2.367,2.367,2.366,2.366,2.365,2.365,2.365,2.364,2.326
    };

    size_t count_x = 0;
    size_t count_y = 0;
    Ret sum_x = 0;
    Ret sqrt_sum_x = 0;
    Ret sum_y = 0;
    Ret sqrt_sum_y = 0;

    void add(X x, Y y)
    {
        count_x += 1;
        count_y += 1;
        sum_x += x;
        sqrt_sum_x += x * x;
        sum_y += y;
        sqrt_sum_y += y * y;
        // todo придумать какие данные сторить чтобы потом так вжух и ответ получить
    }

    void merge(const AggregateFunctionWelchTTestData & other)
    {
        count_x += other.count_x;
        count_y += other.count_y;
        sum_x += other.sum_x;
        sqrt_sum_x += other.sqrt_sum_x;
        sum_y += other.sum_y;
        sqrt_sum_y += other.sqrt_sum_y;
        // todo пали add()
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(count_x, buf);
        writeBinary(count_y, buf);
        writeBinary(sum_x, buf);
        writeBinary(sqrt_sum_x, buf);
        writeBinary(sum_y, buf);
        writeBinary(sqrt_sum_y, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(count_x, buf);
        readBinary(count_y, buf);
        readBinary(sum_x, buf);
        readBinary(sqrt_sum_x, buf);
        readBinary(sum_y, buf);
        readBinary(sqrt_sum_y, buf);
    }

    int result() {
        // todo figure out how to
    }

};

template <typename X, typename Y, typename Ret>
class AggregateFunctionWelchTTest final : public
  IAggregateFunctionDataHelper<
    AggregateFunctionWelchTTestData<X, Y, Ret>,
    AggregateFunctionWelchTTest<X, Y, Ret>
>
{
public:
    AggregateFunctionWelchTTest(
            const DataTypes & arguments,
            const Array & params
    ):
        IAggregateFunctionDataHelper<
            AggregateFunctionWelchTTestData<X, Y, Ret>,
            AggregateFunctionWelchTTest<X, Y, Ret>
        > {arguments, params}
    {
        // notice: arguments has been in factory
    }

    String getName() const override
    {
        return "WelchTTest";
    }

    void add(
            AggregateDataPtr place,
            const IColumn ** columns,
            size_t row_num,
            Arena *
    ) const override
    {
        auto col_x = assert_cast<const ColumnVector<X> *>(columns[0]);
        auto col_y = assert_cast<const ColumnVector<Y> *>(columns[1]);

        X x = col_x->getData()[row_num];
        Y y = col_y->getData()[row_num];

        this->data(place).add(x, y);
    }

    void merge(
            AggregateDataPtr place,
            ConstAggregateDataPtr rhs, Arena *
    ) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(
            ConstAggregateDataPtr place,
            WriteBuffer & buf
    ) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(
            AggregateDataPtr place,
            ReadBuffer & buf, Arena *
    ) const override
    {
        this->data(place).deserialize(buf);
    }

    DataTypePtr getReturnType() const override
    {
        // todo return return type (may be different)
    }

    void insertResultInto(
            ConstAggregateDataPtr place,
            IColumn & to
    ) const override
    {
        // todo AggregateFunctionWelchTTestData.result() you know
    }

};

};
