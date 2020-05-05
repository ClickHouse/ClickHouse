#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <limits>

#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


#include <type_traits>

#include <DataTypes/DataTypesDecimal.h>


namespace DB
{
// hard-codded values - part of the algorithm

#define SIGN_LVL_CNT 6

Float64 CriticalValuesTable[SIGN_LVL_CNT][102] = {
    // for significance level = 0.2
    {0.2, 3.078, 1.886, 1.638, 1.533, 1.476, 1.44, 1.415, 1.397, 1.383, 1.372, 1.363, 1.356, 1.35, 1.345, 1.341, 1.337, 1.333, 1.33, 1.328, 1.325, 1.323, 1.321, 1.319, 1.318, 1.316, 1.315, 1.314, 1.313, 1.311, 1.31, 1.309, 1.309, 1.308, 1.307, 1.306, 1.306, 1.305, 1.304, 1.304, 1.303, 1.303, 1.302, 1.302, 1.301, 1.301, 1.3, 1.3, 1.299, 1.299, 1.299, 1.298, 1.298, 1.298, 1.297, 1.297, 1.297, 1.297, 1.296, 1.296, 1.296, 1.296, 1.295, 1.295, 1.295, 1.295, 1.295, 1.294, 1.294, 1.294, 1.294, 1.294, 1.293, 1.293, 1.293, 1.293, 1.293, 1.293, 1.292, 1.292, 1.292, 1.292, 1.292, 1.292, 1.292, 1.292, 1.291, 1.291, 1.291, 1.291, 1.291, 1.291, 1.291, 1.291, 1.291, 1.291, 1.29, 1.29, 1.29, 1.29, 1.29, 1.282},

    // for significance level = 0.1
    {0.1, 6.314, 2.92, 2.353, 2.132, 2.015, 1.943, 1.895, 1.86, 1.833, 1.812, 1.796, 1.782, 1.771, 1.761, 1.753, 1.746, 1.74, 1.734, 1.729, 1.725, 1.721, 1.717, 1.714, 1.711, 1.708, 1.706, 1.703, 1.701, 1.699, 1.697, 1.696, 1.694, 1.692, 1.691, 1.69, 1.688, 1.687, 1.686, 1.685, 1.684, 1.683, 1.682, 1.681, 1.68, 1.679, 1.679, 1.678, 1.677, 1.677, 1.676, 1.675, 1.675, 1.674, 1.674, 1.673, 1.673, 1.672, 1.672, 1.671, 1.671, 1.67, 1.67, 1.669, 1.669, 1.669, 1.668, 1.668, 1.668, 1.667, 1.667, 1.667, 1.666, 1.666, 1.666, 1.665, 1.665, 1.665, 1.665, 1.664, 1.664, 1.664, 1.664, 1.663, 1.663, 1.663, 1.663, 1.663, 1.662, 1.662, 1.662, 1.662, 1.662, 1.661, 1.661, 1.661, 1.661, 1.661, 1.661, 1.66, 1.66, 1.645},

    // for significance level = 0.05
    {0.05, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.16, 2.145, 2.131, 2.12, 2.11, 2.101, 2.093, 2.086, 2.08, 2.074, 2.069, 2.064, 2.06, 2.056, 2.052, 2.048, 2.045, 2.042, 2.04, 2.037, 2.035, 2.032, 2.03, 2.028, 2.026, 2.024, 2.023, 2.021, 2.02, 2.018, 2.017, 2.015, 2.014, 2.013, 2.012, 2.011, 2.01, 2.009, 2.008, 2.007, 2.006, 2.005, 2.004, 2.003, 2.002, 2.002, 2.001, 2.0, 2.0, 1.999, 1.998, 1.998, 1.997, 1.997, 1.996, 1.995, 1.995, 1.994, 1.994, 1.993, 1.993, 1.993, 1.992, 1.992, 1.991, 1.991, 1.99, 1.99, 1.99, 1.989, 1.989, 1.989, 1.988, 1.988, 1.988, 1.987, 1.987, 1.987, 1.986, 1.986, 1.986, 1.986, 1.985, 1.985, 1.985, 1.984, 1.984, 1.984, 1.96},

    // for significance level = 0.02
    {0.02, 31.821, 6.965, 4.541, 3.747, 3.365, 3.143, 2.998, 2.896, 2.821, 2.764, 2.718, 2.681, 2.65, 2.624, 2.602, 2.583, 2.567, 2.552, 2.539, 2.528, 2.518, 2.508, 2.5, 2.492, 2.485, 2.479, 2.473, 2.467, 2.462, 2.457, 2.453, 2.449, 2.445, 2.441, 2.438, 2.434, 2.431, 2.429, 2.426, 2.423, 2.421, 2.418, 2.416, 2.414, 2.412, 2.41, 2.408, 2.407, 2.405, 2.403, 2.402, 2.4, 2.399, 2.397, 2.396, 2.395, 2.394, 2.392, 2.391, 2.39, 2.389, 2.388, 2.387, 2.386, 2.385, 2.384, 2.383, 2.382, 2.382, 2.381, 2.38, 2.379, 2.379, 2.378, 2.377, 2.376, 2.376, 2.375, 2.374, 2.374, 2.373, 2.373, 2.372, 2.372, 2.371, 2.37, 2.37, 2.369, 2.369, 2.368, 2.368, 2.368, 2.367, 2.367, 2.366, 2.366, 2.365, 2.365, 2.365, 2.364, 2.326},

    // for significance level = 0.01
    {0.01, 63.657, 9.925, 5.841, 4.604, 4.032, 3.707, 3.499, 3.355, 3.25, 3.169, 3.106, 3.055, 3.012, 2.977, 2.947, 2.921, 2.898, 2.878, 2.861, 2.845, 2.831, 2.819, 2.807, 2.797, 2.787, 2.779, 2.771, 2.763, 2.756, 2.75, 2.744, 2.738, 2.733, 2.728, 2.724, 2.719, 2.715, 2.712, 2.708, 2.704, 2.701, 2.698, 2.695, 2.692, 2.69, 2.687, 2.685, 2.682, 2.68, 2.678, 2.676, 2.674, 2.672, 2.67, 2.668, 2.667, 2.665, 2.663, 2.662, 2.66, 2.659, 2.657, 2.656, 2.655, 2.654, 2.652, 2.651, 2.65, 2.649, 2.648, 2.647, 2.646, 2.645, 2.644, 2.643, 2.642, 2.641, 2.64, 2.64, 2.639, 2.638, 2.637, 2.636, 2.636, 2.635, 2.634, 2.634, 2.633, 2.632, 2.632, 2.631, 2.63, 2.63, 2.629, 2.629, 2.628, 2.627, 2.627, 2.626, 2.626, 2.576},

    // for significance level = 0.002
    {0.002, 318.313, 22.327, 10.215, 7.173, 5.893, 5.208, 4.782, 4.499, 4.296, 4.143, 4.024, 3.929, 3.852, 3.787, 3.733, 3.686, 3.646, 3.61, 3.579, 3.552, 3.527, 3.505, 3.485, 3.467, 3.45, 3.435, 3.421, 3.408, 3.396, 3.385, 3.375, 3.365, 3.356, 3.348, 3.34, 3.333, 3.326, 3.319, 3.313, 3.307, 3.301, 3.296, 3.291, 3.286, 3.281, 3.277, 3.273, 3.269, 3.265, 3.261, 3.258, 3.255, 3.251, 3.248, 3.245, 3.242, 3.239, 3.237, 3.234, 3.232, 3.229, 3.227, 3.225, 3.223, 3.22, 3.218, 3.216, 3.214, 3.213, 3.211, 3.209, 3.207, 3.206, 3.204, 3.202, 3.201, 3.199, 3.198, 3.197, 3.195, 3.194, 3.193, 3.191, 3.19, 3.189, 3.188, 3.187, 3.185, 3.184, 3.183, 3.182, 3.181, 3.18, 3.179, 3.178, 3.177, 3.176, 3.175, 3.175, 3.174, 3.09}
};

// our algorithm implementation via vectors:
// https://gist.github.com/ltybc-coder/792748cfdb2f7cadef424ffb7b011c71
// col, col, bool
template <typename X, typename Y, typename Ret = UInt8>
struct AggregateFunctionWelchTTestData final {

    size_t size_x = 0;
    size_t size_y = 0;
    X sum_x = 0;
    Y sum_y = 0;
    X square_sum_x = 0;
    Y square_sum_y = 0;
    Float64 mean_x = 0;
    Float64 mean_y = 0;

    /*
     not yet sure how to use them
    void add_x(X x) {
        mean_x = (Float64)(sum_x + x) / (size_x + 1);
        size_x ++;
        sum_x += x;
        square_sum_x += x * x;
    }

    void add_y(Y y) {
        mean_y = (sum_y + y) / (size_y + 1);
        size_y ++;
        sum_y += y;
        square_sum_y += y * y;
    }
    */

    void add(X x, Y y) {
        sum_x += x;
        sum_y += y;
        size_x++;
        size_y++;
        mean_x = static_cast<Float64>(sum_x) / size_x;
        mean_y = static_cast<Float64>(sum_y) / size_y;
        square_sum_x += x * x;
        square_sum_y += y * y;
    }

    void merge(const AggregateFunctionWelchTTestData &other) {
        sum_x += other.sum_x;
        sum_y += other.sum_y;
        size_x += other.size_x;
        size_y += other.size_y;
        mean_x = static_cast<Float64>(sum_x) / size_x;
        mean_y = static_cast<Float64>(sum_y) / size_y;
        square_sum_x += other.square_sum_x;
        square_sum_y += other.square_sum_y;
    }

    void serialize(WriteBuffer &buf) const {
        writeBinary(mean_x, buf);
        writeBinary(mean_y, buf);
        writeBinary(sum_x, buf);
        writeBinary(sum_y, buf);
        writeBinary(square_sum_x, buf);
        writeBinary(square_sum_y, buf);
        writeBinary(size_x, buf);
        writeBinary(size_y, buf);
    }

    void deserialize(ReadBuffer &buf) {
        readBinary(mean_x, buf);
        readBinary(mean_y, buf);
        readBinary(sum_x, buf);
        readBinary(sum_y, buf);
        readBinary(square_sum_x, buf);
        readBinary(square_sum_y, buf);
        readBinary(size_x, buf);
        readBinary(size_y, buf);
    }

    Float64 get_sx() const {
        return static_cast<Float64>(square_sum_x + size_x * mean_x * mean_x - 2 * mean_x * sum_x) / (size_x - 1);
    }

    Float64 get_sy() const {
        return static_cast<Float64>(square_sum_y + size_y * mean_y * mean_y - 2 * mean_y * sum_y) / (size_y - 1);
    }

    Float64 get_T(Float64 sx, Float64 sy) const {
        return static_cast<Float64>(mean_x - mean_y) / std::sqrt(sx / size_x + sy / size_y);
    }

    Float64 get_degrees_of_freed(Float64 sx, Float64 sy) const {
        return static_cast<Float64>(sx / size_x + sy / size_y) * (sx / size_x + sy / size_y) /
               ((sx * sx / (size_x * size_x * (size_x - 1))) + (sy * sy / (size_y * size_y * (size_y - 1))));
    }

    Ret get_result(Float64 t, Float64 dof, Float64 parametr) const
    {
        //find our table
        int table = 0;
        for (int i = 0; i < SIGN_LVL_CNT; ++i)
        {
            if (CriticalValuesTable[i][0] == parametr)
            {
                table = i;
            }
        }

        //round or make infinity dof
        i_dof = static_cast<int>(dof);
        if (i_dof > 100)
        {
            i_dof = 101;
        }
        //check if abs of t is greater than table[dof]
        t = abs(t);
        if(t > CriticalValuesTable[table][i_dof]) {
            return static_cast<UInt8>(1);
            //in this case we reject the null hypothesis
        }
        else {
            return static_cast<UInt8>(0);
        }
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
        const  Array & params
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

    void insertResultInto(
        ConstAggregateDataPtr place,
        IColumn & to
    ) const override
    {
        Float64 significance_level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);

        Float64 sx = this->data(place).get_sx();
        Float64 sy = this->data(place).get_sy();
        Float64 t_value = this->data(place).get_T(sx, sy);
        Float64 dof = this->data(place).get_degrees_of_freed(sx, sy);
        Ret result = this->data(place).get_result(t_value, dof, significance_level);

        //check the type
        auto & column = static_cast<ColumnVector<Ret> &>(to);
        column.getData().push_back(result);
    }


};
};
