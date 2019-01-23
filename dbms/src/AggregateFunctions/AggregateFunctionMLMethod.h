#pragma once

#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <cmath>
#include <exception>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Common/FieldVisitors.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


struct LinearRegressionData
{
    LinearRegressionData()
    {}
    LinearRegressionData(Float64 learning_rate_, UInt32 param_num_)
    : learning_rate(learning_rate_) {
        weights.resize(param_num_);
    }

    Float64 bias{0.0};
    std::vector<Float64> weights;
    Float64 learning_rate;
    UInt32 iter_num = 0;

    void add(Float64 target, const IColumn ** columns, size_t row_num)
    {
        Float64 derivative = (target - bias);
        for (size_t i = 0; i < weights.size(); ++i)
        {
            derivative -= weights[i] * static_cast<const ColumnVector<Float64> &>(*columns[i + 1]).getData()[row_num];

        }
        derivative *= (2 * learning_rate);

        bias += derivative;
        for (size_t i = 0; i < weights.size(); ++i)
        {
            weights[i] += derivative * static_cast<const ColumnVector<Float64> &>(*columns[i + 1]).getData()[row_num];;
        }

        ++iter_num;
    }

    void merge(const LinearRegressionData & rhs)
    {
        if (iter_num == 0 && rhs.iter_num == 0)
            return;

        Float64 frac = static_cast<Float64>(iter_num) / (iter_num + rhs.iter_num);
        Float64 rhs_frac = static_cast<Float64>(rhs.iter_num) / (iter_num + rhs.iter_num);

        for (size_t i = 0; i < weights.size(); ++i)
        {
            weights[i] = weights[i] * frac + rhs.weights[i] * rhs_frac;
        }

        bias = bias * frac + rhs.bias * rhs_frac;
        iter_num += rhs.iter_num;
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(bias, buf);
        writeBinary(weights, buf);
        writeBinary(iter_num, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(bias, buf);
        readBinary(weights, buf);
        readBinary(iter_num, buf);
    }

    Float64 predict(const std::vector<Float64>& predict_feature) const
    {
        Float64 res{0.0};
        for (size_t i = 0; i < predict_feature.size(); ++i)
        {
            res += predict_feature[i] * weights[i];
        }
        res += bias;

        return res;
    }
};

template <
        /// Implemented Machine Learning method
        typename Data,
        /// Name of the method
        typename Name
>
class AggregateFunctionMLMethod final : public IAggregateFunctionDataHelper<Data, AggregateFunctionMLMethod<Data, Name>>
{
public:
    String getName() const override { return Name::name; }

    explicit AggregateFunctionMLMethod(UInt32 param_num, Float64 learning_rate)
            : param_num(param_num), learning_rate(learning_rate)
    {
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    void create(AggregateDataPtr place) const override
    {
        new (place) Data(learning_rate, param_num);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & target = static_cast<const ColumnVector<Float64> &>(*columns[0]);

        this->data(place).add(target.getData()[row_num], columns, row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void predictResultInto(ConstAggregateDataPtr place, IColumn & to, Block & block, size_t row_num, const ColumnNumbers & arguments) const
    {
        if (arguments.size() != param_num + 1)
            throw Exception("Predict got incorrect number of arguments. Got: " + std::to_string(arguments.size()) + ". Required: " + std::to_string(param_num + 1),
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto &column = dynamic_cast<ColumnVector<Float64> &>(to);

        std::vector<Float64> predict_features(arguments.size() - 1);
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto& element = (*block.getByPosition(arguments[i]).column)[row_num];
            if (element.getType() != Field::Types::Float64)
                throw Exception("Prediction arguments must be values of type Float",
                        ErrorCodes::BAD_ARGUMENTS);

            predict_features[i - 1] = element.get<Float64>();
        }
        column.getData().push_back(this->data(place).predict(predict_features));
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto &column = dynamic_cast<ColumnVector<Float64> &>(to);
        std::ignore = column;
        std::ignore = place;
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

private:
    UInt32 param_num;
    Float64 learning_rate;
};

struct NameLinearRegression { static constexpr auto name = "LinearRegression"; };

}
