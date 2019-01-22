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


namespace DB {

struct LinearRegressionData {
    Float64 bias{0.0};
    std::vector<Float64> w1;
    Float64 learning_rate{0.01};
    UInt32 iter_num = 0;
    UInt32 param_num = 0;


    void add(Float64 target, std::vector<Float64>& feature, Float64 learning_rate_, UInt32 param_num_) {
        if (w1.empty()) {
            learning_rate = learning_rate_;
            param_num = param_num_;
            w1.resize(param_num);
        }

        Float64 derivative = (target - bias);
        for (size_t i = 0; i < param_num; ++i)
        {
            derivative -= w1[i] * feature[i];
        }
        derivative *= (2 * learning_rate);

        bias += derivative;
        for (size_t i = 0; i < param_num; ++i)
        {
            w1[i] += derivative * feature[i];
        }

        ++iter_num;
    }

    void merge(const LinearRegressionData & rhs) {
        if (iter_num == 0 && rhs.iter_num == 0)
            throw std::runtime_error("Strange...");

        if (param_num == 0) {
            param_num = rhs.param_num;
            w1.resize(param_num);
        }

        Float64 frac = static_cast<Float64>(iter_num) / (iter_num + rhs.iter_num);
        Float64 rhs_frac = static_cast<Float64>(rhs.iter_num) / (iter_num + rhs.iter_num);

        for (size_t i = 0; i < param_num; ++i)
        {
            w1[i] = w1[i] * frac + rhs.w1[i] * rhs_frac;
        }

        bias = bias * frac + rhs.bias * rhs_frac;
        iter_num += rhs.iter_num;
    }

    void write(WriteBuffer & buf) const {
        writeBinary(bias, buf);
        writeBinary(w1, buf);
        writeBinary(iter_num, buf);
    }

    void read(ReadBuffer & buf) {
        readBinary(bias, buf);
        readBinary(w1, buf);
        readBinary(iter_num, buf);
    }
    Float64 predict(std::vector<Float64>& predict_feature) const {
        Float64 res{0.0};
        for (size_t i = 0; i < static_cast<size_t>(param_num); ++i)
        {
            res += predict_feature[i] * w1[i];
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

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & target = static_cast<const ColumnVector<Float64> &>(*columns[0]);

        std::vector<Float64> x(param_num);
        for (size_t i = 0; i < param_num; ++i)
        {
            x[i] = static_cast<const ColumnVector<Float64> &>(*columns[i + 1]).getData()[row_num];
        }

        this->data(place).add(target.getData()[row_num], x, learning_rate, param_num);

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
        auto &column = dynamic_cast<ColumnVector<Float64> &>(to);

        std::vector<Float64> predict_features(arguments.size() - 1);
//        for (size_t row_num = 0, rows = block.rows(); row_num < rows; ++row_num) {
        for (size_t i = 1; i < arguments.size(); ++i) {
//            predict_features[i] = array_elements[i].get<Float64>();
            predict_features[i - 1] = applyVisitor(FieldVisitorConvertToNumber<Float64>(), (*block.getByPosition(arguments[i]).column)[row_num]);
        }
//            column.getData().push_back(this->data(place).predict(predict_features));
        column.getData().push_back(this->data(place).predict(predict_features));
//        }
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override {
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