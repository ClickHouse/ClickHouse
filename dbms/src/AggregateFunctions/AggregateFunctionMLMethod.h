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

/**
IGradientComputer class computes gradient according to its loss function
and stores mini-batch
*/
class IGradientComputer
{
public:
    IGradientComputer()
    {}

    virtual ~IGradientComputer() = default;

    /// Adds to batch_gradient computed gradient in point (weigts, bias) using corresponding loss function
    virtual void compute(std::vector<Float64> * batch_gradient, const std::vector<Float64> &weights, Float64 bias,
                         Float64 learning_rate, Float64 target, const IColumn **columns, size_t row_num) = 0;

    virtual Float64 predict(const std::vector<Float64> &predict_feature,
                            const std::vector<Float64> &weights,
                            Float64 bias) const = 0;

    /// Now we should use predict_for_all function instead of predict
    virtual void predict_for_all(ColumnVector<Float64>::Container &container,
                                 Block &block, const ColumnNumbers &arguments,
                                 const std::vector<Float64> &weights,
                                 Float64 bias) const = 0;
};


class LinearRegression : public IGradientComputer
{
public:
    LinearRegression()
    {}

    void compute(std::vector<Float64> * batch_gradient, const std::vector<Float64> &weights, Float64 bias,
                 Float64 learning_rate, Float64 target, const IColumn **columns, size_t row_num) override
    {
        Float64 derivative = (target - bias);
        for (size_t i = 0; i < weights.size(); ++i)
        {
            derivative -= weights[i] * static_cast<const ColumnVector<Float64> &>(*columns[i]).getData()[row_num];
        }
        derivative *= (2 * learning_rate);

        (*batch_gradient)[weights.size()] += derivative;
        for (size_t i = 0; i < weights.size(); ++i)
        {
            (*batch_gradient)[i] +=
                    derivative * static_cast<const ColumnVector<Float64> &>(*columns[i]).getData()[row_num];
        }
    }

    Float64 predict(const std::vector<Float64> &predict_feature,
                    const std::vector<Float64> &weights, Float64 bias) const override
    {
        /// не обновляем веса при предикте, т.к. это может замедлить предсказание
        /// однако можно например обновлять их при каждом мердже не зависимо от того, сколько элементнов в батче

        Float64 res{0.0};
        for (size_t i = 0; i < predict_feature.size(); ++i)
        {
            res += predict_feature[i] * weights[i];
        }
        res += bias;

        return res;
    }

    void predict_for_all(ColumnVector<Float64>::Container &container,
                         Block &block,
                         const ColumnNumbers &arguments,
                         const std::vector<Float64> &weights, Float64 bias) const override
    {
        size_t rows_num = block.rows();
        std::vector<Float64> results(rows_num, bias);

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            ColumnPtr cur_col = block.getByPosition(arguments[i]).column;
            for (size_t row_num = 0; row_num != rows_num; ++row_num)
            {

                const auto &element = (*cur_col)[row_num];
                if (element.getType() != Field::Types::Float64)
                    throw Exception("Prediction arguments must be values of type Float",
                                    ErrorCodes::BAD_ARGUMENTS);

                results[row_num] += weights[i - 1] * element.get<Float64>();
            }
        }

        for (size_t row_num = 0; row_num != rows_num; ++row_num)
        {
            container.emplace_back(results[row_num]);
        }
    }
};


class LogisticRegression : public IGradientComputer
{
public:
    LogisticRegression()
    {}

    void compute(std::vector<Float64> * batch_gradient, const std::vector<Float64> &weights, Float64 bias,
                 Float64 learning_rate, Float64 target, const IColumn **columns, size_t row_num) override
    {
        Float64 derivative = bias;
        for (size_t i = 0; i < weights.size(); ++i)
        {
            derivative += weights[i] * static_cast<const ColumnVector<Float64> &>(*columns[i]).getData()[row_num];
        }
        derivative *= target;
        derivative = exp(derivative);

        (*batch_gradient)[weights.size()] += learning_rate * target / (derivative + 1);;
        for (size_t i = 0; i < weights.size(); ++i)
        {
            (*batch_gradient)[i] +=
                    learning_rate * target *
                    static_cast<const ColumnVector<Float64> &>(*columns[i]).getData()[row_num]
                    / (derivative + 1);
        }
    }

    Float64 predict(const std::vector<Float64> &predict_feature,
                    const std::vector<Float64> &weights, Float64 bias) const override
    {
        /// не обновляем веса при предикте, т.к. это может замедлить предсказание
        /// однако можно например обновлять их при каждом мердже не зависимо от того, сколько элементнов в батче

        Float64 res{0.0};
        for (size_t i = 0; i < predict_feature.size(); ++i)
        {
            res += predict_feature[i] * weights[i];
        }
        res += bias;
        res = 1 / (1 + exp(-res));
        return res;
    }

    void predict_for_all(ColumnVector<Float64>::Container & container,
                         Block & block,
                         const ColumnNumbers & arguments,
                         const std::vector<Float64> & weights, Float64 bias) const override
    {
        size_t rows_num = block.rows();
        std::vector<Float64> results(rows_num, bias);

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            ColumnPtr cur_col = block.getByPosition(arguments[i]).column;
            for (size_t row_num = 0; row_num != rows_num; ++row_num)
            {
                const auto &element = (*cur_col)[row_num];
                if (element.getType() != Field::Types::Float64)
                    throw Exception("Prediction arguments must be values of type Float",
                                    ErrorCodes::BAD_ARGUMENTS);

                results[row_num] += weights[i - 1] * element.get<Float64>();
            }
        }
        for (size_t row_num = 0; row_num != rows_num; ++row_num)
        {
            container.emplace_back(1 / (1 + exp(-results[row_num])));
        }
    }
};


/**
* IWeightsUpdater class defines the way to update current state
* and uses GradientComputer on each iteration
*/
class IWeightsUpdater
{
public:
    virtual ~IWeightsUpdater() = default;

    virtual void add_to_batch(std::vector<Float64> * batch_gradient, std::shared_ptr<IGradientComputer> gc,
                              const std::vector<Float64> & weights, Float64 bias,
                              Float64 learning_rate, Float64 target, const IColumn **columns, size_t row_num)
    {
        gc->compute(batch_gradient, weights, bias, learning_rate, target, columns, row_num);
    }

    virtual void update(UInt32 batch_size,
                        std::vector<Float64> & weights, Float64 & bias,
                        const std::vector<Float64> & gradient) = 0;

    virtual void merge(const IWeightsUpdater &, Float64, Float64)
    {}

    virtual void write(WriteBuffer &) const
    {}

    virtual void read(ReadBuffer &)
    {}
};


class StochasticGradientDescent : public IWeightsUpdater
{
public:
    void update(UInt32 batch_size,
                std::vector<Float64> & weights, Float64 & bias,
                const std::vector<Float64> & batch_gradient) override
    {
        /// batch_size is already checked to be greater than  0
        for (size_t i = 0; i < weights.size(); ++i)
        {
            weights[i] += batch_gradient[i] / batch_size;
        }
        bias += batch_gradient[weights.size()] / batch_size;
    }
};


class Momentum : public IWeightsUpdater
{
public:
    Momentum()
    {}

    Momentum(Float64 alpha) : alpha_(alpha)
    {}

    void update(UInt32 batch_size,
                std::vector<Float64> & weights, Float64 & bias,
                const std::vector<Float64> & batch_gradient) override
    {
        /// batch_size is already checked to be greater than 0
        if (accumulated_gradient.empty())
        {
            accumulated_gradient.resize(batch_gradient.size(), Float64{0.0});
        }

        for (size_t i = 0; i < batch_gradient.size(); ++i)
        {
            accumulated_gradient[i] = accumulated_gradient[i] * alpha_ + batch_gradient[i] / batch_size;
        }
        for (size_t i = 0; i < weights.size(); ++i)
        {
            weights[i] += accumulated_gradient[i];
        }
        bias += accumulated_gradient[weights.size()];
    }

    virtual void merge(const IWeightsUpdater & rhs, Float64 frac, Float64 rhs_frac) override
    {
        auto &momentum_rhs = static_cast<const Momentum &>(rhs);
        for (size_t i = 0; i < accumulated_gradient.size(); ++i)
        {
            accumulated_gradient[i] = accumulated_gradient[i] * frac +
                                      momentum_rhs.accumulated_gradient[i] * rhs_frac;
        }
    }

    void write(WriteBuffer &buf) const override
    {
        writeBinary(accumulated_gradient, buf);
    }

    void read(ReadBuffer &buf) override
    {
        readBinary(accumulated_gradient, buf);
    }

private:
    Float64 alpha_{0.1};
    std::vector<Float64> accumulated_gradient;
};


class Nesterov : public IWeightsUpdater
{
public:
    Nesterov()
    {}

    Nesterov(Float64 alpha) : alpha_(alpha)
    {}

    void add_to_batch(std::vector<Float64> * batch_gradient, std::shared_ptr<IGradientComputer> gc,
                      const std::vector<Float64> & weights, Float64 bias,
                      Float64 learning_rate, Float64 target, const IColumn ** columns, size_t row_num) override
    {
        if (accumulated_gradient.empty())
        {
            accumulated_gradient.resize(batch_gradient->size(), Float64{0.0});
        }

        std::vector<Float64> shifted_weights(weights.size());
        for (size_t i = 0; i != shifted_weights.size(); ++i)
        {
            shifted_weights[i] = weights[i] + accumulated_gradient[i] * alpha_;
        }
        auto shifted_bias = bias + accumulated_gradient[weights.size()] * alpha_;

        gc->compute(batch_gradient, shifted_weights, shifted_bias, learning_rate, target, columns, row_num);
    }

    void update(UInt32 batch_size,
                std::vector<Float64> & weights, Float64 & bias,
                const std::vector<Float64> & batch_gradient) override
    {
        if (accumulated_gradient.empty())
        {
            accumulated_gradient.resize(batch_gradient.size(), Float64{0.0});
        }

        for (size_t i = 0; i < batch_gradient.size(); ++i)
        {
            accumulated_gradient[i] = accumulated_gradient[i] * alpha_ + batch_gradient[i] / batch_size;
        }
        for (size_t i = 0; i < weights.size(); ++i)
        {
            weights[i] += accumulated_gradient[i];
        }
        bias += accumulated_gradient[weights.size()];
    }

    virtual void merge(const IWeightsUpdater & rhs, Float64 frac, Float64 rhs_frac) override
    {
        auto & nesterov_rhs = static_cast<const Nesterov &>(rhs);
        for (size_t i = 0; i < accumulated_gradient.size(); ++i)
        {
            accumulated_gradient[i] =
                    accumulated_gradient[i] * frac + nesterov_rhs.accumulated_gradient[i] * rhs_frac;
        }
    }

    void write(WriteBuffer &buf) const override
    {
        writeBinary(accumulated_gradient, buf);
    }

    void read(ReadBuffer &buf) override
    {
        readBinary(accumulated_gradient, buf);
    }

private:
    Float64 alpha_{0.1};
    std::vector<Float64> accumulated_gradient;
};


// TODO: проверить после изменения логики моментума
/*
class Adam : public IWeightsUpdater
{
public:
    Adam()
    {}

    Adam(Float64 betta1, Float64 betta2) : betta1_(betta1), betta2_(betta2), betta1t_(betta1), betta2t_(betta2)
    {}

    void update(UInt32 cur_batch,
                std::vector<Float64> & weights, Float64 & bias,
                std::vector<Float64> * batch_gradient) override
    {
        if (mt_.size() == 0)
        {
            mt_.resize(batch_gradient.size(), Float64{0.0});
            vt_.resize(batch_gradient.size(), Float64{0.0});
        }
        Float64 eps = 0.01;
        for (size_t i = 0; i < batch_gradient.size(); ++i)
        {
            mt_[i] = mt_[i] * betta1_ + (1 - betta1_) * batch_gradient[i];
            vt_[i] = vt_[i] * betta2_ + (1 - betta2_) * batch_gradient[i] * batch_gradient[i];
            if (t < 8)
            {
                mt_[i] = mt_[i] / (1 - betta1t_);
                betta1t_ *= betta1_;
            }
            if (t < 850)
            {
                vt_[i] = vt_[i] / (1 - betta2t_);
                betta2t_ *= betta2_;
            }
        }
        for (size_t i = 0; i < weights.size(); ++i)
        {
            weights[i] += (mt_[i] / (sqrt(vt_[i] + eps))) / cur_batch;
        }
        bias += (mt_[weights.size()] / (sqrt(vt_[weights.size()] + eps))) / cur_batch;
        t += 1;
    }

    virtual void merge(const IWeightsUpdater &rhs, Float64 frac, Float64 rhs_frac) override
    {
        auto &adam_rhs = static_cast<const Adam &>(rhs);
        for (size_t i = 0; i < mt_.size(); ++i)
        {
            mt_[i] = mt_[i] * frac + adam_rhs.mt_[i] * rhs_frac;
            vt_[i] = vt_[i] * frac + adam_rhs.vt_[i] * rhs_frac;
        }
    }

private:
    Float64 betta1_{0.2};
    Float64 betta2_{0.3};
    Float64 betta1t_{0.3};
    Float64 betta2t_{0.3};
    UInt32 t = 0;
    std::vector<Float64> mt_;
    std::vector<Float64> vt_;
};
 */


/**
* LinearModelData is a class which manages current state of learning
* and is stored as AggregateFunctionState
*/
class LinearModelData
{
public:
    LinearModelData()
    {}

    LinearModelData(Float64 learning_rate,
                    UInt32 param_num,
                    UInt32 batch_capacity,
                    std::shared_ptr<IGradientComputer> gc,
                    std::shared_ptr<IWeightsUpdater> wu)
    : learning_rate(learning_rate),
      batch_capacity(batch_capacity),
      batch_size(0),
      gradient_computer(std::move(gc)),
      weights_updater(std::move(wu))
    {
        weights.resize(param_num, Float64{0.0});
        gradient_batch.resize(param_num + 1, Float64{0.0});
    }

    void add(const IColumn **columns, size_t row_num)
    {
        /// first column stores target; features start from (columns + 1)
        const auto &target = static_cast<const ColumnVector<Float64> &>(*columns[0]).getData()[row_num];

        /// Here we have columns + 1 as first column corresponds to target value, and others - to features
        weights_updater->add_to_batch(&gradient_batch, gradient_computer,
                                      weights, bias, learning_rate, target, columns + 1, row_num);

        ++batch_size;
        if (batch_size == batch_capacity)
        {
            update_state();
        }
    }

    void merge(const LinearModelData &rhs)
    {
        if (iter_num == 0 && rhs.iter_num == 0)
            return;

        update_state();
        /// нельзя обновить из-за константости
//        rhs.update_weights();

        Float64 frac = static_cast<Float64>(iter_num) / (iter_num + rhs.iter_num);
        Float64 rhs_frac = static_cast<Float64>(rhs.iter_num) / (iter_num + rhs.iter_num);

        for (size_t i = 0; i < weights.size(); ++i)
        {
            weights[i] = weights[i] * frac + rhs.weights[i] * rhs_frac;
        }

        bias = bias * frac + rhs.bias * rhs_frac;
        iter_num += rhs.iter_num;
        weights_updater->merge(*rhs.weights_updater, frac, rhs_frac);
    }

    void write(WriteBuffer &buf) const
    {
        writeBinary(bias, buf);
        writeBinary(weights, buf);
        writeBinary(iter_num, buf);
        writeBinary(gradient_batch, buf);
        writeBinary(batch_size, buf);
        weights_updater->write(buf);
    }

    void read(ReadBuffer &buf)
    {
        readBinary(bias, buf);
        readBinary(weights, buf);
        readBinary(iter_num, buf);
        readBinary(gradient_batch, buf);
        readBinary(batch_size, buf);
        weights_updater->read(buf);
    }

    Float64 predict(const std::vector<Float64> &predict_feature) const
    {
        /// не обновляем веса при предикте, т.к. это может замедлить предсказание
        /// однако можно например обновлять их при каждом мердже независимо от того, сколько элементнов в батче
//        if (cur_batch)
//        {
//            update_weights();
//        }

        return gradient_computer->predict(predict_feature, weights, bias);
    }

    void predict_for_all(ColumnVector<Float64>::Container &container, Block &block, const ColumnNumbers &arguments) const
    {
        gradient_computer->predict_for_all(container, block, arguments, weights, bias);
    }

private:
    std::vector<Float64> weights;
    Float64 bias{0.0};

    Float64 learning_rate;
    UInt32 batch_capacity;

    UInt32 iter_num = 0;
    std::vector<Float64> gradient_batch;
    UInt32 batch_size;

    std::shared_ptr<IGradientComputer> gradient_computer;
    std::shared_ptr<IWeightsUpdater> weights_updater;

    /**
     * The function is called when we want to flush current batch and make a step with it
    */
    void update_state()
    {
        if (batch_size == 0)
            return;

        weights_updater->update(batch_size, weights, bias, gradient_batch);
        batch_size = 0;
        ++iter_num;
        gradient_batch.assign(gradient_batch.size(), Float64{0.0});
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

    explicit AggregateFunctionMLMethod(UInt32 param_num,
                                       std::shared_ptr<IGradientComputer> gradient_computer,
                                       std::shared_ptr<IWeightsUpdater> weights_updater,
                                       Float64 learning_rate,
                                       UInt32 batch_size,
                                       const DataTypes & argument_types,
                                       const Array & params)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionMLMethod<Data, Name>>(argument_types, params),
            param_num(param_num),
            learning_rate(learning_rate),
            batch_size(batch_size),
            gc(std::move(gradient_computer)),
            wu(std::move(weights_updater)) {
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    void create(AggregateDataPtr place) const override
    {
        new (place) Data(learning_rate, param_num, batch_size, gc, wu);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(columns, row_num);
    }

    /// хочется не константный rhs
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

    void predictResultInto(ConstAggregateDataPtr place, IColumn & to, Block & block, const ColumnNumbers & arguments) const
    {
        if (arguments.size() != param_num + 1)
            throw Exception("Predict got incorrect number of arguments. Got: " +
                            std::to_string(arguments.size()) + ". Required: " + std::to_string(param_num + 1),
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto &column = dynamic_cast<ColumnVector<Float64> &>(to);

        /// Так делали с одним предиктом, пока пусть побудет тут
//        std::vector<Float64> predict_features(arguments.size() - 1);
//        for (size_t i = 1; i < arguments.size(); ++i)
//        {
//            const auto& element = (*block.getByPosition(arguments[i]).column)[row_num];
//            if (element.getType() != Field::Types::Float64)
//                throw Exception("Prediction arguments must be values of type Float",
//                        ErrorCodes::BAD_ARGUMENTS);
//
////            predict_features[i - 1] = element.get<Float64>();
//        }
//        column.getData().push_back(this->data(place).predict(predict_features));
//        column.getData().push_back(this->data(place).predict_for_all());
        this->data(place).predict_for_all(column.getData(), block, arguments);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        std::ignore = place;
        std::ignore = to;
        return;
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

private:
    UInt32 param_num;
    Float64 learning_rate;
    UInt32 batch_size;
    std::shared_ptr<IGradientComputer> gc;
    std::shared_ptr<IWeightsUpdater> wu;
};

struct NameLinearRegression { static constexpr auto name = "LinearRegression"; };
struct NameLogisticRegression { static constexpr auto name = "LogisticRegression"; };
}
