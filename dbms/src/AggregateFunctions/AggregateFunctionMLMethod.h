#pragma once

#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include "IAggregateFunction.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

/**
GradientComputer class computes gradient according to its loss function
*/
class IGradientComputer
{
public:
    IGradientComputer() {}

    virtual ~IGradientComputer() = default;

    /// Adds computed gradient in new point (weights, bias) to batch_gradient
    virtual void compute(
        std::vector<Float64> & batch_gradient,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 learning_rate,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num)
        = 0;

    virtual void predict(
        ColumnVector<Float64>::Container & container,
        Block & block,
        const ColumnNumbers & arguments,
        const std::vector<Float64> & weights,
        Float64 bias,
        const Context & context) const = 0;
};


class LinearRegression : public IGradientComputer
{
public:
    LinearRegression() {}

    void compute(
        std::vector<Float64> & batch_gradient,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 learning_rate,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num) override;

    void predict(
        ColumnVector<Float64>::Container & container,
        Block & block,
        const ColumnNumbers & arguments,
        const std::vector<Float64> & weights,
        Float64 bias,
        const Context & context) const override;
};


class LogisticRegression : public IGradientComputer
{
public:
    LogisticRegression() {}

    void compute(
        std::vector<Float64> & batch_gradient,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 learning_rate,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num) override;

    void predict(
        ColumnVector<Float64>::Container & container,
        Block & block,
        const ColumnNumbers & arguments,
        const std::vector<Float64> & weights,
        Float64 bias,
        const Context & context) const override;
};


/**
* IWeightsUpdater class defines the way to update current weights
* and uses GradientComputer class on each iteration
*/
class IWeightsUpdater
{
public:
    virtual ~IWeightsUpdater() = default;

    /// Calls GradientComputer to update current mini-batch
    virtual void add_to_batch(
        std::vector<Float64> & batch_gradient,
        IGradientComputer & gradient_computer,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 learning_rate,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num);

    /// Updates current weights according to the gradient from the last mini-batch
    virtual void update(UInt32 batch_size, std::vector<Float64> & weights, Float64 & bias, const std::vector<Float64> & gradient) = 0;

    /// Used during the merge of two states
    virtual void merge(const IWeightsUpdater &, Float64, Float64) {}

    /// Used for serialization when necessary
    virtual void write(WriteBuffer &) const {}

    /// Used for serialization when necessary
    virtual void read(ReadBuffer &) {}
};


class StochasticGradientDescent : public IWeightsUpdater
{
public:
    void update(UInt32 batch_size, std::vector<Float64> & weights, Float64 & bias, const std::vector<Float64> & batch_gradient) override;
};


class Momentum : public IWeightsUpdater
{
public:
    Momentum() {}

    Momentum(Float64 alpha) : alpha_(alpha) {}

    void update(UInt32 batch_size, std::vector<Float64> & weights, Float64 & bias, const std::vector<Float64> & batch_gradient) override;

    virtual void merge(const IWeightsUpdater & rhs, Float64 frac, Float64 rhs_frac) override;

    void write(WriteBuffer & buf) const override;

    void read(ReadBuffer & buf) override;

private:
    Float64 alpha_{0.1};
    std::vector<Float64> accumulated_gradient;
};


class Nesterov : public IWeightsUpdater
{
public:
    Nesterov() {}

    Nesterov(Float64 alpha) : alpha_(alpha) {}

    void add_to_batch(
        std::vector<Float64> & batch_gradient,
        IGradientComputer & gradient_computer,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 learning_rate,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num) override;

    void update(UInt32 batch_size, std::vector<Float64> & weights, Float64 & bias, const std::vector<Float64> & batch_gradient) override;

    virtual void merge(const IWeightsUpdater & rhs, Float64 frac, Float64 rhs_frac) override;

    void write(WriteBuffer & buf) const override;

    void read(ReadBuffer & buf) override;

private:
    Float64 alpha_{0.1};
    std::vector<Float64> accumulated_gradient;
};


/**
* LinearModelData is a class which manages current state of learning
*/
class LinearModelData
{
public:
    LinearModelData() {}

    LinearModelData(
        Float64 learning_rate,
        Float64 l2_reg_coef,
        UInt32 param_num,
        UInt32 batch_capacity,
        std::shared_ptr<IGradientComputer> gradient_computer,
        std::shared_ptr<IWeightsUpdater> weights_updater);

    void add(const IColumn ** columns, size_t row_num);

    void merge(const LinearModelData & rhs);

    void write(WriteBuffer & buf) const;

    void read(ReadBuffer & buf);

    void
    predict(ColumnVector<Float64>::Container & container, Block & block, const ColumnNumbers & arguments, const Context & context) const;

private:
    std::vector<Float64> weights;
    Float64 bias{0.0};

    Float64 learning_rate;
    Float64 l2_reg_coef;
    UInt32 batch_capacity;

    UInt32 iter_num = 0;
    std::vector<Float64> gradient_batch;
    UInt32 batch_size;

    std::shared_ptr<IGradientComputer> gradient_computer;
    std::shared_ptr<IWeightsUpdater> weights_updater;

    /**
     * The function is called when we want to flush current batch and update our weights
     */
    void update_state();
};


template <
    /// Implemented Machine Learning method
    typename Data,
    /// Name of the method
    typename Name>
class AggregateFunctionMLMethod final : public IAggregateFunctionDataHelper<Data, AggregateFunctionMLMethod<Data, Name>>
{
public:
    String getName() const override { return Name::name; }

    explicit AggregateFunctionMLMethod(
        UInt32 param_num,
        std::shared_ptr<IGradientComputer> gradient_computer,
        std::shared_ptr<IWeightsUpdater> weights_updater,
        Float64 learning_rate,
        Float64 l2_reg_coef,
        UInt32 batch_size,
        const DataTypes & arguments_types,
        const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionMLMethod<Data, Name>>(arguments_types, params)
        , param_num(param_num)
        , learning_rate(learning_rate)
        , l2_reg_coef(l2_reg_coef)
        , batch_size(batch_size)
        , gradient_computer(std::move(gradient_computer))
        , weights_updater(std::move(weights_updater))
    {
    }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<Float64>>(); }

    void create(AggregateDataPtr place) const override
    {
        new (place) Data(learning_rate, l2_reg_coef, param_num, batch_size, gradient_computer, weights_updater);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(columns, row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override { this->data(place).merge(this->data(rhs)); }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override { this->data(place).write(buf); }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override { this->data(place).read(buf); }

    void predictValues(
        ConstAggregateDataPtr place, IColumn & to, Block & block, const ColumnNumbers & arguments, const Context & context) const override
    {
        if (arguments.size() != param_num + 1)
            throw Exception(
                "Predict got incorrect number of arguments. Got: " + std::to_string(arguments.size())
                    + ". Required: " + std::to_string(param_num + 1),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto & column = dynamic_cast<ColumnVector<Float64> &>(to);

        this->data(place).predict(column.getData(), block, arguments, context);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        std::ignore = place;
        std::ignore = to;
        throw std::runtime_error("not implemented");
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

private:
    UInt32 param_num;
    Float64 learning_rate;
    Float64 l2_reg_coef;
    UInt32 batch_size;
    std::shared_ptr<IGradientComputer> gradient_computer;
    std::shared_ptr<IWeightsUpdater> weights_updater;
};

struct NameLinearRegression
{
    static constexpr auto name = "linearRegression";
};
struct NameLogisticRegression
{
    static constexpr auto name = "logisticRegression";
};
}
