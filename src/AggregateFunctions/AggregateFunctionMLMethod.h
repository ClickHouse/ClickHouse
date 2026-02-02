#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/typeid_cast.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/**
GradientComputer class computes gradient according to its loss function
*/
class IGradientComputer
{
public:
    IGradientComputer() = default;

    virtual ~IGradientComputer() = default;

    /// Adds computed gradient in new point (weights, bias) to batch_gradient
    virtual void compute(
        std::vector<Float64> & batch_gradient,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num) = 0;

    virtual void predict(
        ColumnVector<Float64>::Container & container,
        const ColumnsWithTypeAndName & arguments,
        size_t offset,
        size_t limit,
        const std::vector<Float64> & weights,
        Float64 bias,
        ContextPtr context) const = 0;
};


class LinearRegression : public IGradientComputer
{
public:
    LinearRegression() = default;

    void compute(
        std::vector<Float64> & batch_gradient,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num) override;

    void predict(
        ColumnVector<Float64>::Container & container,
        const ColumnsWithTypeAndName & arguments,
        size_t offset,
        size_t limit,
        const std::vector<Float64> & weights,
        Float64 bias,
        ContextPtr context) const override;
};


class LogisticRegression : public IGradientComputer
{
public:
    LogisticRegression() = default;

    void compute(
        std::vector<Float64> & batch_gradient,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num) override;

    void predict(
        ColumnVector<Float64>::Container & container,
        const ColumnsWithTypeAndName & arguments,
        size_t offset,
        size_t limit,
        const std::vector<Float64> & weights,
        Float64 bias,
        ContextPtr context) const override;
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
    virtual void addToBatch(
        std::vector<Float64> & batch_gradient,
        IGradientComputer & gradient_computer,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num);

    /// Updates current weights according to the gradient from the last mini-batch
    virtual void update(
        UInt64 batch_size,
        std::vector<Float64> & weights,
        Float64 & bias,
        Float64 learning_rate,
        const std::vector<Float64> & gradient) = 0;

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
    void update(UInt64 batch_size, std::vector<Float64> & weights, Float64 & bias, Float64 learning_rate, const std::vector<Float64> & batch_gradient) override;
};


class Momentum : public IWeightsUpdater
{
public:

    explicit Momentum(size_t num_params, Float64 alpha_ = 0.1) : alpha(alpha_)
    {
        accumulated_gradient.resize(num_params + 1, 0);
    }

    void update(UInt64 batch_size, std::vector<Float64> & weights, Float64 & bias, Float64 learning_rate, const std::vector<Float64> & batch_gradient) override;

    void merge(const IWeightsUpdater & rhs, Float64 frac, Float64 rhs_frac) override;

    void write(WriteBuffer & buf) const override;

    void read(ReadBuffer & buf) override;

private:
    Float64 alpha{0.1};
    std::vector<Float64> accumulated_gradient;
};


class Nesterov : public IWeightsUpdater
{
public:
    explicit Nesterov(size_t num_params, Float64 alpha_ = 0.9) : alpha(alpha_)
    {
        accumulated_gradient.resize(num_params + 1, 0);
    }

    void addToBatch(
        std::vector<Float64> & batch_gradient,
        IGradientComputer & gradient_computer,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num) override;

    void update(UInt64 batch_size, std::vector<Float64> & weights, Float64 & bias, Float64 learning_rate, const std::vector<Float64> & batch_gradient) override;

    void merge(const IWeightsUpdater & rhs, Float64 frac, Float64 rhs_frac) override;

    void write(WriteBuffer & buf) const override;

    void read(ReadBuffer & buf) override;

private:
    const Float64 alpha = 0.9;
    std::vector<Float64> accumulated_gradient;
};


class Adam : public IWeightsUpdater
{
public:
    explicit Adam(size_t num_params)
    {
        beta1_powered = beta1;
        beta2_powered = beta2;


        average_gradient.resize(num_params + 1, 0);
        average_squared_gradient.resize(num_params + 1, 0);
    }

    void addToBatch(
            std::vector<Float64> & batch_gradient,
            IGradientComputer & gradient_computer,
            const std::vector<Float64> & weights,
            Float64 bias,
            Float64 l2_reg_coef,
            Float64 target,
            const IColumn ** columns,
            size_t row_num) override;

    void update(UInt64 batch_size, std::vector<Float64> & weights, Float64 & bias, Float64 learning_rate, const std::vector<Float64> & batch_gradient) override;

    void merge(const IWeightsUpdater & rhs, Float64 frac, Float64 rhs_frac) override;

    void write(WriteBuffer & buf) const override;

    void read(ReadBuffer & buf) override;

private:
    /// beta1 and beta2 hyperparameters have such recommended values
    const Float64 beta1 = 0.9;
    const Float64 beta2 = 0.999;
    const Float64 eps = 0.000001;
    Float64 beta1_powered;
    Float64 beta2_powered;

    std::vector<Float64> average_gradient;
    std::vector<Float64> average_squared_gradient;
};


/** LinearModelData is a class which manages current state of learning
  */
class LinearModelData
{
public:
    LinearModelData() = default;

    LinearModelData(
        Float64 learning_rate_,
        Float64 l2_reg_coef_,
        UInt64 param_num_,
        UInt64 batch_capacity_,
        std::shared_ptr<IGradientComputer> gradient_computer_,
        std::shared_ptr<IWeightsUpdater> weights_updater_);

    void add(const IColumn ** columns, size_t row_num);

    void merge(const LinearModelData & rhs);

    void write(WriteBuffer & buf) const;

    void read(ReadBuffer & buf);

    void predict(
        ColumnVector<Float64>::Container & container,
        const ColumnsWithTypeAndName & arguments,
        size_t offset,
        size_t limit,
        ContextPtr context) const;

    void returnWeights(IColumn & to) const;
private:
    std::vector<Float64> weights;
    Float64 bias{0.0};

    Float64 learning_rate;
    Float64 l2_reg_coef;
    UInt64 batch_capacity;

    UInt64 iter_num = 0;
    std::vector<Float64> gradient_batch;
    UInt64 batch_size;

    std::shared_ptr<IGradientComputer> gradient_computer;
    std::shared_ptr<IWeightsUpdater> weights_updater;

    /** The function is called when we want to flush current batch and update our weights
      */
    void updateState();
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
        UInt32 param_num_,
        std::unique_ptr<IGradientComputer> gradient_computer_,
        std::string weights_updater_name_,
        Float64 learning_rate_,
        Float64 l2_reg_coef_,
        UInt64 batch_size_,
        const DataTypes & arguments_types,
        const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionMLMethod<Data, Name>>(arguments_types, params, createResultType())
        , param_num(param_num_)
        , learning_rate(learning_rate_)
        , l2_reg_coef(l2_reg_coef_)
        , batch_size(batch_size_)
        , gradient_computer(std::move(gradient_computer_))
        , weights_updater_name(std::move(weights_updater_name_))
    {
    }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    bool allocatesMemoryInArena() const override { return false; }

    /// This function is called from evalMLMethod function for correct predictValues call
    DataTypePtr getReturnTypeToPredict() const override
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        std::shared_ptr<IWeightsUpdater> new_weights_updater;
        if (weights_updater_name == "SGD")
            new_weights_updater = std::make_shared<StochasticGradientDescent>();
        else if (weights_updater_name == "Momentum")
            new_weights_updater = std::make_shared<Momentum>(param_num);
        else if (weights_updater_name == "Nesterov")
            new_weights_updater = std::make_shared<Nesterov>(param_num);
        else if (weights_updater_name == "Adam")
            new_weights_updater = std::make_shared<Adam>(param_num);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal name of weights updater (should have been checked earlier)");

        new (place) Data(learning_rate, l2_reg_coef, param_num, batch_size, gradient_computer, new_weights_updater);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(columns, row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override { this->data(place).merge(this->data(rhs)); }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override { this->data(place).write(buf); }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override { this->data(place).read(buf); }

    void predictValues(
        ConstAggregateDataPtr __restrict place,
        IColumn & to,
        const ColumnsWithTypeAndName & arguments,
        size_t offset,
        size_t limit,
        ContextPtr context) const override
    {
        if (arguments.size() != param_num + 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Predict got incorrect number of arguments. Got: {}. Required: {}",
                arguments.size(), param_num + 1);

        /// This cast might be correct because column type is based on getReturnTypeToPredict.
        auto * column = typeid_cast<ColumnFloat64 *>(&to);
        if (!column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cast of column of predictions is incorrect. "
                            "getReturnTypeToPredict must return same value as it is cast to");

        this->data(place).predict(column->getData(), arguments, offset, limit, context);
    }

    /** This function is called if aggregate function without State modifier is selected in a query.
     *  Inserts all weights of the model into the column 'to', so user may use such information if needed
     */
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).returnWeights(to);
    }

private:
    UInt64 param_num;
    Float64 learning_rate;
    Float64 l2_reg_coef;
    UInt64 batch_size;
    std::shared_ptr<IGradientComputer> gradient_computer;
    std::string weights_updater_name;
};

struct NameLinearRegression
{
    static constexpr auto name = "stochasticLinearRegression";
};
struct NameLogisticRegression
{
    static constexpr auto name = "stochasticLogisticRegression";
};

}
