#include "AggregateFunctionMLMethod.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include "AggregateFunctionFactory.h"
#include "FactoryHelpers.h"
#include "Helpers.h"


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

namespace
{
    using FuncLinearRegression = AggregateFunctionMLMethod<LinearModelData, NameLinearRegression>;
    using FuncLogisticRegression = AggregateFunctionMLMethod<LinearModelData, NameLogisticRegression>;
    template <typename Method>
    AggregateFunctionPtr createAggregateFunctionMLMethod(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        if (parameters.size() > 4)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "Aggregate function {} requires at most four parameters: "
                "learning_rate, l2_regularization_coef, mini-batch size and weights_updater method", name);

        if (argument_types.size() < 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Aggregate function {} requires at least two arguments: target and model's parameters", name);

        for (size_t i = 0; i < argument_types.size(); ++i)
        {
            if (!isNativeNumber(argument_types[i]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Argument {} of type {} must be numeric for aggregate function {}",
                                i, argument_types[i]->getName(), name);
        }

        /// Such default parameters were picked because they did good on some tests,
        /// though it still requires to fit parameters to achieve better result
        auto learning_rate = static_cast<Float64>(1.0);
        auto l2_reg_coef = static_cast<Float64>(0.5);
        UInt64 batch_size = 15;

        std::string weights_updater_name = "Adam";
        std::unique_ptr<IGradientComputer> gradient_computer;

        if (!parameters.empty())
        {
            learning_rate = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
        }
        if (parameters.size() > 1)
        {
            l2_reg_coef = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[1]);
        }
        if (parameters.size() > 2)
        {
            batch_size = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), parameters[2]);
        }
        if (parameters.size() > 3)
        {
            weights_updater_name = parameters[3].safeGet<String>();
            if (weights_updater_name != "SGD" && weights_updater_name != "Momentum" && weights_updater_name != "Nesterov" && weights_updater_name != "Adam")
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid parameter for weights updater. "
                                "The only supported are 'SGD', 'Momentum' and 'Nesterov'");
        }

        if constexpr (std::is_same_v<Method, FuncLinearRegression>)
        {
            gradient_computer = std::make_unique<LinearRegression>();
        }
        else if constexpr (std::is_same_v<Method, FuncLogisticRegression>)
        {
            gradient_computer = std::make_unique<LogisticRegression>();
        }
        else
        {
            []<bool flag = false>() {static_assert(flag, "Such gradient computer is not implemented yet");}(); // delay static_asssert in constexpr if until template instantiation
        }

        return std::make_shared<Method>(
            argument_types.size() - 1,
            std::move(gradient_computer),
            weights_updater_name,
            learning_rate,
            l2_reg_coef,
            batch_size,
            argument_types,
            parameters);
    }
}

void registerAggregateFunctionMLMethod(AggregateFunctionFactory & factory)
{
    factory.registerFunction("stochasticLinearRegression", createAggregateFunctionMLMethod<FuncLinearRegression>);
    factory.registerFunction("stochasticLogisticRegression", createAggregateFunctionMLMethod<FuncLogisticRegression>);
}

LinearModelData::LinearModelData(
    Float64 learning_rate_,
    Float64 l2_reg_coef_,
    UInt64 param_num_,
    UInt64 batch_capacity_,
    std::shared_ptr<DB::IGradientComputer> gradient_computer_,
    std::shared_ptr<DB::IWeightsUpdater> weights_updater_)
    : learning_rate(learning_rate_)
    , l2_reg_coef(l2_reg_coef_)
    , batch_capacity(batch_capacity_)
    , batch_size(0)
    , gradient_computer(std::move(gradient_computer_))
    , weights_updater(std::move(weights_updater_))
{
    weights.resize(param_num_, Float64{0.0});
    gradient_batch.resize(param_num_ + 1, Float64{0.0});
}

void LinearModelData::updateState()
{
    if (batch_size == 0)
        return;

    weights_updater->update(batch_size, weights, bias, learning_rate, gradient_batch);
    batch_size = 0;
    ++iter_num;
    gradient_batch.assign(gradient_batch.size(), Float64{0.0});
}

void LinearModelData::predict(
    ColumnVector<Float64>::Container & container,
    const ColumnsWithTypeAndName & arguments,
    size_t offset,
    size_t limit,
    ContextPtr context) const
{
    gradient_computer->predict(container, arguments, offset, limit, weights, bias, context);
}

void LinearModelData::returnWeights(IColumn & to) const
{
    size_t size = weights.size() + 1;

    ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
    ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

    size_t old_size = offsets_to.back();
    offsets_to.push_back(old_size + size);

    typename ColumnFloat64::Container & val_to
            = assert_cast<ColumnFloat64 &>(arr_to.getData()).getData();

    val_to.reserve(old_size + size);
    for (size_t i = 0; i + 1 < size; ++i)
        val_to.push_back(weights[i]);

    val_to.push_back(bias);
}

void LinearModelData::read(ReadBuffer & buf)
{
    readBinary(bias, buf);
    readBinary(weights, buf);
    readBinary(iter_num, buf);
    readBinary(gradient_batch, buf);
    readBinary(batch_size, buf);
    weights_updater->read(buf);
}

void LinearModelData::write(WriteBuffer & buf) const
{
    writeBinary(bias, buf);
    writeBinary(weights, buf);
    writeBinary(iter_num, buf);
    writeBinary(gradient_batch, buf);
    writeBinary(batch_size, buf);
    weights_updater->write(buf);
}

void LinearModelData::merge(const DB::LinearModelData & rhs)
{
    if (iter_num == 0 && rhs.iter_num == 0)
        return;

    updateState();
    /// can't update rhs state because it's constant

    /// squared mean is more stable (in sense of quality of prediction) when two states with quietly different number of learning steps are merged
    Float64 frac = (static_cast<Float64>(iter_num) * iter_num) / (iter_num * iter_num + rhs.iter_num * rhs.iter_num);

    for (size_t i = 0; i < weights.size(); ++i)
    {
        weights[i] = weights[i] * frac + rhs.weights[i] * (1 - frac);
    }
    bias = bias * frac + rhs.bias * (1 - frac);

    iter_num += rhs.iter_num;
    weights_updater->merge(*rhs.weights_updater, frac, 1 - frac);
}

void LinearModelData::add(const IColumn ** columns, size_t row_num)
{
    /// first column stores target; features start from (columns + 1)
    Float64 target = (*columns[0]).getFloat64(row_num);

    /// Here we have columns + 1 as first column corresponds to target value, and others - to features
    weights_updater->addToBatch(
        gradient_batch, *gradient_computer, weights, bias, l2_reg_coef, target, columns + 1, row_num);

    ++batch_size;
    if (batch_size == batch_capacity)
    {
        updateState();
    }
}

/// Weights updaters

void Adam::write(WriteBuffer & buf) const
{
    writeBinary(average_gradient, buf);
    writeBinary(average_squared_gradient, buf);
}

void Adam::read(ReadBuffer & buf)
{
    readBinary(average_gradient, buf);
    readBinary(average_squared_gradient, buf);
}

void Adam::merge(const IWeightsUpdater & rhs, Float64 frac, Float64 rhs_frac)
{
    const auto & adam_rhs = static_cast<const Adam &>(rhs);

    if (adam_rhs.average_gradient.empty())
        return;

    average_gradient.resize(adam_rhs.average_gradient.size(), Float64{0.0});
    average_squared_gradient.resize(adam_rhs.average_squared_gradient.size(), Float64{0.0});

    for (size_t i = 0; i < average_gradient.size(); ++i)
    {
        average_gradient[i] = average_gradient[i] * frac + adam_rhs.average_gradient[i] * rhs_frac;
        average_squared_gradient[i] = average_squared_gradient[i] * frac + adam_rhs.average_squared_gradient[i] * rhs_frac;
    }
    beta1_powered *= adam_rhs.beta1_powered;
    beta2_powered *= adam_rhs.beta2_powered;
}

void Adam::update(UInt64 batch_size, std::vector<Float64> & weights, Float64 & bias, Float64 learning_rate, const std::vector<Float64> & batch_gradient)
{
    average_gradient.resize(batch_gradient.size(), Float64{0.0});
    average_squared_gradient.resize(batch_gradient.size(), Float64{0.0});

    for (size_t i = 0; i != average_gradient.size(); ++i)
    {
        Float64 normed_gradient = batch_gradient[i] / batch_size;
        average_gradient[i] = beta1 * average_gradient[i] + (1 - beta1) * normed_gradient;
        average_squared_gradient[i] = beta2 * average_squared_gradient[i] +
                (1 - beta2) * normed_gradient * normed_gradient;
    }

    for (size_t i = 0; i < weights.size(); ++i)
    {
        weights[i] += (learning_rate * average_gradient[i]) /
                ((1 - beta1_powered) * (sqrt(average_squared_gradient[i] / (1 - beta2_powered)) + eps));
    }
    bias += (learning_rate * average_gradient[weights.size()]) /
            ((1 - beta1_powered) * (sqrt(average_squared_gradient[weights.size()] / (1 - beta2_powered)) + eps));

    beta1_powered *= beta1;
    beta2_powered *= beta2;
}

void Adam::addToBatch(
        std::vector<Float64> & batch_gradient,
        IGradientComputer & gradient_computer,
        const std::vector<Float64> & weights,
        Float64 bias,
        Float64 l2_reg_coef,
        Float64 target,
        const IColumn ** columns,
        size_t row_num)
{
    if (average_gradient.empty())
    {
        average_gradient.resize(batch_gradient.size(), Float64{0.0});
        average_squared_gradient.resize(batch_gradient.size(), Float64{0.0});
    }
    gradient_computer.compute(batch_gradient, weights, bias, l2_reg_coef, target, columns, row_num);
}

void Nesterov::read(ReadBuffer & buf)
{
    readBinary(accumulated_gradient, buf);
}

void Nesterov::write(WriteBuffer & buf) const
{
    writeBinary(accumulated_gradient, buf);
}

void Nesterov::merge(const IWeightsUpdater & rhs, Float64 frac, Float64 rhs_frac)
{
    const auto & nesterov_rhs = static_cast<const Nesterov &>(rhs);
    accumulated_gradient.resize(nesterov_rhs.accumulated_gradient.size(), Float64{0.0});

    for (size_t i = 0; i < accumulated_gradient.size(); ++i)
    {
        accumulated_gradient[i] = accumulated_gradient[i] * frac + nesterov_rhs.accumulated_gradient[i] * rhs_frac;
    }
}

void Nesterov::update(UInt64 batch_size, std::vector<Float64> & weights, Float64 & bias, Float64 learning_rate, const std::vector<Float64> & batch_gradient)
{
    accumulated_gradient.resize(batch_gradient.size(), Float64{0.0});

    for (size_t i = 0; i < batch_gradient.size(); ++i)
    {
        accumulated_gradient[i] = accumulated_gradient[i] * alpha + (learning_rate * batch_gradient[i]) / batch_size;
    }
    for (size_t i = 0; i < weights.size(); ++i)
    {
        weights[i] += accumulated_gradient[i];
    }
    bias += accumulated_gradient[weights.size()];
}

void Nesterov::addToBatch(
    std::vector<Float64> & batch_gradient,
    IGradientComputer & gradient_computer,
    const std::vector<Float64> & weights,
    Float64 bias,
    Float64 l2_reg_coef,
    Float64 target,
    const IColumn ** columns,
    size_t row_num)
{
    if (accumulated_gradient.empty())
    {
        accumulated_gradient.resize(batch_gradient.size(), Float64{0.0});
    }

    std::vector<Float64> shifted_weights(weights.size());
    for (size_t i = 0; i != shifted_weights.size(); ++i)
    {
        shifted_weights[i] = weights[i] + accumulated_gradient[i] * alpha;
    }
    auto shifted_bias = bias + accumulated_gradient[weights.size()] * alpha;

    gradient_computer.compute(batch_gradient, shifted_weights, shifted_bias, l2_reg_coef, target, columns, row_num);
}

void Momentum::read(ReadBuffer & buf)
{
    readBinary(accumulated_gradient, buf);
}

void Momentum::write(WriteBuffer & buf) const
{
    writeBinary(accumulated_gradient, buf);
}

void Momentum::merge(const IWeightsUpdater & rhs, Float64 frac, Float64 rhs_frac)
{
    const auto & momentum_rhs = static_cast<const Momentum &>(rhs);
    for (size_t i = 0; i < accumulated_gradient.size(); ++i)
    {
        accumulated_gradient[i] = accumulated_gradient[i] * frac + momentum_rhs.accumulated_gradient[i] * rhs_frac;
    }
}

void Momentum::update(UInt64 batch_size, std::vector<Float64> & weights, Float64 & bias, Float64 learning_rate, const std::vector<Float64> & batch_gradient)
{
    /// batch_size is already checked to be greater than 0
    accumulated_gradient.resize(batch_gradient.size(), Float64{0.0});

    for (size_t i = 0; i < batch_gradient.size(); ++i)
    {
        accumulated_gradient[i] = accumulated_gradient[i] * alpha + (learning_rate * batch_gradient[i]) / batch_size;
    }
    for (size_t i = 0; i < weights.size(); ++i)
    {
        weights[i] += accumulated_gradient[i];
    }
    bias += accumulated_gradient[weights.size()];
}

void StochasticGradientDescent::update(
    UInt64 batch_size, std::vector<Float64> & weights, Float64 & bias, Float64 learning_rate, const std::vector<Float64> & batch_gradient)
{
    /// batch_size is already checked to be greater than  0
    for (size_t i = 0; i < weights.size(); ++i)
    {
        weights[i] += (learning_rate * batch_gradient[i]) / batch_size;
    }
    bias += (learning_rate * batch_gradient[weights.size()]) / batch_size;
}

void IWeightsUpdater::addToBatch(
    std::vector<Float64> & batch_gradient,
    IGradientComputer & gradient_computer,
    const std::vector<Float64> & weights,
    Float64 bias,
    Float64 l2_reg_coef,
    Float64 target,
    const IColumn ** columns,
    size_t row_num)
{
    gradient_computer.compute(batch_gradient, weights, bias, l2_reg_coef, target, columns, row_num);
}

/// Gradient computers

void LogisticRegression::predict(
    ColumnVector<Float64>::Container & container,
    const ColumnsWithTypeAndName & arguments,
    size_t offset,
    size_t limit,
    const std::vector<Float64> & weights,
    Float64 bias,
    ContextPtr /*context*/) const
{
    size_t rows_num = arguments.front().column->size();

    if (offset > rows_num || offset + limit > rows_num)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid offset and limit for LogisticRegression::predict. "
                        "Block has {} rows, but offset is {} and limit is {}",
                        rows_num, offset, toString(limit));

    std::vector<Float64> results(limit, bias);

    for (size_t i = 1; i < arguments.size(); ++i)
    {
        const ColumnWithTypeAndName & cur_col = arguments[i];

        if (!isNativeNumber(cur_col.type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prediction arguments must have numeric type");

        const auto & features_column = cur_col.column;

        for (size_t row_num = 0; row_num < limit; ++row_num)
            results[row_num] += weights[i - 1] * features_column->getFloat64(offset + row_num);
    }

    container.reserve(container.size() + limit);
    for (size_t row_num = 0; row_num < limit; ++row_num)
        container.emplace_back(1 / (1 + exp(-results[row_num])));
}

void LogisticRegression::compute(
    std::vector<Float64> & batch_gradient,
    const std::vector<Float64> & weights,
    Float64 bias,
    Float64 l2_reg_coef,
    Float64 target,
    const IColumn ** columns,
    size_t row_num)
{
    Float64 derivative = bias;

    std::vector<Float64> values(weights.size());

    for (size_t i = 0; i < weights.size(); ++i)
    {
        values[i] = (*columns[i]).getFloat64(row_num);
    }

    for (size_t i = 0; i < weights.size(); ++i)
    {
        derivative += weights[i] * values[i];
    }
    derivative *= target;
    derivative = exp(derivative);

    batch_gradient[weights.size()] += target / (derivative + 1);
    for (size_t i = 0; i < weights.size(); ++i)
    {
        batch_gradient[i] += target * values[i] / (derivative + 1) - 2 * l2_reg_coef * weights[i];
    }
}

void LinearRegression::predict(
    ColumnVector<Float64>::Container & container,
    const ColumnsWithTypeAndName & arguments,
    size_t offset,
    size_t limit,
    const std::vector<Float64> & weights,
    Float64 bias,
    ContextPtr /*context*/) const
{
    if (weights.size() + 1 != arguments.size())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "In predict function number of arguments differs from the size of weights vector");
    }

    size_t rows_num = arguments.front().column->size();

    if (offset > rows_num || offset + limit > rows_num)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid offset and limit for LogisticRegression::predict. "
                        "Block has {} rows, but offset is {} and limit is {}",
                        rows_num, offset, toString(limit));

    std::vector<Float64> results(limit, bias);

    for (size_t i = 1; i < arguments.size(); ++i)
    {
        const ColumnWithTypeAndName & cur_col = arguments[i];

        if (!isNativeNumber(cur_col.type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prediction arguments must have numeric type");

        auto features_column = cur_col.column;

        if (!features_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpectedly cannot dynamically cast features column {}", i);

        for (size_t row_num = 0; row_num < limit; ++row_num)
            results[row_num] += weights[i - 1] * features_column->getFloat64(row_num + offset);
    }

    container.reserve(container.size() + limit);
    for (size_t row_num = 0; row_num < limit; ++row_num)
        container.emplace_back(results[row_num]);
}

void LinearRegression::compute(
    std::vector<Float64> & batch_gradient,
    const std::vector<Float64> & weights,
    Float64 bias,
    Float64 l2_reg_coef,
    Float64 target,
    const IColumn ** columns,
    size_t row_num)
{
    Float64 derivative = (target - bias);

    std::vector<Float64> values(weights.size());


    for (size_t i = 0; i < weights.size(); ++i)
    {
        values[i] = (*columns[i]).getFloat64(row_num);
    }

    for (size_t i = 0; i < weights.size(); ++i)
    {
        derivative -= weights[i] * values[i];
    }
    derivative *= 2;

    batch_gradient[weights.size()] += derivative;
    for (size_t i = 0; i < weights.size(); ++i)
    {
        batch_gradient[i] += derivative * values[i] - 2 * l2_reg_coef * weights[i];
    }
}

}
