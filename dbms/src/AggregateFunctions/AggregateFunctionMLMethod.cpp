#include "AggregateFunctionMLMethod.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include "AggregateFunctionFactory.h"
#include "FactoryHelpers.h"
#include "Helpers.h"


namespace DB
{
namespace
{
    using FuncLinearRegression = AggregateFunctionMLMethod<LinearModelData, NameLinearRegression>;
    using FuncLogisticRegression = AggregateFunctionMLMethod<LinearModelData, NameLogisticRegression>;
    template <class Method>
    AggregateFunctionPtr
    createAggregateFunctionMLMethod(const std::string & name, const DataTypes & argument_types, const Array & parameters)
    {
        if (parameters.size() > 4)
            throw Exception(
                "Aggregate function " + name
                    + " requires at most four parameters: learning_rate, l2_regularization_coef, mini-batch size and weights_updater "
                      "method",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (argument_types.size() < 2)
            throw Exception(
                "Aggregate function " + name + " requires at least two arguments: target and model's parameters",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < argument_types.size(); ++i)
        {
            if (!isNativeNumber(argument_types[i]))
                throw Exception(
                    "Argument " + std::to_string(i) + " of type " + argument_types[i]->getName()
                        + " must be numeric for aggregate function " + name,
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        /// Such default parameters were picked because they did good on some tests,
        /// though it still requires to fit parameters to achieve better result
        auto learning_rate = Float64(0.00001);
        auto l2_reg_coef = Float64(0.1);
        UInt32 batch_size = 15;

        std::string weights_updater_name = "\'SGD\'";
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
            batch_size = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), parameters[2]);
        }
        if (parameters.size() > 3)
        {
            weights_updater_name = applyVisitor(FieldVisitorToString(), parameters[3]);
            if (weights_updater_name != "\'SGD\'" && weights_updater_name != "\'Momentum\'" && weights_updater_name != "\'Nesterov\'")
            {
                throw Exception("Invalid parameter for weights updater", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        if (std::is_same<Method, FuncLinearRegression>::value)
        {
            gradient_computer = std::make_unique<LinearRegression>();
        }
        else if (std::is_same<Method, FuncLogisticRegression>::value)
        {
            gradient_computer = std::make_unique<LogisticRegression>();
        }
        else
        {
            throw Exception("Such gradient computer is not implemented yet", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
    Float64 learning_rate,
    Float64 l2_reg_coef,
    UInt32 param_num,
    UInt32 batch_capacity,
    std::shared_ptr<DB::IGradientComputer> gradient_computer,
    std::shared_ptr<DB::IWeightsUpdater> weights_updater)
    : learning_rate(learning_rate)
    , l2_reg_coef(l2_reg_coef)
    , batch_capacity(batch_capacity)
    , batch_size(0)
    , gradient_computer(std::move(gradient_computer))
    , weights_updater(std::move(weights_updater))
{
    weights.resize(param_num, Float64{0.0});
    gradient_batch.resize(param_num + 1, Float64{0.0});
}

void LinearModelData::update_state()
{
    if (batch_size == 0)
        return;

    weights_updater->update(batch_size, weights, bias, gradient_batch);
    batch_size = 0;
    ++iter_num;
    gradient_batch.assign(gradient_batch.size(), Float64{0.0});
}

void LinearModelData::predict(
    ColumnVector<Float64>::Container & container, Block & block, const ColumnNumbers & arguments, const Context & context) const
{
    gradient_computer->predict(container, block, arguments, weights, bias, context);
}

void LinearModelData::returnWeights(IColumn & to) const
{
    size_t size = weights.size() + 1;

    ColumnArray & arr_to = static_cast<ColumnArray &>(to);
    ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

    size_t old_size = offsets_to.back();
    offsets_to.push_back(old_size + size);

    typename ColumnFloat64::Container & val_to
            = static_cast<ColumnFloat64 &>(arr_to.getData()).getData();

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

    update_state();
    /// can't update rhs state because it's constant

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
    weights_updater->add_to_batch(
        gradient_batch, *gradient_computer, weights, bias, learning_rate, l2_reg_coef, target, columns + 1, row_num);

    ++batch_size;
    if (batch_size == batch_capacity)
    {
        update_state();
    }
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
    auto & nesterov_rhs = static_cast<const Nesterov &>(rhs);
    for (size_t i = 0; i < accumulated_gradient.size(); ++i)
    {
        accumulated_gradient[i] = accumulated_gradient[i] * frac + nesterov_rhs.accumulated_gradient[i] * rhs_frac;
    }
}

void Nesterov::update(UInt32 batch_size, std::vector<Float64> & weights, Float64 & bias, const std::vector<Float64> & batch_gradient)
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

void Nesterov::add_to_batch(
    std::vector<Float64> & batch_gradient,
    IGradientComputer & gradient_computer,
    const std::vector<Float64> & weights,
    Float64 bias,
    Float64 learning_rate,
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
        shifted_weights[i] = weights[i] + accumulated_gradient[i] * alpha_;
    }
    auto shifted_bias = bias + accumulated_gradient[weights.size()] * alpha_;

    gradient_computer.compute(batch_gradient, shifted_weights, shifted_bias, learning_rate, l2_reg_coef, target, columns, row_num);
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
    auto & momentum_rhs = static_cast<const Momentum &>(rhs);
    for (size_t i = 0; i < accumulated_gradient.size(); ++i)
    {
        accumulated_gradient[i] = accumulated_gradient[i] * frac + momentum_rhs.accumulated_gradient[i] * rhs_frac;
    }
}

void Momentum::update(UInt32 batch_size, std::vector<Float64> & weights, Float64 & bias, const std::vector<Float64> & batch_gradient)
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

void StochasticGradientDescent::update(
    UInt32 batch_size, std::vector<Float64> & weights, Float64 & bias, const std::vector<Float64> & batch_gradient)
{
    /// batch_size is already checked to be greater than  0
    for (size_t i = 0; i < weights.size(); ++i)
    {
        weights[i] += batch_gradient[i] / batch_size;
    }
    bias += batch_gradient[weights.size()] / batch_size;
}

void IWeightsUpdater::add_to_batch(
    std::vector<Float64> & batch_gradient,
    IGradientComputer & gradient_computer,
    const std::vector<Float64> & weights,
    Float64 bias,
    Float64 learning_rate,
    Float64 l2_reg_coef,
    Float64 target,
    const IColumn ** columns,
    size_t row_num)
{
    gradient_computer.compute(batch_gradient, weights, bias, learning_rate, l2_reg_coef, target, columns, row_num);
}

void LogisticRegression::predict(
    ColumnVector<Float64>::Container & container,
    Block & block,
    const ColumnNumbers & arguments,
    const std::vector<Float64> & weights,
    Float64 bias,
    const Context & context) const
{
    size_t rows_num = block.rows();
    std::vector<Float64> results(rows_num, bias);

    for (size_t i = 1; i < arguments.size(); ++i)
    {
        const ColumnWithTypeAndName & cur_col = block.getByPosition(arguments[i]);
        if (!isNativeNumber(cur_col.type))
        {
            throw Exception("Prediction arguments must have numeric type", ErrorCodes::BAD_ARGUMENTS);
        }

        /// If column type is already Float64 then castColumn simply returns it
        auto features_col_ptr = castColumn(cur_col, std::make_shared<DataTypeFloat64>(), context);
        auto features_column = typeid_cast<const ColumnFloat64 *>(features_col_ptr.get());

        if (!features_column)
        {
            throw Exception("Unexpectedly cannot dynamically cast features column " + std::to_string(i), ErrorCodes::LOGICAL_ERROR);
        }

        for (size_t row_num = 0; row_num != rows_num; ++row_num)
        {
            results[row_num] += weights[i - 1] * features_column->getElement(row_num);
        }
    }

    container.reserve(rows_num);
    for (size_t row_num = 0; row_num != rows_num; ++row_num)
    {
        container.emplace_back(1 / (1 + exp(-results[row_num])));
    }
}

void LogisticRegression::compute(
    std::vector<Float64> & batch_gradient,
    const std::vector<Float64> & weights,
    Float64 bias,
    Float64 learning_rate,
    Float64 l2_reg_coef,
    Float64 target,
    const IColumn ** columns,
    size_t row_num)
{
    Float64 derivative = bias;
    for (size_t i = 0; i < weights.size(); ++i)
    {
        auto value = (*columns[i]).getFloat64(row_num);
        derivative += weights[i] * value;
    }
    derivative *= target;
    derivative = exp(derivative);

    batch_gradient[weights.size()] += learning_rate * target / (derivative + 1);
    for (size_t i = 0; i < weights.size(); ++i)
    {
        auto value = (*columns[i]).getFloat64(row_num);
        batch_gradient[i] += learning_rate * target * value / (derivative + 1) - 2 * learning_rate * l2_reg_coef * weights[i];
    }
}

void LinearRegression::predict(
    ColumnVector<Float64>::Container & container,
    Block & block,
    const ColumnNumbers & arguments,
    const std::vector<Float64> & weights,
    Float64 bias,
    const Context & context) const
{
    if (weights.size() + 1 != arguments.size())
    {
        throw Exception("In predict function number of arguments differs from the size of weights vector", ErrorCodes::LOGICAL_ERROR);
    }

    size_t rows_num = block.rows();
    std::vector<Float64> results(rows_num, bias);

    for (size_t i = 1; i < arguments.size(); ++i)
    {
        const ColumnWithTypeAndName & cur_col = block.getByPosition(arguments[i]);
        if (!isNativeNumber(cur_col.type))
        {
            throw Exception("Prediction arguments must have numeric type", ErrorCodes::BAD_ARGUMENTS);
        }

        /// If column type is already Float64 then castColumn simply returns it
        auto features_col_ptr = castColumn(cur_col, std::make_shared<DataTypeFloat64>(), context);
        auto features_column = typeid_cast<const ColumnFloat64 *>(features_col_ptr.get());

        if (!features_column)
        {
            throw Exception("Unexpectedly cannot dynamically cast features column " + std::to_string(i), ErrorCodes::LOGICAL_ERROR);
        }

        for (size_t row_num = 0; row_num != rows_num; ++row_num)
        {
            results[row_num] += weights[i - 1] * features_column->getElement(row_num);
        }
    }

    container.reserve(rows_num);
    for (size_t row_num = 0; row_num != rows_num; ++row_num)
    {
        container.emplace_back(results[row_num]);
    }
}

void LinearRegression::compute(
    std::vector<Float64> & batch_gradient,
    const std::vector<Float64> & weights,
    Float64 bias,
    Float64 learning_rate,
    Float64 l2_reg_coef,
    Float64 target,
    const IColumn ** columns,
    size_t row_num)
{
    Float64 derivative = (target - bias);
    for (size_t i = 0; i < weights.size(); ++i)
    {
        auto value = (*columns[i]).getFloat64(row_num);
        derivative -= weights[i] * value;
    }
    derivative *= (2 * learning_rate);

    batch_gradient[weights.size()] += derivative;
    for (size_t i = 0; i < weights.size(); ++i)
    {
        auto value = (*columns[i]).getFloat64(row_num);
        batch_gradient[i] += derivative * value - 2 * learning_rate * l2_reg_coef * weights[i];
    }
}

}
