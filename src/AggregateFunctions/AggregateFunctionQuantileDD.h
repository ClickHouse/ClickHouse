#pragma once

#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileDD.h>


namespace DB
{

/// The functions that `quantileDD` and `quantilesDD` create for an argument of type `Value`. `float_return` is false for
/// `Date` and `DateTime`: their quantiles have the type of the argument
template <typename Value, bool float_return> using FuncQuantileDD = AggregateFunctionQuantile<Value, QuantileDD<Value>, NameQuantileDD, void, std::conditional_t<float_return, Float64, void>, false, true>;
template <typename Value, bool float_return> using FuncQuantilesDD = AggregateFunctionQuantile<Value, QuantileDD<Value>, NameQuantilesDD, void, std::conditional_t<float_return, Float64, void>, true, true>;

}
