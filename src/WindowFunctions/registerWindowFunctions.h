#pragma once

namespace DB
{

/// TODO(Michicosun): Completely untie window functions from aggregate functions.

class AggregateFunctionFactory;

void registerWindowFunctions(AggregateFunctionFactory & factory);

}
