#pragma once

namespace DB
{

class AggregateFunctionFactory;

void registerWindowFunctions(AggregateFunctionFactory & factory);

}
