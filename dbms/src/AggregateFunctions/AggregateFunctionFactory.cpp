#include <boost/assign/list_inserter.hpp>

#include <DB/AggregateFunctions/AggregateFunctionCount.h>

#include <DB/AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{


AggregateFunctionFactory::AggregateFunctionFactory()
{
	boost::assign::insert(non_parametric_aggregate_functions)
		("count",		new AggregateFunctionCount)
		;
}


AggregateFunctionPtr AggregateFunctionFactory::get(const String & name) const
{
	NonParametricAggregateFunctions::const_iterator it = non_parametric_aggregate_functions.find(name);
	if (it != non_parametric_aggregate_functions.end())
		return it->second->cloneEmpty();

	throw Exception("Unknown aggregate function " + name, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
}


AggregateFunctionPtr AggregateFunctionFactory::tryGet(const String & name) const
{
	NonParametricAggregateFunctions::const_iterator it = non_parametric_aggregate_functions.find(name);
	if (it != non_parametric_aggregate_functions.end())
		return it->second->cloneEmpty();
	return NULL;
}


}
