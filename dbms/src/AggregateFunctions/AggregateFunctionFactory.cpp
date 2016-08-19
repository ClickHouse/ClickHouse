#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Common/StringUtils.h>
#include <Poco/String.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_AGGREGATE_FUNCTION;
	extern const int LOGICAL_ERROR;
	extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


namespace
{

/// Ничего не проверяет.
std::string trimRight(const std::string & in, const char * suffix)
{
	return in.substr(0, in.size() - strlen(suffix));
}

}

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory);
void registerAggregateFunctionCount(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupArray(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantile(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileExact(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileExactWeighted(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileDeterministic(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileTiming(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileTDigest(AggregateFunctionFactory & factory);
void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory & factory);
void registerAggregateFunctionsMinMaxAny(AggregateFunctionFactory & factory);
void registerAggregateFunctionsStatistics(AggregateFunctionFactory & factory);
void registerAggregateFunctionSum(AggregateFunctionFactory & factory);
void registerAggregateFunctionsUniq(AggregateFunctionFactory & factory);
void registerAggregateFunctionUniqUpTo(AggregateFunctionFactory & factory);
void registerAggregateFunctionDebug(AggregateFunctionFactory & factory);

AggregateFunctionPtr createAggregateFunctionArray(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionIf(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionState(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionMerge(AggregateFunctionPtr & nested);


AggregateFunctionFactory::AggregateFunctionFactory()
{
	registerAggregateFunctionAvg(*this);
	registerAggregateFunctionCount(*this);
	registerAggregateFunctionGroupArray(*this);
	registerAggregateFunctionGroupUniqArray(*this);
	registerAggregateFunctionsQuantile(*this);
	registerAggregateFunctionsQuantileExact(*this);
	registerAggregateFunctionsQuantileExactWeighted(*this);
	registerAggregateFunctionsQuantileDeterministic(*this);
	registerAggregateFunctionsQuantileTiming(*this);
	registerAggregateFunctionsQuantileTDigest(*this);
	registerAggregateFunctionsSequenceMatch(*this);
	registerAggregateFunctionsMinMaxAny(*this);
	registerAggregateFunctionsStatistics(*this);
	registerAggregateFunctionSum(*this);
	registerAggregateFunctionsUniq(*this);
	registerAggregateFunctionUniqUpTo(*this);
	registerAggregateFunctionDebug(*this);
}


void AggregateFunctionFactory::registerFunction(const String & name, Creator creator, CaseSensitiveness case_sensitiveness)
{
	if (creator == nullptr)
		throw Exception("AggregateFunctionFactory: the aggregate function " + name + " has been provided "
			" a null constructor", ErrorCodes::LOGICAL_ERROR);

	if (!aggregate_functions.emplace(name, creator).second)
		throw Exception("AggregateFunctionFactory: the aggregate function name " + name + " is not unique",
			ErrorCodes::LOGICAL_ERROR);

	if (case_sensitiveness == CaseInsensitive
		&& !case_insensitive_aggregate_functions.emplace(Poco::toLower(name), creator).second)
		throw Exception("AggregateFunctionFactory: the case insensitive aggregate function name " + name + " is not unique",
			ErrorCodes::LOGICAL_ERROR);
}


AggregateFunctionPtr AggregateFunctionFactory::get(const String & name, const DataTypes & argument_types, int recursion_level) const
{
	{
		auto it = aggregate_functions.find(name);
		if (it != aggregate_functions.end())
			return it->second(name, argument_types);
	}

	if (recursion_level == 0)
	{
		auto it = case_insensitive_aggregate_functions.find(Poco::toLower(name));
		if (it != case_insensitive_aggregate_functions.end())
			return it->second(name, argument_types);
	}

	if ((recursion_level == 0) && endsWith(name, "State"))
	{
		/// Для агрегатных функций вида aggState, где agg - имя другой агрегатной функции.
		AggregateFunctionPtr nested = get(trimRight(name, "State"), argument_types, recursion_level + 1);
		return createAggregateFunctionState(nested);
	}

	if ((recursion_level <= 1) && endsWith(name, "Merge"))
	{
		/// Для агрегатных функций вида aggMerge, где agg - имя другой агрегатной функции.
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(&*argument_types[0]);
		if (!function)
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		AggregateFunctionPtr nested = get(trimRight(name, "Merge"), function->getArgumentsDataTypes(), recursion_level + 1);

		if (nested->getName() != function->getFunctionName())
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return createAggregateFunctionMerge(nested);
	}

	if ((recursion_level <= 2) && endsWith(name, "If"))
	{
		if (argument_types.empty())
			throw Exception{
				"Incorrect number of arguments for aggregate function " + name,
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		/// Для агрегатных функций вида aggIf, где agg - имя другой агрегатной функции.
		DataTypes nested_dt = argument_types;
		nested_dt.pop_back();
		AggregateFunctionPtr nested = get(trimRight(name, "If"), nested_dt, recursion_level + 1);
		return createAggregateFunctionIf(nested);
	}

	if ((recursion_level <= 3) && endsWith(name, "Array"))
	{
		/// Для агрегатных функций вида aggArray, где agg - имя другой агрегатной функции.
		size_t num_agruments = argument_types.size();

		DataTypes nested_arguments;
		for (size_t i = 0; i < num_agruments; ++i)
		{
			if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(&*argument_types[i]))
				nested_arguments.push_back(array->getNestedType());
			else
				throw Exception("Illegal type " + argument_types[i]->getName() + " of argument #" + toString(i + 1) +
					" for aggregate function " + name + ". Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
		/// + 3, чтобы ни один другой модификатор не мог идти перед Array
		AggregateFunctionPtr nested = get(trimRight(name, "Array"), nested_arguments, recursion_level + 3);
		return createAggregateFunctionArray(nested);
	}

	throw Exception("Unknown aggregate function " + name, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
}


AggregateFunctionPtr AggregateFunctionFactory::tryGet(const String & name, const DataTypes & argument_types) const
{
	return isAggregateFunctionName(name)
		? get(name, argument_types)
		: nullptr;
}


bool AggregateFunctionFactory::isAggregateFunctionName(const String & name, int recursion_level) const
{
	if (aggregate_functions.count(name))
		return true;

	if (recursion_level == 0 && case_insensitive_aggregate_functions.count(Poco::toLower(name)))
		return true;

	/// Для агрегатных функций вида aggState, где agg - имя другой агрегатной функции.
	if ((recursion_level <= 0) && endsWith(name, "State"))
		return isAggregateFunctionName(trimRight(name, "State"), recursion_level + 1);

	/// Для агрегатных функций вида aggMerge, где agg - имя другой агрегатной функции.
	if ((recursion_level <= 1) && endsWith(name, "Merge"))
		return isAggregateFunctionName(trimRight(name, "Merge"), recursion_level + 1);

	/// Для агрегатных функций вида aggIf, где agg - имя другой агрегатной функции.
	if ((recursion_level <= 2) && endsWith(name, "If"))
		return isAggregateFunctionName(trimRight(name, "If"), recursion_level + 1);

	/// Для агрегатных функций вида aggArray, где agg - имя другой агрегатной функции.
	if ((recursion_level <= 3) && endsWith(name, "Array"))
	{
		/// + 3, чтобы ни один другой модификатор не мог идти перед Array
		return isAggregateFunctionName(trimRight(name, "Array"), recursion_level + 3);
	}

	return false;
}

}
