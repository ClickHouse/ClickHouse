#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/DataTypes/DataTypeArray.h>

namespace DB
{

namespace
{

constexpr size_t strlen_constexpr(const char * in)
{
	return (*in == '\0') ? 0 : 1 + strlen_constexpr(in + 1);
}

struct SuffixState
{
	static constexpr auto name = "State";
	static constexpr auto length = strlen_constexpr(name);
};

struct SuffixIf
{
	static constexpr auto name = "If";
	static constexpr auto length = strlen_constexpr(name);
};

struct SuffixArray
{
	static constexpr auto name = "Array";
	static constexpr auto length = strlen_constexpr(name);
};

struct SuffixMerge
{
	static constexpr auto name = "Merge";
	static constexpr auto length = strlen_constexpr(name);
};

template <typename Suffix>
inline bool endsWith(const std::string & in)
{
	return (in.length() > Suffix::length)
		&& (in.compare(in.length() - Suffix::length, Suffix::length, Suffix::name) == 0);
}

/// Ничего не проверяет.
template <typename Suffix>
inline std::string trimRight(const std::string & in)
{
	return in.substr(0, in.length() - Suffix::length);
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


void AggregateFunctionFactory::registerFunction(const std::vector<std::string> & names, Creator creator)
{
	if (names.empty())
		throw Exception("AggregateFunctionFactory: no name given for aggregate function", ErrorCodes::LOGICAL_ERROR);
	if (creator == nullptr)
		throw Exception("AggregateFunctionFactory: the aggregate function " + names[0] + " has been provided "
			" a null constructor", ErrorCodes::LOGICAL_ERROR);

	bool is_first = true;

	for (const auto & name : names)
	{
		Descriptor desc;
		desc.creator = creator;
		desc.is_alias = !is_first;

		auto res = aggregate_functions.emplace(name, desc);
		if (!res.second)
			throw Exception("AggregateFunctionFactory: the aggregate function name " + name + " is not unique",
				ErrorCodes::LOGICAL_ERROR);

		is_first = false;
	}
}


AggregateFunctionPtr AggregateFunctionFactory::get(const String & name, const DataTypes & argument_types, int recursion_level) const
{
	auto it = aggregate_functions.find(name);
	if (it != aggregate_functions.end())
	{
		const auto & desc = it->second;
		const auto & creator = desc.creator;
		return creator(name, argument_types);
	}
	else if ((recursion_level == 0) && endsWith<SuffixState>(name))
	{
		/// Для агрегатных функций вида aggState, где agg - имя другой агрегатной функции.
		AggregateFunctionPtr nested = get(trimRight<SuffixState>(name), argument_types, recursion_level + 1);
		return createAggregateFunctionState(nested);
	}
	else if ((recursion_level <= 1) && endsWith<SuffixMerge>(name))
	{
		/// Для агрегатных функций вида aggMerge, где agg - имя другой агрегатной функции.
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(&*argument_types[0]);
		if (!function)
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		AggregateFunctionPtr nested = get(trimRight<SuffixMerge>(name), function->getArgumentsDataTypes(), recursion_level + 1);

		if (nested->getName() != function->getFunctionName())
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return createAggregateFunctionMerge(nested);
	}
	else if ((recursion_level <= 2) && endsWith<SuffixIf>(name))
	{
		if (argument_types.empty())
			throw Exception{
				"Incorrect number of arguments for aggregate function " + name,
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		/// Для агрегатных функций вида aggIf, где agg - имя другой агрегатной функции.
		DataTypes nested_dt = argument_types;
		nested_dt.pop_back();
		AggregateFunctionPtr nested = get(trimRight<SuffixIf>(name), nested_dt, recursion_level + 1);
		return createAggregateFunctionIf(nested);
	}
	else if ((recursion_level <= 3) && endsWith<SuffixArray>(name))
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
		AggregateFunctionPtr nested = get(trimRight<SuffixArray>(name), nested_arguments, recursion_level + 3);
		return createAggregateFunctionArray(nested);
	}
	else
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
	if (aggregate_functions.count(name) > 0)
		return true;
	/// Для агрегатных функций вида aggState, где agg - имя другой агрегатной функции.
	else if ((recursion_level <= 0) && endsWith<SuffixState>(name))
		return isAggregateFunctionName(trimRight<SuffixState>(name), recursion_level + 1);
	/// Для агрегатных функций вида aggMerge, где agg - имя другой агрегатной функции.
	else if ((recursion_level <= 1) && endsWith<SuffixMerge>(name))
		return isAggregateFunctionName(trimRight<SuffixMerge>(name), recursion_level + 1);
	/// Для агрегатных функций вида aggIf, где agg - имя другой агрегатной функции.
	else if ((recursion_level <= 2) && endsWith<SuffixIf>(name))
		return isAggregateFunctionName(trimRight<SuffixIf>(name), recursion_level + 1);
	/// Для агрегатных функций вида aggArray, где agg - имя другой агрегатной функции.
	else if ((recursion_level <= 3) && endsWith<SuffixArray>(name))
	{
		/// + 3, чтобы ни один другой модификатор не мог идти перед Array
		return isAggregateFunctionName(trimRight<SuffixArray>(name), recursion_level + 3);
	}
	else
		return false;
}


AggregateFunctionFactory::Details AggregateFunctionFactory::getDetails(const AggregateFunctionFactory::AggregateFunctions::value_type & entry)
{
	const auto & desc = entry.second;
	return {entry.first, desc.is_alias};
}


AggregateFunctionFactory::const_iterator AggregateFunctionFactory::begin() const
{
	return boost::make_transform_iterator(aggregate_functions.begin(), getDetails);
}


AggregateFunctionFactory::const_iterator AggregateFunctionFactory::end() const
{
	return boost::make_transform_iterator(aggregate_functions.end(), getDetails);
}


}
