#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/AggregateFunctionUniq.h>
#include <DB/AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

template <typename Data>
AggregateFunctionPtr createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() == 1)
	{
		const IDataType & argument_type = *argument_types[0];

		AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq, Data>(*argument_types[0]);

		if (res)
			return res;
		else if (typeid_cast<const DataTypeDate *>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDate::FieldType, Data>;
		else if (typeid_cast<const DataTypeDateTime *>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDateTime::FieldType, Data>;
		else if (typeid_cast<const DataTypeString *>(&argument_type) || typeid_cast<const DataTypeFixedString *>(&argument_type))
			return new AggregateFunctionUniq<String, Data>;
		else if (typeid_cast<const DataTypeTuple *>(&argument_type))
			return new AggregateFunctionUniqVariadic<Data, true>;
		else
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (argument_types.size() > 1)
	{
		/// Если аргументов несколько, то среди них недопустимо наличие кортежей.
		for (const auto & type : argument_types)
			if (typeid_cast<const DataTypeTuple *>(type.get()))
				throw Exception("Tuple argument of function " + name + " must be the only argument",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new AggregateFunctionUniqVariadic<Data, false>;
	}
	else
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

template <template <typename> class Data>
AggregateFunctionPtr createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() == 1)
	{
		const IDataType & argument_type = *argument_types[0];

		AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq, Data>(*argument_types[0]);

		if (res)
			return res;
		else if (typeid_cast<const DataTypeDate *>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDate::FieldType, Data<DataTypeDate::FieldType>>;
		else if (typeid_cast<const DataTypeDateTime *>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>>;
		else if (typeid_cast<const DataTypeString *>(&argument_type) || typeid_cast<const DataTypeFixedString *>(&argument_type))
			return new AggregateFunctionUniq<String, Data<String>>;
		else if (typeid_cast<const DataTypeTuple *>(&argument_type))
			return new AggregateFunctionUniqVariadic<Data<UInt64>, true>;
		else
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (argument_types.size() > 1)
	{
		/// Если аргументов несколько, то среди них недопустимо наличие кортежей.
		for (const auto & type : argument_types)
			if (typeid_cast<const DataTypeTuple *>(type.get()))
				throw Exception("Tuple argument of function " + name + " must be the only argument",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new AggregateFunctionUniqVariadic<Data<UInt64>, false>;
	}
	else
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

}

void registerAggregateFunctionsUniq(AggregateFunctionFactory & factory)
{
	factory.registerFunction({"uniq"}, createAggregateFunctionUniq<AggregateFunctionUniqUniquesHashSetData>);
	factory.registerFunction({"uniqHLL12"}, createAggregateFunctionUniq<AggregateFunctionUniqHLL12Data>);
	factory.registerFunction({"uniqExact"}, createAggregateFunctionUniq<AggregateFunctionUniqExactData>);
	factory.registerFunction({"uniqCombinedRaw"}, createAggregateFunctionUniq<AggregateFunctionUniqCombinedRawData>);
	factory.registerFunction({"uniqCombinedLinearCounting"}, createAggregateFunctionUniq<AggregateFunctionUniqCombinedLinearCountingData>);
	factory.registerFunction({"uniqCombinedBiasCorrected"}, createAggregateFunctionUniq<AggregateFunctionUniqCombinedBiasCorrectedData>);
	factory.registerFunction({"uniqCombined"}, createAggregateFunctionUniq<AggregateFunctionUniqCombinedData>);
}

}
