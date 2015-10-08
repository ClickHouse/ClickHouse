#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/AggregateFunctionUniq.h>
#include <DB/AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

	AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq, AggregateFunctionUniqUniquesHashSetData>(*argument_types[0]);

	if (res)
		return res;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDate::FieldType, AggregateFunctionUniqUniquesHashSetData>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDateTime::FieldType, AggregateFunctionUniqUniquesHashSetData>;
	else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
		return new AggregateFunctionUniq<String, AggregateFunctionUniqUniquesHashSetData>;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

AggregateFunctionPtr createAggregateFunctionUniqExact(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

	AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq, AggregateFunctionUniqExactData>(*argument_types[0]);

	if (res)
		return res;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDate::FieldType, AggregateFunctionUniqExactData<DataTypeDate::FieldType> >;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDateTime::FieldType, AggregateFunctionUniqExactData<DataTypeDateTime::FieldType> >;
	else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
		return new AggregateFunctionUniq<String, AggregateFunctionUniqExactData<String> >;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

AggregateFunctionPtr createAggregateFunctionUniqHLL12(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

	AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq, AggregateFunctionUniqHLL12Data>(*argument_types[0]);

	if (res)
		return res;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDate::FieldType, AggregateFunctionUniqHLL12Data<DataTypeDate::FieldType>>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDateTime::FieldType, AggregateFunctionUniqHLL12Data<DataTypeDateTime::FieldType>>;
	else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
		return new AggregateFunctionUniq<String, AggregateFunctionUniqHLL12Data<String>>;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

AggregateFunctionPtr createAggregateFunctionUniqCombined(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

	AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq, AggregateFunctionUniqCombinedData>(*argument_types[0]);

	if (res)
		return res;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDate::FieldType, AggregateFunctionUniqCombinedData<DataTypeDate::FieldType>>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDateTime::FieldType, AggregateFunctionUniqCombinedData<DataTypeDateTime::FieldType>>;
	else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
		return new AggregateFunctionUniq<String, AggregateFunctionUniqCombinedData<String>>;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() +
			" of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

AggregateFunctionPtr createAggregateFunctionUniqCombinedRaw(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

	AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq, AggregateFunctionUniqCombinedRawData>(*argument_types[0]);

	if (res)
		return res;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDate::FieldType, AggregateFunctionUniqCombinedRawData<DataTypeDate::FieldType>>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDateTime::FieldType, AggregateFunctionUniqCombinedRawData<DataTypeDateTime::FieldType>>;
	else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
		return new AggregateFunctionUniq<String, AggregateFunctionUniqCombinedRawData<String>>;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() +
			" of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

AggregateFunctionPtr createAggregateFunctionUniqCombinedLinearCounting(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

	AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq,
			AggregateFunctionUniqCombinedLinearCountingData>(*argument_types[0]);

	if (res)
		return res;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDate::FieldType,
			AggregateFunctionUniqCombinedLinearCountingData<DataTypeDate::FieldType>>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDateTime::FieldType,
			AggregateFunctionUniqCombinedLinearCountingData<DataTypeDateTime::FieldType>>;
	else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
		return new AggregateFunctionUniq<String, AggregateFunctionUniqCombinedLinearCountingData<String>>;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() +
			" of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

AggregateFunctionPtr createAggregateFunctionUniqCombinedBiasCorrected(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

	AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq,
		AggregateFunctionUniqCombinedBiasCorrectedData>(*argument_types[0]);

	if (res)
		return res;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDate::FieldType,
			AggregateFunctionUniqCombinedBiasCorrectedData<DataTypeDate::FieldType>>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return new AggregateFunctionUniq<DataTypeDateTime::FieldType,
			AggregateFunctionUniqCombinedBiasCorrectedData<DataTypeDateTime::FieldType>>;
	else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
		return new AggregateFunctionUniq<String, AggregateFunctionUniqCombinedBiasCorrectedData<String>>;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() +
			" of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsUniq(AggregateFunctionFactory & factory)
{
	factory.registerFunction({"uniq"}, createAggregateFunctionUniq);
	factory.registerFunction({"uniqHLL12"}, createAggregateFunctionUniqHLL12);
	factory.registerFunction({"uniqExact"}, createAggregateFunctionUniqExact);
	factory.registerFunction({"uniqCombinedRaw"}, createAggregateFunctionUniqCombinedRaw);
	factory.registerFunction({"uniqCombinedLinearCounting"}, createAggregateFunctionUniqCombinedLinearCounting);
	factory.registerFunction({"uniqCombinedBiasCorrected"}, createAggregateFunctionUniqCombinedBiasCorrected);
	factory.registerFunction({"uniqCombined"}, createAggregateFunctionUniqCombined);
}

}
