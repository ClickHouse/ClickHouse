#include <boost/assign/list_inserter.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <mysqlxx/String.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeFactory.h>

#include <DB/AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{


DataTypeFactory::DataTypeFactory()
	: fixed_string_regexp("^FixedString\\s*\\(\\s*(\\d+)\\s*\\)$"),
	nested_regexp("^(\\w+)\\s*\\(\\s*(.+)\\s*\\)$")
{
	boost::assign::insert(non_parametric_data_types)
		("UInt8",				new DataTypeUInt8)
		("UInt16",				new DataTypeUInt16)
		("UInt32",				new DataTypeUInt32)
		("UInt64",				new DataTypeUInt64)
		("Int8",				new DataTypeInt8)
		("Int16",				new DataTypeInt16)
		("Int32",				new DataTypeInt32)
		("Int64",				new DataTypeInt64)
		("Float32",				new DataTypeFloat32)
		("Float64",				new DataTypeFloat64)
		("VarUInt",				new DataTypeVarUInt)
		("VarInt",				new DataTypeVarInt)
		("Date",				new DataTypeDate)
		("DateTime",			new DataTypeDateTime)
		("String",				new DataTypeString)
		;
}


DataTypePtr DataTypeFactory::get(const String & name) const
{
	NonParametricDataTypes::const_iterator it = non_parametric_data_types.find(name);
	if (it != non_parametric_data_types.end())
		return it->second;

	Poco::RegularExpression::MatchVec matches;
	if (fixed_string_regexp.match(name, 0, matches) && matches.size() == 2)
		return new DataTypeFixedString(mysqlxx::String(name.data() + matches[1].offset, matches[1].length, NULL).getUInt());

	if (nested_regexp.match(name, 0, matches) && matches.size() == 3)
	{
		String base_name(name.data() + matches[1].offset, matches[1].length);
		String parameters(name.data() + matches[2].offset, matches[2].length);

		if (base_name == "Array")
			return new DataTypeArray(get(parameters));
		if (base_name == "AggregateFunction")
		{
			String function_name;
			AggregateFunctionPtr function;
			DataTypes argument_types;

			// TODO: парсить нормально. А то будут проблемы с параметризованными типами данных.
			Strings split_res;
			boost::split(split_res, parameters, boost::is_any_of(", \t\r\n\f"), boost::token_compress_on);

			if (split_res.empty())
				throw Exception("Data type AggregateFunction requires parameters: "
					"name of aggregate function and list of data types for arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

			Strings::const_iterator it = split_res.begin();
			function_name = *it;

			for (++it; it != split_res.end(); ++it)
				argument_types.push_back(get(*it));

			function = AggregateFunctionFactory().get(function_name, argument_types);
			return new DataTypeAggregateFunction(function, argument_types);
		}
		else
			throw Exception("Unknown type " + base_name, ErrorCodes::UNKNOWN_TYPE);
	}

	throw Exception("Unknown type " + name, ErrorCodes::UNKNOWN_TYPE);
}


}
