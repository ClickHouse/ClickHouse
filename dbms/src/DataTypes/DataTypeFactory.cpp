#include <boost/assign/list_inserter.hpp>

#include <mysqlxx/String.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/DataTypes/DataTypeFactory.h>


namespace DB
{


DataTypeFactory::DataTypeFactory()
	: fixed_string_regexp("^FixedString\\s*\\(\\s*(\\d+)\\s*\\)$")
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
		("AggregateFunction",	new DataTypeAggregateFunction)
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

	throw Exception("Unknown type " + name, ErrorCodes::UNKNOWN_TYPE);
}


}
