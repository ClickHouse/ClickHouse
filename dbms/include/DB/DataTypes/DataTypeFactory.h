#pragma once

#include <Poco/RegularExpression.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>


namespace DB
{


/** Позволяет создать тип данных по его имени. 
  */
class DataTypeFactory
{
public:
	DataTypeFactory();
	DataTypePtr get(const String & name);

private:
	typedef std::map<String, DataTypePtr> NonParametricDataTypes;
	NonParametricDataTypes non_parametric_data_types;

	Poco::RegularExpression fixed_string_regexp;
};


}
