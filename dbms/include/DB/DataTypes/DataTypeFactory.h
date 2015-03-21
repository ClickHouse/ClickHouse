#pragma once

#include <map>

#include <Yandex/singleton.h>

#include <Poco/RegularExpression.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

/** Позволяет создать тип данных по его имени.
  */
class DataTypeFactory : public Singleton<DataTypeFactory>
{
public:
	DataTypeFactory();
	DataTypePtr get(const String & name) const;

private:
	typedef std::map<String, DataTypePtr> NonParametricDataTypes;
	NonParametricDataTypes non_parametric_data_types;

	Poco::RegularExpression fixed_string_regexp {R"--(^FixedString\s*\(\s*(\d+)\s*\)$)--"};

	Poco::RegularExpression nested_regexp {R"--(^(\w+)\s*\(\s*(.+)\s*\)$)--",
		Poco::RegularExpression::RE_MULTILINE | Poco::RegularExpression::RE_DOTALL};
};

}
