#pragma once

#include <map>
#include <common/singleton.h>
#include <Poco/RegularExpression.h>
#include <DataTypes/IDataType.h>


namespace DB
{

/** Creates data type by its name (possibly name contains parameters in parens).
  */
class DataTypeFactory : public Singleton<DataTypeFactory>
{
public:
    DataTypeFactory();
    DataTypePtr get(const String & name) const;

private:
    using NonParametricDataTypes = std::map<String, DataTypePtr>;
    NonParametricDataTypes non_parametric_data_types;

    Poco::RegularExpression fixed_string_regexp {R"--(^FixedString\s*\(\s*(\d+)\s*\)$)--"};

    Poco::RegularExpression nested_regexp {R"--(^(\w+)\s*\(\s*(.+)\s*\)$)--",
        Poco::RegularExpression::RE_MULTILINE | Poco::RegularExpression::RE_DOTALL};
};

}
