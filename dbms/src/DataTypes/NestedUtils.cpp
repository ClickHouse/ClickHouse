#include <string.h>

#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeFactory.h>

#include <Parsers/IAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_NESTED_NAME;
}

namespace Nested
{

std::string concatenateName(const std::string & nested_table_name, const std::string & nested_field_name)
{
    return nested_table_name + "." + nested_field_name;
}


std::string extractTableName(const std::string & nested_name)
{
    const char * first_pos = strchr(nested_name.data(), '.');
    const char * last_pos = strrchr(nested_name.data(), '.');
    if (first_pos != last_pos)
        throw Exception("Invalid nested column name: " + nested_name, ErrorCodes::INVALID_NESTED_NAME);
    return first_pos == nullptr ? nested_name : nested_name.substr(0, first_pos - nested_name.data());
}


std::string extractElementName(const std::string & nested_name)
{
    const char * first_pos = strchr(nested_name.data(), '.');
    const char * last_pos = strrchr(nested_name.data(), '.');
    if (first_pos != last_pos)
        throw Exception("Invalid nested column name: " + nested_name, ErrorCodes::INVALID_NESTED_NAME);
    return last_pos == nullptr ? nested_name : nested_name.substr(last_pos - nested_name.data() + 1);
}


NamesAndTypesListPtr flatten(const NamesAndTypesList & names_and_types)
{
    NamesAndTypesListPtr columns = std::make_shared<NamesAndTypesList>();
    for (NamesAndTypesList::const_iterator it = names_and_types.begin(); it != names_and_types.end(); ++it)
    {
        if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(it->type.get()))
        {
            if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(type_arr->getNestedType().get()))
            {
                const DataTypes & elements = type_tuple->getElements();
                const Strings & names = type_tuple->getElementNames();
                size_t tuple_size = elements.size();

                for (size_t i = 0; i < tuple_size; ++i)
                {
                    String nested_name = concatenateName(it->name, names[i]);
                    columns->push_back(NameAndTypePair(nested_name, std::make_shared<DataTypeArray>(elements[i])));
                }
            }
            else
                columns->push_back(*it);
        }
        else
            columns->push_back(*it);
    }
    return columns;
}

}

}
