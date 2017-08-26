#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeFactory.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTNameTypePair.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_NESTED_NAME;
    extern const int EMPTY_DATA_PASSED;
    extern const int NESTED_TYPE_TOO_DEEP;
}


DataTypeNested::DataTypeNested(NamesAndTypesListPtr nested_)
    : nested(nested_)
{
}


std::string DataTypeNested::concatenateNestedName(const std::string & nested_table_name, const std::string & nested_field_name)
{
    return nested_table_name + "." + nested_field_name;
}


std::string DataTypeNested::extractNestedTableName(const std::string & nested_name)
{
    const char * first_pos = strchr(nested_name.data(), '.');
    const char * last_pos = strrchr(nested_name.data(), '.');
    if (first_pos != last_pos)
        throw Exception("Invalid nested column name: " + nested_name, ErrorCodes::INVALID_NESTED_NAME);
    return first_pos == nullptr ? nested_name : nested_name.substr(0, first_pos - nested_name.data());
}


std::string DataTypeNested::extractNestedColumnName(const std::string & nested_name)
{
    const char * first_pos = strchr(nested_name.data(), '.');
    const char * last_pos = strrchr(nested_name.data(), '.');
    if (first_pos != last_pos)
        throw Exception("Invalid nested column name: " + nested_name, ErrorCodes::INVALID_NESTED_NAME);
    return last_pos == nullptr ? nested_name : nested_name.substr(last_pos - nested_name.data() + 1);
}


std::string DataTypeNested::getName() const
{
    WriteBufferFromOwnString out;

    writeCString("Nested(", out);

    for (NamesAndTypesList::const_iterator it = nested->begin(); it != nested->end(); ++it)
    {
        if (it != nested->begin())
            writeCString(", ", out);
        writeString(it->name, out);
        writeChar(' ', out);
        writeString(it->type->getName(), out);
    }

    writeChar(')', out);
    return out.str();
}


NamesAndTypesListPtr DataTypeNested::expandNestedColumns(const NamesAndTypesList & names_and_types)
{
    NamesAndTypesListPtr columns = std::make_shared<NamesAndTypesList>();
    for (NamesAndTypesList::const_iterator it = names_and_types.begin(); it != names_and_types.end(); ++it)
    {
        if (const DataTypeNested * type_nested = typeid_cast<const DataTypeNested *>(&*it->type))
        {
            const NamesAndTypesList & nested = *type_nested->getNestedTypesList();
            for (NamesAndTypesList::const_iterator jt = nested.begin(); jt != nested.end(); ++jt)
            {
                String nested_name = DataTypeNested::concatenateNestedName(it->name, jt->name);
                columns->push_back(NameAndTypePair(nested_name, std::make_shared<DataTypeArray>(jt->type)));
            }
        }
        else
            columns->push_back(*it);
    }
    return columns;
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (arguments->children.empty())
        throw Exception("Nested structure cannot be empty", ErrorCodes::EMPTY_DATA_PASSED);

    NamesAndTypesListPtr columns = std::make_shared<NamesAndTypesList>();

    for (const auto & child : arguments->children)
    {
        const ASTNameTypePair & name_and_type_pair = typeid_cast<const ASTNameTypePair &>(*child);
        DataTypePtr type = DataTypeFactory::instance().get(name_and_type_pair.type);
        if (typeid_cast<const DataTypeNested *>(type.get()))
            throw Exception("Nested inside Nested is not allowed", ErrorCodes::NESTED_TYPE_TOO_DEEP);
        columns->emplace_back(name_and_type_pair.name, type);
    }

    return std::make_shared<DataTypeNested>(columns);
}


void registerDataTypeNested(DataTypeFactory & factory)
{
    factory.registerDataType("Nested", create);
}

}
