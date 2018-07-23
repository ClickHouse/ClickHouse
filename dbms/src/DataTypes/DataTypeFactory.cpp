#include <DataTypes/DataTypeFactory.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Poco/String.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int DATA_TYPE_CANNOT_HAVE_ARGUMENTS;
}


DataTypePtr DataTypeFactory::get(const String & full_name) const
{
    ParserIdentifierWithOptionalParameters parser;
    ASTPtr ast = parseQuery(parser, full_name.data(), full_name.data() + full_name.size(), "data type", 0);
    return get(ast);
}

DataTypePtr DataTypeFactory::get(const ASTPtr & ast) const
{
    if (const ASTFunction * func = typeid_cast<const ASTFunction *>(ast.get()))
    {
        if (func->parameters)
            throw Exception("Data type cannot have multiple parenthesed parameters.", ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE);
        return get(func->name, func->arguments);
    }

    if (const ASTIdentifier * ident = typeid_cast<const ASTIdentifier *>(ast.get()))
    {
        return get(ident->name, {});
    }

    if (const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(ast.get()))
    {
        if (lit->value.isNull())
            return get("Null", {});
    }

    throw Exception("Unexpected AST element for data type.", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
}

DataTypePtr DataTypeFactory::get(const String & family_name, const ASTPtr & parameters) const
{
    {
        DataTypesDictionary::const_iterator it = data_types.find(family_name);
        if (data_types.end() != it)
            return it->second(parameters);
    }

    String family_name_lowercase = Poco::toLower(family_name);

    {
        DataTypesDictionary::const_iterator it = case_insensitive_data_types.find(family_name_lowercase);
        if (case_insensitive_data_types.end() != it)
            return it->second(parameters);
    }

    if (auto it = aliases.find(family_name); it != aliases.end())
        return get(it->second, parameters);
    else if (auto it = case_insensitive_aliases.find(family_name_lowercase); it != case_insensitive_aliases.end())
        return get(it->second, parameters);

    throw Exception("Unknown data type family: " + family_name, ErrorCodes::UNKNOWN_TYPE);
}


void DataTypeFactory::registerDataType(const String & family_name, Creator creator, CaseSensitiveness case_sensitiveness)
{
    if (creator == nullptr)
        throw Exception("DataTypeFactory: the data type family " + family_name + " has been provided "
            " a null constructor", ErrorCodes::LOGICAL_ERROR);

    String family_name_lowercase = Poco::toLower(family_name);

    if (aliases.count(family_name) || case_insensitive_aliases.count(family_name_lowercase))
        throw Exception("DataTypeFactory: the data type family name '" + family_name + "' is already registered as alias",
                        ErrorCodes::LOGICAL_ERROR);

    if (!data_types.emplace(family_name, creator).second)
        throw Exception("DataTypeFactory: the data type family name '" + family_name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);


    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_data_types.emplace(family_name_lowercase, creator).second)
        throw Exception("DataTypeFactory: the case insensitive data type family name '" + family_name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}

void DataTypeFactory::registerAlias(const String & alias_name, const String & real_name, CaseSensitiveness case_sensitiveness) {
    String real_type_dict_name;
    if (data_types.count(real_name))
        real_type_dict_name = real_name;
    else if (auto type_name_lowercase = Poco::toLower(real_name); case_insensitive_data_types.count(type_name_lowercase))
        real_type_dict_name = type_name_lowercase;
    else
        throw Exception("DataTypeFactory: can't create alias '" + alias_name + "' the data type family '" + real_name + "' is not registered", ErrorCodes::LOGICAL_ERROR);

    String alias_name_lowercase = Poco::toLower(alias_name);

    if (data_types.count(alias_name) || case_insensitive_data_types.count(alias_name_lowercase))
        throw Exception("DataTypeFactory: the alias name " + alias_name + " is already registered as datatype", ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive)
        if(!case_insensitive_aliases.emplace(alias_name_lowercase, real_type_dict_name).second)
            throw Exception("DataTypeFactory: case insensitive alias name '" + alias_name + "' is not unique", ErrorCodes::LOGICAL_ERROR);

    if(!aliases.emplace(alias_name, real_type_dict_name).second)
        throw Exception("DataTypeFactory: alias name '" + alias_name + "' is not unique", ErrorCodes::LOGICAL_ERROR);
}

void DataTypeFactory::registerSimpleDataType(const String & name, SimpleCreator creator, CaseSensitiveness case_sensitiveness)
{
    if (creator == nullptr)
        throw Exception("DataTypeFactory: the data type " + name + " has been provided "
            " a null constructor", ErrorCodes::LOGICAL_ERROR);

    registerDataType(name, [name, creator](const ASTPtr & ast)
    {
        if (ast)
            throw Exception("Data type " + name + " cannot have arguments", ErrorCodes::DATA_TYPE_CANNOT_HAVE_ARGUMENTS);
        return creator();
    }, case_sensitiveness);
}

std::vector<String> DataTypeFactory::getAllDataTypeNames() const
{
    std::vector<String> result;
    auto getter = [] (const auto& pair) { return pair.first; };
    std::transform(data_types.begin(), data_types.end(), std::back_inserter(result), getter);
    std::transform(aliases.begin(), aliases.end(), std::back_inserter(result), getter);
    return result;
}

bool DataTypeFactory::isCaseInsensitive(const String & name) const
{
    String name_lowercase = Poco::toLower(name);
    return case_insensitive_data_types.count(name_lowercase) || case_insensitive_aliases.count(name_lowercase);
}

const String & DataTypeFactory::aliasTo(const String & name) const
{
    if (auto it = aliases.find(name); it != aliases.end())
        return it->second;
    else if (auto it = case_insensitive_aliases.find(Poco::toLower(name)); it != case_insensitive_aliases.end())
        return it->second;

    throw Exception("DataTypeFactory: the data type '" + name + "' is not alias", ErrorCodes::LOGICAL_ERROR);
}


bool DataTypeFactory::isAlias(const String & name) const {
    return aliases.count(name) || case_insensitive_aliases.count(name);
}

void registerDataTypeNumbers(DataTypeFactory & factory);
void registerDataTypeDate(DataTypeFactory & factory);
void registerDataTypeDateTime(DataTypeFactory & factory);
void registerDataTypeString(DataTypeFactory & factory);
void registerDataTypeFixedString(DataTypeFactory & factory);
void registerDataTypeEnum(DataTypeFactory & factory);
void registerDataTypeArray(DataTypeFactory & factory);
void registerDataTypeTuple(DataTypeFactory & factory);
void registerDataTypeNullable(DataTypeFactory & factory);
void registerDataTypeNothing(DataTypeFactory & factory);
void registerDataTypeUUID(DataTypeFactory & factory);
void registerDataTypeAggregateFunction(DataTypeFactory & factory);
void registerDataTypeNested(DataTypeFactory & factory);
void registerDataTypeInterval(DataTypeFactory & factory);


DataTypeFactory::DataTypeFactory()
{
    registerDataTypeNumbers(*this);
    registerDataTypeDate(*this);
    registerDataTypeDateTime(*this);
    registerDataTypeString(*this);
    registerDataTypeFixedString(*this);
    registerDataTypeEnum(*this);
    registerDataTypeArray(*this);
    registerDataTypeTuple(*this);
    registerDataTypeNullable(*this);
    registerDataTypeNothing(*this);
    registerDataTypeUUID(*this);
    registerDataTypeAggregateFunction(*this);
    registerDataTypeNested(*this);
    registerDataTypeInterval(*this);
}

}
