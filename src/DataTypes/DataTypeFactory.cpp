#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustom.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Poco/String.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>


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
    /// Data type parser can be invoked from coroutines with small stack.
    /// Value 315 is known to cause stack overflow in some test configurations (debug build, sanitizers)
    /// let's make the threshold significantly lower.
    /// It is impractical for user to have complex data types with this depth.

#if defined(SANITIZER) || !defined(NDEBUG)
    static constexpr size_t data_type_max_parse_depth = 150;
#else
    static constexpr size_t data_type_max_parse_depth = 300;
#endif

    ParserDataType parser;
    ASTPtr ast = parseQuery(parser, full_name.data(), full_name.data() + full_name.size(), "data type", 0, data_type_max_parse_depth);
    return get(ast);
}

DataTypePtr DataTypeFactory::get(const ASTPtr & ast) const
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        if (func->parameters)
            throw Exception("Data type cannot have multiple parenthesized parameters.", ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE);
        return get(func->name, func->arguments);
    }

    if (const auto * ident = ast->as<ASTIdentifier>())
    {
        return get(ident->name(), {});
    }

    if (const auto * lit = ast->as<ASTLiteral>())
    {
        if (lit->value.isNull())
            return get("Null", {});
    }

    throw Exception("Unexpected AST element for data type.", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
}

DataTypePtr DataTypeFactory::get(const String & family_name_param, const ASTPtr & parameters) const
{
    String family_name = getAliasToOrName(family_name_param);

    if (endsWith(family_name, "WithDictionary"))
    {
        ASTPtr low_cardinality_params = std::make_shared<ASTExpressionList>();
        String param_name = family_name.substr(0, family_name.size() - strlen("WithDictionary"));
        if (parameters)
        {
            auto func = std::make_shared<ASTFunction>();
            func->name = param_name;
            func->arguments = parameters;
            low_cardinality_params->children.push_back(func);
        }
        else
            low_cardinality_params->children.push_back(std::make_shared<ASTIdentifier>(param_name));

        return get("LowCardinality", low_cardinality_params);
    }

    return findCreatorByName(family_name)(parameters);
}

DataTypePtr DataTypeFactory::getCustom(DataTypeCustomDescPtr customization) const
{
    if (!customization->name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create custom type without name");

    auto type = get(customization->name->getName());
    type->setCustomization(std::move(customization));
    return type;
}


void DataTypeFactory::registerDataType(const String & family_name, Value creator, CaseSensitiveness case_sensitiveness)
{
    if (creator == nullptr)
        throw Exception("DataTypeFactory: the data type family " + family_name + " has been provided "
            " a null constructor", ErrorCodes::LOGICAL_ERROR);

    String family_name_lowercase = Poco::toLower(family_name);

    if (isAlias(family_name) || isAlias(family_name_lowercase))
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

void DataTypeFactory::registerDataTypeCustom(const String & family_name, CreatorWithCustom creator, CaseSensitiveness case_sensitiveness)
{
    registerDataType(family_name, [creator](const ASTPtr & ast)
    {
        auto res = creator(ast);
        res.first->setCustomization(std::move(res.second));

        return res.first;
    }, case_sensitiveness);
}

void DataTypeFactory::registerSimpleDataTypeCustom(const String &name, SimpleCreatorWithCustom creator, CaseSensitiveness case_sensitiveness)
{
    registerDataTypeCustom(name, [creator](const ASTPtr & /*ast*/)
    {
        return creator();
    }, case_sensitiveness);
}

const DataTypeFactory::Value & DataTypeFactory::findCreatorByName(const String & family_name) const
{
    ContextPtr query_context;
    if (CurrentThread::isInitialized())
        query_context = CurrentThread::get().getQueryContext();

    {
        DataTypesDictionary::const_iterator it = data_types.find(family_name);
        if (data_types.end() != it)
        {
            if (query_context && query_context->getSettingsRef().log_queries)
                query_context->addQueryFactoriesInfo(Context::QueryLogFactories::DataType, family_name);
            return it->second;
        }
    }

    String family_name_lowercase = Poco::toLower(family_name);

    {
        DataTypesDictionary::const_iterator it = case_insensitive_data_types.find(family_name_lowercase);
        if (case_insensitive_data_types.end() != it)
        {
            if (query_context && query_context->getSettingsRef().log_queries)
                query_context->addQueryFactoriesInfo(Context::QueryLogFactories::DataType, family_name_lowercase);
            return it->second;
        }
    }

    auto hints = this->getHints(family_name);
    if (!hints.empty())
        throw Exception("Unknown data type family: " + family_name + ". Maybe you meant: " + toString(hints), ErrorCodes::UNKNOWN_TYPE);
    else
        throw Exception("Unknown data type family: " + family_name, ErrorCodes::UNKNOWN_TYPE);
}

DataTypeFactory::DataTypeFactory()
{
    registerDataTypeNumbers(*this);
    registerDataTypeDecimal(*this);
    registerDataTypeDate(*this);
    registerDataTypeDate32(*this);
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
    registerDataTypeLowCardinality(*this);
    registerDataTypeDomainIPv4AndIPv6(*this);
    registerDataTypeDomainBool(*this);
    registerDataTypeDomainSimpleAggregateFunction(*this);
    registerDataTypeDomainGeo(*this);
    registerDataTypeMap(*this);
    registerDataTypeObject(*this);
}

DataTypeFactory & DataTypeFactory::instance()
{
    static DataTypeFactory ret;
    return ret;
}

}
