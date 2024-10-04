#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustom.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Poco/String.h>
#include <Common/StringUtils.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool log_queries;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int DATA_TYPE_CANNOT_HAVE_ARGUMENTS;
}

DataTypePtr DataTypeFactory::get(const String & full_name) const
{
    return getImpl<false>(full_name);
}

DataTypePtr DataTypeFactory::tryGet(const String & full_name) const
{
    return getImpl<true>(full_name);
}

template <bool nullptr_on_error>
DataTypePtr DataTypeFactory::getImpl(const String & full_name) const
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
    ASTPtr ast;
    if constexpr (nullptr_on_error)
    {
        String out_err;
        const char * start = full_name.data();
        ast = tryParseQuery(parser, start, start + full_name.size(), out_err, false, "data type", false,
            DBMS_DEFAULT_MAX_QUERY_SIZE, data_type_max_parse_depth, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, true);
        if (!ast)
            return nullptr;
    }
    else
    {
        ast = parseQuery(parser, full_name.data(), full_name.data() + full_name.size(), "data type", false, data_type_max_parse_depth, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }

    return getImpl<nullptr_on_error>(ast);
}

DataTypePtr DataTypeFactory::get(const ASTPtr & ast) const
{
    return getImpl<false>(ast);
}

DataTypePtr DataTypeFactory::tryGet(const ASTPtr & ast) const
{
    return getImpl<true>(ast);
}

template <bool nullptr_on_error>
DataTypePtr DataTypeFactory::getImpl(const ASTPtr & ast) const
{
    if (const auto * type = ast->as<ASTDataType>())
    {
        return getImpl<nullptr_on_error>(type->name, type->arguments);
    }

    if (const auto * ident = ast->as<ASTIdentifier>())
    {
        return getImpl<nullptr_on_error>(ident->name(), {});
    }

    if (const auto * lit = ast->as<ASTLiteral>())
    {
        if (lit->value.isNull())
            return getImpl<nullptr_on_error>("Null", {});
    }

    if constexpr (nullptr_on_error)
        return nullptr;
    throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected AST element for data type: {}.", ast->getID());
}

DataTypePtr DataTypeFactory::get(const String & family_name_param, const ASTPtr & parameters) const
{
    return getImpl<false>(family_name_param, parameters);
}

DataTypePtr DataTypeFactory::tryGet(const String & family_name_param, const ASTPtr & parameters) const
{
    return getImpl<true>(family_name_param, parameters);
}

template <bool nullptr_on_error>
DataTypePtr DataTypeFactory::getImpl(const String & family_name_param, const ASTPtr & parameters) const
{
    String family_name = getAliasToOrName(family_name_param);

    const auto * creator = findCreatorByName<nullptr_on_error>(family_name);
    if constexpr (nullptr_on_error)
    {
        if (!creator)
            return nullptr;

        try
        {
            return (*creator)(parameters);
        }
        catch (...)
        {
            return nullptr;
        }
    }
    else
    {
        assert(creator);
        return (*creator)(parameters);
    }
}

DataTypePtr DataTypeFactory::getCustom(DataTypeCustomDescPtr customization) const
{
    if (!customization->name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create custom type without name");

    auto type = get(customization->name->getName());
    type->setCustomization(std::move(customization));
    return type;
}

DataTypePtr DataTypeFactory::getCustom(const String & base_name, DataTypeCustomDescPtr customization) const
{
    auto type = get(base_name);
    type->setCustomization(std::move(customization));
    return type;
}

void DataTypeFactory::registerDataType(const String & family_name, Value creator, Case case_sensitiveness)
{
    if (creator == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DataTypeFactory: the data type family {} has been provided  a null constructor", family_name);

    String family_name_lowercase = Poco::toLower(family_name);

    if (isAlias(family_name) || isAlias(family_name_lowercase))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DataTypeFactory: the data type family name '{}' is already registered as alias", family_name);

    if (!data_types.emplace(family_name, creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DataTypeFactory: the data type family name '{}' is not unique",
            family_name);

    if (case_sensitiveness == Case::Insensitive
        && !case_insensitive_data_types.emplace(family_name_lowercase, creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DataTypeFactory: the case insensitive data type family name '{}' is not unique", family_name);
}

void DataTypeFactory::registerSimpleDataType(const String & name, SimpleCreator creator, Case case_sensitiveness)
{
    if (creator == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DataTypeFactory: the data type {} has been provided  a null constructor",
            name);

    registerDataType(name, [name, creator](const ASTPtr & ast)
    {
        if (ast)
            throw Exception(ErrorCodes::DATA_TYPE_CANNOT_HAVE_ARGUMENTS, "Data type {} cannot have arguments", name);
        return creator();
    }, case_sensitiveness);
}

void DataTypeFactory::registerDataTypeCustom(const String & family_name, CreatorWithCustom creator, Case case_sensitiveness)
{
    registerDataType(family_name, [creator](const ASTPtr & ast)
    {
        auto res = creator(ast);
        res.first->setCustomization(std::move(res.second));

        return res.first;
    }, case_sensitiveness);
}

void DataTypeFactory::registerSimpleDataTypeCustom(const String & name, SimpleCreatorWithCustom creator, Case case_sensitiveness)
{
    registerDataTypeCustom(name, [name, creator](const ASTPtr & ast)
    {
        if (ast)
            throw Exception(ErrorCodes::DATA_TYPE_CANNOT_HAVE_ARGUMENTS, "Data type {} cannot have arguments", name);
        return creator();
    }, case_sensitiveness);
}

template <bool nullptr_on_error>
const DataTypeFactory::Value * DataTypeFactory::findCreatorByName(const String & family_name) const
{
    ContextPtr query_context;
    if (CurrentThread::isInitialized())
        query_context = CurrentThread::get().getQueryContext();
    {
        DataTypesDictionary::const_iterator it = data_types.find(family_name);
        if (data_types.end() != it)
        {
            if (query_context && query_context->getSettingsRef()[Setting::log_queries])
                query_context->addQueryFactoriesInfo(Context::QueryLogFactories::DataType, family_name);
            return &it->second;
        }
    }

    String family_name_lowercase = Poco::toLower(family_name);

    {
        DataTypesDictionary::const_iterator it = case_insensitive_data_types.find(family_name_lowercase);
        if (case_insensitive_data_types.end() != it)
        {
            if (query_context && query_context->getSettingsRef()[Setting::log_queries])
                query_context->addQueryFactoriesInfo(Context::QueryLogFactories::DataType, family_name_lowercase);
            return &it->second;
        }
    }

    if constexpr (nullptr_on_error)
        return nullptr;

    auto hints = this->getHints(family_name);
    if (!hints.empty())
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown data type family: {}. Maybe you meant: {}", family_name, toString(hints));
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown data type family: {}", family_name);
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
    registerDataTypeIPv4andIPv6(*this);
    registerDataTypeAggregateFunction(*this);
    registerDataTypeNested(*this);
    registerDataTypeInterval(*this);
    registerDataTypeLowCardinality(*this);
    registerDataTypeDomainBool(*this);
    registerDataTypeDomainSimpleAggregateFunction(*this);
    registerDataTypeDomainGeo(*this);
    registerDataTypeMap(*this);
    registerDataTypeObjectDeprecated(*this);
    registerDataTypeVariant(*this);
    registerDataTypeDynamic(*this);
    registerDataTypeJSON(*this);
}

DataTypeFactory & DataTypeFactory::instance()
{
    static DataTypeFactory ret;
    return ret;
}

}
