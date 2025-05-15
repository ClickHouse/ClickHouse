#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/typeid_cast.h>
#include <Poco/String.h>
#include <Common/StringUtils.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <unordered_map>
#include <Common/logger_useful.h>


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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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

class ASTIdentifierSubstituter
{
public:
    static ASTPtr substitute(const ASTPtr & ast_node, const std::unordered_map<String, ASTPtr> & substitutions)
    {
        if (!ast_node)
            return nullptr;

        auto * logger = &Poco::Logger::get("ASTIdentifierSubstituter");
        LOG_TRACE(logger, "Processing AST node ID: {}, Children: {}", ast_node->getID(), ast_node->children.size());

        if (const auto * identifier_node = ast_node->as<ASTIdentifier>())
        {
            LOG_TRACE(logger, "Node is ASTIdentifier: {}", identifier_node->name());
            auto it = substitutions.find(identifier_node->name());
            if (it != substitutions.end())
            {
                LOG_DEBUG(logger, "Substituted ASTIdentifier '{}' with AST {}", identifier_node->name(), it->second->getID());
                return it->second->clone(); 
            }
        }
        else if (const auto * data_type_node = ast_node->as<ASTDataType>())
        {
            LOG_TRACE(logger, "Node is ASTDataType: {}", data_type_node->name);
            // Only substitute if it's a simple type name without its own arguments, acting as a placeholder
            if (!data_type_node->name.empty() && (!data_type_node->arguments || data_type_node->arguments->children.empty()))
            {
                auto it = substitutions.find(data_type_node->name);
                if (it != substitutions.end())
                {
                    LOG_DEBUG(logger, "Substituted ASTDataType '{}' with AST {}", data_type_node->name, it->second->getID());
                    return it->second->clone(); 
                }
            }
        }
        
        ASTPtr new_node = ast_node->clone();
        for (auto & child : new_node->children)
        {
            child = substitute(child, substitutions); 
        }

        // Дополнительный шаг для синхронизации arguments в ASTDataType
        if (auto * new_data_type_node = new_node->as<ASTDataType>())
        {
            if (!new_data_type_node->children.empty()) // Если есть обработанные аргументы
            {
                // Предполагая, что аргументы всегда являются первым (и единственным) ребенком для ASTDataType
                new_data_type_node->arguments = new_data_type_node->children.at(0);
            }
            else if (new_data_type_node->arguments && new_data_type_node->arguments->children.empty())
            {
                if (new_data_type_node->children.empty())
                     new_data_type_node->arguments = nullptr;
            }
        }

        return new_node;
    }
};

template <bool nullptr_on_error>
DataTypePtr DataTypeFactory::getImpl(const String & family_name_param, const ASTPtr & parameters) const
{
    String family_name = getAliasToOrName(family_name_param);
    auto * log = &Poco::Logger::get("DataTypeFactory");

    if (UserDefinedTypeFactory::instance().isTypeRegistered(family_name))
    {
        LOG_DEBUG(log, "Processing user-defined type: {}", family_name);
        auto udt_type_info = UserDefinedTypeFactory::instance().getTypeInfo(family_name);
        ASTPtr udt_formal_params_ast = udt_type_info.type_parameters;
        ASTPtr udt_base_type_definition_ast = udt_type_info.base_type_ast;

        if (!udt_base_type_definition_ast)
        {
            LOG_ERROR(log, "User-defined type '{}' has no base type definition AST.", family_name);
            if constexpr (nullptr_on_error) return nullptr;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "User-defined type '{}' has no base type definition AST.", family_name);
        }

        const auto * actual_args_list_node = parameters ? parameters->as<ASTExpressionList>() : nullptr;
        size_t num_actual_args = actual_args_list_node ? actual_args_list_node->children.size() : 0;

        const auto * formal_params_list_node = udt_formal_params_ast ? udt_formal_params_ast->as<ASTExpressionList>() : nullptr;
        size_t num_formal_params = formal_params_list_node ? formal_params_list_node->children.size() : 0;

        if (num_formal_params != num_actual_args)
        {
            LOG_WARNING(log, "Argument number mismatch for UDT '{}': expects {}, got {}", family_name, num_formal_params, num_actual_args);
            if constexpr (nullptr_on_error) return nullptr;
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "User-defined type '{}' expects {} argument(s), but {} provided",
                            family_name, num_formal_params, num_actual_args);
        }

        if (num_formal_params == 0) // No parameters defined or provided
        {
            LOG_DEBUG(log, "Resolving non-parameterized UDT '{}' using its base AST.", family_name);
            return getImpl<nullptr_on_error>(udt_base_type_definition_ast);
        }
        else // Parameters are present and counts match
        {
            LOG_DEBUG(log, "Resolving parameterized UDT '{}' with {} arguments.", family_name, num_actual_args);
            std::unordered_map<String, ASTPtr> substitutions;
            for (size_t i = 0; i < num_formal_params; ++i)
            {
                const auto * formal_param_ident_node = formal_params_list_node->children[i]->as<ASTIdentifier>();
                if (!formal_param_ident_node)
                {
                    LOG_ERROR(log, "Formal parameter #{} for UDT '{}' is not an identifier.", i + 1, family_name);
                    if constexpr (nullptr_on_error) return nullptr;
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Formal parameter for user-defined type '{}' at position {} is not an identifier.", family_name, i + 1);
                }
                substitutions[formal_param_ident_node->name()] = actual_args_list_node->children[i];
                WriteBufferFromOwnString actual_arg_ast_str_buf;
                IAST::FormatSettings log_fmt_settings(true);
                log_fmt_settings.show_secrets = false;
                actual_args_list_node->children[i]->format(actual_arg_ast_str_buf, log_fmt_settings);
                LOG_DEBUG(log, "Substitution map for UDT '{}': formal param '{}' -> actual arg AST: {}", 
                          family_name, formal_param_ident_node->name(), actual_arg_ast_str_buf.str());
            }

            WriteBufferFromOwnString base_ast_before_str_buf;
            IAST::FormatSettings format_settings_before(true);
            format_settings_before.show_secrets = false;
            udt_base_type_definition_ast->format(base_ast_before_str_buf, format_settings_before);
            LOG_DEBUG(log, "UDT '{}': Base AST before substitution: {}", family_name, base_ast_before_str_buf.str());

            ASTPtr substituted_ast = ASTIdentifierSubstituter::substitute(udt_base_type_definition_ast, substitutions);
            
            WriteBufferFromOwnString substituted_ast_str_buf;
            IAST::FormatSettings log_substituted_fmt_settings(true);
            log_substituted_fmt_settings.show_secrets = false;
            
            if (substituted_ast)
            {
                // Лог для отладки структуры substituted_ast
                LOG_DEBUG(log, "UDT '{}': substituted_ast ID: {}, Children count: {}", family_name, substituted_ast->getID(), substituted_ast->children.size());
                if (const auto * s_ast_data_type = substituted_ast->as<ASTDataType>())
                {
                    if (s_ast_data_type->arguments)
                    {
                        WriteBufferFromOwnString args_buf;
                        s_ast_data_type->arguments->format(args_buf, log_substituted_fmt_settings);
                        LOG_DEBUG(log, "UDT '{}': substituted_ast arguments formatted: {}", family_name, args_buf.str());
                    }
                    else
                    {
                        LOG_DEBUG(log, "UDT '{}': substituted_ast has no arguments field.", family_name);
                    }
                }

                substituted_ast->format(substituted_ast_str_buf, log_substituted_fmt_settings);
            }
            else
            {
                substituted_ast_str_buf.write("nullptr", 7);
            }
            LOG_DEBUG(log, "UDT '{}': Substituted AST (overall format): {}", family_name, substituted_ast_str_buf.str());

            LOG_DEBUG(log, "Substituted AST for UDT '{}' created, resolving it.", family_name);
            return getImpl<nullptr_on_error>(substituted_ast);
        }
    }

    const auto * creator = findCreatorByName<nullptr_on_error>(family_name);
    DataTypePtr data_type;
    if constexpr (nullptr_on_error)
    {
        if (!creator)
            return nullptr;

        try
        {
            data_type = (*creator)(parameters);
        }
        catch (...)
        {
            return nullptr;
        }
    }
    else
    {
        assert(creator);
        data_type = (*creator)(parameters);
    }

    auto query_context = CurrentThread::getQueryContext();
    if (query_context && query_context->getSettingsRef()[Setting::log_queries])
    {
        query_context->addQueryFactoriesInfo(Context::QueryLogFactories::DataType, data_type->getName());
    }

    return data_type;
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
    auto * const log = &Poco::Logger::get("DataTypeFactory");
    LOG_DEBUG(log, "Finding creator for type: {}", family_name);
    
    {
        DataTypesDictionary::const_iterator it = data_types.find(family_name);
        if (data_types.end() != it)
        {
            LOG_DEBUG(log, "Found in data_types dictionary");
            return &it->second;
        }
    }

    String family_name_lowercase = Poco::toLower(family_name);

    {
        DataTypesDictionary::const_iterator it = case_insensitive_data_types.find(family_name_lowercase);
        if (case_insensitive_data_types.end() != it)
        {
            LOG_DEBUG(log, "Found in case_insensitive_data_types dictionary");
            return &it->second;
        }
    }

    if constexpr (nullptr_on_error)
    {
        LOG_DEBUG(log, "Type {} not found, returning nullptr", family_name);
        return nullptr;
    }

    LOG_DEBUG(log, "Type {} not found, looking for hints", family_name);
    auto hints = this->getHints(family_name);
    if (!hints.empty())
    {
        LOG_DEBUG(log, "Found hints for {}: {}", family_name, toString(hints));
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown data type family: {}. Maybe you meant: {}", family_name, toString(hints));
    }
    LOG_DEBUG(log, "No hints found for {}", family_name);
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
