#include <DataTypes/UserDefinedTypeFactory.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Parsers/IAST.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int LOGICAL_ERROR;
}


UserDefinedTypeFactory & UserDefinedTypeFactory::instance()
{
    static UserDefinedTypeFactory udt_factory;
    return udt_factory;
}

void UserDefinedTypeFactory::registerType(
    const String & name,
    const ASTPtr & base_type_ast,
    ASTPtr type_parameters,
    const String & input_expression,
    const String & output_expression,
    const String & default_expression)
{
    std::lock_guard lock(mutex);
    
    if (types.contains(name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Type with name {} already registered", name);

    TypeInfo info;
    info.base_type_ast = base_type_ast;
    info.type_parameters = type_parameters;
    info.input_expression = input_expression;
    info.output_expression = output_expression;
    info.default_expression = default_expression;

    types[name] = info;
    
    WriteBufferFromOwnString ostr_buf;
    IAST::FormatSettings ast_format_settings(true);
    ast_format_settings.show_secrets = false;
    base_type_ast->format(ostr_buf, ast_format_settings);

    LOG_INFO(&Poco::Logger::get("UserDefinedTypeFactory"), "Registered user-defined type '{}' with base type AST: {}", 
        name, ostr_buf.str());
}

void UserDefinedTypeFactory::removeType(const String & name)
{
    std::lock_guard lock(mutex);
    
    if (!types.contains(name))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown type {}", name);

    types.erase(name);
    
    LOG_INFO(&Poco::Logger::get("UserDefinedTypeFactory"), "Removed user-defined type '{}'", name);
}

UserDefinedTypeFactory::TypeInfo UserDefinedTypeFactory::getTypeInfo(const String & name) const
{
    std::lock_guard lock(mutex);
    
    auto it = types.find(name);
    if (it == types.end())
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown type {}", name);

    return it->second;
}

bool UserDefinedTypeFactory::isTypeRegistered(const String & name) const
{
    std::lock_guard lock(mutex);
    return types.contains(name);
}

std::vector<String> UserDefinedTypeFactory::getAllTypeNames() const
{
    std::lock_guard lock(mutex);
    
    std::vector<String> result;
    result.reserve(types.size());
    
    for (const auto & [name, _] : types)
        result.push_back(name);
        
    return result;
}

}
