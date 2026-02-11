#include <Storages/checkAndGetLiteralArgument.h>
#include <Core/Field.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <typename T>
T checkAndGetLiteralArgument(const ASTPtr & arg, const String & arg_name)
{
    if (arg)
    {
        if (const auto * func = arg->as<const ASTFunction>(); func && func->name == "_CAST")
            return T(checkAndGetLiteralArgument<T>(func->arguments->children.at(0), arg_name));

        if (arg->as<ASTLiteral>())
            return T(checkAndGetLiteralArgument<T>(*arg->as<ASTLiteral>(), arg_name));
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Argument '{}' must be a literal, get {} (value: {})",
        arg_name,
        arg ? arg->getID() : "NULL",
        arg ? arg->formatForErrorMessage() : "NULL");
}

template <typename T>
T checkAndGetLiteralArgument(const ASTLiteral & arg, const String & arg_name)
{
    auto requested_type = Field::TypeToEnum<NearestFieldType<std::decay_t<T>>>::value;
    auto provided_type = arg.value.getType();
    if (requested_type != provided_type)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Argument '{}' must be a literal with type {}, got {}",
            arg_name,
            fieldTypeToString(requested_type),
            fieldTypeToString(provided_type));

    return T(arg.value.safeGet<T>());
}

template <>
bool checkAndGetLiteralArgument(const ASTLiteral & arg, const String & arg_name)
{
    /// Traditionally Bool literals were stored as UInt64 with value 0 or 1, but now we have proper Bool type.
    if (arg.value.getType() == Field::Types::Which::Bool)
        return arg.value.safeGet<bool>();

    if (arg.value.getType() == Field::Types::Which::UInt64)
    {
        auto value = arg.value.safeGet<UInt64>();
        return value != 0;
    }

    auto requested_type = Field::TypeToEnum<NearestFieldType<std::decay_t<bool>>>::value;
    auto provided_type = arg.value.getType();
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Argument '{}' must be a literal with type {}, got {}",
        arg_name,
        fieldTypeToString(requested_type),
        fieldTypeToString(provided_type));
}

template <typename T>
std::optional<T> tryGetLiteralArgument(const ASTPtr & arg, const String & arg_name)
{
    if (arg)
    {
        if (const auto * func = arg->as<const ASTFunction>(); func && func->name == "_CAST")
        {
            return tryGetLiteralArgument<T>(func->arguments->children.at(0), arg_name);
        }

        if (arg->as<ASTLiteral>())
        {
            try
            {
                return checkAndGetLiteralArgument<T>(*arg->as<ASTLiteral>(), arg_name);
            }
            catch (...)
            {
                return std::nullopt;
            }
        }
    }

    return std::nullopt;
}

template String checkAndGetLiteralArgument(const ASTPtr &, const String &);
template UInt64 checkAndGetLiteralArgument(const ASTPtr &, const String &);
template UInt8 checkAndGetLiteralArgument(const ASTPtr &, const String &);
template bool checkAndGetLiteralArgument(const ASTPtr &, const String &);
template String checkAndGetLiteralArgument(const ASTLiteral &, const String &);
template UInt64 checkAndGetLiteralArgument(const ASTLiteral &, const String &);
template std::optional<String> tryGetLiteralArgument(const ASTPtr & arg, const String & arg_name);
}
