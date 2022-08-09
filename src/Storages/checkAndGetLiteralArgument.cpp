#include <Storages/checkAndGetLiteralArgument.h>
#include <Core/Field.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <typename T>
T checkAndGetLiteralArgument(const ASTPtr & arg, const String & arg_name)
{
    return checkAndGetLiteralArgument<T>(*arg->as<ASTLiteral>(), arg_name);
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

    return arg.value.safeGet<T>();
}

template String checkAndGetLiteralArgument(const ASTPtr &, const String &);
template UInt64 checkAndGetLiteralArgument(const ASTPtr &, const String &);
template Int32 checkAndGetLiteralArgument(const ASTPtr &, const String &);
template UInt8 checkAndGetLiteralArgument(const ASTPtr &, const String &);
template bool checkAndGetLiteralArgument(const ASTPtr &, const String &);
template String checkAndGetLiteralArgument(const ASTLiteral &, const String &);

}
