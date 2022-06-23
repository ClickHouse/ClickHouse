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
    T res;
    if (arg.value.tryGet(res))
        return res;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Argument '{}' must be a literal with type {}, got {}",
        arg_name,
        fieldTypeToString(Field::TypeToEnum<T>::value),
        fieldTypeToString(arg.value.getType()));
}

template String checkAndGetLiteralArgument(const ASTPtr &, const String &);
template UInt64 checkAndGetLiteralArgument(const ASTPtr &, const String &);
template String checkAndGetLiteralArgument(const ASTLiteral &, const String &);

}
