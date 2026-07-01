#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverUtils.h>

#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/IAST.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
#include <Common/escapeForFileName.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DIRECTORY_DOESNT_EXIST;
}

namespace UserDefinedExecutableFunctionDriverUtils
{

String formatArgsSignature(const ASTPtr & arguments_ast)
{
    if (!arguments_ast)
        return {};

    WriteBufferFromOwnString out;
    bool first = true;
    for (const auto & child : arguments_ast->children)
    {
        if (!first)
            out << ", ";
        first = false;

        if (const auto * name_type_pair = child->as<ASTNameTypePair>())
        {
            out << name_type_pair->name << ' ';
            IAST::FormatSettings settings(/*one_line=*/true);
            IAST::FormatState state;
            IAST::FormatStateStacked frame;
            name_type_pair->type->format(out, settings, state, frame);
        }
        else
        {
            IAST::FormatSettings settings(/*one_line=*/true);
            IAST::FormatState state;
            IAST::FormatStateStacked frame;
            child->format(out, settings, state, frame);
        }
    }
    return out.str();
}

String formatReturnType(const ASTPtr & return_type_ast)
{
    if (!return_type_ast)
        return {};
    WriteBufferFromOwnString out;
    IAST::FormatSettings settings(/*one_line=*/true);
    IAST::FormatState state;
    IAST::FormatStateStacked frame;
    return_type_ast->format(out, settings, state, frame);
    return out.str();
}

String engineArgumentToString(const ASTLiteral & literal)
{
    /// String literals are passed to the driver verbatim, without quoting
    /// (`FieldVisitorToString` would wrap them in single quotes).
    if (literal.value.getType() == Field::Types::String)
        return literal.value.safeGet<String>();
    return applyVisitor(FieldVisitorToString(), literal.value);
}

String driverDynamicConfigPath(const ContextPtr & context, const String & function_name, const String & extension)
{
    String path = context->getDynamicUserDefinedExecutableFunctionsPath();
    if (path.empty())
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
            "Server setting `dynamic_user_defined_executable_functions_path` is not set");
    if (!path.ends_with('/'))
        path.push_back('/');
    return path + escapeForFileName(function_name) + extension;
}

String driverWorkingDirectoryMetadataPath(const ContextPtr & context, const String & function_name)
{
    return driverDynamicConfigPath(context, function_name, ".workdir");
}

String driverWorkingDirectory(const ContextPtr & context, const String & directory_name)
{
    String path = context->getDynamicUserDefinedExecutableFunctionsPath();
    if (!path.ends_with('/'))
        path.push_back('/');
    return path + directory_name;
}

void validateDriverWorkingDirectoryName(const String & directory_name, const String & function_name)
{
    UUID uuid;
    if (!tryParseUUID({reinterpret_cast<const UInt8 *>(directory_name.data()), directory_name.size()}, uuid))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Invalid executable UDF driver working directory name '{}' for function '{}'",
            directory_name, function_name);
}

String readDriverWorkingDirectory(const ContextPtr & context, const String & function_name)
{
    const String metadata_path = driverWorkingDirectoryMetadataPath(context, function_name);
    if (!std::filesystem::exists(metadata_path))
        return {};

    String directory_name;
    ReadBufferFromFile in(metadata_path);
    readStringUntilEOF(directory_name, in);
    if (directory_name.empty())
        return {};

    validateDriverWorkingDirectoryName(directory_name, function_name);
    return driverWorkingDirectory(context, directory_name);
}

}

}
