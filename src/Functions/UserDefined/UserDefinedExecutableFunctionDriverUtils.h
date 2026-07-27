#pragma once

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTLiteral;

/// Helpers shared between `CREATE FUNCTION ... ENGINE = Driver(...)` and `DROP FUNCTION`
/// for functions created through executable UDF drivers.
namespace UserDefinedExecutableFunctionDriverUtils
{

/// Format the declared arguments as `name1 Type1, name2 Type2` for passing to the driver.
String formatArgsSignature(const ASTPtr & arguments_ast);

/// Format the declared return type for passing to the driver.
String formatReturnType(const ASTPtr & return_type_ast);

/// Convert an engine argument literal to the string passed to the driver.
/// String literals are passed verbatim, without quoting.
String engineArgumentToString(const ASTLiteral & literal);

/// Path of the driver-generated dynamic config for a function,
/// e.g. `<dynamic_path>/<escaped_name>.xml`. Throws if the server setting
/// `dynamic_user_defined_executable_functions_path` is not set.
String driverDynamicConfigPath(const ContextPtr & context, const String & function_name, const String & extension);

/// Path of the sidecar file that stores the name of the function's working directory.
String driverWorkingDirectoryMetadataPath(const ContextPtr & context, const String & function_name);

/// Path of a working directory with the given name inside the dynamic config directory.
String driverWorkingDirectory(const ContextPtr & context, const String & directory_name);

/// Working directory names are UUIDs; reject anything else to avoid removing arbitrary paths.
void validateDriverWorkingDirectoryName(const String & directory_name, const String & function_name);

/// Read the function's working directory path from the sidecar file.
/// Returns an empty string if the sidecar file does not exist or is empty.
String readDriverWorkingDirectory(const ContextPtr & context, const String & function_name);

}

}
