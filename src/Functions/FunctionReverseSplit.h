#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Core/Types.h>
#include <Core/ValuesWithType.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

/** reverseSplit(string[, separator]) - splits a string into substrings separated by a string separator, starting from the end.
  * This function processes the string from right to left, which is useful for parsing domain names,
  * file paths, or other hierarchical data where the rightmost parts are more significant.
  *
  * Examples:
  * - reverseSplit('www.google.com') returns 'com.google.www'
  * - reverseSplit('a/b/c', '/') returns 'c/b/a'
  * - reverseSplit('x::y::z', '::') returns 'z::y::x'
  * - reverseSplit('single') returns 'single'
  * - reverseSplit('abcde', '') returns 'edcba' (reverses character order)
  *
  * Arguments:
  * - string: The input string to split and reverse (String)
  * - separator: The separator string. If not provided, splits by '.' (dot). Default: '.' (String, optional)
  *
  * Returns: A string with substrings ordered from right to left of the original string, joined by the same separator (String)
  *
  * The function is optimized for performance with:
  * - Special handling for single-character separators
  * - Efficient memory allocation with size estimation
  * - Support for both constant and variable separators
  * - Vectorized execution for processing multiple rows
  */
class FunctionReverseSplit : public IFunction
{
public:
    static constexpr auto name = "reverseSplit";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReverseSplit>(); }

    String getName() const override { return name; }

    /// Function accepts 1 or 2 arguments
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    
    /// Use default implementation for constant arguments to optimize performance
    bool useDefaultImplementationForConstants() const override { return true; }
    
    /// Use default implementation for NULL arguments
    bool useDefaultImplementationForNulls() const override { return true; }
    
    /// Second argument (separator) should be constant for better performance
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Validates argument types and returns String as result type
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    /// Main execution function that performs the reverse split operation
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    /// This function is not suitable for short-circuit evaluation due to its computational nature
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
};

}
