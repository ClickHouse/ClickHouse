#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/castColumn.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TYPE_MISMATCH;
extern const int CANNOT_CONVERT_TYPE;
extern const int NO_COMMON_TYPE;
}

/// Whether building or executing a nested function, or casting its result, failed because of the types
/// involved. Anything else (e.g. MEMORY_LIMIT_EXCEEDED) can surface from the same call and is unrelated to
/// the Variant/Dynamic type reconciliation the adaptors do.
inline bool isTypeMismatchError(int code)
{
    return code == ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT || code == ErrorCodes::TYPE_MISMATCH
        || code == ErrorCodes::CANNOT_CONVERT_TYPE || code == ErrorCodes::NO_COMMON_TYPE;
}

/// Cast the result of a nested function to the type the Variant/Dynamic adaptor declared for it. Both types
/// are expected to be convertible (like FixedString and String), so a type error from the cast is a logical
/// error. Other failures keep their own error code.
inline ColumnPtr castNestedResult(const ColumnWithTypeAndName & nested_result, const DataTypePtr & result_type, const String & function_name)
{
    try
    {
        return castColumn(nested_result, result_type);
    }
    catch (const Exception & e)
    {
        if (!isTypeMismatchError(e.code()))
            throw;

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot convert nested result of function {} with type {} to the expected result type {}: {}",
            function_name,
            nested_result.type->getName(),
            result_type->getName(),
            e.message());
    }
}

}
