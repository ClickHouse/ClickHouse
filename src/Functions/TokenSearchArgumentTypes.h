#pragma once

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

/// Argument types shared by the token-search functions `hasAnyTokens`, `hasAllTokens` and `hasPhrase`.

/// Function input accepts string, fixed string, array of string or array of fixed strings.
inline bool isStringOrFixedStringOrArrayOfStringOrFixedString(const IDataType & type)
{
    const IDataType * nested_type = &type;

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(nested_type))
        nested_type = nullable->getNestedType().get();

    if (isStringOrFixedString(*nested_type))
        return true;

    if (const auto * array_type = checkAndGetDataType<DataTypeArray>(nested_type))
    {
        const IDataType * element_type = array_type->getNestedType().get();

        if (const auto * nullable_elem = typeid_cast<const DataTypeNullable *>(element_type))
            element_type = nullable_elem->getNestedType().get();

        return isStringOrFixedString(*element_type);
    }

    return false;
}

/// Needles are a string to tokenize or an array of tokens used as-is; `Array(Nothing)` is the type of `[]`.
inline bool isStringOrArrayOfStringType(const IDataType & type)
{
    if (isString(type))
        return true;

    if (const auto * array_type = checkAndGetDataType<DataTypeArray>(&type); array_type)
    {
        const DataTypePtr & nested_type = array_type->getNestedType();
        return isString(nested_type) || isNothing(nested_type);
    }

    return false;
}

}
