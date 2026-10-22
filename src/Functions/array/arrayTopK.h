#pragma once

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/array/FunctionArrayMapped.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}


/** Return the top K (largest) or bottom K (smallest) elements of an array, sorted.
  * Nulls are skipped and not present in the result, so the result's element type
  * is the non-nullable counterpart of the input array's element type, and the
  * result may be shorter than K.
  */
template <bool IsAscending>
struct ArrayTopKImpl
{
    /// The K argument is required.
    static constexpr auto num_fixed_params = 1;

    /// Lambda is optional.
    static bool needExpression() { return false; }

    /// Lambda can return any type.
    static bool needBoolean() { return false; }

    /// Additional arrays can be specified with arguments for lambda.
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        /// `LowCardinality` is already stripped recursively by IFunctionOverloadResolver::getReturnType()
        /// (via the default `useDefaultImplementationForLowCardinalityColumns`), so only `Nullable` is left to handle.
        chassert(!array_element->lowCardinality());
        return std::make_shared<DataTypeArray>(removeNullable(array_element));
    }

    static void checkArguments(
        const String & name,
        const ColumnWithTypeAndName * fixed_arguments)
    {
        if (!fixed_arguments)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected fixed arguments to get K for {}", name);

        if (!isNativeInteger(*fixed_arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the K argument of function {} (must be a native integer: UInt8/16/32/64 or Int8/16/32/64)",
                fixed_arguments[0].type->getName(),
                name);
    }

    static ColumnPtr execute(
        const ColumnArray & array,
        ColumnPtr mapped,
        const ColumnWithTypeAndName * fixed_arguments);
};

struct NameArrayTopK
{
    static constexpr auto name = "arrayTopK";
};
struct NameArrayBottomK
{
    static constexpr auto name = "arrayBottomK";
};

/// `arrayBottomK` returns the K smallest elements in ascending order.
/// `arrayTopK` returns the K largest elements in descending order.
/// Equal elements keep their original order, so the result is deterministic.
using FunctionArrayBottomK = FunctionArrayMapped<ArrayTopKImpl</* IsAscending = */ true>, NameArrayBottomK, /* IsDeterministic = */ true>;
using FunctionArrayTopK = FunctionArrayMapped<ArrayTopKImpl</* IsAscending = */ false>, NameArrayTopK, /* IsDeterministic = */ true>;

}
