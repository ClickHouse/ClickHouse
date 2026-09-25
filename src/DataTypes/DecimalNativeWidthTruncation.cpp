#include <DataTypes/DecimalNativeWidthTruncation.h>

#include <Common/FieldAccurateComparison.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace
{

/// Whether `constant` does not fit the signed native width of `decimal_type`.
bool constantExceedsDecimalWidth(const DataTypePtr & decimal_type, const Field & constant)
{
    auto exceeds = [&constant]<typename T>(std::type_identity<T>)
    {
        return accurateLess(constant, Field(std::numeric_limits<T>::min()))
            || accurateLess(Field(std::numeric_limits<T>::max()), constant);
    };

    switch (decimal_type->getSizeOfValueInMemory())
    {
        case sizeof(Int32): return exceeds(std::type_identity<Int32>{});
        case sizeof(Int64): return exceeds(std::type_identity<Int64>{});
        case sizeof(Int128): return exceeds(std::type_identity<Int128>{});
        case sizeof(Int256): return exceeds(std::type_identity<Int256>{});
        default: return true; /// unreachable for the four `Decimal` widths above; fails close if a new one appears
    }
}

/// Whether some value of `integer_type` does not fit the signed native width of `decimal_type`.
bool integerTypeExceedsDecimalWidth(const DataTypePtr & decimal_type, const DataTypePtr & integer_type)
{
    const size_t decimal_width = decimal_type->getSizeOfValueInMemory();
    const size_t integer_width = integer_type->getSizeOfValueInMemory();

    /// The native width is signed, so an unsigned argument of the same width already overflows it.
    if (isUInt(integer_type))
        return integer_width >= decimal_width;
    return integer_width > decimal_width;
}

}

bool operandTruncatesIntoDecimalWidth(const DataTypePtr & argument_type, const DataTypePtr & constant_type, const Field & constant)
{
    const auto argument = removeNullable(removeLowCardinality(argument_type));
    const auto constant_without_wrappers = removeNullable(removeLowCardinality(constant_type));

    /// A `Decimal` argument with an integer constant that its native width cannot represent.
    if (isDecimal(argument) && isInteger(constant_without_wrappers))
        return constantExceedsDecimalWidth(argument, constant);

    /// The mirrored shape: an integer argument wider than the native width of a `Decimal` constant.
    if (isDecimal(constant_without_wrappers) && isInteger(argument))
        return integerTypeExceedsDecimalWidth(constant_without_wrappers, argument);

    return false;
}

}
