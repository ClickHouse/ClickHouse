#pragma once

#include <DataTypes/IDataType.h>

#include <optional>

namespace DB
{

struct IntegerKeyTypeRepresentation
{
    bool is_unsigned;
    size_t size;
};

/// Enum values are serialized using the same fixed-width representation as their underlying signed integer.
inline std::optional<IntegerKeyTypeRepresentation> getIntegerKeyTypeRepresentation(const IDataType * type)
{
    if (!type)
        return std::nullopt;

    WhichDataType which_type(type);
    if (which_type.isNativeUInt())
        return IntegerKeyTypeRepresentation{true, type->getSizeOfValueInMemory()};

    if (which_type.isNativeInt() || which_type.isEnum8() || which_type.isEnum16())
        return IntegerKeyTypeRepresentation{false, type->getSizeOfValueInMemory()};

    return std::nullopt;
}

/// Returns true for signed or unsigned integer widenings whose ordering is unchanged and whose
/// primary-index values can be promoted without losing information.
inline bool isOrderPreservingIntegerWidening(const IDataType * from, const IDataType * to)
{
    auto from_representation = getIntegerKeyTypeRepresentation(from);
    auto to_representation = getIntegerKeyTypeRepresentation(to);

    if (!from_representation || !to_representation)
        return false;

    WhichDataType to_type(to);
    /// The ALTER compatibility rules only allow widening to a native integer.
    /// Do not treat an integer-to-Enum conversion as widening: number-to-Enum
    /// is not a safe key conversion because the Enum may not represent all values.
    if (!to_type.isNativeUInt() && !to_type.isNativeInt())
        return false;

    return from_representation->is_unsigned == to_representation->is_unsigned
        && from_representation->size < to_representation->size;
}

}
