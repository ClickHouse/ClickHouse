#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/// Returns true only for the native signed or unsigned integer widenings whose
/// ordering is unchanged and whose primary-index values can be promoted without
/// losing information.
inline bool isOrderPreservingIntegerWidening(const IDataType * from, const IDataType * to)
{
    if (!from || !to)
        return false;

    WhichDataType from_type(from);
    WhichDataType to_type(to);

    if (from_type.isNativeUInt() != to_type.isNativeUInt()
        || from_type.isNativeInt() != to_type.isNativeInt()
        || (!from_type.isNativeUInt() && !from_type.isNativeInt()))
        return false;

    return from->getSizeOfValueInMemory() < to->getSizeOfValueInMemory();
}

}
