#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// A left-closed and right-open interval representing the preimage of a function.
struct FieldInterval
{
    Field first;
    Field second;
};

/// Comparing two different date/time types casts both sides to a common type, so the constant is no
/// longer a point of the function result domain and its field value has no meaningful preimage.
/// Wrapped types are declined rather than unwrapped, since they hide the type this has to compare.
inline bool canCalculatePreimageForConstant(const IDataType & result_type, const IDataType & constant_type)
{
    const WhichDataType result(result_type);
    const WhichDataType constant(constant_type);
    if (result.isNullable() || result.isLowCardinality() || constant.isNullable() || constant.isLowCardinality())
        return false;

    return result.idx == constant.idx
        || !result.isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64()
        || !constant.isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64();
}

}
