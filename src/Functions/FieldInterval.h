#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType_fwd.h>

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
bool canCalculatePreimageForConstant(const DataTypePtr & result_type, const DataTypePtr & constant_type);

}
