#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

namespace DataTypeTraits
{

/// If the input type is nullable, return its nested type.
/// Otherwise it is an identity mapping.
const DataTypePtr & removeNullable(const DataTypePtr & type);

}

}
