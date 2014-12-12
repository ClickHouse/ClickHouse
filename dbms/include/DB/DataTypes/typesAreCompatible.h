#pragma once

#include <DB/DataTypes/IDataType.h>

namespace DB
{

bool typesAreCompatible(const IDataType& lhs, const IDataType& rhs);

}
