#pragma once

#include <DB/DataTypes/IDataType.h>

namespace DB
{

/** Проверить, что типы совместимые.
 */
bool typesAreCompatible(const IDataType& lhs, const IDataType& rhs);

}
