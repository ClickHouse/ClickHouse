#pragma once

#include <Core/ColumnWithTypeAndName.h>


namespace DB
{
ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type);
}
