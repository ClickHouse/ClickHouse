#pragma once

#include <Common/COW.h>

namespace DB
{

class IColumn;
using ColumnPtr = COW<IColumn>::Ptr;

class ColumnNullable;
ColumnPtr makeNullable(const ColumnPtr & column);

}
