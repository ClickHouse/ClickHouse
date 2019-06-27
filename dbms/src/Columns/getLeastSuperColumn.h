#pragma once

#include <Core/ColumnWithTypeAndName.h>


namespace DB
{

/// getLeastSupertype + related column changes
ColumnWithTypeAndName getLeastSuperColumn(std::vector<const ColumnWithTypeAndName *> columns);

}
