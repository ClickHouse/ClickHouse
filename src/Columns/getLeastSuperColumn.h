#pragma once

#include <Core/ColumnWithTypeAndName.h>


namespace DB
{

/// getLeastSupertype + related column changes
ColumnWithTypeAndName getLeastSuperColumn(const std::vector<const ColumnWithTypeAndName *> & columns, bool alllow_type_promotion_after_64_bits = false);

}
