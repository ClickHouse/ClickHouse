#pragma once

#include <Core/ColumnWithTypeAndName.h>


namespace DB
{

/// getLeastSupertype + related column changes with an option to use variant as common type
ColumnWithTypeAndName getLeastSuperColumn(const std::vector<const ColumnWithTypeAndName *> & columns, bool use_variant_as_common_type = false);

}
