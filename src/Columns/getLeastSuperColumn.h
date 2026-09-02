#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/ColumnWithTypeAndName.h>


namespace DB
{

/// getLeastSupertype + related column changes with an option to use variant as common type
ColumnWithTypeAndName getLeastSuperColumn(const VectorWithMemoryTracking<const ColumnWithTypeAndName *> & columns, bool use_variant_as_common_type = false);

}
