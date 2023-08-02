#pragma once

#include <Core/Settings.h>

#include <DataTypes/IDataType.h>

#include <QueryPipeline/SizeLimits.h>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;

/** Get set elements for constant part of IN subquery.
  * Throws exception if parameters are not valid for IN function.
  *
  * Example: SELECT id FROM test_table WHERE id IN (1, 2, 3, 4);
  * Example: SELECT id FROM test_table WHERE id IN ((1, 2), (3, 4));
  */
Block getSetElementsForConstantValue(const DataTypePtr & expression_type, const Field & value, const DataTypePtr & value_type, bool transform_null_in);

}
