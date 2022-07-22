#pragma once

#include <Core/Settings.h>

#include <DataTypes/IDataType.h>

#include <QueryPipeline/SizeLimits.h>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;

/** Make set for constant part of IN subquery.
  * Throws exception if parameters are not valid for IN function.
  *
  * Example: SELECT id FROM test_table WHERE id IN (1, 2, 3, 4);
  * Example: SELECT id FROM test_table WHERE id IN ((1, 2), (3, 4));
  *
  * @param expression_type - type of first argument of function IN.
  * @param value_type - type of second argument of function IN.
  * @param value - constant value of second argument of function IN.
  * @param settings - query settings.
  *
  * @return SetPtr for constant value.
  */
SetPtr makeSetForConstantValue(const DataTypePtr & expression_type, const DataTypePtr & value_type, const Field & value, const Settings & settings);

}
