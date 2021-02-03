#pragma once

#include <vector>

#include <Common/AutoArray.h>
#include <Core/Field.h>


namespace DB
{

/** The data type for representing one row of the table in the RAM.
  * Warning! It is preferable to store column blocks instead of single rows. See Block.h
  */

using Row = AutoArray<Field>;

}
