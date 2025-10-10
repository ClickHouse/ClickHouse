#pragma once

#include <base/types.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>


namespace DB
{

/** Convenience typedefs for columns of SQL types Date, Date32, DateTime and DateTime64. */

using ColumnDate = DataTypeDate::ColumnType;
using ColumnDate32 = DataTypeDate32::ColumnType;
using ColumnDateTime = DataTypeDateTime::ColumnType;
using ColumnDateTime64 = DataTypeDateTime64::ColumnType;

}
