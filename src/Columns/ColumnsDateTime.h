#pragma once

#include <base/types.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>


namespace DB
{

/** Convenience typedefs for columns of SQL types Date, Date32, DateTime, DateTime64, Time and Time64. */

using ColumnDate = DataTypeDate::ColumnType;
using ColumnDate32 = DataTypeDate32::ColumnType;
using ColumnDateTime = DataTypeDateTime::ColumnType;
using ColumnDateTime64 = DataTypeDateTime64::ColumnType;
using ColumnTime = DataTypeTime::ColumnType;
using ColumnTime64 = DataTypeTime64::ColumnType;

}
