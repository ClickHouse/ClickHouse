#pragma once

#include <DB/Core/Types.h>
#include <DB/Columns/ColumnVector.h>


namespace DB
{

/** Столбцы чисел. */

typedef ColumnVector<UInt8> ColumnUInt8;
typedef ColumnVector<UInt16> ColumnUInt16;
typedef ColumnVector<UInt32> ColumnUInt32;
typedef ColumnVector<UInt64> ColumnUInt64;

typedef ColumnVector<Int8> ColumnInt8;
typedef ColumnVector<Int16> ColumnInt16;
typedef ColumnVector<Int32> ColumnInt32;
typedef ColumnVector<Int64> ColumnInt64;

typedef ColumnVector<Float32> ColumnFloat32;
typedef ColumnVector<Float64> ColumnFloat64;

}
