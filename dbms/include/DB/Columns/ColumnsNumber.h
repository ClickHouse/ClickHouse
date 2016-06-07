#pragma once

#include <DB/Core/Types.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnConst.h>


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


typedef ColumnConst<UInt8> ColumnConstUInt8;
typedef ColumnConst<UInt16> ColumnConstUInt16;
typedef ColumnConst<UInt32> ColumnConstUInt32;
typedef ColumnConst<UInt64> ColumnConstUInt64;

typedef ColumnConst<Int8> ColumnConstInt8;
typedef ColumnConst<Int16> ColumnConstInt16;
typedef ColumnConst<Int32> ColumnConstInt32;
typedef ColumnConst<Int64> ColumnConstInt64;

typedef ColumnConst<Float32> ColumnConstFloat32;
typedef ColumnConst<Float64> ColumnConstFloat64;

}
