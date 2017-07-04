#pragma once

#include <Core/Types.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>


namespace DB
{

/** Columns with numbers. */

using ColumnUInt8 = ColumnVector<UInt8>;
using ColumnUInt16 = ColumnVector<UInt16>;
using ColumnUInt32 = ColumnVector<UInt32>;
using ColumnUInt64 = ColumnVector<UInt64>;
using ColumnUInt128 = ColumnVector<UInt128>;

using ColumnInt8 = ColumnVector<Int8>;
using ColumnInt16 = ColumnVector<Int16>;
using ColumnInt32 = ColumnVector<Int32>;
using ColumnInt64 = ColumnVector<Int64>;

using ColumnFloat32 = ColumnVector<Float32>;
using ColumnFloat64 = ColumnVector<Float64>;


using ColumnConstUInt8 = ColumnConst<UInt8>;
using ColumnConstUInt16 = ColumnConst<UInt16>;
using ColumnConstUInt32 = ColumnConst<UInt32>;
using ColumnConstUInt64 = ColumnConst<UInt64>;
using ColumnConstUInt128 = ColumnConst<UInt128>;

using ColumnConstInt8 = ColumnConst<Int8>;
using ColumnConstInt16 = ColumnConst<Int16>;
using ColumnConstInt32 = ColumnConst<Int32>;
using ColumnConstInt64 = ColumnConst<Int64>;

using ColumnConstFloat32 = ColumnConst<Float32>;
using ColumnConstFloat64 = ColumnConst<Float64>;

}
