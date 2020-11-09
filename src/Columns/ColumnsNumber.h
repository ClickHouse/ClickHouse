#pragma once

#include <Core/Types.h>
#include <Columns/ColumnVector.h>


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

}
