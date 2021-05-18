#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Common/PODArray.h>

namespace DB
{

/// Expand data by mask. After expanding data will satisfy the following: if we filter data
/// by given mask, we get initial data. In places where mask[i] = 0 we insert given default_value.
/// If inverse is true, we will work with inverted mask. This function is used in implementations of
/// expand() method in IColumn interface.
template <typename T>
void expandDataByMask(PaddedPODArray<T> & data, const PaddedPODArray<UInt8> & mask, bool inverse);

/// Expand offsets by mask. Used in expand() method in ColumnArray and ColumnString to expand their offsets.
/// In places where mask[i] = 0 we insert empty array/string.
void expandOffsetsByMask(PaddedPODArray<UInt64> & offsets, const PaddedPODArray<UInt8> & mask, bool inverse);

/// Convert given column to mask. If inverse is true, we will use inverted values.
/// Usually this function is used after expanding column where we cannot specify default value
/// for places where mask[i] = 0, but sometimes we want it (to reduce unnecessary coping).
/// If mask_used_in_expanding is passed, we will use default_value_in_expanding instead of
/// value from column when mask_used_in_expanding[i] = 0. If inverse_mask_used_in_expanding
/// is true, we will work with inverted mask_used_in_expanding.
/// If column is nullable, null_value will be used when column value is Null.
void getMaskFromColumn(
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & res,
    bool inverse = false,
    const PaddedPODArray<UInt8> * mask_used_in_expanding = nullptr,
    UInt8 default_value_in_expanding = 1,
    bool inverse_mask_used_in_expanding = false,
    UInt8 null_value = 1,
    const PaddedPODArray<UInt8> * null_bytemap = nullptr);

/// Make a disjunction of two masks and write result un the first one (mask1 = mask1 | mask2).
void disjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2);

/// If given column is lazy executed argument (ColumnFunction with isShortCircuitArgument() = true),
/// filter it by mask, reduce and then expand by mask. If inverse is true, we will work with inverted mask.
void maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, bool inverse = false);

/// If given column is lazy executed argument, just reduce it.
void executeColumnIfNeeded(ColumnWithTypeAndName & column);

/// Check if arguments contain lazy executed argument. If contain, return index of the last one,
/// otherwise return -1.
int checkShirtCircuitArguments(const ColumnsWithTypeAndName & arguments);

}
