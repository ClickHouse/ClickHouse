#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Common/PODArray.h>

namespace DB
{

/// Expand data by mask. After expanding data will satisfy the following: if we filter data
/// by given mask, we get initial data. In places where mask[i] = 0 we insert default value.
/// If inverted is true, we will work with inverted mask. This function is used in implementations of
/// expand() method in IColumn interface.
template <typename T>
void expandDataByMask(PaddedPODArray<T> & data, const PaddedPODArray<UInt8> & mask, bool inverted);

/// Expand offsets by mask. Used in expand() method in ColumnArray and ColumnString to expand their offsets.
/// In places where mask[i] = 0 we insert empty array/string.
void expandOffsetsByMask(PaddedPODArray<UInt64> & offsets, const PaddedPODArray<UInt8> & mask, bool inverted);

struct MaskInfo
{
    bool has_once = false;
    bool has_zeros = false;
};

/// Convert given column to mask. If inverted is true, we will use inverted values.
/// Usually this function is used after expanding column where we cannot specify default value
/// for places where mask[i] = 0, but sometimes we want it (to reduce unnecessary coping).
/// If mask_used_in_expanding is passed, we will use default_value_in_expanding instead of
/// value from column when mask_used_in_expanding[i] = 0. If inverted_mask_used_in_expanding
/// is true, we will work with inverted mask_used_in_expanding.
/// If column is nullable and i-th value is Null, null_value will be used in the result and
/// nulls[i] will be set to 1.
MaskInfo getMaskFromColumn(
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & res,
    bool inverted = false,
    const PaddedPODArray<UInt8> * mask_used_in_expanding = nullptr,
    UInt8 default_value_in_expanding = 1,
    bool inverted_mask_used_in_expanding = false,
    UInt8 null_value = 0,
    const PaddedPODArray<UInt8> * null_bytemap = nullptr,
    PaddedPODArray<UInt8> * nulls = nullptr);

/// Make a disjunction of two masks and write result un the first one (mask1 = mask1 | mask2).
MaskInfo disjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2);

/// Inplace inversion.
void inverseMask(PaddedPODArray<UInt8> & mask);

/// If given column is lazy executed argument (ColumnFunction with isShortCircuitArgument() = true),
/// filter it by mask and then reduce. If inverted is true, we will work with inverted mask.
void maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, const MaskInfo & mask_info, bool inverted = false);

/// If given column is lazy executed argument, reduce it. If empty is true,
/// create an empty column with the execution result type.
void executeColumnIfNeeded(ColumnWithTypeAndName & column, bool empty = false);

/// Check if arguments contain lazy executed argument. If contain, return index of the last one,
/// otherwise return -1.
int checkShirtCircuitArguments(const ColumnsWithTypeAndName & arguments);

}
