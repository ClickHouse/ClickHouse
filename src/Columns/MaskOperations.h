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

struct MaskInfo
{
    bool has_ones;
    bool has_zeros;
};

/// The next 3 functions are used to extract UInt8 mask from a column,
/// filtered by some condition (mask). We will use value from a column
/// only when value in condition is 1. Column should satisfy the
/// condition: sum(mask) = column.size() or mask.size() = column.size().
/// In all functions you can set flag 'inverted' to use inverted values
/// from a column. You can also determine value that will be used when
/// column value is Null (argument null_value).

/// Write resulting mask in the given result mask.
template <bool inverted = false>
MaskInfo extractMask(
    const PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & result,
    UInt8 null_value = 0);

/// Write result in mask inplace.
template <bool inverted = false>
MaskInfo extractMaskInplace(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    UInt8 null_value = 0);

/// The same as extractMaskInplace, but fills
/// nulls so that nulls[i] = 1 when column[i] = Null.
template <bool inverted = false>
MaskInfo extractMaskInplaceWithNulls(
    PaddedPODArray<UInt8> & mask,
    const ColumnPtr & column,
    PaddedPODArray<UInt8> * nulls,
    UInt8 null_value = 0);

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
