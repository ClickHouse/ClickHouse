#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Common/PODArray.h>

namespace DB
{
template <typename T>
void expandDataByMask(PaddedPODArray<T> & data, const PaddedPODArray<UInt8> & mask, bool reverse, T default_value);

void expandOffsetsByMask(PaddedPODArray<UInt64> & offsets, const PaddedPODArray<UInt8> & mask, bool reverse);

void getMaskFromColumn(
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & res,
    bool reverse = false,
    const PaddedPODArray<UInt8> * expanding_mask = nullptr,
    UInt8 default_value = 1,
    bool expanding_mask_reverse = false,
    const PaddedPODArray<UInt8> * null_bytemap = nullptr,
    UInt8 null_value = 1);

void conjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2);

void disjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2);

void maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, bool reverse = false);

void executeColumnIfNeeded(ColumnWithTypeAndName & column);

int checkShirtCircuitArguments(const ColumnsWithTypeAndName & arguments);

}
