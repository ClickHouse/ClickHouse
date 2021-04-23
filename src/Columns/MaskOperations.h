#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Common/PODArray.h>

namespace DB
{

void getMaskFromColumn(const ColumnPtr & column, PaddedPODArray<UInt8> & mask, bool reverse = false, const PaddedPODArray<UInt8> * null_bytemap = nullptr, UInt8 null_value = 1);

void conjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2);

void disjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2);

void maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, Field * default_value = nullptr, bool reverse = false);

void executeColumnIfNeeded(ColumnWithTypeAndName & column);

}
