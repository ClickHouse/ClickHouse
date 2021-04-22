#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Common/PODArray.h>

namespace DB
{

PaddedPODArray<UInt8> getMaskFromColumn(const ColumnPtr & column, bool reverse = false, const PaddedPODArray<UInt8> * null_bytemap = nullptr, UInt8 null_value = 1);

PaddedPODArray<UInt8> reverseMask(const PaddedPODArray<UInt8> & mask);

void conjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2);

void disjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2);

void maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, Field * default_value = nullptr);

void executeColumnIfNeeded(ColumnWithTypeAndName & column);

}
