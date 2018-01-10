#pragma once

#include <Columns/ColumnArray.h>

namespace DB::GatherUtils
{

struct IArraySource
{
    virtual ~IArraySource() = default;

    virtual size_t getSizeForReserve() const = 0;
    virtual const typename ColumnArray::Offsets & getOffsets() const = 0;
    virtual size_t getColumnSize() const = 0;
    virtual bool isConst() const { return false; }
    virtual bool isNullable() const { return false; }
};

}
