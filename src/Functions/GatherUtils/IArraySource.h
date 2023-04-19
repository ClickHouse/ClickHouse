#pragma once

#include <Columns/ColumnArray.h>
#include "ArraySourceVisitor.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace GatherUtils
{

struct IArraySource
{
    virtual ~IArraySource() = default;

    virtual size_t getSizeForReserve() const = 0;
    virtual const typename ColumnArray::Offsets & getOffsets() const = 0;
    virtual size_t getColumnSize() const = 0;
    virtual bool isConst() const { return false; }
    virtual bool isNullable() const { return false; }

    virtual void accept(ArraySourceVisitor &)
    {
        throw Exception("Accept not implemented for " + demangle(typeid(*this).name()), ErrorCodes::NOT_IMPLEMENTED);
    }
};

#pragma GCC visibility push(hidden)

template <typename Derived>
class ArraySourceImpl : public Visitable<Derived, IArraySource, ArraySourceVisitor> {};

#pragma GCC visibility pop
}

}
