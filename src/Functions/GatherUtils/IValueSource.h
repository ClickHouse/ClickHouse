#pragma once

#include "ValueSourceVisitor.h"
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace GatherUtils
{

struct IValueSource
{
    virtual ~IValueSource() = default;

    virtual void accept(ValueSourceVisitor &)
    {
        throw Exception("Accept not implemented for " + demangle(typeid(*this).name()), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual bool isConst() const { return false; }
};

#pragma GCC visibility push(hidden)

template <typename Derived>
class ValueSourceImpl : public Visitable<Derived, IValueSource, ValueSourceVisitor> {};

#pragma GCC visibility pop
}

}
